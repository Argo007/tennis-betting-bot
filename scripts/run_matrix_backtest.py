#!/usr/bin/env python3
"""
Run matrix backtest — with robust input auto-detection.

Priority:
1) --dataset (if provided)
2) outputs/edge_enriched.csv
3) outputs/prob_enriched.csv
4) data/raw/vigfree_matches.csv
5) data/raw/odds/sample_odds.csv  (very last resort; converts to probs = 1/odds)

Writes a tiny diagnostics json so it's easy to see what got picked.
"""

from __future__ import annotations
import argparse, csv, json, math, sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# ---------- configuration defaults ----------
DEFAULT_MIN_EDGE   = 0.005      # 0.5% minimum true edge to place a bet
DEFAULT_STAKING    = "kelly"    # "kelly" or "flat"
DEFAULT_KELLY_SCALE= 0.5        # half-kelly by default
DEFAULT_BANKROLL   = 1000.0
DEFAULT_BANDS      = "2.0,2.6|2.6,3.2|3.2,4.0"  # just used to echo/compatibility

# ---------- fs helpers ----------
REPO_ROOT = Path(__file__).resolve().parents[1]  # repo/
CANDIDATES = [
    "outputs/edge_enriched.csv",
    "outputs/prob_enriched.csv",
    "data/raw/vigfree_matches.csv",
    "data/raw/odds/sample_odds.csv",
]

DIAG_DIR   = REPO_ROOT / "results" / "backtests"
DOCS_DIR   = REPO_ROOT / "docs"    / "backtests"
OUT_DIR    = REPO_ROOT / "results" / "backtests" / "logs"
for p in [DIAG_DIR, DOCS_DIR, OUT_DIR]:
    p.mkdir(parents=True, exist_ok=True)

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Matrix backtest runner (auto-detect input)")
    ap.add_argument("--dataset", help="Optional explicit CSV to use", default=None)
    ap.add_argument("--min-edge", type=float, default=DEFAULT_MIN_EDGE)
    ap.add_argument("--staking", choices=["kelly","flat"], default=DEFAULT_STAKING)
    ap.add_argument("--kelly-scale", type=float, default=DEFAULT_KELLY_SCALE)
    ap.add_argument("--bankroll", type=float, default=DEFAULT_BANKROLL)
    ap.add_argument("--bands", default=DEFAULT_BANDS, help="Only echoed in summary")
    return ap.parse_args()

# ---------- input resolution ----------
def resolve_dataset(explicit: Optional[str]) -> Tuple[Optional[Path], Dict]:
    tried: List[str] = []
    diag: Dict = {"source": None, "total_rows": 0, "usable_rows": 0,
                  "skipped_missing": 0, "notes": [], "tried": tried}

    # helper
    def _exists(rel: str) -> Optional[Path]:
        p = (REPO_ROOT / rel).resolve()
        tried.append(str(p))
        return p if p.exists() else None

    if explicit:
        p = Path(explicit).expanduser()
        if not p.is_absolute():
            p = (REPO_ROOT / explicit).resolve()
        tried.append(str(p))
        if p.exists():
            diag["source"] = str(p)
            return p, diag
        diag["notes"].append(f"--dataset not found: {p}")

    # priority list
    for rel in CANDIDATES:
        p = _exists(rel)
        if p:
            diag["source"] = str(p)
            return p, diag

    return None, diag

# ---------- csv normalization ----------
# We normalize to: oa, ob, pa, pb (odds/probs for A/B)
ODDS_A_CANDS = ["oa","odds_a","odds1","odds_a_","odds_a.","oddsA","odds_a_vig","odds_a_close","odds_a_close_"]
ODDS_B_CANDS = ["ob","odds_b","odds2","odds_b_","odds_b.","oddsB","odds_b_vig","odds_b_close","odds_b_close_"]
PROB_A_CANDS = ["pa","prob_a","probA","p_a","implied_prob_a","prob_a_vigfree","prob_a_fair"]
PROB_B_CANDS = ["pb","prob_b","probB","p_b","implied_prob_b","prob_b_vigfree","prob_b_fair"]

def _find_col(header: List[str], names: List[str]) -> Optional[int]:
    lower = [h.strip().lower() for h in header]
    for name in names:
        if name.lower() in lower:
            return lower.index(name.lower())
    return None

def read_and_normalize(path: Path) -> List[Dict]:
    rows: List[Dict] = []
    with path.open(newline="", encoding="utf-8") as f:
        sn = csv.reader(f)
        header = next(sn, None)
        if not header:
            return rows

        # allow some common date/name columns, but they’re optional
        idx_oa = _find_col(header, ODDS_A_CANDS)
        idx_ob = _find_col(header, ODDS_B_CANDS)
        idx_pa = _find_col(header, PROB_A_CANDS)
        idx_pb = _find_col(header, PROB_B_CANDS)

        # last-resort: if probs missing but we have odds → infer fair probs 1/odds (no vig removal)
        infer_probs = False
        if (idx_pa is None or idx_pb is None) and (idx_oa is not None and idx_ob is not None):
            infer_probs = True

        for r in sn:
            try:
                oa = float(r[idx_oa]) if idx_oa is not None and r[idx_oa] != "" else None
                ob = float(r[idx_ob]) if idx_ob is not None and r[idx_ob] != "" else None
                if oa is None or ob is None or oa <= 1.0 or ob <= 1.0:
                    continue

                if infer_probs:
                    pa = 1.0 / oa
                    pb = 1.0 / ob
                    s = pa + pb
                    if s > 0:
                        pa, pb = pa / s, pb / s  # normalize to 1.0
                else:
                    pa = float(r[idx_pa]) if idx_pa is not None and r[idx_pa] != "" else None
                    pb = float(r[idx_pb]) if idx_pb is not None and r[idx_pb] != "" else None
                    if pa is None or pb is None:
                        continue

                rows.append({"oa": oa, "ob": ob, "pa": pa, "pb": pb})
            except Exception:
                # skip bad row
                continue
    return rows

# ---------- simple backtest (edge threshold + Kelly/flat) ----------
def kelly_stake(bankroll: float, p: float, odds: float, scale: float) -> float:
    # Kelly fraction for decimal odds
    b = odds - 1.0
    edge = (p * (b + 1) - 1)  # same as p*odds - 1
    frac = (p*(b+1) - (1-p)) / b if b > 0 else 0.0
    frac = max(0.0, frac)
    return bankroll * scale * frac

def run_backtest(rows: List[Dict], min_edge: float, staking: str, kscale: float, bankroll0: float) -> Dict:
    bankroll = bankroll0
    n_bets = 0
    total_staked = 0.0
    pnl = 0.0

    # Edge for A = pa*oa - 1 ; for B = pb*ob - 1
    for x in rows:
        ea = x["pa"] * x["oa"] - 1.0
        eb = x["pb"] * x["ob"] - 1.0

        # choose best positive edge side (if any)
        side = None
        edge = 0.0
        p, o = None, None
        if ea >= eb and ea >= min_edge:
            side = "A"; edge = ea; p = x["pa"]; o = x["oa"]
        elif eb > ea and eb >= min_edge:
            side = "B"; edge = eb; p = x["pb"]; o = x["ob"]

        if side is None:
            continue

        # stake sizing
        if staking == "kelly":
            stake = kelly_stake(bankroll, p, o, kscale)
        else:  # flat = 1 unit per bet
            stake = 1.0
        stake = max(0.0, min(stake, bankroll))  # cannot exceed bankroll

        if stake <= 0.0:
            continue

        # Expected value bet outcome simulator (deterministic EV for summary):
        ev = (p * (stake * (o - 1.0))) - ((1.0 - p) * stake)
        bankroll += ev
        pnl += ev
        total_staked += stake
        n_bets += 1

    roi = (pnl / total_staked) if total_staked > 0 else 0.0
    sharpe = (roi / 0.02) if roi != 0 else 0.0  # toy metric (placeholder)
    return {
        "cfg_id": 1,
        "n_bets": n_bets,
        "total_staked": round(total_staked, 4),
        "pnl": round(pnl, 4),
        "roi": round(roi, 6),
        "hitrate": 0.0,          # we don’t simulate coin flips here; EV-only
        "sharpe": round(sharpe, 6),
        "end_bankroll": round(bankroll, 4),
    }

# ---------- html summary ----------
def write_html_summary(diag: Dict, result: Dict, args: argparse.Namespace) -> None:
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    out = DOCS_DIR / "index.html"
    rows_preview = diag.get("preview", [])
    html = []
    html.append("<!doctype html><meta charset='utf-8'>")
    html.append("<title>Tennis Bot — Backtest Report</title>")
    html.append("<h1>Tennis Bot — Backtest Report</h1>")
    html.append("<p><em>Generated</em></p>")

    html.append("<h3>Recommended Config (cfg 1)</h3><pre>")
    html.append(json.dumps(result, indent=2))
    html.append("</pre>")

    html.append("<p><b>Params:</b> results/backtests/params_cfg1.json</p>")
    html.append("<p><b>Picks:</b> results/backtests/logs/picks_cfg1.csv</p>")

    html.append("<h3>Top Backtest Results</h3>")
    html.append("<table border='1' cellpadding='6' cellspacing='0'>")
    html.append("<tr><th>cfg_id</th><th>n_bets</th><th>total_staked</th><th>pnl</th>"
                "<th>roi</th><th>hitrate</th><th>sharpe</th><th>end_bankroll</th></tr>")
    html.append(
        f"<tr><td>{result['cfg_id']}</td>"
        f"<td>{result['n_bets']}</td>"
        f"<td>{result['total_staked']:.4f}</td>"
        f"<td>{result['pnl']:.4f}</td>"
        f"<td>{result['roi']:.6f}</td>"
        f"<td>{result['hitrate']:.4f}</td>"
        f"<td>{result['sharpe']:.6f}</td>"
        f"<td>{result['end_bankroll']:.4f}</td></tr>"
    )
    html.append("</table>")

    html.append("<h3>Diagnostics</h3><pre>")
    html.append(json.dumps({k:v for k,v in diag.items() if k!="preview"}, indent=2))
    html.append("</pre>")

    if rows_preview:
        html.append("<h3>Normalized Input Preview (first 20)</h3>")
        html.append("<table border='1' cellpadding='6' cellspacing='0'>")
        html.append("<tr><th>oa</th><th>ob</th><th>pa</th><th>pb</th></tr>")
        for r in rows_preview[:20]:
            html.append(f"<tr><td>{r['oa']}</td><td>{r['ob']}</td><td>{r['pa']}</td><td>{r['pb']}</td></tr>")
        html.append("</table>")

    out.write_text("\n".join(html), encoding="utf-8")

def write_csv(path: Path, header: List[str], rows: List[List]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)

def main() -> int:
    args = parse_args()

    # 1) resolve / choose dataset
    dataset_path, diag = resolve_dataset(args.dataset)
    if not dataset_path:
        msg = "ERROR: No usable dataset found\nTried:\n" + "\n".join(diag.get("tried", []))
        print(msg, file=sys.stderr)
        (DIAG_DIR / "_diagnostics.json").write_text(json.dumps({"reason":"no_dataset","tried":diag.get("tried", [])}, indent=2))
        return 1

    # 2) read + normalize
    rows = read_and_normalize(dataset_path)
    diag["total_rows"] = len(rows)
    diag["usable_rows"] = len(rows)
    diag["preview"] = rows[:20]

    # 3) run a simple EV-based backtest
    result = run_backtest(rows, args.min_edge, args.staking, args.kelly_scale, args.bankroll)

    # 4) write artifacts (summary + params + picks skeleton)
    DIAG_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # params cfg (echo actual knobs used)
    (DIAG_DIR / "params_cfg1.json").write_text(json.dumps({
        "cfg_id": 1,
        "bands": args.bands,
        "min_edge": args.min_edge,
        "staking": args.staking,
        "kelly_scale": args.kelly_scale,
        "bankroll": args.bankroll,
        "dataset": str(dataset_path),
    }, indent=2), encoding="utf-8")

    # picks log (we don’t simulate flips; keep empty header for compatibility)
    write_csv(OUT_DIR / "picks_cfg1.csv",
              ["ts","match_id","selection","odds","p","edge","stake"],
              [])

    # summary csv
    write_csv(DIAG_DIR / "summary.csv",
              ["cfg_id","n_bets","total_staked","pnl","roi","hitrate","sharpe","end_bankroll"],
              [[result["cfg_id"], result["n_bets"], result["total_staked"], result["pnl"],
                result["roi"], result["hitrate"], result["sharpe"], result["end_bankroll"]]])

    # 5) html report
    write_html_summary(diag, result, args)

    print(f"[backtest] dataset: {dataset_path}")
    print(f"[backtest] rows: {len(rows)} | bets: {result['n_bets']} | pnl: {result['pnl']:.4f} | end: {result['end_bankroll']:.2f}")
    return 0

if __name__ == "__main__":
    sys.exit(main())

