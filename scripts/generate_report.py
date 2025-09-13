#!/usr/bin/env python3
"""
generate_report.py

Purpose:
1) Normalize the enriched dataset so the backtester always
   sees the canonical columns: oa, ob, pa, pb.
   - Reads from outputs/prob_enriched.csv (preferred)
     or falls back to data/raw/vigfree_matches.csv
   - Writes a backtest-ready file back to outputs/prob_enriched.csv
     (preserving original columns and adding oa,ob,pa,pb)
   - Writes diagnostics to results/backtests/_diagnostics.json

2) Render docs/backtests/index.html
   - Prefers results/backtests/summary.csv
   - If empty, falls back to showing a preview table from the
     normalized outputs/prob_enriched.csv, plus diagnostics.
"""

from __future__ import annotations
import csv, json, sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional

ROOT     = Path(__file__).resolve().parents[1]
OUT_DIR  = ROOT / "outputs"
RAW_DIR  = ROOT / "data" / "raw"
RES_DIR  = ROOT / "results" / "backtests"
DOCS_DIR = ROOT / "docs" / "backtests"

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def now_utc_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def read_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_rows(path: Path, rows: List[Dict[str, str]], header: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in header})

def write_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2))

def num(x) -> float:
    try: return float(x)
    except: return 0.0

# ------------------------------------------------------------
# Normalization (adds oa, ob, pa, pb)
# ------------------------------------------------------------

ALIASES = {
    "oa": ["oa", "odds_a", "price_a", "decimal_odds_a", "oddsA", "oA", "a_odds"],
    "ob": ["ob", "odds_b", "price_b", "decimal_odds_b", "oddsB", "oB", "b_odds"],
    "pa": ["pa", "prob_a_vigfree", "prob_a", "p_a", "implied_prob_a", "probA", "pA"],
    "pb": ["pb", "prob_b_vigfree", "prob_b", "p_b", "implied_prob_b", "probB", "pB"],
    # nice-to-haves for report tables:
    "date": ["event_date","date","match_date","dt"],
    "tour": ["tournament","event","comp","league"],
    "pla":  ["player_a","home","playerA","A_name","a_player"],
    "plb":  ["player_b","away","playerB","B_name","b_player"],
}

def find_col(header: List[str], want: str) -> Optional[str]:
    lower = {h.lower(): h for h in header}
    for alias in ALIASES[want]:
        if alias.lower() in lower:
            return lower[alias.lower()]
    return None

def normalize_prob_enriched() -> Tuple[List[Dict[str,str]], Dict]:
    """
    Returns (normalized_rows, diagnostics)
    - Reads OUT_DIR/prob_enriched.csv or RAW_DIR/vigfree_matches.csv
    - Ensures oa, ob, pa, pb columns exist (duplicated from aliases)
    - Writes normalized back to OUT_DIR/prob_enriched.csv
    """
    srcs = [OUT_DIR / "prob_enriched.csv", RAW_DIR / "vigfree_matches.csv"]
    src: Optional[Path] = None
    for p in srcs:
        if p.exists() and p.stat().st_size > 0:
            src = p
            break

    diags = {
        "source": str(src) if src else None,
        "total_rows": 0,
        "usable_rows": 0,
        "skipped_missing": 0,
        "notes": [],
    }

    if not src:
        diags["notes"].append("No source file found.")
        return [], diags

    rows = read_rows(src)
    diags["total_rows"] = len(rows)
    if not rows:
        diags["notes"].append("Source has header only or zero rows.")
        return [], diags

    header = list(rows[0].keys())
    col_oa = find_col(header, "oa")
    col_ob = find_col(header, "ob")
    col_pa = find_col(header, "pa")
    col_pb = find_col(header, "pb")

    # Build normalized rows (duplicate to canonical names)
    normalized: List[Dict[str,str]] = []
    for r in rows:
        oa = r.get(col_oa) if col_oa else None
        ob = r.get(col_ob) if col_ob else None
        pa = r.get(col_pa) if col_pa else None
        pb = r.get(col_pb) if col_pb else None

        valid = True
        for v in (oa, ob, pa, pb):
            try:
                if v is None: valid = False
                else: _ = float(v)
            except Exception:
                valid = False

        if not valid:
            diags["skipped_missing"] += 1
            continue

        # duplicate original row and add canonical columns
        out = dict(r)
        out["oa"] = oa
        out["ob"] = ob
        out["pa"] = pa
        out["pb"] = pb
        normalized.append(out)

    diags["usable_rows"] = len(normalized)

    # Decide final header: original header + canonical columns (dedup)
    final_header = list(header)
    for k in ["oa","ob","pa","pb"]:
        if k not in final_header:
            final_header.append(k)

    # Overwrite outputs/prob_enriched.csv with normalized data
    if normalized:
        write_rows(OUT_DIR / "prob_enriched.csv", normalized, final_header)
    else:
        # still write a header so downstream steps don't crash
        write_rows(OUT_DIR / "prob_enriched.csv", [], final_header)

    return normalized, diags

# ------------------------------------------------------------
# Report rendering
# ------------------------------------------------------------

def pick_winner(summary_rows: List[Dict[str,str]]) -> Optional[Dict]:
    if not summary_rows:
        return None
    scored = []
    for r in summary_rows:
        scored.append({
            **r,
            "cfg_id": int(float(r.get("cfg_id", 0))),
            "n_bets": int(float(r.get("n_bets", 0))),
            "roi":    num(r.get("roi")),
            "sharpe": num(r.get("sharpe")),
        })
    scored.sort(key=lambda x: (x["sharpe"], x["roi"], x["n_bets"]), reverse=True)
    return scored[0] if scored else None

def render_bt_table(rows: List[Dict[str,str]]) -> str:
    if not rows:
        return "<p>No backtest results available.</p>"
    cols = ["cfg_id","n_bets","total_staked","pnl","roi","hitrate","sharpe","end_bankroll"]
    head = "".join(f"<th>{c}</th>" for c in cols)
    rows_sorted = sorted(rows, key=lambda r: (num(r.get("sharpe",0)), num(r.get("roi",0))), reverse=True)[:25]
    body = []
    for r in rows_sorted:
        tds = "".join(f"<td>{r.get(c, '')}</td>" for c in cols)
        body.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"

def render_preview_table(rows: List[Dict[str,str]]) -> str:
    if not rows:
        return "<p>(No normalized rows to preview.)</p>"
    # small preview
    cols = []
    for c in ["event_date","tournament","player_a","player_b","oa","ob","pa","pb"]:
        if c in rows[0]: cols.append(c)
    head = "".join(f"<th>{c}</th>" for c in cols)
    body = []
    for r in rows[:20]:
        tds = "".join(f"<td>{r.get(c,'')}</td>" for c in cols)
        body.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"

def build_html(summary_rows: List[Dict[str,str]], winner: Optional[Dict], normalized_rows: List[Dict[str,str]], diags: Dict) -> str:
    ts = now_utc_str()
    win_html = "<p>No winner found.</p>"
    if winner:
        cfg = winner["cfg_id"]
        win_html = f"""
        <h3>Recommended Config (cfg {cfg})</h3>
        <pre>{json.dumps(winner, indent=2)}</pre>
        <p>Params: <code>results/backtests/params_cfg{cfg}.json</code></p>
        <p>Picks:  <code>results/backtests/logs/picks_cfg{cfg}.csv</code></p>
        """

    diag_block = f"<pre>{json.dumps(diags, indent=2)}</pre>"

    html = f"""<!doctype html>
<html><head>
<meta charset="utf-8">
<title>Tennis Bot — Backtest Report</title>
<style>
 body {{ font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial; margin: 24px; }}
 table {{ border-collapse: collapse; width: 100%; }}
 th, td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: right; }}
 th {{ background: #f5f5f5; }}
 pre {{ background:#f6f8fa; padding: 12px; overflow:auto; }}
</style>
</head>
<body>
<h1>Tennis Bot — Backtest Report</h1>
<p><em>Generated {ts}</em></p>

{win_html}

<h3>Top Backtest Results</h3>
{render_bt_table(summary_rows)}

<h3>Diagnostics</h3>
{diag_block}

<h3>Normalized Input Preview (first 20)</h3>
{render_preview_table(normalized_rows)}

</body></html>
"""
    return html

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    # 1) Normalize/ensure oa,ob,pa,pb
    normalized_rows, diags = normalize_prob_enriched()
    write_json(RES_DIR / "_diagnostics.json", diags)

    # 2) Load summary if any
    summary = read_rows(RES_DIR / "summary.csv")
    winner  = pick_winner(summary)

    # 3) Render HTML
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    html = build_html(summary, winner, normalized_rows, diags)
    (DOCS_DIR / "index.html").write_text(html, encoding="utf-8")

    print(f"[report] normalized_rows={len(normalized_rows)} | summary_rows={len(summary)}")
    print(f"[report] wrote {DOCS_DIR/'index.html'}")

if __name__ == "__main__":
    sys.exit(main())

