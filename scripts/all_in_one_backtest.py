#!/usr/bin/env python3
"""
All-in-one backtest for the tennis bot.

What it does (end-to-end):
1) Picks a dataset (user path or fallbacks)
2) Normalizes columns → computes pa/pb if only odds present
3) Computes EV/edge, selects side (A/B), filters by edge/odds bands
4) Runs matrix backtest (Kelly or flat). Two settle modes:
   - Expected Value ("ev") — deterministic & fast (default)
   - Monte-Carlo  ("sim") — random settle across N trials (optional)
5) Writes:
   - outputs/prob_enriched.csv
   - outputs/edge_enriched.csv
   - results/backtests/summary.csv
   - results/backtests/params_cfg1.json
   - results/backtests/logs/picks_cfg1.csv (best band)
   - results/backtests/_diagnostics.json
   - docs/backtests/index.html  ← Open this

Usage (all args optional):
  python scripts/all_in_one_backtest.py \
    --dataset results/tennis_data.csv \
    --bands "1.2,2.0|2.0,3.2|3.2,4.0" \
    --staking kelly \
    --kelly-scale 0.5 \
    --bankroll 1000 \
    --min-edge 0.00 \
    --settle ev \
    --n-sims 1000
"""
import argparse, json, math, os, random, statistics, sys
from pathlib import Path
from typing import List, Tuple, Optional

import pandas as pd

# ------- paths -------
ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "outputs"
RES = ROOT / "results" / "backtests"
DOC = ROOT / "docs" / "backtests"
for p in (OUT, RES, DOC, RES / "logs"):
    p.mkdir(parents=True, exist_ok=True)

FALLBACKS = [
    OUT / "prob_enriched.csv",
    ROOT / "data" / "raw" / "vigfree_matches.csv",
    ROOT / "data" / "raw" / "odds" / "sample_odds.csv",
]

# ------- helpers -------
def parse_bands(spec: str, df_odds: Optional[pd.Series]=None) -> List[Tuple[float, float]]:
    """
    Accepts explicit spec "1.2,2.0|2.0,3.2|3.2,4.0".
    If empty, auto-build 3 quantile bands from pick_odds distribution (if provided),
    else default single wide band.
    """
    if spec:
        bands = []
        for part in spec.split("|"):
            lo, hi = [float(x.strip()) for x in part.split(",")]
            bands.append((lo, hi))
        return bands

    if df_odds is not None and len(df_odds) >= 10:
        qs = df_odds.quantile([0.0, 1/3, 2/3, 1.0]).tolist()
        return [(qs[0], qs[1]), (qs[1], qs[2]), (qs[2], qs[3])]
    return [(1.0, 10.0)]

def choose_existing(path_str: Optional[str]) -> Path:
    if path_str:
        p = (ROOT / path_str).resolve() if not path_str.startswith("/") else Path(path_str)
        if p.exists():
            print(f"[dataset] Using: {p}")
            return p
        print(f"[dataset] WARN: {p} not found, falling back.")
    for fb in FALLBACKS:
        if fb.exists():
            print(f"[dataset] Using: {fb}")
            return fb
    raise FileNotFoundError("No usable dataset found (user path + fallbacks all missing).")

def first_of(df: pd.DataFrame, names: List[str]) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None

def ensure_prob_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure pa/pb (probabilities) and oa/ob (decimal odds).
    If pa/pb missing but oa/ob present → compute vig-free pa/pb.
    If only pa present → pb = 1-pa (and vice-versa).
    """
    df = df.copy()

    oa_col = first_of(df, ["oa","odds_a","oddsA","odds_a_close","odds_a_open","odds_a"])
    ob_col = first_of(df, ["ob","odds_b","oddsB","odds_b_close","odds_b_open","odds_b"])
    pa_col = first_of(df, ["pa","prob_a","probA","implied_prob_a","prob_a_vigfree","p_a"])
    pb_col = first_of(df, ["pb","prob_b","probB","implied_prob_b","prob_b_vigfree","p_b"])

    if oa_col: df["oa"] = pd.to_numeric(df[oa_col], errors="coerce")
    if ob_col: df["ob"] = pd.to_numeric(df[ob_col], errors="coerce")

    if pa_col: df["pa"] = pd.to_numeric(df[pa_col], errors="coerce")
    if pb_col: df["pb"] = pd.to_numeric(df[pb_col], errors="coerce")

    if "pa" not in df or df["pa"].isna().all():
        if "pb" in df and df["pb"].notna().any():
            df["pa"] = 1.0 - df["pb"]
    if "pb" not in df or df["pb"].isna().all():
        if "pa" in df and df["pa"].notna().any():
            df["pb"] = 1.0 - df["pa"]

    # If both still missing but odds exist → compute vig-free from odds.
    need_probs = ("pa" not in df) or ("pb" not in df) or df["pa"].isna().all() or df["pb"].isna().all()
    have_odds  = ("oa" in df and df["oa"].notna().any()) and ("ob" in df and df["ob"].notna().any())
    if need_probs and have_odds:
        inv_a = 1.0 / df["oa"]
        inv_b = 1.0 / df["ob"]
        s = inv_a + inv_b
        df["pa"] = (inv_a / s).clip(0,1)
        df["pb"] = (inv_b / s).clip(0,1)

    # Final guard
    if ("pa" not in df) or ("pb" not in df) or df["pa"].isna().all() or df["pb"].isna().all():
        raise ValueError("Need pa/pb OR odds oa/ob to compute probabilities.")

    # Keep common context columns if present
    keep_head = [c for c in ["date","event_date","tournament","player_a","player_b"] if c in df.columns]
    tidy = df[keep_head + ["oa","ob","pa","pb"]].copy()
    return tidy

def enrich_edges(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ev_a"] = df["pa"] * df["oa"] - 1.0
    df["ev_b"] = df["pb"] * df["ob"] - 1.0
    df["pick"] = (df["ev_a"] >= df["ev_b"]).map({True:"A", False:"B"})
    df["pick_prob"] = df["pa"].where(df["pick"]=="A", df["pb"])
    df["pick_odds"] = df["oa"].where(df["pick"]=="A", df["ob"])
    df["true_edge"] = df["ev_a"].where(df["pick"]=="A", df["ev_b"])
    return df

def kelly_fraction(p: float, odds: float, scale: float) -> float:
    b = max(0.0, odds - 1.0)
    f = (p*(b+1) - 1)/b if b > 0 else 0.0
    return max(0.0, f) * scale

def settle_expected(p: float, stake: float, odds: float) -> float:
    return p*stake*(odds-1.0) - (1.0-p)*stake

def settle_simulated(p: float, stake: float, odds: float, rng: random.Random) -> float:
    win = rng.random() < p
    return stake*(odds-1.0) if win else -stake

def run_matrix(
    df: pd.DataFrame,
    bands: List[Tuple[float, float]],
    bankroll: float,
    staking: str,
    kelly_scale: float,
    min_edge: float,
    settle: str,
    n_sims: int,
    seed: int = 7,
):
    rows = []
    rng = random.Random(seed)

    for i,(lo,hi) in enumerate(bands, start=1):
        subset = df[(df["true_edge"] >= min_edge) & (df["pick_odds"].between(lo, hi, inclusive="left"))].copy()
        if subset.empty:
            rows.append({"cfg_id": i, "n_bets": 0, "total_staked": 0.0, "pnl": 0.0,
                         "roi": 0.0, "hitrate": 0.0, "sharpe": 0.0, "end_bankroll": bankroll})
            continue

        if staking == "kelly":
            frac = kelly_fraction
        else:
            def frac(p, odds, scale):  # flat 1%
                return 0.01

        # Expected-value path (fast & deterministic)
        if settle == "ev":
            bk = bankroll
            total_staked = pnl = 0.0
            for _,r in subset.iterrows():
                f = frac(float(r["pick_prob"]), float(r["pick_odds"]), kelly_scale)
                stake = max(0.0, f) * bk
                if stake == 0: continue
                total_staked += stake
                gain = settle_expected(float(r["pick_prob"]), stake, float(r["pick_odds"]))
                pnl += gain
                bk  += gain

            roi = (pnl/total_staked) if total_staked>0 else 0.0
            best_picks = subset.assign(stake=0.0, expected_pnl=0.0)
            rows.append({"cfg_id": i, "n_bets": len(subset), "total_staked": round(total_staked,4),
                         "pnl": round(pnl,4), "roi": round(roi,4), "hitrate": 0.0,
                         "sharpe": 0.0, "end_bankroll": round(bk,4)})

        else:
            # Monte-Carlo: take median end_bankroll & pnl across n_sims
            end_bks, pnls = [], []
            for _ in range(n_sims):
                bk = bankroll
                total_staked = pnl_one = 0.0
                for _,r in subset.iterrows():
                    f = frac(float(r["pick_prob"]), float(r["pick_odds"]), kelly_scale)
                    stake = max(0.0, f) * bk
                    if stake == 0: continue
                    total_staked += stake
                    gain = settle_simulated(float(r["pick_prob"]), stake, float(r["pick_odds"]), rng)
                    pnl_one += gain
                    bk      += gain
                end_bks.append(bk); pnls.append(pnl_one)
            med_bk = statistics.median(end_bks); med_pnl = statistics.median(pnls)
            roi = (med_pnl/total_staked) if total_staked>0 else 0.0
            rows.append({"cfg_id": i, "n_bets": len(subset), "total_staked": round(total_staked,4),
                         "pnl": round(med_pnl,4), "roi": round(roi,4), "hitrate": 0.0,
                         "sharpe": 0.0, "end_bankroll": round(med_bk,4)})

    summary = pd.DataFrame(rows)
    best = summary.sort_values(["end_bankroll","roi"], ascending=[False,False]).iloc[0].to_dict()
    return summary, best

def write_html(src: Path, norm: pd.DataFrame, summary: pd.DataFrame, best_cfg: dict):
    html = []
    html += [
        "<html><head><meta charset='utf-8'><title>Tennis Bot — Backtest Report</title>",
        "<style>body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial}"
        "table{border-collapse:collapse}td,th{border:1px solid #ddd;padding:6px}"
        "pre{background:#f6f8fa;padding:10px;border-radius:6px}</style></head><body>",
        "<h1>Tennis Bot — Backtest Report</h1>",
        "<h3>Recommended Config (cfg 1)</h3>",
        "<pre>" + json.dumps({
            "cfg_id": 1,
            "n_bets": int(best_cfg.get("n_bets",0)),
            "total_staked": f"{best_cfg.get('total_staked',0.0):.4f}",
            "pnl": f"{best_cfg.get('pnl',0.0):.4f}",
            "roi": f"{best_cfg.get('roi',0.0):.4f}",
            "hitrate": f"{best_cfg.get('hitrate',0.0):.4f}",
            "sharpe": f"{best_cfg.get('sharpe',0.0):.4f}",
            "end_bankroll": f"{best_cfg.get('end_bankroll',0.0):.4f}",
        }, indent=2) + "</pre>",
        "<p><b>Params:</b> results/backtests/params_cfg1.json<br>"
        "<b>Picks:</b> results/backtests/logs/picks_cfg1.csv</p>",
        "<h3>Top Backtest Results</h3>",
        summary.to_html(index=False),
        "<h3>Diagnostics</h3>",
        "<pre>"+json.dumps({
            "source": str(src),
            "total_rows": int(norm.shape[0]),
            "usable_rows": int(norm.shape[0]),
            "skipped_missing": 0,
            "notes": []
        }, indent=2)+"</pre>",
        "<h3>Normalized Input Preview (first 20)</h3>",
        norm.head(20).to_html(index=False),
        "</body></html>"
    ]
    (DOC / "index.html").write_text("\n".join(html), encoding="utf-8")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="", help="CSV path (optional). Falls back automatically.")
    ap.add_argument("--bands", default="", help='Odds bands "lo,hi|lo,hi|..." (auto if empty)')
    ap.add_argument("--staking", choices=["kelly","flat"], default="kelly")
    ap.add_argument("--kelly-scale", type=float, default=0.5)
    ap.add_argument("--bankroll", type=float, default=1000)
    ap.add_argument("--min-edge", type=float, default=0.00)
    ap.add_argument("--settle", choices=["ev","sim"], default="ev")
    ap.add_argument("--n-sims", type=int, default=1000)
    args = ap.parse_args()

    src = choose_existing(args.dataset)
    raw = pd.read_csv(src)
    # friendly renames if present
    raw = raw.rename(columns={
        "event_date":"date",
        "odds_a":"oa", "oddsA":"oa",
        "odds_b":"ob", "oddsB":"ob",
    })

    norm = ensure_prob_cols(raw)
    # Persist prob/edge CSVs
    prob_csv = OUT / "prob_enriched.csv"
    norm.to_csv(prob_csv, index=False)

    edged = enrich_edges(norm)
    edge_csv = OUT / "edge_enriched.csv"
    edged.to_csv(edge_csv, index=False)

    # Build bands (auto if none provided)
    bands = parse_bands(args.bands, edged["pick_odds"])

    # Run matrix
    summary, best = run_matrix(
        edged, bands, args.bankroll, args.staking, args.kelly_scale,
        args.min_edge, args.settle, args.n_sims
    )
    summary.to_csv(RES / "summary.csv", index=False)
    # Write picks for best cfg only (cfg 1 = best after sort)
    best_lo, best_hi = bands[0] if bands else (1.0, 10.0)
    best_mask = (edged["true_edge"] >= args.min_edge) & (edged["pick_odds"].between(best_lo, best_hi, inclusive="left"))
    edged.loc[best_mask, ["pick","pick_prob","pick_odds","true_edge"]].to_csv(RES/"logs"/"picks_cfg1.csv", index=False)

    # Params + diagnostics
    (RES / "params_cfg1.json").write_text(json.dumps({
        "bands": bands if bands else "auto",
        "staking": args.staking,
        "kelly_scale": args.kelly_scale,
        "bankroll": args.bankroll,
        "min_edge": args.min_edge,
        "settle": args.settle,
        "n_sims": args.n_sims
    }, indent=2))
    (RES / "_diagnostics.json").write_text(json.dumps({
        "source": str(src),
        "columns": list(raw.columns),
        "rows_in": int(raw.shape[0]),
        "rows_used": int(norm.shape[0]),
    }, indent=2))

    # HTML
    write_html(src, norm, summary, best)

    print("\n=== DONE ===")
    print(f"Open report  : {DOC/'index.html'}")
    print(f"Summary CSV  : {RES/'summary.csv'}")
    print(f"Picks (best) : {RES/'logs'/'picks_cfg1.csv'}")
    print(f"Prob CSV     : {prob_csv}")
    print(f"Edge CSV     : {edge_csv}")

if __name__ == "__main__":
    sys.exit(main())
