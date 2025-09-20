#!/usr/bin/env python3
import argparse, json, math, os
from pathlib import Path
import pandas as pd

# ---------- constants / paths ----------
ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = ROOT / "outputs"
RES_DIR = ROOT / "results" / "backtests"
LOG_DIR = RES_DIR / "logs"
OUT_DIR.mkdir(parents=True, exist_ok=True)
RES_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

FALLBACKS = [
    OUT_DIR / "prob_enriched.csv",
    ROOT / "data" / "raw" / "vigfree_matches.csv",
    ROOT / "data" / "raw" / "odds" / "sample_odds.csv",
]

HTML_REPORT = RES_DIR / "index.html"        # still produced, but optional to open
SUMMARY_CSV = RES_DIR / "summary.csv"
SUMMARY_MD  = RES_DIR / "summary.md"        # <- NEW: job-summary friendly
PARAMS_JSON = RES_DIR / "params_cfg1.json"
PICKS_CSV   = LOG_DIR / "picks_cfg1.csv"
PROB_ENRICHED = OUT_DIR / "prob_enriched.csv"
EDGE_ENRICHED = OUT_DIR / "edge_enriched.csv"

# ---------- helpers ----------
def alias(df, want, aliases):
    for a in aliases:
        if a in df.columns:
            return a
    if want in df.columns:
        return want
    return None

def ensure_probs_and_odds(df: pd.DataFrame) -> pd.DataFrame:
    """Guarantee columns: date?, player_a, player_b, oa, ob, pa, pb."""
    df = df.copy()

    pa_col = alias(df, "pa", ["pa","prob_a","probA","p_a","implied_prob_a","prob_a_vigfree","pA"])
    pb_col = alias(df, "pb", ["pb","prob_b","probB","p_b","implied_prob_b","prob_b_vigfree","pB"])
    oa_col = alias(df, "oa", ["oa","odds_a","oddsA","a_odds"])
    ob_col = alias(df, "ob", ["ob","odds_b","oddsB","b_odds"])

    if pa_col and pb_col and (oa_col is None or ob_col is None):
        df["oa"] = 1.0 / df[pa_col].astype(float)
        df["ob"] = 1.0 / df[pb_col].astype(float)
        oa_col, ob_col = "oa","ob"

    if oa_col and ob_col and (pa_col is None or pb_col is None):
        ia = 1.0/df[oa_col].astype(float)
        ib = 1.0/df[ob_col].astype(float)
        s = ia + ib
        df["pa"] = (ia/s).astype(float)
        df["pb"] = (ib/s).astype(float)
        pa_col, pb_col = "pa","pb"

    if oa_col != "oa" and oa_col is not None:
        df["oa"] = df[oa_col].astype(float)
    if ob_col != "ob" and ob_col is not None:
        df["ob"] = df[ob_col].astype(float)
    if pa_col != "pa" and pa_col is not None:
        df["pa"] = df[pa_col].astype(float)
    if pb_col != "pb" and pb_col is not None:
        df["pb"] = df[pb_col].astype(float)

    a_name = alias(df, "player_a", ["player_a","home","a","team_a"])
    b_name = alias(df, "player_b", ["player_b","away","b","team_b"])
    date_col = alias(df, "date", ["date","event_date","match_date"])

    if a_name and a_name != "player_a": df["player_a"] = df[a_name]
    if b_name and b_name != "player_b": df["player_b"] = df[b_name]
    if date_col and date_col != "date": df["date"] = df[date_col]

    need = {"oa","ob","pa","pb"}
    missing = [c for c in need if c not in df.columns]
    if missing:
        raise ValueError(f"Dataset still missing {missing}. Provide odds or probabilities for both sides.")

    keep = [c for c in ["date","player_a","player_b","oa","ob","pa","pb"] if c in df.columns]
    return df[keep].reset_index(drop=True)

def enrich_edges(df: pd.DataFrame, min_edge: float) -> pd.DataFrame:
    df = df.copy()
    df["ev_a"] = df["pa"]*df["oa"] - 1.0
    df["ev_b"] = df["pb"]*df["ob"] - 1.0
    df["pick"] = df.apply(lambda r: "A" if r["ev_a"] >= r["ev_b"] else "B", axis=1)
    df["pick_prob"] = df.apply(lambda r: r["pa"] if r["pick"]=="A" else r["pb"], axis=1)
    df["pick_odds"] = df.apply(lambda r: r["oa"] if r["pick"]=="A" else r["ob"], axis=1)
    df["true_edge"] = df["pick_prob"]*df["pick_odds"] - 1.0
    return df[df["true_edge"] >= float(min_edge)].reset_index(drop=True)

def kelly_fraction(p, o, kscale):
    b = o - 1.0
    edge = p*o - 1.0
    f = edge / b if b > 0 else 0.0
    return max(0.0, kscale * f)

def run_backtest(df_edges: pd.DataFrame, staking: str, kelly_scale: float, bankroll: float) -> dict:
    bank = float(bankroll)
    n_bets = 0
    results = df_edges["result"].astype(float) if "result" in df_edges.columns else None

    stakes = []
    for i, row in df_edges.iterrows():
        p = float(row["pick_prob"]); o = float(row["pick_odds"])
        f = kelly_fraction(p, o, kelly_scale) if staking=="kelly" else 0.01
        stake = bank * f
        stakes.append(stake)
        if results is not None and not math.isnan(results.iloc[i]):
            n_bets += 1
            bank = bank + stake*(o-1.0) if results.iloc[i] > 0.5 else bank - stake

    total_staked = sum(stakes)
    pnl = bank - bankroll
    roi = (pnl / total_staked) if total_staked > 0 else 0.0
    return {
        "cfg_id": 1,
        "n_bets": n_bets,
        "total_staked": round(total_staked, 4),
        "pnl": round(pnl, 4),
        "roi": round(roi, 4),
        "hitrate": 0.0,
        "sharpe": 0.0,
        "end_bankroll": round(bank, 4),
    }

def write_html(params, summary_row, src_path, sample_df):
    HTML_REPORT.write_text(f"""<!doctype html><html><head><meta charset="utf-8">
<title>Tennis Bot — Backtest Report</title>
<style>
 body {{ font-family: -apple-system, Segoe UI, Roboto, Arial; margin:20px; }}
 table {{ border-collapse: collapse; }}
 th, td {{ border:1px solid #ccc; padding:6px 10px; text-align:right; }}
 th:first-child, td:first-child {{ text-align:left; }}
 code {{ background:#f6f8fa; padding:2px 4px; }}
</style></head><body>
<h1>Tennis Bot — Backtest Report</h1>
<h3>Recommended Config (cfg 1)</h3>
<pre>{json.dumps(summary_row | {k:v for k,v in params.items()}, indent=2)}</pre>
<p><b>Params:</b> <code>{PARAMS_JSON.as_posix()}</code><br>
<b>Picks:</b> <code>{PICKS_CSV.as_posix()}</code></p>
<h3>Top Backtest Results</h3>
{pd.DataFrame([summary_row]).to_html(index=False)}
<h3>Diagnostics</h3>
<pre>{json.dumps({{
  "source": Path(src_path).as_posix(),
  "total_rows": int(len(sample_df)),
  "usable_rows": int(len(sample_df)),
  "skipped_missing": 0,
  "notes": []
}}, indent=2)}</pre>
<h4>Normalized Input Preview (first 20)</h4>
{sample_df.head(20).to_html(index=False)}
</body></html>""")

def write_markdown(params, summary_row, src_path, df_norm, df_edges):
    """Job-summary friendly markdown (also printed to stdout)."""
    # save CSVs for downstream use
    pd.DataFrame([summary_row]).to_csv(SUMMARY_CSV, index=False)
    PARAMS_JSON.write_text(json.dumps(params, indent=2))

    top_picks = df_edges.sort_values("true_edge", ascending=False).head(10)
    # lightweight markdown (no extra deps)
    def md_table(df: pd.DataFrame, max_rows=10):
        df = df.head(max_rows).copy()
        cols = list(df.columns)
        lines = ["|" + "|".join(cols) + "|",
                 "|" + "|".join(["---"]*len(cols)) + "|"]
        for _, r in df.iterrows():
            lines.append("|" + "|".join(str(r[c]) for c in cols) + "|")
        return "\n".join(lines)

    md = []
    md.append("# Tennis Bot — Backtest Summary\n")
    md.append("## Config")
    md.append("```json\n" + json.dumps(params, indent=2) + "\n```")
    md.append("## Results")
    md.append(md_table(pd.DataFrame([summary_row])))
    md.append("\n## Diagnostics")
    md.append("```json\n" + json.dumps({
        "source": Path(src_path).as_posix(),
        "total_rows": int(len(df_norm)),
        "usable_rows": int(len(df_norm)),
        "skipped_missing": 0,
        "notes": []
    }, indent=2) + "\n```")
    md.append("## Top picks (by true_edge)")
    show_cols = [c for c in ["date","player_a","player_b","pick","pick_prob","pick_odds","true_edge"] if c in top_picks.columns]
    md.append(md_table(top_picks[show_cols]))
    md.append("\n## Files")
    md.append(f"- Summary CSV: `{SUMMARY_CSV.as_posix()}`")
    md.append(f"- Picks CSV: `{PICKS_CSV.as_posix()}`")
    md.append(f"- Prob file: `{PROB_ENRICHED.as_posix()}`")
    md.append(f"- Edge file: `{EDGE_ENRICHED.as_posix()}`")
    content = "\n\n".join(md)

    SUMMARY_MD.write_text(content)
    # Also print to STDOUT so you see it in logs
    print("\n" + content + "\n")

    # If running in GitHub Actions, also append to the Job Summary
    step_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if step_summary:
        with open(step_summary, "a", encoding="utf-8") as f:
            f.write(content + "\n")

def main():
    ap = argparse.ArgumentParser(description="All-in-one local backtest runner")
    ap.add_argument("--dataset", default="", help="Path to CSV (optional). Falls back to outputs/prob_enriched.csv -> data/raw/vigfree_matches.csv -> data/raw/odds/sample_odds.csv")
    ap.add_argument("--min-edge", type=float, default=0.00, help="Minimum true edge to keep")
    ap.add_argument("--staking", choices=["kelly","flat"], default="kelly")
    ap.add_argument("--kelly-scale", type=float, default=0.5, help="0.5 = half Kelly")
    ap.add_argument("--bankroll", type=float, default=1000.0)
    args = ap.parse_args()

    # 1) Choose dataset
    candidates = [Path(args.dataset)] if args.dataset else []
    candidates += FALLBACKS
    src = next((c for c in candidates if c and c.exists()), None)
    if src is None:
        raise FileNotFoundError(f"No usable dataset found. Tried: {', '.join(p.as_posix() for p in candidates)}")

    # 2) Normalize
    df_raw = pd.read_csv(src)
    df_norm = ensure_probs_and_odds(df_raw)
    df_norm.to_csv(PROB_ENRICHED, index=False)

    # 3) Enrich edges + write picks
    df_edges = enrich_edges(df_norm, args.min_edge)
    df_edges.to_csv(EDGE_ENRICHED, index=False)
    picks = (df_edges[["date","player_a","player_b","pick","pick_odds","pick_prob","true_edge"]]
             if "date" in df_edges.columns else
             df_edges[["player_a","player_b","pick","pick_odds","pick_prob","true_edge"]])
    picks.to_csv(PICKS_CSV, index=False)

    # 4) Backtest
    summary = run_backtest(df_edges, args.staking, args.kelly_scale, args.bankroll)

    # 5) Params + reports
    params = {
        "cfg_id": 1,
        "staking": args.staking,
        "kelly_scale": args.kelly_scale,
        "min_edge": args.min_edge,
        "bankroll": args.bankroll,
        "dataset": Path(src).as_posix()
    }

    # HTML for local viewing (kept), plus Markdown for console/Actions
    write_html(params, summary, src, df_norm)
    write_markdown(params, summary, src, df_norm, df_edges)

if __name__ == "__main__":
    main()
