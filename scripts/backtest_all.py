#!/usr/bin/env python3
"""
All-in-one backtest runner.

- Resolves input dataset (explicit path -> prob_enriched -> vigfree_matches -> sample_odds)
- Ensures pa/pb exist (computes from oa/ob if missing)
- Computes 'edge' and a single best 'pick' per row
- Filters by min true edge and by odds bands
- Runs a simple bankroll simulation:
    - staking = kelly: fraction = kelly_scale * edge / (price - 1), clipped to [0, 1]
    - staking = flat:  stake = kelly_scale * bankroll (per bet), clipped to bankroll
  (If no realized result column is present, computes *expected* PnL instead)
- Writes:
    - <outdir>/picks_cfg1.csv           (bets taken)
    - <outdir>/summary.csv              (one-row summary)
    - <outdir>/params_cfg1.json         (inputs)
- Prints a Markdown scoreboard and writes it to $GITHUB_STEP_SUMMARY if available.
"""

import argparse, json, os, sys, math
from pathlib import Path
from typing import List, Tuple
import pandas as pd
import numpy as np


# ---------- helpers

ALIASES_OA = ["oa", "odds_a", "oddsA", "odds1", "odds_home", "odds", "price_a"]
ALIASES_OB = ["ob", "odds_b", "oddsB", "odds2", "odds_away", "price_b"]
ALIASES_PA = ["pa", "prob_a", "probA", "implied_prob_a", "p_a", "prob_a_vigfree", "pa_vigfree"]
ALIASES_PB = ["pb", "prob_b", "probB", "implied_prob_b", "p_b", "prob_b_vigfree", "pb_vigfree"]

FALLBACKS = [
    "outputs/prob_enriched.csv",
    "data/raw/vigfree_matches.csv",
    "data/raw/odds/sample_odds.csv",
]


def find_col(df: pd.DataFrame, aliases: List[str]) -> str | None:
    for a in aliases:
        if a in df.columns:
            return a
    return None


def parse_bands(spec: str) -> List[Tuple[float, float]]:
    """
    '1.2,2.0|2.0,3.2|3.2,4.0' -> [(1.2,2.0), (2.0,3.2), (3.2,4.0)]
    Empty string -> one wide band (0, inf) so nothing is filtered by odds.
    """
    spec = (spec or "").strip()
    if not spec:
        return [(0.0, float("inf"))]
    out = []
    for part in spec.split("|"):
        lo, hi = (x.strip() for x in part.split(","))
        out.append((float(lo), float(hi)))
    return out


def write_job_summary(md: str) -> None:
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        print("\n--- Job Summary (local) ---\n")
        print(md)
        return
    with open(path, "a", encoding="utf-8") as f:
        f.write(md)
        f.write("\n")


# ---------- core

def resolve_dataset(explicit: str | None) -> Path:
    candidates = [explicit] if explicit else []
    candidates += FALLBACKS
    tried = []
    for c in candidates:
        if not c:
            continue
        p = Path(c)
        tried.append(str(p))
        if p.exists():
            print(f"[dataset] Using: {p}")
            return p
    print("[dataset] ERROR: none found. Tried:\n  - " + "\n  - ".join(tried), file=sys.stderr)
    sys.exit(1)


def ensure_pa_pb(df: pd.DataFrame) -> pd.DataFrame:
    col_oa = find_col(df, ALIASES_OA)
    col_ob = find_col(df, ALIASES_OB)
    col_pa = find_col(df, ALIASES_PA)
    col_pb = find_col(df, ALIASES_PB)

    # If pa/pb already exist and are numeric, keep them
    if col_pa and col_pb:
        # Standardize names
        df = df.rename(columns={col_pa: "pa", col_pb: "pb"})
        return df

    # Otherwise need oa/ob to derive pa/pb
    if not (col_oa and col_ob):
        raise SystemExit("[prep] ERROR: need oa/ob to compute pa/pb")

    df = df.rename(columns={col_oa: "oa", col_ob: "ob"})
    # Robust numeric conversion
    df["oa"] = pd.to_numeric(df["oa"], errors="coerce")
    df["ob"] = pd.to_numeric(df["ob"], errors="coerce")

    # Convert to implied probabilities and remove vig by normalization
    va = 1.0 / df["oa"]
    vb = 1.0 / df["ob"]
    s = va + vb
    df["pa"] = (va / s).clip(0, 1)
    df["pb"] = (vb / s).clip(0, 1)
    return df


def compute_edges(df: pd.DataFrame) -> pd.DataFrame:
    # Best pick per row (A or B) based on higher expected value pa*oa vs pb*ob
    if "oa" not in df.columns: df["oa"] = np.where("oa" in df.columns, df["oa"], 1.0/df["pa"])
    if "ob" not in df.columns: df["ob"] = np.where("ob" in df.columns, df["ob"], 1.0/df["pb"])

    ev_a = df["pa"] * df["oa"] - 1.0
    ev_b = df["pb"] * df["ob"] - 1.0

    pick = np.where(ev_a >= ev_b, "A", "B")
    pick_prob = np.where(pick == "A", df["pa"], df["pb"])
    pick_odds = np.where(pick == "A", df["oa"], df["ob"])
    true_edge = np.where(pick == "A", ev_a, ev_b)

    out = df.copy()
    out["pick"] = pick
    out["pick_prob"] = pick_prob
    out["pick_odds"] = pick_odds
    out["edge"] = true_edge
    return out


def bankroll_sim(rows: pd.DataFrame, staking: str, kelly_scale: float, bankroll0: float,
                 use_realized: bool) -> tuple[float, float, float, float, int]:
    """
    Return (final_bankroll, total_staked, pnl, roi, n_bets)
    If use_realized is False (no 'result' column), compute *expected* pnl.
    """
    bk = bankroll0
    total_staked = 0.0
    pnl = 0.0
    n_bets = 0

    has_result = use_realized and ("result" in rows.columns)

    for _, r in rows.iterrows():
        price = float(r["pick_odds"])
        edge = float(r["edge"])           # E[profit/unitstake]
        if staking == "kelly":
            frac = max(0.0, min(1.0, kelly_scale * edge / max(1e-9, (price - 1.0))))
            stake = min(bk, bk * frac)
        else:  # flat
            stake = min(bk, bk * kelly_scale)

        if stake <= 0:
            continue

        n_bets += 1
        total_staked += stake

        if has_result:
            res = int(r["result"])  # 1 if pick wins, 0 otherwise
            pl = stake * (price - 1.0) if res == 1 else -stake
        else:
            # expected profit: edge * stake
            pl = stake * edge

        pnl += pl
        bk += pl

    roi = (pnl / total_staked) if total_staked > 0 else 0.0
    return bk, total_staked, pnl, roi, n_bets


def make_md_summary(params: dict, diag: dict, table_row: dict, top_picks: pd.DataFrame) -> str:
    def fmt(x, d=4): 
        return f"{x:.{d}f}" if isinstance(x, (int, float, np.floating)) else str(x)

    md = []
    md.append("# Tennis Bot — Backtest Summary\n")
    md.append("**Params**\n")
    md.append("```json\n" + json.dumps(params, indent=2) + "\n```\n")

    md.append("**Diagnostics**\n")
    md.append("```json\n" + json.dumps(diag, indent=2) + "\n```\n")

    md.append("**Results**\n")
    md.append("| cfg_id | n_bets | total_staked | pnl | roi | sharpe | end_bankroll |\n")
    md.append("|---:|---:|---:|---:|---:|---:|---:|\n")
    md.append(f"| 1 | {table_row['n_bets']} | {fmt(table_row['total_staked'])} | "
              f"{fmt(table_row['pnl'])} | {fmt(table_row['roi'])} | {fmt(table_row['sharpe'])} | "
              f"{fmt(table_row['end_bankroll'])} |\n")

    if not top_picks.empty:
        preview = top_picks[["date","player_a","player_b","pick","pick_odds","pick_prob","edge"]].head(10)
        md.append("\n**Top Picks (first 10)**\n\n")
        md.append(preview.to_markdown(index=False))
        md.append("\n")
    return "\n".join(md)


# ---------- main

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="", help="Path to CSV. Optional.")
    ap.add_argument("--min-edge", type=float, default=0.0, help="Minimum true edge to consider")
    ap.add_argument("--bands", default="", help="Odds bands like '1.2,2.0|2.0,3.2'")
    ap.add_argument("--staking", choices=["kelly","flat"], default="kelly")
    ap.add_argument("--kelly-scale", type=float, default=0.5, help="Kelly scaler (0.5 = half-Kelly)")
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--outdir", default="results/allinone")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    logs_dir = outdir / "logs"
    outdir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)

    dataset = resolve_dataset(args.dataset)
    df = pd.read_csv(dataset)

    # Normalize common name columns for report friendliness
    if "player_a" not in df.columns and "playerA" in df.columns: df = df.rename(columns={"playerA":"player_a"})
    if "player_b" not in df.columns and "playerB" in df.columns: df = df.rename(columns={"playerB":"player_b"})
    if "date" not in df.columns and "event_date" in df.columns: df = df.rename(columns={"event_date":"date"})

    df = ensure_pa_pb(df)

    # Keep the original odds columns if present, else derive for visibility
    if "oa" not in df.columns: df["oa"] = 1.0 / df["pa"]
    if "ob" not in df.columns: df["ob"] = 1.0 / df["pb"]

    enriched = compute_edges(df)

    # Filter by min edge
    filtered = enriched[enriched["edge"] >= float(args.min_edge)].copy()

    # Odds band filtering (on the chosen side’s odds)
    bands = parse_bands(args.bands)
    band_lo, band_hi = bands[0]  # single-config (cfg1)
    band_mask = (filtered["pick_odds"] >= band_lo) & (filtered["pick_odds"] < band_hi)
    picks = filtered[band_mask].copy()

    use_realized = "result" in picks.columns  # realized pnl only if result is present
    end_bk, total_staked, pnl, roi, n_bets = bankroll_sim(
        picks, args.staking, args.kelly_scale, args.bankroll, use_realized
    )

    # naive Sharpe on expected returns (mean/std of edge * stake), fallback 0 if not enough bets
    if n_bets > 1:
        # per-bet return approximated by edge (exp) or realized
        if use_realized:
            returns = picks.apply(
                lambda r: (r["pick_odds"] - 1.0) if int(r.get("result",0)) == 1 else -1.0, axis=1
            )
        else:
            returns = picks["edge"]
        sharpe = float(returns.mean() / (returns.std(ddof=1) + 1e-9)) * math.sqrt(max(1, n_bets))
    else:
        sharpe = 0.0

    # ---- persist
    params = {
        "cfg_id": 1,
        "dataset": str(dataset),
        "bands": [band_lo, band_hi],
        "min_edge": args.min_edge,
        "staking": args.staking,
        "kelly_scale": args.kelly_scale,
        "bankroll": args.bankroll,
    }
    with open(outdir / "params_cfg1.json", "w", encoding="utf-8") as f:
        json.dump(params, f, indent=2)

    # picks log
    picks_cols = ["date","player_a","player_b","oa","ob","pa","pb","pick","pick_odds","pick_prob","edge"]
    for col in picks_cols:
        if col not in picks.columns:
            picks[col] = np.nan
    picks[picks_cols].to_csv(logs_dir / "picks_cfg1.csv", index=False)

    # summary
    summary_row = {
        "cfg_id": 1,
        "n_bets": int(n_bets),
        "total_staked": float(total_staked),
        "pnl": float(pnl),
        "roi": float(roi),
        "hitrate": float(picks.get("result", pd.Series(dtype=float)).mean()) if "result" in picks.columns and n_bets>0 else 0.0,
        "sharpe": float(sharpe),
        "end_bankroll": float(end_bk),
    }
    pd.DataFrame([summary_row]).to_csv(outdir / "summary.csv", index=False)

    # diagnostics for the report block
    diagnostics = {
        "source": str(dataset),
        "total_rows": int(len(df)),
        "usable_rows": int(len(enriched)),
        "skipped_missing": int(len(df) - len(enriched)),
        "notes": [],
    }

    # ---- human-friendly summary (stdout + GitHub Job Summary)
    md = make_md_summary(params, diagnostics, summary_row, picks)
    print(md)
    write_job_summary(md)


if __name__ == "__main__":
    main()
