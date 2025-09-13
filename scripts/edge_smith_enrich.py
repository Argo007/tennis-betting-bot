
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EdgeSmith — enrich a prob+odds CSV with EV/edge metrics and a recommended pick.

Inputs can use several alias column names; we normalize to:
  oa, ob  -> decimal odds for sides A/B
  pa, pb  -> win probabilities for sides A/B (already vig-free or fair)

Output includes:
  ev_a, ev_b     -> expected value (pa*oa - 1, pb*ob - 1)
  te_a, te_b     -> true edge in probability space (pa - 1/oa, pb - 1/ob)
  pick, pick_prob, pick_odds, pick_ev, pick_te  -> best positive edge (if any)
"""

from __future__ import annotations
import argparse
import sys
from pathlib import Path
import math
import pandas as pd


# --------- helpers ---------
ALIASES = {
    # odds
    "oa": ["oa", "odds_a", "price_a", "dec_odds_a", "o_a", "oddsa"],
    "ob": ["ob", "odds_b", "price_b", "dec_odds_b", "o_b", "oddsb"],
    # probs
    "pa": [
        "pa", "prob_a", "probA", "p_a",
        "implied_prob_a", "implied_pa",
        "prob_a_vigfree", "prob_a_vig_free", "pa_vigfree", "pavf"
    ],
    "pb": [
        "pb", "prob_b", "probB", "p_b",
        "implied_prob_b", "implied_pb",
        "prob_b_vigfree", "prob_b_vig_free", "pb_vigfree", "pbvf"
    ],
}

def _find_col(df: pd.DataFrame, canonical: str) -> str | None:
    """Return the first matching column alias present in df for canonical name."""
    for alias in ALIASES[canonical]:
        if alias in df.columns:
            return alias
    return None

def _require_cols(df: pd.DataFrame, required=("oa", "ob", "pa", "pb")):
    missing = []
    for canon in required:
        if _find_col(df, canon) is None:
            missing.append(canon)
    if missing:
        # Build helpful message listing aliases we looked for
        parts = []
        for canon in missing:
            parts.append(f"{canon}: {ALIASES[canon]}")
        msg = " | ".join(parts)
        raise SystemExit(
            f"[enrich] FATAL: Missing required columns.\n"
            f"Looked for these aliases -> {msg}\n"
            f"Available columns: {list(df.columns)}"
        )

def _breakeven_p(odds: float) -> float:
    try:
        return 1.0 / float(odds) if odds and float(odds) > 0 else math.nan
    except Exception:
        return math.nan


# --------- core ---------
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Compute EV/TE and pick columns; returns a new dataframe."""
    # Normalize names by creating oa/ob/pa/pb views without losing originals
    oa_col = _find_col(df, "oa")
    ob_col = _find_col(df, "ob")
    pa_col = _find_col(df, "pa")
    pb_col = _find_col(df, "pb")

    # cast to numeric defensively
    df["_oa"] = pd.to_numeric(df[oa_col], errors="coerce")
    df["_ob"] = pd.to_numeric(df[ob_col], errors="coerce")
    df["_pa"] = pd.to_numeric(df[pa_col], errors="coerce")
    df["_pb"] = pd.to_numeric(df[pb_col], errors="coerce")

    # EV = p * odds - 1
    df["ev_a"] = df["_pa"] * df["_oa"] - 1.0
    df["ev_b"] = df["_pb"] * df["_ob"] - 1.0

    # True edge = p - 1/odds  (probability advantage vs breakeven)
    df["te_a"] = df["_pa"] - df["_oa"].map(_breakeven_p)
    df["te_b"] = df["_pb"] - df["_ob"].map(_breakeven_p)

    # Which side is better by EV?
    best_is_a = df["ev_a"].fillna(-1e9) >= df["ev_b"].fillna(-1e9)
    df["pick"] = best_is_a.map({True: "A", False: "B"})
    df["pick_prob"] = df["_pa"].where(best_is_a, df["_pb"])
    df["pick_odds"] = df["_oa"].where(best_is_a, df["_ob"])
    df["pick_ev"]   = df["ev_a"].where(best_is_a, df["ev_b"])
    df["pick_te"]   = df["te_a"].where(best_is_a, df["te_b"])

    # Keep original columns intact + new metrics (drop temp fields)
    out_cols = list(df.columns)
    # temp markers we will remove from the final order (but keep metrics)
    for c in ["_oa", "_ob", "_pa", "_pb"]:
        out_cols.remove(c)
    return df[out_cols]


# --------- CLI ---------
def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Enrich a prob+odds CSV with EV and edge metrics (EdgeSmith)."
    )
    p.add_argument("--input", required=True, help="Input CSV (must have odds+probs).")
    p.add_argument("--output", required=True, help="Where to write enriched CSV.")
    p.add_argument(
        "--min-edge",
        type=float,
        default=0.0,
        help="Optional: minimum pick_ev required to be considered (for info column only).",
    )
    # Accept but ignore --method for compatibility with upstream calls
    p.add_argument("--method", default="", help="Compatibility flag (ignored).")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    in_path = Path(args.input)
    out_path = Path(args.output)

    if not in_path.exists():
        print(f"[enrich] ERROR: input not found: {in_path}", file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(in_path)
    if df.empty:
        print("[enrich] ERROR: input has header only or zero rows.", file=sys.stderr)
        # Still write an empty file with expected headers for downstream robustness
        pd.DataFrame().to_csv(out_path, index=False)
        sys.exit(0)

    # Validate required columns (by aliases), then enrich
    _require_cols(df, required=("oa", "ob", "pa", "pb"))
    enriched = enrich(df)

    # (Optional) convenience flag showing whether pick passes EV threshold
    try:
        enriched["pick_pass_min_edge"] = (enriched["pick_ev"] >= float(args.min_edge)).astype(int)
    except Exception:
        enriched["pick_pass_min_edge"] = 0

    out_path.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(out_path, index=False)

    # Summary
    n_rows = len(enriched)
    n_pos = int((enriched["pick_ev"] >= float(args.min_edge)).sum())
    print(
        f"[enrich] wrote {n_rows} rows -> {out_path}\n"
        f"[enrich] picks >= min_edge({args.min_edge}): {n_pos}"
    )


if __name__ == "__main__":
    main()
