#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
check_probabilities.py

Diagnostics for prob_enriched.csv:
- Verifies presence of price/odds and probability columns
- Compares p_model vs implied 1/price
- Shows basic stats, missing counts
- Estimates how many rows would pass edge filters
- Writes outputs/diag_prob.md and prints to stdout

Run:
  python scripts/check_probabilities.py \
      --input outputs/prob_enriched.csv \
      --min-edge 0.02 \
      --edge 0.08
"""

from __future__ import annotations
import argparse, csv, math, pathlib, statistics as stats

def pick_col(header, candidates):
    hset = {c.lower(): c for c in header}
    for c in candidates:
        if c.lower() in hset:
            return hset[c.lower()]
    return None

def f(x, d=6):
    try:
        return round(float(x), d)
    except Exception:
        return float("nan")

def clip(x, lo, hi):
    return max(lo, min(hi, x))

def read_rows(path: pathlib.Path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", "-i", default="outputs/prob_enriched.csv",
                    help="Path to prob_enriched.csv")
    ap.add_argument("--min-edge", type=float, default=0.02,
                    help="Current engine min-edge filter (edge_model >= min_edge)")
    ap.add_argument("--edge", type=float, default=0.08,
                    help="True-Edge booster used in Kelly sizing (not filter)")
    args = ap.parse_args()

    p = pathlib.Path(args.input)
    out_md = pathlib.Path("outputs/diag_prob.md")
    out_md.parent.mkdir(parents=True, exist_ok=True)

    if not p.is_file():
        msg = f"File not found: {p}"
        print(msg)
        out_md.write_text(f"## Probability Diagnostics\n\n{msg}\n", encoding="utf-8")
        return

    rows = read_rows(p)
    if not rows:
        msg = f"No rows in {p}"
        print(msg)
        out_md.write_text(f"## Probability Diagnostics\n\n{msg}\n", encoding="utf-8")
        return

    hdr = list(rows[0].keys())
    col_price = pick_col(hdr, ["price","odds","decimal_odds"])
    col_prob  = pick_col(hdr, ["p_model","p","prob","model_prob","probability"])

    lines = []
    lines.append("## Probability Diagnostics")
    lines.append("")
    lines.append(f"- File: `{p}`")
    lines.append(f"- Rows: **{len(rows)}**")
    lines.append(f"- Detected price col: **{col_price or '-'}**")
    lines.append(f"- Detected prob  col: **{col_prob or '-'}**")
    lines.append("")

    if not col_price:
        lines.append("> ❌ No price/odds column found. Expect one of: price / odds / decimal_odds.")
        out_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print("\n".join(lines))
        return

    # Extract metrics
    prices, be_list, pmodel_list, diffs = [], [], [], []
    pmodel_present = 0
    pmodel_valid = 0

    for r in rows:
        price = f(r.get(col_price, "nan"))
        if not (price > 1.0 and math.isfinite(price)):
            continue
        prices.append(price)
        breakeven = 1.0 / price
        be_list.append(breakeven)

        p_model = float("nan")
        if col_prob and r.get(col_prob, "") not in ("", None, "NA"):
            pmodel_present += 1
            p_model = f(r[col_prob])
            if math.isfinite(p_model) and 0.0 <= p_model <= 1.0:
                pmodel_valid += 1
                pmodel_list.append(p_model)
                diffs.append(p_model - breakeven)

    n = len(prices)
    lines.append(f"### Basic Stats")
    lines.append(f"- Valid price rows: **{n}**")
    if n:
        lines.append(f"- Mean price: **{stats.mean(prices):.3f}**  |  Mean breakeven (1/price): **{stats.mean(be_list):.3f}**")
    else:
        lines.append("- Mean price: -")

    lines.append(f"- Prob column present on rows: **{pmodel_present}**")
    lines.append(f"- Prob values valid (0..1): **{pmodel_valid}**")
    if pmodel_valid:
        lines.append(f"- Mean p_model: **{stats.mean(pmodel_list):.3f}**")
        lines.append(f"- Mean (p_model - 1/price): **{stats.mean(diffs):.4f}**")
        lines.append(f"- Share with edge > 0: **{sum(1 for d in diffs if d > 0):d}/{len(diffs)}**")
    lines.append("")

    # Current engine filter is on edge_model = p_model - 1/price
    # If p_model missing, edge_model≈0 → filtered out
    if pmodel_valid:
        edge_thresholds = [0.00, 0.01, 0.02, 0.03]
        lines.append("### Picks forecast under engine edge filter (edge_model = p_model − 1/price)")
        for th in edge_thresholds:
            count = sum(1 for d in diffs if d >= th)
            mark = " (current)" if abs(th - args.min_edge) < 1e-9 else ""
            lines.append(f"- min_edge {th:.2f}: **{count}** rows{mark}")
        lines.append("")
    else:
        lines.append("> ⚠️ p_model is missing/invalid on most rows. Engine falls back to implied 1/price (edge≈0), hence 0 picks.")
        lines.append("")

    # Show effect of TE on Kelly (not a filter, but good to see signal)
    if pmodel_valid:
        te = args.edge
        boosted = [clip(pm * (1.0 + te), 0.0, 1.0) - be for pm, be in zip(pmodel_list, be_list)]
        lines.append(f"### TE(={te:.2f})-boosted margin (p_used − 1/price) — *Kelly sizing signal*")
        lines.append(f"- Mean boosted margin: **{stats.mean(boosted):.4f}**")
        lines.append(f"- Share boosted > 0: **{sum(1 for b in boosted if b > 0)}/{len(boosted)}**")
        lines.append("")

    # Recommendations
    lines.append("### Recommendation")
    if not pmodel_valid:
        lines.append("- Ensure `compute_prob_vigfree.py` writes a **p_model** column for each row.")
        lines.append("- For a quick test, run with `gamma=1.10–1.12` to move p_model off the breakeven line.")
        lines.append("- Temporarily set `min_edge=0.00` to confirm picks flow; tighten later.")
    else:
        lines.append(f"- With current `min_edge={args.min_edge:.2f}`, expected picks ≈ **{sum(1 for d in diffs if d >= args.min_edge)}**.")
        lines.append("- If that’s still 0, lower `min_edge` slightly or increase `gamma` a touch (1.06–1.10).")

    report = "\n".join(lines) + "\n"
    out_md.write_text(report, encoding="utf-8")
    print(report)

if __name__ == "__main__":
    main()
