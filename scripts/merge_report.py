#!/usr/bin/env python3
"""
Merge engine summary + matrix backtest into one Markdown:
- Engine summary (verbatim if present)
- Best-by-ROI snapshot from backtest_metrics.json
- ROI by band (from matrix_rankings.csv)
- Bankroll curve (sampled) + max drawdown (from results.csv)

Robust to missing files/columns.
"""
from __future__ import annotations
import argparse, csv, json, math, pathlib, statistics as stats

def read_text(path: str) -> str | None:
    p = pathlib.Path(path)
    if p.exists() and p.stat().st_size > 0:
        return p.read_text(encoding="utf-8")
    return None

def read_json(path: str) -> dict:
    p = pathlib.Path(path)
    if p.exists() and p.stat().st_size > 0:
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def read_csv(path: str) -> list[dict]:
    p = pathlib.Path(path)
    if p.exists() and p.stat().st_size > 0:
        with p.open(newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    return []

def fmt(x):
    if x is None: return "-"
    if isinstance(x, (int, float)):
        if math.isnan(x): return "-"
        # keep more precision for bankroll numbers
        return f"{x:.4f}" if abs(x) < 1000 else f"{x:.2f}"
    return str(x)

def md_table(headers, rows):
    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("|" + "|".join(["---"]*len(headers)) + "|")
    for r in rows:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out)

def bankroll_stats(rows: list[dict]):
    """Return sampled curve rows + final BR + max drawdown."""
    if not rows: 
        return [], None, None
    # column fallback
    br_after_col = "bankroll_after"
    if br_after_col not in rows[0]:
        # legacy name fallback
        for k in rows[0].keys():
            if k.lower().startswith("bankroll") and "after" in k.lower():
                br_after_col = k
                break
    # in case there is an index/step
    idx_col = "row_idx" if "row_idx" in rows[0] else None
    curve = []
    for i, r in enumerate(rows):
        step = int(r.get(idx_col, i))
        try:
            br = float(r.get(br_after_col, "nan"))
        except Exception:
            br = float("nan")
        curve.append((step, br))
    # sort by step
    curve.sort(key=lambda x: x[0])
    # sample ~10 points
    n = len(curve)
    k = max(1, n // 10)
    sampled = [(s, b) for i,(s,b) in enumerate(curve) if i % k == 0]
    if sampled[-1][0] != curve[-1][0]:
        sampled.append(curve[-1])
    # max drawdown
    max_peak = -float("inf")
    max_dd = 0.0
    for _, br in curve:
        if not math.isnan(br):
            max_peak = max(max_peak, br) if max_peak != -float("inf") else br
            if max_peak != 0 and not math.isnan(max_peak):
                dd = (max_peak - br) / max_peak
                if dd > max_dd: max_dd = dd
    final_br = curve[-1][1] if curve else None
    return sampled, final_br, max_dd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine-md", default="outputs/engine_summary.md")
    ap.add_argument("--matrix-metrics", default="outputs/backtest_metrics.json")
    ap.add_argument("--matrix-rankings", default="outputs/matrix_rankings.csv")
    ap.add_argument("--matrix-results", default="outputs/results.csv")
    ap.add_argument("--out", default="outputs/pipeline_summary.md")
    args = ap.parse_args()

    parts = []
    parts.append("# Pipeline Summary\n")

    # 1) Engine summary
    eng = read_text(args.engine_md)
    if eng:
        parts.append(eng.strip())
    else:
        parts.append("_No engine summary produced._")

    # 2) Best by ROI
    parts.append("\n## Matrix Backtest — Best by ROI")
    m = read_json(args.matrix_metrics)
    b = (m or {}).get("best_by_roi") or {}
    if b:
        parts.append(f"- **Config:** {fmt(b.get('config_id'))}")
        parts.append(f"- **Band:** {fmt(b.get('label'))}")
        parts.append(f"- **Bets:** {fmt(b.get('bets'))} | **ROI:** {fmt(b.get('roi'))} | **PnL:** {fmt(b.get('pnl'))} | **End BR:** {fmt(b.get('end_bankroll'))}")
    else:
        parts.append("_No metrics available — no bets met criteria or file missing._")

    # 3) ROI by band (top rows)
    ranks = read_csv(args.matrix_rankings)
    parts.append("\n## ROI by Band")
    if ranks:
        # Normalize field names
        for r in ranks:
            r.setdefault("label", r.get("band", r.get("label")))
            r.setdefault("roi", r.get("ROI", r.get("roi")))
            r.setdefault("bets", r.get("Bets", r.get("bets")))
            r.setdefault("pnl", r.get("PnL", r.get("pnl")))
        # sort by ROI desc
        def roi_val(r):
            try: return float(r.get("roi", "nan"))
            except: return float("nan")
        ranks_sorted = sorted(ranks, key=roi_val, reverse=True)[:6]
        rows = []
        for r in ranks_sorted:
            rows.append([
                fmt(r.get("label")),
                fmt(r.get("roi")),
                fmt(r.get("bets")),
                fmt(r.get("pnl"))
            ])
        parts.append(md_table(["Band", "ROI", "Bets", "PnL"], rows))
    else:
        parts.append("_No matrix_rankings.csv found._")

    # 4) Bankroll curve + max drawdown
    parts.append("\n## Bankroll curve (sampled)")
    res = read_csv(args.matrix_results)
    if res:
        sampled, final_br, max_dd = bankroll_stats(res)
        if sampled:
            rows = [[str(s), fmt(br)] for s,br in sampled]
            parts.append(md_table(["Step", "Bankroll"], rows))
        parts.append(f"\n- **Final BR:** {fmt(final_br)}")
        parts.append(f"- **Max drawdown:** {fmt(max_dd)}")
    else:
        parts.append("_No results.csv found (no per-bet history available)._")

    outp = pathlib.Path(args.out)
    outp.write_text("\n".join(parts) + "\n", encoding="utf-8")

if __name__ == "__main__":
    main()
