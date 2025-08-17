# scripts/generate_report.py
"""
Generate a simple HTML report (index.html) from:
- backtest_metrics.json (optional)
- results.csv (optional)

Also writes an equity curve chart if stake/return columns exist.
Robust to missing files/columns.
"""

import json
import os
import io
import base64
from datetime import datetime

import pandas as pd

METRICS_PATH = "backtest_metrics.json"
RESULTS_PATH = "results.csv"
OUT_HTML = "index.html"

def load_metrics():
    if os.path.exists(METRICS_PATH):
        try:
            with open(METRICS_PATH, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def load_results():
    if os.path.exists(RESULTS_PATH):
        try:
            return pd.read_csv(RESULTS_PATH)
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()

def safe_fmt_pct(x):
    try:
        return f"{float(x)*100:.2f}%"
    except Exception:
        return "—"

def safe_fmt(x):
    try:
        if isinstance(x, float):
            return f"{x:.4f}"
        return str(x)
    except Exception:
        return "—"

def make_equity_png_b64(df: pd.DataFrame) -> str | None:
    """Returns a base64 PNG of equity curve if columns allow; else None."""
    needed = {"stake", "return"}
    if not needed.issubset(set(c.lower() for c in df.columns)):
        # Try case-insensitive mapping
        cols_lower = {c.lower(): c for c in df.columns}
        if not needed.issubset(cols_lower.keys()):
            return None
        stake_col = cols_lower["stake"]
        ret_col = cols_lower["return"]
    else:
        # exact match available
        stake_col = "stake"
        ret_col = "return"

    try:
        import matplotlib
        matplotlib.use("Agg")  # headless
        import matplotlib.pyplot as plt

        stake = df[stake_col].fillna(0).astype(float)
        ret = df[ret_col].fillna(0).astype(float)
        equity = (ret.cumsum() - stake.cumsum())

        fig, ax = plt.subplots(figsize=(8, 3))
        ax.plot(equity.index, equity.values)
        ax.set_title("Equity Curve (units)")
        ax.set_xlabel("Bet #")
        ax.set_ylabel("Cumulative P&L")
        fig.tight_layout()

        buf = io.BytesIO()
        fig.savefig(buf, format="png", dpi=150)
        plt.close(fig)
        b64 = base64.b64encode(buf.getvalue()).decode("ascii")
        return f"data:image/png;base64,{b64}"
    except Exception:
        return None

def infer_extras(df: pd.DataFrame) -> dict:
    extras = {}
    if df.empty:
        return extras

    # Try to infer hit-rate if a 'won' column exists (bool/int)
    for won_col in ("won", "win", "is_win"):
        if won_col in df.columns:
            try:
                hr = df[won_col].astype(float).mean()
                extras["hit_rate_inferred"] = hr
            except Exception:
                pass
            break

    # Try to infer ROI if stake/return exist
    cols = {c.lower(): c for c in df.columns}
    if {"stake", "return"}.issubset(cols.keys()):
        try:
            stake_sum = df[cols["stake"]].fillna(0).astype(float).sum()
            ret_sum = df[cols["return"]].fillna(0).astype(float).sum()
            if stake_sum != 0:
                extras["roi_inferred"] = (ret_sum - stake_sum) / stake_sum
        except Exception:
            pass

    # Basic counts
    extras["n_rows"] = int(len(df))
    return extras

def main():
    metrics = load_metrics()
    df = load_results()
    extras = infer_extras(df)
    img_b64 = make_equity_png_b64(df)

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    # Compose metrics table
    rows = []
    def add_row(k, v, is_pct=False):
        if v is None:
            return
        rows.append(f"<tr><td>{k}</td><td>{safe_fmt_pct(v) if is_pct else safe_fmt(v)}</td></tr>")

    add_row("Bets", metrics.get("n_bets"))
    add_row("Hit-rate", metrics.get("hit_rate"), is_pct=True)
    add_row("ROI", metrics.get("roi"), is_pct=True)
    add_row("Max Drawdown (units)", metrics.get("max_drawdown"))

    # If we inferred anything, include it (but label clearly)
    if "hit_rate_inferred" in extras and "hit_rate" not in metrics:
        add_row("Hit-rate (inferred)", extras["hit_rate_inferred"], is_pct=True)
    if "roi_inferred" in extras and "roi" not in metrics:
        add_row("ROI (inferred)", extras["roi_inferred"], is_pct=True)
    add_row("Rows in results.csv", extras.get("n_rows"))

    metrics_table = (
        "<table>\n<tr><th>Metric</th><th>Value</th></tr>\n" + "\n".join(rows) + "\n</table>"
        if rows else "<p><em>No metrics available.</em></p>"
    )

    # If results exist, show a small preview
    results_preview = "<p><em>No results.csv found.</em></p>"
    if not df.empty:
        preview_cols = list(df.columns)[:10]
        preview = df[preview_cols].head(25).to_html(index=False, border=0)
        results_preview = f"""
        <details open>
          <summary><strong>Results preview</strong></summary>
          {preview}
        </details>
        """

    chart_html = f'<img alt="Equity curve" src="{img_b64}" />' if img_b64 else "<p><em>No equity curve available.</em></p>"

    html = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<title>Courtsense Report</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
  body {{ font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }}
  h1 {{ margin-bottom: 4px; }}
  .sub {{ color: #666; margin-top: 0; }}
  table {{ border-collapse: collapse; margin-top: 12px; }}
  th, td {{ border: 1px solid #ddd; padding: 6px 10px; }}
  th {{ background: #fafafa; text-align: left; }}
  details summary {{ cursor: pointer; margin: 12px 0; }}
  .section {{ margin: 24px 0; }}
</style>
</head>
<body>
  <h1>Courtsense Backtest Report</h1>
  <p class="sub">Generated {now}</p>

  <div class="section">
    <h2>Key Metrics</h2>
    {metrics_table}
  </div>

  <div class="section">
    <h2>Equity Curve</h2>
    {chart_html}
  </div>

  <div class="section">
    <h2>Results</h2>
    {results_preview}
  </div>

  <div class="section">
    <p><small>Tip: artifacts also include <code>summary.md</code>, <code>results.csv</code>, and <code>backtest_metrics.json</code> if produced.</small></p>
  </div>
</body>
</html>
"""
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_HTML}")

if __name__ == "__main__":
    main()
