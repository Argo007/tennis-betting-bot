# scripts/generate_report.py
import json, os, io, base64, shutil
from datetime import datetime
import pandas as pd

METRICS_PATH = "backtest_metrics.json"
RESULTS_PATH = "results.csv"
SITE_DIR = "site"
OUT_HTML = os.path.join(SITE_DIR, "index.html")

def load_metrics():
    if os.path.exists(METRICS_PATH):
        try:
            with open(METRICS_PATH,"r") as f:
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
    try: return f"{float(x)*100:.2f}%"
    except Exception: return "—"

def safe_fmt(x):
    try:
        if isinstance(x,float): return f"{x:.4f}"
        return str(x)
    except Exception: return "—"

def make_equity_png_b64(df: pd.DataFrame):
    needed = {"stake","return"}
    cols_lower = {c.lower(): c for c in df.columns}
    if not needed.issubset(cols_lower): return None
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        stake = pd.to_numeric(df[cols_lower["stake"]], errors="coerce").fillna(0)
        ret   = pd.to_numeric(df[cols_lower["return"]], errors="coerce").fillna(0)
        equity = (ret - stake).cumsum()
        fig, ax = plt.subplots(figsize=(8,3))
        ax.plot(equity.index, equity.values)
        ax.set_title("Equity Curve (units)")
        ax.set_xlabel("Bet #"); ax.set_ylabel("Cumulative P&L")
        fig.tight_layout()
        buf = io.BytesIO(); fig.savefig(buf, format="png", dpi=150); plt.close(fig)
        return "data:image/png;base64,"+base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception:
        return None

def infer_extras(df: pd.DataFrame):
    extras = {}
    if df.empty: return extras
    if "won" in df.columns:
        try: extras["hit_rate_inferred"] = float(pd.to_numeric(df["won"], errors="coerce").fillna(0).mean())
        except Exception: pass
    cols = {c.lower(): c for c in df.columns}
    if "stake" in cols and "return" in cols:
        try:
            stake = pd.to_numeric(df[cols["stake"]], errors="coerce").fillna(0).sum()
            ret   = pd.to_numeric(df[cols["return"]], errors="coerce").fillna(0).sum()
            if stake: extras["roi_inferred"] = (ret - stake) / stake
        except Exception: pass
    extras["n_rows"] = int(len(df))
    return extras

def main():
    os.makedirs(SITE_DIR, exist_ok=True)

    metrics = load_metrics()
    df = load_results()
    extras = infer_extras(df)
    img_b64 = make_equity_png_b64(df)

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    rows=[]
    def add_row(k,v,is_pct=False):
        if v is None: return
        rows.append(f"<tr><td>{k}</td><td>{safe_fmt_pct(v) if is_pct else safe_fmt(v)}</td></tr>")
    add_row("Bets", metrics.get("n_bets"))
    add_row("Hit-rate", metrics.get("hit_rate"), True)
    add_row("ROI", metrics.get("roi"), True)
    add_row("Max Drawdown (units)", metrics.get("max_drawdown"))
    if "hit_rate_inferred" in extras and "hit_rate" not in metrics:
        add_row("Hit-rate (inferred)", extras["hit_rate_inferred"], True)
    if "roi_inferred" in extras and "roi" not in metrics:
        add_row("ROI (inferred)", extras["roi_inferred"], True)
    add_row("Rows in results.csv", extras.get("n_rows"))

    metrics_table = "<p><em>No metrics available.</em></p>" if not rows \
        else "<table>\n<tr><th>Metric</th><th>Value</th></tr>\n"+"\n".join(rows)+"\n</table>"

    preview_html = "<p><em>No results.csv found.</em></p>"
    if not df.empty:
        preview_html = f"<details open><summary><strong>Results preview</strong></summary>{df.head(25).to_html(index=False, border=0)}</details>"
    chart_html = f'<img alt="Equity curve" src="{img_b64}" />' if img_b64 else "<p><em>No equity curve available.</em></p>"

    html = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"/>
<title>Courtsense Backtest Report</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
body{{font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;margin:24px}}
h1{{margin-bottom:4px}} .sub{{color:#666;margin-top:0}}
table{{border-collapse:collapse;margin-top:12px}} th,td{{border:1px solid #ddd;padding:6px 10px}}
th{{background:#fafafa;text-align:left}} .section{{margin:24px 0}}
details summary{{cursor:pointer;margin:12px 0}}
</style></head><body>
<h1>Courtsense Backtest Report</h1>
<p class="sub">Generated {now}</p>
<div class="section"><h2>Key Metrics</h2>{metrics_table}</div>
<div class="section"><h2>Equity Curve</h2>{chart_html}</div>
<div class="section"><h2>Results</h2>{preview_html}</div>
<p><small>This page is auto-published by GitHub Actions. Artifacts also include summary.md, results.csv, and backtest_metrics.json.</small></p>
</body></html>"""
    with open(OUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)

    # Copy raw files into site/ for convenience
    for p in ("summary.md", "results.csv", "backtest_metrics.json"):
        if os.path.exists(p):
            shutil.copy2(p, os.path.join(SITE_DIR, p))

    print(f"Wrote {OUT_HTML} and copied any available artifacts into {SITE_DIR}/")

if __name__ == "__main__":
    main()

