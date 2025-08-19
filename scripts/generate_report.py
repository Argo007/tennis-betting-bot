import json, os, io, base64, shutil
from datetime import datetime
import pandas as pd

SITE_DIR = "site"
OUT_HTML = os.path.join(SITE_DIR, "index.html")

def _load_json(p):
    if os.path.exists(p):
        try: return json.load(open(p))
        except Exception: return {}
    return {}

def _load_csv(p):
    if os.path.exists(p):
        try: return pd.read_csv(p)
        except Exception: return pd.DataFrame()
    return pd.DataFrame()

def _b64_equity(df):
    cols = {c.lower(): c for c in df.columns}
    if "stake" not in cols or "return" not in cols: return None
    try:
        import matplotlib; matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        stake = pd.to_numeric(df[cols["stake"]], errors="coerce").fillna(0)
        ret   = pd.to_numeric(df[cols["return"]], errors="coerce").fillna(0)
        eq = (ret - stake).cumsum()
        fig, ax = plt.subplots(figsize=(8,3))
        ax.plot(eq.index, eq.values); ax.set_title("Equity Curve (units)")
        ax.set_xlabel("Bet #"); ax.set_ylabel("Cumulative P&L"); fig.tight_layout()
        import io, base64
        buf = io.BytesIO(); fig.savefig(buf, format="png", dpi=150); plt.close(fig)
        return "data:image/png;base64,"+base64.b64encode(buf.getvalue()).decode("ascii")
    except Exception:
        return None

def _fmt_pct(x):
    try: return f"{float(x)*100:.2f}%"
    except Exception: return "—"

def _fmt(x):
    try:
        if isinstance(x,float): return f"{x:.4f}"
        return str(x)
    except Exception: return "—"

def main():
    os.makedirs(SITE_DIR, exist_ok=True)
    metrics = _load_json("backtest_metrics.json")
    results = _load_csv("results.csv")
    matrix  = _load_csv("matrix_rankings.csv")

    # metrics table (best config summary)
    rows=[]
    for label, key, pct in [
        ("Bets","n_bets",False),
        ("Hit-rate","hit_rate",True),
        ("ROI","roi",True),
        ("Max Drawdown (units)","max_drawdown",False),
    ]:
        v = metrics.get(key)
        if v is not None:
            rows.append(f"<tr><td>{label}</td><td>{_fmt_pct(v) if pct else _fmt(v)}</td></tr>")
    if rows:
        metrics_html = "<table><tr><th>Metric</th><th>Value</th></tr>" + "".join(rows) + "</table>"
    else:
        metrics_html = "<p><em>No metrics available.</em></p>"

    # matrix table (if present)
    matrix_html = "<p><em>No matrix results.</em></p>"
    if not matrix.empty:
        show = matrix[["config","n_bets","hit_rate","roi","max_drawdown","final_bankroll","band_low","band_high","strategy","stake_mode"]]
        matrix_html = show.to_html(index=False, border=1)

    # results preview + equity
    preview_html = "<p><em>No results.csv found.</em></p>"
    if not results.empty:
        preview_html = f"<details open><summary><strong>Results preview</strong></summary>{results.head(25).to_html(index=False, border=0)}</details>"
    chart_b64 = _b64_equity(results)
    chart_html = f'<img alt="Equity" src="{chart_b64}"/>' if chart_b64 else "<p><em>No equity curve available.</em></p>"

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    html = f"""<!doctype html><html><head><meta charset="utf-8"/>
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
<div class="section"><h2>Key Metrics (best config)</h2>{metrics_html}</div>
<div class="section"><h2>Config Matrix</h2>{matrix_html}</div>
<div class="section"><h2>Equity Curve</h2>{chart_html}</div>
<div class="section"><h2>Results</h2>{preview_html}</div>
<p><small>Artifacts: summary.md, results.csv, backtest_metrics.json, matrix_rankings.csv.</small></p>
</body></html>"""
    with open(OUT_HTML,"w",encoding="utf-8") as f: f.write(html)

    # copy artifacts to site/ for browsing
    for p in ("summary.md","results.csv","backtest_metrics.json","matrix_rankings.csv"):
        if os.path.exists(p): shutil.copy2(p, os.path.join(SITE_DIR, p))

    print(f"Wrote {OUT_HTML} and copied artifacts to {SITE_DIR}/")

if __name__ == "__main__":
    main()
