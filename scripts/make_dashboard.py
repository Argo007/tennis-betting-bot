#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build a minimal dashboard at docs/index.html showing bankroll history and recent picks.
Usage:
  python scripts/make_dashboard.py --state-dir state --results results --live live_results --out docs
"""
import argparse, os, json, pandas as pd
from datetime import datetime

ap = argparse.ArgumentParser()
ap.add_argument("--state-dir", default="state")
ap.add_argument("--results", default="results")
ap.add_argument("--live", default="live_results")
ap.add_argument("--out", default="docs")
args = ap.parse_args()

os.makedirs(args.out, exist_ok=True)

# Load bankroll history
hist_path = os.path.join(args.state_dir, "bankroll_history.csv")
hist = pd.read_csv(hist_path) if os.path.isfile(hist_path) else pd.DataFrame(columns=["ts","bankroll"])

# Load picks
pf = os.path.join(args.results, "picks_final.csv")
pl = os.path.join(args.live, "picks_live.csv")
hist_picks = pd.read_csv(pf) if os.path.isfile(pf) else pd.DataFrame()
live_picks = pd.read_csv(pl) if os.path.isfile(pl) else pd.DataFrame()

def table_html(df, title, max_rows=20):
    if df.empty:
        return f"<h3>{title}</h3><p>No data.</p>"
    df2 = df.copy().head(max_rows)
    # Show only common, readable cols
    cols = [c for c in ["match_id","player_a","player_b","sel","odds","p","edge"] if c in df2.columns]
    if "edge" in cols:
        df2["edge"] = (df2["edge"]*100).round(1)
    if "p" in cols:
        df2["p"] = (df2["p"]*100).round(1)
    html = df2[cols].to_html(index=False, escape=False)
    return f"<h3>{title}</h3>{html}"

# Prepare data for sparkline
spark = {
    "ts": hist["ts"].astype(int).tolist(),
    "bankroll": [float(x) for x in hist["bankroll"].tolist()]
}

html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Tennis Value Engine Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 20px; }}
    .spark {{ height: 140px; width: 100%; border: 1px solid #ddd; }}
    .grid {{ display: grid; grid-template-columns: 1fr; gap: 24px; }}
    @media (min-width: 900px) {{ .grid {{ grid-template-columns: 1fr 1fr; }} }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #eee; padding: 6px 8px; text-align: left; }}
    th {{ background: #fafafa; }}
    .muted {{ color:#666; font-size: 0.9em; }}
  </style>
</head>
<body>
  <h1>Tennis Value Engine</h1>
  <div class="muted">Last updated: {datetime.utcnow().isoformat()}Z</div>

  <h2>Bankroll</h2>
  <canvas id="spark" class="spark"></canvas>

  <div class="grid">
    <div>{table_html(hist_picks, "Historical Picks (latest)")}</div>
    <div>{table_html(live_picks, "Live Picks (latest)")}</div>
  </div>

  <script>
    const data = {json.dumps(spark)};
    const canvas = document.getElementById('spark');
    const ctx = canvas.getContext('2d');

    function drawSpark(values) {{
      ctx.clearRect(0,0,canvas.width,canvas.height);
      const W = canvas.width, H = canvas.height;
      const pad = 10;
      const xs = values.map((v,i)=>pad + i*(W-2*pad)/Math.max(1,values.length-1));
      const minV = Math.min(...values, 0);
      const maxV = Math.max(...values, 1);
      const ys = values.map(v => H - pad - (v - minV) * (H-2*pad) / Math.max(1e-9, (maxV - minV)));
      ctx.beginPath();
      for (let i=0;i<xs.length;i++) {{
        if (i===0) ctx.moveTo(xs[i], ys[i]); else ctx.lineTo(xs[i], ys[i]);
      }}
      ctx.lineWidth = 2;
      ctx.strokeStyle = '#2b8a3e';
      ctx.stroke();
    }}

    function resize() {{
      canvas.width = canvas.clientWidth;
      canvas.height = canvas.clientHeight;
      drawSpark(data.bankroll);
    }}
    window.addEventListener('resize', resize);
    resize();
  </script>
</body>
</html>"""

out_path = os.path.join(args.out, "index.html")
with open(out_path, "w", encoding="utf-8") as f:
    f.write(html)
print("Wrote dashboard ->", out_path)
