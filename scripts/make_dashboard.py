#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Build a richer dashboard at docs/index.html showing:
- Equity curve (persistent bankroll_history.csv)
- CLV histogram for settled trades (from state/trade_log.csv)
- KPIs: last-24h PnL, avg CLV, #open/#settled, total stakes
- Recent settled trades table (PnL, CLV)
- Open trades table (stake, odds, p)

Usage:
  python scripts/make_dashboard.py --state-dir state --results results --live live_results --out docs
"""

import argparse, os, json
from datetime import datetime, timezone, timedelta
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--state-dir", default="state")
ap.add_argument("--results", default="results")
ap.add_argument("--live", default="live_results")
ap.add_argument("--out", default="docs")
ap.add_argument("--max-rows", type=int, default=20)
args = ap.parse_args()

os.makedirs(args.out, exist_ok=True)

# ---------- Load sources ----------
state_dir = args.state_dir
hist_path = os.path.join(state_dir, "bankroll_history.csv")
log_path  = os.path.join(state_dir, "trade_log.csv")

def read_csv(path, cols=None):
    if not os.path.isfile(path):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
        return df if not df.empty else pd.DataFrame(columns=cols or [])
    except Exception:
        return pd.DataFrame(columns=cols or [])

hist = read_csv(hist_path, cols=["ts","bankroll"])
log  = read_csv(log_path)

# Also show latest picks for context (optional)
pf = read_csv(os.path.join(args.results, "picks_final.csv"))
pl = read_csv(os.path.join(args.live, "picks_live.csv"))

# ---------- Transformations ----------
def to_iso(ts):
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except Exception:
        return ""

# Equity data
equity_ts = hist["ts"].astype(int).tolist()
equity_bn = [float(x) for x in hist["bankroll"].tolist()]

# Trade log enrich
settled = pd.DataFrame()
open_tr = pd.DataFrame()
kpis = {
    "n_open": 0,
    "n_settled": 0,
    "pnl_24h": 0.0,
    "avg_clv": 0.0,
    "sum_stake": 0.0,
}

if not log.empty:
    # Normalize types
    for c in ("odds","p","edge","stake_eur","pnl","clv","close_odds","bankroll_snapshot"):
        if c in log.columns:
            log[c] = pd.to_numeric(log[c], errors="coerce")

    if "status" in log.columns:
        open_tr = log[log["status"].str.lower().eq("open")].copy()
        settled = log[log["status"].str.lower().eq("settled")].copy()
    else:
        open_tr = log.iloc[0:0].copy()
        settled = log.copy()

    # KPIs
    kpis["n_open"] = int(len(open_tr))
    kpis["n_settled"] = int(len(settled))
    if "stake_eur" in log.columns:
        kpis["sum_stake"] = float(log["stake_eur"].fillna(0).sum())

    if not settled.empty:
        if "clv" in settled.columns:
            clv_valid = settled["clv"].dropna()
            kpis["avg_clv"] = float(clv_valid.mean()) if len(clv_valid) else 0.0

        # last 24h pnl
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        day_ago = now_ts - 24*3600
        if "settled_ts" in settled.columns:
            last = settled[settled["settled_ts"].fillna(0).astype(int) >= day_ago]
            kpis["pnl_24h"] = float(last["pnl"].fillna(0).sum())

# Picks preview tables (keep narrow)
def table_html(df, title, max_rows=args.max_rows, cols=None):
    if df.empty:
        return f"<h3>{title}</h3><p class='muted'>No data.</p>"
    use = df.copy()
    if cols:
        keep = [c for c in cols if c in use.columns]
        if keep:
            use = use[keep]
    use = use.head(max_rows)
    # nicer numbers
    for c in ("odds","p","edge","stake_eur","pnl","clv","close_odds"):
        if c in use.columns:
            if c in ("p","edge"):
                use[c] = (use[c].astype(float)*100).round(2)
            else:
                use[c] = use[c].astype(float).round(3)
    if "ts" in use.columns:
        use["ts"] = use["ts"].apply(to_iso)
    if "settled_ts" in use.columns:
        use["settled_ts"] = use["settled_ts"].apply(lambda x: to_iso(x) if str(x).isdigit() else "")
    html = use.to_html(index=False, escape=False)
    return f"<h3>{title}</h3>{html}"

# recent settled (most recent first)
recent_cols = ["ts","match_id","selection","odds","p","edge","stake_eur","close_odds","clv","pnl","settled_ts","bankroll_snapshot"]
recent_settled = settled.sort_values("settled_ts", ascending=False) if not settled.empty else settled

# open trades table
open_cols = ["ts","match_id","selection","odds","p","edge","stake_eur","bankroll_snapshot"]
recent_open = open_tr.sort_values("ts", ascending=False) if not open_tr.empty else open_tr

# picks (light)
hist_cols = ["match_id","player_a","player_b","odds","p","edge"]
live_cols = ["match_id","sel","odds","p","edge"]

# ---------- HTML/JS ----------
def fmt_currency(x): return f"{x:,.2f}"

kpi_html = f"""
<div class="kpi-grid">
  <div class="kpi"><div class="kpi-label">Open Trades</div><div class="kpi-value">{kpis['n_open']}</div></div>
  <div class="kpi"><div class="kpi-label">Settled Trades</div><div class="kpi-value">{kpis['n_settled']}</div></div>
  <div class="kpi"><div class="kpi-label">Last 24h PnL</div><div class="kpi-value">{fmt_currency(kpis['pnl_24h'])}</div></div>
  <div class="kpi"><div class="kpi-label">Avg CLV</div><div class="kpi-value">{kpis['avg_clv']:.4f}</div></div>
  <div class="kpi"><div class="kpi-label">Total Staked</div><div class="kpi-value">{fmt_currency(kpis['sum_stake'])}</div></div>
</div>
"""

html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>Tennis Value Engine Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <style>
    :root {{ --fg:#111; --muted:#666; --line:#e8e8e8; --good:#2b8a3e; --bad:#c92a2a; }}
    body {{ font-family: system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif; color:var(--fg); margin: 24px; }}
    h1 {{ margin: 0 0 4px 0; }}
    .muted {{ color:var(--muted); font-size: 0.92em; }}
    .kpi-grid {{ display:grid; grid-template-columns: repeat(auto-fit,minmax(160px,1fr)); gap:12px; margin:16px 0 10px; }}
    .kpi {{ border:1px solid var(--line); border-radius:8px; padding:10px 12px; background:#fff; }}
    .kpi-label {{ color:var(--muted); font-size:12px; }}
    .kpi-value {{ font-size:20px; font-weight:600; margin-top:4px; }}
    .grid {{ display:grid; grid-template-columns:1fr; gap:24px; }}
    @media(min-width:980px){{ .grid {{ grid-template-columns:1.2fr 1fr; }} }}
    .card {{ border:1px solid var(--line); border-radius:8px; padding:12px; background:#fff; }}
    canvas {{ width:100%; height:300px; display:block; }}
    table {{ border-collapse:collapse; width:100%; font-size:14px; }}
    th, td {{ border:1px solid var(--line); padding:6px 8px; text-align:left; }}
    th {{ background:#fafafa; }}
  </style>
</head>
<body>
  <h1>Tennis Value Engine</h1>
  <div class="muted">Updated: {datetime.utcnow().isoformat()}Z</div>

  {kpi_html}

  <div class="grid">
    <div class="card">
      <h3>Equity Curve</h3>
      <canvas id="equity"></canvas>
    </div>
    <div class="card">
      <h3>CLV Histogram (settled)</h3>
      <canvas id="clv"></canvas>
    </div>
  </div>

  <div class="grid" style="margin-top:24px;">
    <div class="card">
      {table_html(recent_settled, "Recent Settled Trades", cols={recent_cols})}
    </div>
    <div class="card">
      {table_html(recent_open, "Open Trades", cols={open_cols})}
    </div>
  </div>

  <div class="grid" style="margin-top:24px;">
    <div class="card">
      {table_html(pf, "Historical Picks (latest)", cols={hist_cols})}
    </div>
    <div class="card">
      {table_html(pl, "Live Picks (latest)", cols={live_cols})}
    </div>
  </div>

  <script>
    // Data blobs embedded
    const equity_ts = {json.dumps(equity_ts)};
    const equity_bn = {json.dumps(equity_bn)};
    const clv_vals = {json.dumps([float(x) for x in settled.get("clv",[]).dropna().tolist()])};

    // --- Drawing utils (no external libs) ---
    function lineChart(canvasId, xs, ys, color) {{
      const c = document.getElementById(canvasId), ctx = c.getContext('2d');
      // Size for device pixel ratio
      const dpr = window.devicePixelRatio || 1;
      c.width = c.clientWidth * dpr;
      c.height = c.clientHeight * dpr;
      ctx.scale(dpr, dpr);
      ctx.clearRect(0,0,c.clientWidth,c.clientHeight);

      if (!xs.length || !ys.length) {{
        ctx.fillStyle = '#666'; ctx.fillText('No data', 10, 20); return;
      }}

      const W = c.clientWidth, H = c.clientHeight, pad=24;
      const xmin = 0, xmax = xs.length-1;
      const ymin = Math.min(...ys), ymax = Math.max(...ys);
      const sx = i => pad + (W-2*pad) * (i - xmin) / Math.max(1, (xmax - xmin));
      const sy = v => H - pad - (H-2*pad) * (v - ymin) / Math.max(1e-9, (ymax - ymin));

      ctx.beginPath();
      for (let i=0; i<ys.length; i++) {{ const x=sx(i), y=sy(ys[i]); i?ctx.lineTo(x,y):ctx.moveTo(x,y); }}
      ctx.lineWidth = 2; ctx.strokeStyle = color || '#2b8a3e'; ctx.stroke();

      // draw axis
      ctx.strokeStyle = '#e8e8e8'; ctx.lineWidth = 1;
      ctx.beginPath(); ctx.moveTo(pad, pad); ctx.lineTo(pad, H-pad); ctx.lineTo(W-pad, H-pad); ctx.stroke();
    }}

    function histChart(canvasId, values, bins=20) {{
      const c = document.getElementById(canvasId), ctx = c.getContext('2d');
      const dpr = window.devicePixelRatio || 1;
      c.width = c.clientWidth * dpr; c.height = c.clientHeight * dpr; ctx.scale(dpr, dpr);
      ctx.clearRect(0,0,c.clientWidth,c.clientHeight);
      if (!values.length) {{ ctx.fillStyle='#666'; ctx.fillText('No settled trades yet',10,20); return; }}
      const W=c.clientWidth, H=c.clientHeight, pad=24;
      const minV = Math.min(...values), maxV = Math.max(...values);
      const width = (W-2*pad)/bins;
      // bin counts
      const counts = new Array(bins).fill(0);
      for (const v of values) {{
        let idx = Math.floor((v-minV)/(maxV-minV+1e-9)*bins);
        if (idx<0) idx=0; if (idx>=bins) idx=bins-1;
        counts[idx] += 1;
      }}
      const maxC = Math.max(...counts);
      // axis
      ctx.strokeStyle = '#e8e8e8'; ctx.lineWidth=1;
      ctx.beginPath(); ctx.moveTo(pad, pad); ctx.lineTo(pad, H-pad); ctx.lineTo(W-pad, H-pad); ctx.stroke();
      // bars
      for (let i=0;i<bins;i++) {{
        const h = (H-2*pad) * (counts[i]/Math.max(1,maxC));
        const x = pad + i*width;
        const y = H - pad - h;
        ctx.fillStyle = i < bins/2 ? '#c92a2a' : '#2b8a3e';
        ctx.fillRect(x+1, y, Math.max(1, width-2), h);
      }}
    }}

    function redraw() {{
      lineChart('equity', equity_ts, equity_bn, '#2b8a3e');
      histChart('clv', clv_vals, 24);
    }}
    window.addEventListener('resize', redraw);
    redraw();
  </script>
</body>
</html>
"""

out_path = os.path.join(args.out, "index.html")
with open(out_path, "w", encoding="utf-8") as f:
    f.write(html)

print("Dashboard written ->", out_path)
print("Equity points:", len(equity_bn), "| Settled trades:", len(settled))
