#!/usr/bin/env python3
import argparse, os, json, pandas as pd
from datetime import datetime, timezone

def eur(x): 
    try: return f"€{float(x):,.2f}"
    except: return x

def pct(x):
    try: return f"{float(x)*100:.1f}%"
    except: return x

def read_csv(path, **kw):
    if not os.path.exists(path): return pd.DataFrame()
    try: 
        return pd.read_csv(path, **kw)
    except Exception:
        return pd.DataFrame()

def card(label, value):
    return f"""<div class="card"><div class="label">{label}</div><div class="value">{value}</div></div>"""

def table_html(df, max_rows=30):
    if df.empty: 
        return "<p><i>No data</i></p>"
    shown = df.head(max_rows).copy()
    return shown.to_html(index=False, classes="tbl", border=0, justify="center")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--live-dir", required=True)
    ap.add_argument("--results-dir", required=True)
    ap.add_argument("--min-edge", required=True)
    ap.add_argument("--kelly", required=True)
    ap.add_argument("--max-frac", required=True)
    ap.add_argument("--abs-cap", required=True)
    args = ap.parse_args()

    # state
    bankroll_json = os.path.join(args.state_dir, "bankroll.json")
    bankroll = 1000.0
    settled = 0
    try:
        with open(bankroll_json, "r") as f:
            data = json.load(f)
            bankroll = float(data.get("bankroll", bankroll))
            settled = int(data.get("settled_count", 0))
    except Exception:
        pass

    # derived metrics from trade_log & close_odds if available
    trades = read_csv(os.path.join(args.state_dir, "trade_log.csv"))
    # normalise columns
    for col in ("edge","p","clv"):
        if col in trades.columns:
            try: trades[col] = pd.to_numeric(trades[col], errors="coerce")
            except: pass
    if "pnl" in trades.columns:
        try: trades["pnl"] = pd.to_numeric(trades["pnl"], errors="coerce")
        except: pass

    last24 = trades.tail(25)  # proxy for "recent"
    avg_clv = last24["clv"].mean() if "clv" in last24.columns and not last24.empty else 0.0

    # sections
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    cards = "".join([
        card("Bankroll", eur(bankroll)),
        card("Settled (count)", f"{settled}"),
        card("Open (count)", f"{0}"),
        card("Avg CLV", f"{avg_clv:.4f}"),
    ])

    # recent settled trades (if present)
    cols = [c for c in ["match_id","selection","odds","p","edge","stake_eur","close_odds","clv","pnl","settled_ts"] if c in trades.columns]
    recent_tbl = table_html(trades[cols].sort_values("settled_ts", ascending=False)) if cols else "<p><i>No data</i></p>"

    # live picks table (top)
    live_picks = read_csv(os.path.join(args.live_dir, "picks_live.csv"))
    cols_live = [c for c in ["match_id","sel","odds","p","edge"] if c in live_picks.columns]
    live_tbl = table_html(live_picks[cols_live]) if cols_live else "<p><i>No data</i></p>"

    # historical picks (your sample)
    historical = read_csv(os.path.join(args.results_dir, "picks_final.csv"))
    cols_hist = [c for c in ["match_id","player_a","player_b","odds","p","edge"] if c in historical.columns]
    hist_tbl = table_html(historical[cols_hist]) if cols_hist else "<p><i>No data</i></p>"

    html = f"""
<style>
  .wrap {{ font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; }}
  h1 {{ margin-bottom: 4px; }}
  .meta {{ color:#666; margin-bottom: 14px }}
  .cards {{ display:flex; flex-wrap:wrap; gap:10px; margin:12px 0 20px }}
  .card {{ background:#0d1117; color:#e6edf3; border:1px solid #30363d; padding:10px 14px; border-radius:10px; min-width:140px }}
  .card .label {{ font-size:12px; opacity:.8 }}
  .card .value {{ font-size:18px; font-weight:700 }}
  h2 {{ margin:18px 0 8px }}
  .tbl {{ border-collapse: collapse; width: 100%; }}
  .tbl th, .tbl td {{ padding: 6px 10px; border-bottom: 1px solid #e5e7eb; }}
  .tbl tr:nth-child(even) {{ background: #f9fafb; }}
  .section {{ margin-bottom: 22px }}
  .pill {{ display:inline-block; font-size:12px; border:1px solid #e5e7eb; border-radius:999px; padding:2px 8px; margin-left:6px; color:#374151 }}
</style>
<div class="wrap">
  <h1>Tennis Engine — Summary</h1>
  <div class="meta">Generated: {now}
    <span class="pill">min_edge {args.min_edge}</span>
    <span class="pill">kelly {args.kelly}</span>
    <span class="pill">max_frac {args.max_frac}</span>
    <span class="pill">cap €{args.abs_cap}</span>
  </div>

  <div class="cards">{cards}</div>

  <div class="section">
    <h2>Recent Settled Trades</h2>
    {recent_tbl}
  </div>

  <div class="section">
    <h2>Top Live Picks</h2>
    {live_tbl}
  </div>

  <div class="section">
    <h2>Historical Picks (latest)</h2>
    {hist_tbl}
  </div>
</div>
"""
    # Write to GitHub Actions Job Summary
    try:
        from pathlib import Path
        summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary:
            Path(summary).write_text(html, encoding="utf-8")
        else:
            # Local run fallback
            out = os.path.join(args.results_dir, "dashboard.html")
            os.makedirs(args.results_dir, exist_ok=True)
            with open(out, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"Wrote {out}")
    except Exception as e:
        print("Could not write job summary:", e)

if __name__ == "__main__":
    main()
