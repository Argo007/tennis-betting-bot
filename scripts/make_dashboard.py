#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build a minimal dashboard at docs/index.html showing:
- Bankroll snapshot/history (if present)
- Recent settled trades (from state/trade_log.csv)
- Current top live picks (from live_results/picks_live.csv)
- Latest historical picks (from results/picks_final.csv)

This script is intentionally defensive: missing/empty inputs won't crash it.
"""

import argparse
import os
import pandas as pd
from datetime import datetime, timezone

def read_csv_safe(path, parse_ts_cols=None):
    if not os.path.isfile(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        if parse_ts_cols:
            for c in parse_ts_cols:
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors="coerce", utc=True)
        return df
    except Exception:
        return pd.DataFrame()

def df_preview(df, cols=None, n=10, floatfmt=3):
    if df.empty:
        return "<em>No data</em>"
    if cols:
        existing = [c for c in cols if c in df.columns]
        if existing:
            df = df[existing]
    # nice rounding for numeric display
    for c in df.columns:
        if pd.api.types.is_float_dtype(df[c]):
            df[c] = df[c].round(floatfmt)
        if pd.api.types.is_datetime64_any_dtype(df[c]):
            # show in UTC human-readable
            df[c] = df[c].dt.strftime("%Y-%m-%d %H:%M")
    return df.head(n).to_html(index=False, escape=False)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-dir", default="state")
    ap.add_argument("--results", default="results")
    ap.add_argument("--live", default="live_results")
    ap.add_argument("--out", default="docs")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    # Load sources (all optional)
    hist = read_csv_safe(os.path.join(args.state_dir, "bankroll_history.csv"),
                         parse_ts_cols=["ts"])
    trades = read_csv_safe(os.path.join(args.state_dir, "trade_log.csv"),
                           parse_ts_cols=["ts", "settled_ts"])
    live = read_csv_safe(os.path.join(args.live, "picks_live.csv"))
    hist_picks = read_csv_safe(os.path.join(args.results, "picks_final.csv"))

    # Compute KPIs
    bankroll = None
    settled_cnt = 0
    open_cnt = 0
    pnl_24h = None
    avg_clv = None

    if not hist.empty:
        # last bankroll
        last = hist.sort_values("ts").tail(1)
        if "bankroll" in hist.columns and not last.empty:
            try:
                bankroll = float(last["bankroll"].iloc[0])
            except Exception:
                bankroll = None

    if not trades.empty:
        # settled vs open (settled_ts present)
        settled = trades[trades.get("settled_ts").notna()] if "settled_ts" in trades.columns else pd.DataFrame()
        open_df = trades[trades.get("settled_ts").isna()] if "settled_ts" in trades.columns else pd.DataFrame()
        settled_cnt = len(settled)
        open_cnt = len(open_df)

        # 24h PnL if column exists
        if "pnl" in trades.columns and "settled_ts" in trades.columns:
            now = datetime.now(timezone.utc)
            recent = trades[(trades["settled_ts"].notna()) &
                            (trades["settled_ts"] >= (now - pd.Timedelta(hours=24)))]
            if not recent.empty:
                try:
                    pnl_24h = float(recent["pnl"].sum())
                except Exception:
                    pnl_24h = None

        # average CLV if present
        if "clv" in trades.columns:
            try:
                avg_clv = float(pd.to_numeric(trades["clv"], errors="coerce").dropna().mean())
            except Exception:
                avg_clv = None

    # Compose HTML
    gen_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    kpis = []
    if bankroll is not None:
        kpis.append(f"<li><strong>Bankroll:</strong> €{bankroll:,.2f}</li>")
    kpis.append(f"<li><strong>Settled (count):</strong> {settled_cnt}</li>")
    kpis.append(f"<li><strong>Open (count):</strong> {open_cnt}</li>")
    if pnl_24h is not None:
        kpis.append(f"<li><strong>PnL (last 24h):</strong> €{pnl_24h:,.2f}</li>")
    if avg_clv is not None:
        kpis.append(f"<li><strong>Avg CLV:</strong> {avg_clv:.4f}</li>")

    # Tables
    recent_cols = ["ts","match_id","selection","odds","p","edge","stake_eur","close_odds","clv","pnl","settled_ts"]
    live_cols = ["match_id","sel","odds","p","edge"]
    hist_cols = ["match_id","player_a","player_b","odds","p","edge"]

    recent_html = df_preview(trades.sort_values("ts", ascending=False), recent_cols, n=10)
    live_html = df_preview(live, live_cols, n=10)
    hist_html = df_preview(hist_picks, hist_cols, n=10)

    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Tennis Engine — Summary</title>
<style>
body {{ font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 24px; }}
h1 {{ margin-bottom: 0; }}
small {{ color: #666; }}
table {{ border-collapse: collapse; margin-top: 8px; }}
th, td {{ border: 1px solid #ddd; padding: 6px 10px; font-size: 14px; }}
th {{ background: #f6f6f6; text-align: left; }}
section {{ margin: 24px 0; }}
</style>
</head>
<body>
  <h1>Tennis Engine — Summary</h1>
  <small>Generated: {gen_ts}</small>

  <section>
    <h2>Bankroll & KPIs</h2>
    <ul>
      {''.join(kpis) if kpis else '<li><em>No KPIs available</em></li>'}
    </ul>
  </section>

  <section>
    <h2>Recent Settled Trades</h2>
    {recent_html}
  </section>

  <section>
    <h2>Top Live Picks</h2>
    {live_html}
  </section>

  <section>
    <h2>Historical Picks (latest)</h2>
    {hist_html}
  </section>

  <footer><small>Job summary generated at run-time</small></footer>
</body>
</html>
"""
    out_path = os.path.join(args.out, "index.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote dashboard to {out_path}")

if __name__ == "__main__":
    raise SystemExit(main())
