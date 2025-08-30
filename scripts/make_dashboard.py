#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build a minimal dashboard at docs/index.html showing bankroll history + tables.
Usage:
  python scripts/make_dashboard.py --state-dir state --results results --live live_results --out docs
"""

import argparse
import os
import json
import pandas as pd
from datetime import datetime

def _fmt_currency(x):
    try:
        return f"€{float(x):.2f}"
    except Exception:
        return str(x)

def _fmt_pct(x, digits=1):
    try:
        return f"{100*float(x):.{digits}f}%"
    except Exception:
        return str(x)

def _safe_read_csv(path, **kwargs):
    if not os.path.isfile(path):
        return pd.DataFrame()
    try:
        return pd.read_csv(path, **kwargs)
    except Exception:
        return pd.DataFrame()

def _html_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p><em>No data</em></p>"
    # Avoid pandas styles (keeps deps light in Actions logs)
    return df.to_html(index=False, border=0, justify="center", classes="simple")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-dir", default="state")
    ap.add_argument("--results", default="results")
    ap.add_argument("--live", default="live_results")
    ap.add_argument("--out", default="docs")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    # ---- Bankroll history (robust ts parsing; never cast to int) ----
    hist_path = os.path.join(args.state_dir, "bankroll_history.csv")
    hist = _safe_read_csv(hist_path)
    if not hist.empty:
        # Accept common column names
        for ts_col in ["ts", "settled_ts", "time", "timestamp"]:
            if ts_col in hist.columns:
                hist["ts_parsed"] = pd.to_datetime(hist[ts_col], errors="coerce", utc=True)
                break
        else:
            hist["ts_parsed"] = pd.NaT

        # Sort and select only the meaningful columns if present
        hist = hist.sort_values("ts_parsed")
        for col in ["bankroll", "equity", "pnl", "clv"]:
            if col in hist.columns:
                try:
                    hist[col] = pd.to_numeric(hist[col], errors="coerce")
                except Exception:
                    pass
    else:
        hist["ts_parsed"] = pd.Series([], dtype="datetime64[ns, UTC]")

    # KPIs
    bankroll = 0.0
    if "bankroll" in hist.columns and not hist["bankroll"].dropna().empty:
        bankroll = float(hist["bankroll"].dropna().iloc[-1])
    settled_count = int(hist.shape[0])
    open_count = 0  # this dashboard doesn't track open tickets separately
    pnl_last24 = 0.0
    avg_clv = 0.0
    if "pnl" in hist.columns and not hist["pnl"].dropna().empty:
        # last 24h pnl approximation (if we have timestamps)
        if "ts_parsed" in hist.columns and hist["ts_parsed"].notna().any():
            cutoff = pd.Timestamp.utcnow() - pd.Timedelta(hours=24)
            pnl_last24 = float(hist.loc[hist["ts_parsed"] >= cutoff, "pnl"].sum())
        else:
            pnl_last24 = float(hist["pnl"].tail(1).sum())
    if "clv" in hist.columns and not hist["clv"].dropna().empty:
        avg_clv = float(hist["clv"].mean())

    # ---- Recent settled trades table (take last ~10 if present) ----
    recent_cols = ["ts", "match_id", "selection", "odds", "p", "edge", "stake_eur", "close_odds", "clv", "pnl", "settled_ts"]
    recent = hist.copy()
    # Harmonize column names for display
    if "ts_parsed" in recent.columns:
        recent["ts"] = recent["ts_parsed"].dt.strftime("%Y-%m-%d %H:%M")
    # percentage formatting
    for c in ["p", "edge"]:
        if c in recent.columns:
            recent[c] = recent[c].apply(lambda x: _fmt_pct(x, 1))
    if "clv" in recent.columns:
        recent["clv"] = recent["clv"].apply(lambda x: f"{float(x):.3f}" if pd.notna(x) else x)
    if "stake_eur" in recent.columns:
        recent["stake_eur"] = recent["stake_eur"].apply(_fmt_currency)
    if "pnl" in recent.columns:
        recent["pnl"] = recent["pnl"].apply(lambda x: f"{float(x):.3f}" if pd.notna(x) else x)
    # retain display columns if present
    recent = recent[[c for c in recent_cols if c in recent.columns]].tail(10)

    # ---- Top live picks table (if any) ----
    picks_live_path = os.path.join(args.live, "picks_live.csv")
    live = _safe_read_csv(picks_live_path)
    if not live.empty:
        for c in ["p", "edge"]:
            if c in live.columns:
                live[c] = live[c].apply(lambda x: _fmt_pct(x, 1))
        live = live.rename(columns={"selection": "sel"})
        top_live = live[["match_id", "sel", "odds", "p", "edge"]].head(10) if all(
            k in live.columns for k in ["match_id", "sel", "odds", "p", "edge"]
        ) else pd.DataFrame()
    else:
        top_live = pd.DataFrame()

    # ---- Historical picks sample (from results) ----
    picks_final_path = os.path.join(args.results, "picks_final.csv")
    hist_picks = _safe_read_csv(picks_final_path)
    if not hist_picks.empty:
        for c in ["p", "edge"]:
            if c in hist_picks.columns:
                hist_picks[c] = hist_picks[c].apply(lambda x: _fmt_pct(x, 1))
        historical = hist_picks.head(10)
        # pick a friendly subset if available
        wanted = ["match_id", "player_a", "player_b", "odds", "p", "edge"]
        historical = historical[[c for c in wanted if c in historical.columns]]
    else:
        historical = pd.DataFrame()

    # ---- Render HTML ----
    generated = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    html = f"""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Tennis Engine — Summary</title>
<style>
 body {{ font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; max-width: 980px; margin: 24px auto; padding: 0 16px; }}
 h1 {{ margin-bottom: 4px; }}
 .kpis ul {{ list-style: disc; padding-left: 20px; }}
 table.simple {{ border-collapse: collapse; width: 100%; margin: 8px 0 24px; }}
 table.simple th, table.simple td {{ border: 1px solid #ddd; padding: 6px 8px; text-align: right; }}
 table.simple th:first-child, table.simple td:first-child {{ text-align: left; }}
 em {{ color: #777; }}
</style>
</head>
<body>
  <h1>Tennis Engine — Summary</h1>
  <p><em>Generated: {generated}</em></p>

  <h2>Bankroll & KPIs</h2>
  <div class="kpis">
    <ul>
      <li>Bankroll: {_fmt_currency(bankroll)}</li>
      <li>Settled (count): {settled_count}</li>
      <li>Open (count): {open_count}</li>
      <li>PnL (last 24h): {_fmt_currency(pnl_last24)}</li>
      <li>Avg CLV: {avg_clv:.4f}</li>
    </ul>
  </div>

  <h2>Recent Settled Trades</h2>
  {_html_table(recent)}

  <h2>Open Trades</h2>
  <p><em>No data</em></p>

  <h2>Top Live Picks</h2>
  {_html_table(top_live)}

  <h2>Historical Picks (latest)</h2>
  {_html_table(historical)}

  <p style="color:#999; font-size:12px;">Job summary generated at run-time</p>
</body>
</html>
"""
    out_path = os.path.join(args.out, "index.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[dashboard] wrote {out_path}")

if __name__ == "__main__":
    main()
