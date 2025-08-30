#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Print a clean Markdown summary of bankroll, KPIs, recent settled trades,
and top live picks (with Kelly stakes if available).

If GITHUB_STEP_SUMMARY is set (GitHub Actions), writes there so it renders
as a nice table on the run page. Otherwise prints to stdout.

Reads (if present):
  state/bankroll.json
  state/bankroll_history.csv
  state/trade_log.csv
  live_results/picks_live.csv
  results/picks_final.csv
"""
import os, json, pandas as pd
from datetime import datetime, timezone, timedelta

STATE = os.getenv("STATE_DIR", "state")
LIVE  = os.getenv("LIVE_OUTDIR", "live_results")
RES   = os.getenv("OUTDIR", "results")

def read_json(path, default=None):
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return default

def read_csv(path, cols=None):
    if not os.path.isfile(path):
        return pd.DataFrame(columns=cols or [])
    try:
        df = pd.read_csv(path)
        return df if not df.empty else pd.DataFrame(columns=cols or [])
    except Exception:
        return pd.DataFrame(columns=cols or [])

bank = read_json(os.path.join(STATE, "bankroll.json"), {"bankroll": 0})
hist  = read_csv(os.path.join(STATE, "bankroll_history.csv"))
log   = read_csv(os.path.join(STATE, "trade_log.csv"))
livep = read_csv(os.path.join(LIVE, "picks_live.csv"))
histp = read_csv(os.path.join(RES, "picks_final.csv"))

# KPIs
now = datetime.now(tz=timezone.utc)
day_ago = int((now - timedelta(days=1)).timestamp())

pnl_24h = 0.0
n_open = n_settled = 0
avg_clv = 0.0
if not log.empty:
    if "status" in log.columns:
        n_open = int((log["status"].str.lower() == "open").sum())
        n_settled = int((log["status"].str.lower() == "settled").sum())
    if "settled_ts" in log.columns and "pnl" in log.columns:
        pnl_24h = float(log.loc[log["settled_ts"].fillna(0).astype(int) >= day_ago, "pnl"].sum())
    if "clv" in log.columns:
        clv_vals = log["clv"].dropna()
        if len(clv_vals):
            avg_clv = float(clv_vals.mean())

def fmt_money(x): 
    try: return f"€{float(x):,.2f}"
    except: return "€0.00"

def pct(x):
    try: return f"{100*float(x):.1f}%"
    except: return "—"

def md_table(df, cols, title, max_rows=10):
    if df.empty:
        return f"\n### {title}\n_No data_\n"
    d = df.copy()
    keep = [c for c in cols if c in d.columns]
    d = d[keep].head(max_rows)
    # tidy numbers
    for col in ("odds","p","edge","stake_eur","pnl","clv"):
        if col in d.columns:
            if col in ("p","edge"):
                d[col] = d[col].astype(float).map(lambda v: f"{100*v:.1f}%")
            else:
                d[col] = d[col].astype(float).map(lambda v: f"{v:.3f}" if col!="stake_eur" else fmt_money(v))
    if "ts" in d.columns:
        d["ts"] = pd.to_datetime(d["ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M")
    if "settled_ts" in d.columns:
        d["settled_ts"] = pd.to_datetime(d["settled_ts"], unit="s", utc=True).dt.strftime("%Y-%m-%d %H:%M")
    # markdown
    header = " | ".join(keep)
    sep = " | ".join(["---"]*len(keep))
    rows = [" | ".join(map(str, r)) for r in d.values.tolist()]
    return f"\n### {title}\n{header}\n{sep}\n" + "\n".join(rows) + "\n"

# Build summary
lines = []
lines.append(f"# Tennis Engine — Summary")
lines.append(f"_Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}_\n")
lines.append("## Bankroll & KPIs")
lines.append(f"- **Bankroll:** {fmt_money(bank.get('bankroll', 0))}")
lines.append(f"- **Settled (count):** {n_settled}")
lines.append(f"- **Open (count):** {n_open}")
lines.append(f"- **PnL (last 24h):** {fmt_money(pnl_24h)}")
lines.append(f"- **Avg CLV:** {avg_clv:.4f}\n")

# Tables
lines.append(md_table(
    log[log.get("status","").astype(str).str.lower()=="settled"].sort_values("settled_ts", ascending=False),
    ["ts","match_id","selection","odds","p","edge","stake_eur","close_odds","clv","pnl","settled_ts"],
    "Recent Settled Trades",
    max_rows=12
))
lines.append(md_table(
    log[log.get("status","").astype(str).str.lower()=="open"].sort_values("ts", ascending=False),
    ["ts","match_id","selection","odds","p","edge","stake_eur","bankroll_snapshot"],
    "Open Trades",
    max_rows=12
))
lines.append(md_table(
    livep.sort_values("edge", ascending=False) if "edge" in livep.columns else livep,
    ["match_id","sel","odds","p","edge"],
    "Top Live Picks",
    max_rows=8
))
lines.append(md_table(
    histp.sort_values("edge", ascending=False) if "edge" in histp.columns else histp,
    ["match_id","player_a","player_b","odds","p","edge"],
    "Historical Picks (latest)",
    max_rows=8
))

markdown = "\n".join(lines)

# Write to job summary if available
summary_path = os.getenv("GITHUB_STEP_SUMMARY")
if summary_path:
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(markdown)
else:
    print(markdown)
