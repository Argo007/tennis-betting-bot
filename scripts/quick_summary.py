#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tennis Engine — Clean Job Summary

Prints a Markdown summary to stdout (and to GITHUB_STEP_SUMMARY in GitHub Actions)
showing bankroll/KPIs + tidy tables. Robust to missing/empty files.

Reads (if present):
  state/bankroll.json
  state/bankroll_history.csv
  state/trade_log.csv
  live_results/picks_live.csv
  results/picks_final.csv
"""

import os, json
from datetime import datetime, timezone, timedelta
import pandas as pd

STATE = os.getenv("STATE_DIR", "state")
LIVE  = os.getenv("LIVE_OUTDIR", "live_results")
RES   = os.getenv("OUTDIR", "results")

# ---------- Helpers ----------
def read_json(path, default=None):
    try:
        with open(path, "r", encoding="utf-8") as f:
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

def safe_series(df: pd.DataFrame, col: str, fill=""):
    """Return a Series for df[col]; if missing, return length-matched filler series."""
    if isinstance(df, pd.DataFrame) and col in df.columns:
        return df[col]
    # no df or missing column → filler of correct length
    n = len(df) if isinstance(df, pd.DataFrame) else 0
    return pd.Series([fill] * n)

def fmt_money(x):
    try:
        return f"€{float(x):,.2f}"
    except Exception:
        return "€0.00"

def md_table(df: pd.DataFrame, cols, title, max_rows=10):
    if df is None or df.empty:
        return f"\n### {title}\n_No data_\n"

    use_cols = [c for c in cols if c in df.columns]
    if not use_cols:
        return f"\n### {title}\n_No data_\n"

    d = df.copy()[use_cols].head(max_rows)

    # numeric tidying
    for col in ("odds","p","edge","stake_eur","pnl","clv","close_odds","bankroll_snapshot"):
        if col in d.columns:
            d[col] = pd.to_numeric(d[col], errors="coerce")
            if col in ("p","edge"):
                d[col] = d[col].map(lambda v: f"{v*100:.1f}%" if pd.notna(v) else "—")
            elif col == "stake_eur":
                d[col] = d[col].map(lambda v: fmt_money(v) if pd.notna(v) else "—")
            else:
                d[col] = d[col].map(lambda v: f"{v:.3f}" if pd.notna(v) else "—")

    for tcol in ("ts", "settled_ts"):
        if tcol in d.columns:
            d[tcol] = pd.to_datetime(pd.to_numeric(d[tcol], errors="coerce"), unit="s", utc=True)\
                        .dt.strftime("%Y-%m-%d %H:%M")

    # render markdown
    header = " | ".join(use_cols)
    sep = " | ".join(["---"] * len(use_cols))
    rows = [" | ".join("" if pd.isna(v) else str(v) for v in r) for r in d.values.tolist()]
    return f"\n### {title}\n{header}\n{sep}\n" + "\n".join(rows) + "\n"

# ---------- Load data ----------
bank = read_json(os.path.join(STATE, "bankroll.json"), {"bankroll": 0})
hist  = read_csv(os.path.join(STATE, "bankroll_history.csv"), cols=["ts","bankroll"])
log   = read_csv(os.path.join(STATE, "trade_log.csv"))
livep = read_csv(os.path.join(LIVE,  "picks_live.csv"))
histp = read_csv(os.path.join(RES,   "picks_final.csv"))

# ---------- KPIs ----------
now = datetime.now(tz=timezone.utc)
day_ago = int((now - timedelta(days=1)).timestamp())

# Build a safe 'status' series
status_s = safe_series(log, "status", fill="").astype(str).str.lower()
settled_mask = status_s.eq("settled")
open_mask    = status_s.eq("open")

# Counts
n_open = int(open_mask.sum()) if not log.empty else 0
n_settled = int(settled_mask.sum()) if not log.empty else 0

# 24h PnL
pnl_24h = 0.0
if not log.empty and "pnl" in log.columns:
    settled_ts = pd.to_numeric(safe_series(log, "settled_ts"), errors="coerce").fillna(0).astype(int)
    pnl_series = pd.to_numeric(log["pnl"], errors="coerce").fillna(0.0)
    pnl_24h = float(pnl_series.where(settled_ts >= day_ago, other=0.0).sum())

# Avg CLV
avg_clv = 0.0
if not log.empty and "clv" in log.columns:
    clv_vals = pd.to_numeric(log["clv"], errors="coerce").dropna()
    if len(clv_vals):
        avg_clv = float(clv_vals.mean())

# ---------- Tables ----------
# Sorted views (guard against missing sort keys)
def sort_safe(df, by, ascending=False):
    if df.empty or by not in df.columns:
        return df
    return df.sort_values(by, ascending=ascending)

recent_settled = sort_safe(log[settled_mask].copy(), "settled_ts", ascending=False)
recent_open    = sort_safe(log[open_mask].copy(), "ts", ascending=False)

top_live  = sort_safe(livep.copy(), "edge", ascending=False) if "edge" in livep.columns else livep.copy()
top_hist  = sort_safe(histp.copy(), "edge", ascending=False) if "edge" in histp.columns else histp.copy()

# ---------- Compose Markdown ----------
lines = []
lines.append("# Tennis Engine — Summary")
lines.append(f"_Generated: {now.strftime('%Y-%m-%d %H:%M UTC')}_\n")
lines.append("## Bankroll & KPIs")
lines.append(f"- **Bankroll:** {fmt_money(bank.get('bankroll', 0))}")
lines.append(f"- **Settled (count):** {n_settled}")
lines.append(f"- **Open (count):** {n_open}")
lines.append(f"- **PnL (last 24h):** {fmt_money(pnl_24h)}")
lines.append(f"- **Avg CLV:** {avg_clv:.4f}\n")

lines.append(md_table(
    recent_settled,
    ["ts","match_id","selection","odds","p","edge","stake_eur","close_odds","clv","pnl","settled_ts"],
    "Recent Settled Trades",
    max_rows=12
))
lines.append(md_table(
    recent_open,
    ["ts","match_id","selection","odds","p","edge","stake_eur","bankroll_snapshot"],
    "Open Trades",
    max_rows=12
))
lines.append(md_table(
    top_live,
    ["match_id","sel","odds","p","edge"],
    "Top Live Picks",
    max_rows=8
))
lines.append(md_table(
    top_hist,
    ["match_id","player_a","player_b","odds","p","edge"],
    "Historical Picks (latest)",
    max_rows=8
))

markdown = "\n".join(lines)

# Print to job summary if available; else stdout
summary_path = os.getenv("GITHUB_STEP_SUMMARY")
if summary_path:
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(markdown)
else:
    print(markdown)
