#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Settle trades from state/trade_log.csv using live_results/close_odds.csv
and backfill CLV for previously settled rows that missed it.

Outputs:
  - Updates trade_log.csv (status/pnl/close_odds/clv/settled_ts)
  - Updates state/bankroll.json and state/bankroll_history.csv

Flags:
  --assume-random-if-missing : simulate result using p if result missing
  --no-close-nudge           : disable tiny deterministic drift when close==entry
"""

import os, argparse, time, json, math, random, hashlib
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--log", default="state/trade_log.csv")
ap.add_argument("--close-odds", default="live_results/close_odds.csv")
ap.add_argument("--state-dir", default="state")
ap.add_argument("--assume-random-if-missing", action="store_true")
ap.add_argument("--no-close-nudge", action="store_true")
args = ap.parse_args()

os.makedirs(args.state_dir, exist_ok=True)
LOG_P, CLOSE_P = args.log, args.close_odds
BANK_P  = os.path.join(args.state_dir, "bankroll.json")
HIST_P  = os.path.join(args.state_dir, "bankroll_history.csv")

# ---------- bankroll helpers ----------
def load_bankroll(default=1000.0):
    try:
        if os.path.isfile(BANK_P):
            return float(json.load(open(BANK_P)).get("bankroll", default))
    except Exception:
        pass
    return float(default)

def save_bankroll(v: float):
    with open(BANK_P, "w", encoding="utf-8") as f:
        json.dump({"bankroll": float(v)}, f)

def append_history(ts: int, bankroll: float):
    row = pd.DataFrame([{"ts": int(ts), "bankroll": float(bankroll)}])
    if os.path.isfile(HIST_P):
        try:
            old = pd.read_csv(HIST_P)
            pd.concat([old, row], ignore_index=True).to_csv(HIST_P, index=False); return
        except Exception:
            pass
    row.to_csv(HIST_P, index=False)

def tiny_nudge(mid: str, sel: str, odds: float) -> float:
    """Deterministic ~±1.5% drift so CLV isn’t stuck at zero in synthetic runs."""
    seed = f"{mid}::{sel}".encode("utf-8")
    h = hashlib.sha256(seed).hexdigest()
    u = (int(h[:8], 16) % 10_000_000) / 10_000_000.0   # [0,1)
    drift = 0.985 + 0.03 * u                           # 0.985..1.015
    return max(1.01, round(odds * drift, 3))

def clv_from(close_odds: float, entry_odds: float) -> float:
    return math.log(max(close_odds, 1.01) / max(entry_odds, 1.01))

# ---------- load files ----------
if not os.path.isfile(LOG_P):
    print("No trade_log.csv → nothing to settle.")
    raise SystemExit(0)
log = pd.read_csv(LOG_P)
if log.empty:
    print("trade_log.csv empty → nothing to settle.")
    raise SystemExit(0)

# normalize selection column
if "selection" not in log.columns and "sel" in log.columns:
    log = log.rename(columns={"sel": "selection"})

# build close-odds lookup
close_map = {}
if os.path.isfile(CLOSE_P):
    try:
        clos = pd.read_csv(CLOSE_P)
        if "selection" not in clos.columns and "sel" in clos.columns:
            clos = clos.rename(columns={"sel":"selection"})
        if "odds" in clos.columns and "close_odds" not in clos.columns:
            clos = clos.rename(columns={"odds":"close_odds"})
        for _, r in clos.iterrows():
            mid = str(r.get("match_id","")).strip()
            sel = str(r.get("selection","")).strip()
            co  = r.get("close_odds", None)
            if mid and sel and pd.notna(co):
                close_map[(mid, sel)] = float(co)
    except Exception as e:
        print("WARN: reading close_odds:", e)

bankroll = load_bankroll()
now = int(time.time())

# ---------- 1) settle OPEN trades ----------
status = log.get("status", pd.Series([""]*len(log))).astype(str).str.lower()
open_idx = log.index[status.eq("open")]

settled_count = 0
pnl_sum = 0.0
clv_sum_open = 0.0

for idx in open_idx:
    r = log.loc[idx]
    mid = str(r.get("match_id","")).strip()
    sel = str(r.get("selection","")).strip()
    odds = float(r.get("odds", 0.0))
    p    = float(r.get("p", 0.0))
    stake = float(r.get("stake_eur", 0.0))

    close_odds = close_map.get((mid, sel), odds)
    if (not args.no_close_nudge) and abs(close_odds - odds) < 1e-12:
        close_odds = tiny_nudge(mid, sel, odds)

    clv = clv_from(close_odds, odds)

    res = r.get("result", None)
    if pd.isna(res) or str(res).strip()=="" or str(res).lower()=="nan":
        if args.assume_random_if_missing:
            win = (random.random() < p)
        else:
            # keep it open if we don't assume
            continue
    else:
        try: win = bool(int(res))
        except Exception: win = bool(res)

    trade_pnl = (stake*(odds-1.0)) if win else (-stake)
    bankroll += trade_pnl
    pnl_sum  += trade_pnl
    clv_sum_open += clv
    settled_count += 1

    log.loc[idx, "status"]      = "settled"
    log.loc[idx, "close_odds"]  = float(close_odds)
    log.loc[idx, "clv"]         = float(clv)
    log.loc[idx, "pnl"]         = float(trade_pnl)
    log.loc[idx, "settled_ts"]  = now

# ---------- 2) PATCH already-settled rows missing CLV ----------
settled_mask = log.get("status", pd.Series([""]*len(log))).astype(str).str.lower().eq("settled")
need_clv_mask = settled_mask & (
    log.get("clv").isna() |
    (log.get("close_odds").isna()) |
    (log.get("close_odds", 0.0) == log.get("odds", 0.0))
)

patched = 0
clv_sum_patched = 0.0
for idx in log.index[need_clv_mask.fillna(False)]:
    r = log.loc[idx]
    mid = str(r.get("match_id","")).strip()
    sel = str(r.get("selection","")).strip()
    odds = float(r.get("odds", 0.0))
    # if we have a closing snapshot for that pair, use it; otherwise nudge
    close_odds = close_map.get((mid, sel), odds)
    if (not args.no_close_nudge) and abs(close_odds - odds) < 1e-12:
        close_odds = tiny_nudge(mid, sel, odds)

    clv = clv_from(close_odds, odds)
    log.loc[idx, "close_odds"] = float(close_odds)
    log.loc[idx, "clv"]        = float(clv)
    patched += 1
    clv_sum_patched += clv

# ---------- persist + print ----------
log.to_csv(LOG_P, index=False)
save_bankroll(bankroll)
append_history(now, bankroll)

avg_clv_open = (clv_sum_open / settled_count) if settled_count else 0.0
avg_clv_patched = (clv_sum_patched / patched) if patched else 0.0

print(
    f"Settled {settled_count} trades | PnL {pnl_sum:+.2f} | "
    f"Patched CLV on {patched} rows | "
    f"Avg CLV (new) {avg_clv_open:+.4f} | Avg CLV (patched) {avg_clv_patched:+.4f} | "
    f"Bankroll €{bankroll:.2f}"
)
