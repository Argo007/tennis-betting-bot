#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Log live picks to state/trade_log.csv with Kelly sizing + caps.
Skips duplicates by (match_id, selection).
"""
import os, argparse, time, json
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--picks", required=True)
ap.add_argument("--state-dir", default="state")
ap.add_argument("--kelly", type=float, default=float(os.getenv("KELLY_SCALE","0.5")))
ap.add_argument("--stake-cap", type=float, default=float(os.getenv("STAKE_CAP","0.05")))
ap.add_argument("--max-stake-eur", type=float, default=float(os.getenv("MAX_STAKE_EUR","200")))
args = ap.parse_args()

os.makedirs(args.state_dir, exist_ok=True)
log_path  = os.path.join(args.state_dir, "trade_log.csv")
bank_path = os.path.join(args.state_dir, "bankroll.json")

def load_bankroll():
    if os.path.isfile(bank_path):
        try: return float(json.load(open(bank_path))["bankroll"])
        except Exception: pass
    return 1000.0

def save_bankroll(v: float):
    with open(bank_path, "w") as f: json.dump({"bankroll": float(v)}, f)

def kelly_fraction(p: float, odds: float) -> float:
    b = max(float(odds)-1.0, 1e-9); q = 1.0 - float(p)
    f = (b*float(p) - q) / b
    return max(0.0, f)

picks = pd.read_csv(args.picks)
if "selection" not in picks.columns and "sel" in picks.columns:
    picks = picks.rename(columns={"sel":"selection"})
need = {"match_id","selection","odds"}
miss = need - set(picks.columns)
if miss: raise SystemExit(f"{args.picks} missing {sorted(miss)}")
if "p" not in picks.columns:
    picks["p"] = 1.0 / picks["odds"].clip(lower=1e-9)
if "edge" not in picks.columns:
    picks["edge"] = picks["p"] - 1.0 / picks["odds"].clip(lower=1e-9)

existing = pd.read_csv(log_path) if os.path.isfile(log_path) else pd.DataFrame()
if not existing.empty and "selection" not in existing.columns and "sel" in existing.columns:
    existing = existing.rename(columns={"sel":"selection"})
existing_uids = set()
if not existing.empty:
    for _, r in existing.iterrows():
        existing_uids.add(f"{r.get('match_id','')}::{r.get('selection','')}")

bankroll = load_bankroll()
ts = int(time.time())
rows, skipped = [], 0

for _, r in picks.iterrows():
    mid, sel, odds = r["match_id"], r["selection"], float(r["odds"])
    p, edge = float(r["p"]), float(r["edge"])
    uid = f"{mid}::{sel}"
    if uid in existing_uids: 
        skipped += 1; continue
    f_star = kelly_fraction(p, odds) * float(args.kelly)
    stake = min(f_star*bankroll, args.stake_cap*bankroll, args.max_stake_eur)
    stake = max(0.0, round(stake, 2))
    if stake <= 0: 
        skipped += 1; continue
    rows.append({
        "ts": ts, "match_id": mid, "selection": sel,
        "odds": float(odds), "p": float(p), "edge": float(edge),
        "stake_eur": float(stake), "status": "open",
        "bankroll_snapshot": float(bankroll),
    })

if not rows:
    print(f"Nothing to log (skipped={skipped})."); raise SystemExit(0)

log = pd.concat([existing, pd.DataFrame(rows)], ignore_index=True) if not existing.empty else pd.DataFrame(rows)
log.to_csv(log_path, index=False)
print(f"Logged {len(rows)} trades (skipped {skipped}) -> {log_path} @ bankroll €{bankroll:.2f}")
if not os.path.isfile(bank_path): save_bankroll(bankroll)
