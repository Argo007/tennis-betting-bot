#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fallback: create tiny synthetic live files when no real picks exist.
Idempotent: only writes if picks_live.csv is missing/empty.

Outputs under --outdir (default: live_results):
  - live_matches.csv
  - live_odds.csv
  - picks_live.csv     (edges >= --min-edge)
"""
import os, argparse, time
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("--outdir", default="live_results")
ap.add_argument("--min-edge", type=float, default=float(os.getenv("MIN_EDGE", "0.02")))
args = ap.parse_args()

os.makedirs(args.outdir, exist_ok=True)
matches_p = os.path.join(args.outdir, "live_matches.csv")
odds_p    = os.path.join(args.outdir, "live_odds.csv")
picks_p   = os.path.join(args.outdir, "picks_live.csv")

def isempty(path):
    try:
        return (not os.path.isfile(path)) or pd.read_csv(path).empty
    except Exception:
        return True

# Only synthesize if there are no picks
if not isempty(picks_p):
    print("picks_live.csv has data -> skipping synthetic fallback.")
    raise SystemExit(0)

now = int(time.time())

# Matches
matches = pd.DataFrame([
    {"match_id":"SYN001","player_a":"Player A","player_b":"Player B","tournament":"Synthetic Open","start_time":now+1800},
    {"match_id":"SYN002","player_a":"Player C","player_b":"Player D","tournament":"Synthetic Open","start_time":now+2400},
])
matches.to_csv(matches_p, index=False)

# Odds (two selections per match)
odds = pd.DataFrame([
    {"match_id":"SYN001","book":"Synth","market":"ML","sel":"Player A","odds":2.40,"ts":now},
    {"match_id":"SYN001","book":"Synth","market":"ML","sel":"Player B","odds":1.65,"ts":now},
    {"match_id":"SYN002","book":"Synth","market":"ML","sel":"Player C","odds":3.10,"ts":now},
    {"match_id":"SYN002","book":"Synth","market":"ML","sel":"Player D","odds":1.45,"ts":now},
])
odds.to_csv(odds_p, index=False)

# Picks with clear edge
def edge(odds, p): return p - 1.0/float(odds)
rows = []
for mid, sel, o, p in [
    ("SYN001","Player A",2.40,0.56),  # ~+14% edge
    ("SYN002","Player C",3.10,0.42),  # ~+11% edge
]:
    if edge(o, p) >= args.min_edge:
        rows.append({"match_id":mid,"sel":sel,"odds":o,"p":p,"edge":edge(o,p)})
if not rows:  # ensure at least one row
    rows = [{"match_id":"SYN001","sel":"Player A","odds":2.40,"p":0.52,"edge":edge(2.40,0.52)}]

pd.DataFrame(rows).to_csv(picks_p, index=False)
print(f"Synthetic live written to {args.outdir} with {len(rows)} picks.")

