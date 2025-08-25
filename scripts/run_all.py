#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run the entire pipeline locally or in CI without touching YAML.
Usage:
  python scripts/run_all.py --edge 0.08 --kelly 0.5 --bankroll 1000 --bands "2.0,2.6|2.6,3.2|3.2,4.0" --commit
"""
import argparse, os, subprocess, shlex, sys

def run(cmd):
    print("+", cmd)
    return subprocess.run(cmd, shell=True, check=False)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--edge", type=float, default=float(os.getenv("MIN_EDGE", "0.08")))
    ap.add_argument("--kelly", type=float, default=float(os.getenv("KELLY_SCALE", "0.5")))
    ap.add_argument("--bankroll", type=float, default=float(os.getenv("BANKROLL", "1000")))
    ap.add_argument("--bands", default=os.getenv("BANDS", "2.0,2.6|2.6,3.2|3.2,4.0"))
    ap.add_argument("--commit", action="store_true", help="Commit state/docs/results to git")
    args = ap.parse_args()

    OUT = "results"
    LIVE = "live_results"
    STATE = "state"
    os.makedirs(OUT, exist_ok=True)
    os.makedirs(LIVE, exist_ok=True)
    os.makedirs(STATE, exist_ok=True)

    # 1) Historical → picks → backtest
    run(f"python scripts/fetch_tennis_data.py --outdir {OUT}")
    run(f"python scripts/tennis_value_picks_pro.py --input {OUT}/tennis_data.csv --outdir {OUT} --min-edge {args.edge}")
    if os.path.isfile(f"{OUT}/picks_final.csv"):
        run("python scripts/run_matrix_backtest.py "
            f"--input {OUT}/picks_final.csv --outdir {OUT} --stake-mode kelly "
            f"--edge {args.edge} --kelly-scale {args.kelly} --bankroll {args.bankroll} "
            f"--bands {shlex.quote(args.bands)}")

    # 2) Update bankroll persistent state
    run(f"python scripts/update_bankroll_state.py --equity {OUT}/equity_curve.csv --state-dir {STATE} --initial {args.bankroll}")

    # 3) Live → odds → live picks
    run(f"python scripts/fetch_live_matches.py --out {LIVE}/live_matches.csv")
    run(f"python scripts/fetch_live_odds.py --matches {LIVE}/live_matches.csv --out {LIVE}/live_odds.csv")
    run(f"python scripts/tennis_value_picks_live.py --odds {LIVE}/live_odds.csv --outdir {LIVE} --min-edge {args.edge}")

    # 4) Log live picks with stakes (uses current bankroll from state)
    run(f"python scripts/log_live_picks.py --picks {LIVE}/picks_live.csv --state-dir {STATE} --kelly {args.kelly}")

    # 5) Alerts (Telegram/Discord env vars optional)
    run("python scripts/notify_picks.py --live-outdir live_results --backtest-outdir results --min-rows 1")

    # 6) Dashboard
    run("python scripts/make_dashboard.py --state-dir state --results results --live live_results --out docs")

    # 7) Optional commit (state/results/docs)
    if args.commit:
        run('python scripts/autocommit_state.py --paths state results live_results docs --message "auto: update state+dashboard"')
