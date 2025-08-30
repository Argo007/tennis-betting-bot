#!/usr/bin/env python3
import argparse, json, os, sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-dir", required=True)
    ap.add_argument("--max-frac", required=True, help="fraction of bankroll (e.g. 0.05)")
    ap.add_argument("--floor", default="1", help="minimum euro amount to return")
    args = ap.parse_args()

    p = os.path.join(args.state_dir, "bankroll.json")
    bankroll = 1000.0
    try:
        with open(p, "r") as f:
            data = json.load(f)
            bankroll = float(data.get("bankroll", bankroll))
    except Exception:
        # ok – first run or missing file; keep default
        pass

    try:
        max_frac = float(args.max_frac)
    except Exception:
        max_frac = 0.05

    floor = float(args.floor)
    value = max(floor, bankroll * max_frac)

    # IMPORTANT: print plain number (shell captures it)
    print(f"{value:.6f}")

if __name__ == "__main__":
    sys.exit(main())
