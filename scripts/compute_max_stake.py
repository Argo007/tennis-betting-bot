#!/usr/bin/env python3
import argparse, json, os, sys

def main():
    ap = argparse.ArgumentParser(description="Compute max stake in EUR")
    ap.add_argument("--state-dir", default="state")
    ap.add_argument("--max-frac", type=float, default=0.05)
    ap.add_argument("--fallback-bankroll", type=float, default=1000.0)
    args = ap.parse_args()

    bankroll = args.fallback_bankroll
    try:
        p = os.path.join(args.state_dir, "bankroll.json")
        with open(p, "r") as f:
            data = json.load(f)
        bankroll = float(data.get("bankroll", bankroll))
    except Exception:
        # no state yet is fine
        pass

    val = max(1.0, args.max_frac * bankroll)
    # print the number only so YAML can capture it easily
    sys.stdout.write(f"{val:.6f}")

if __name__ == "__main__":
    main()

