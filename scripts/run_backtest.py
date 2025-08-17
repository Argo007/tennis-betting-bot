import argparse
import os
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--start", default="2024-01-01")
parser.add_argument("--end", default="2024-12-31")
args = parser.parse_args()

with open("summary.md", "w") as f:
    f.write("# TE8 Backtest Summary\n\n")

if os.path.exists("data/historical_matches.csv") and os.path.getsize("data/historical_matches.csv") > 0:
    try:
        subprocess.run([
            "python", "backtest_te8.py",
            "--input", "data/historical_matches.csv",
            "--start", args.start,
            "--end", args.end,
            "--out-csv", "results.csv"
        ], check=True)
    except subprocess.CalledProcessError:
        print("Backtest script failed, continuing anyway...")
else:
    with open("summary.md", "a") as f:
        f.write("Dataset missing or empty.\n")
