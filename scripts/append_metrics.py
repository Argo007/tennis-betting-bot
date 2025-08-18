# scripts/append_metrics.py
import os, json

if os.path.exists("backtest_metrics.json"):
    with open("backtest_metrics.json") as f:
        m = json.load(f)
    with open("summary.md","a") as out:
        out.write(f"**Bets:** {m.get('n_bets',0)}\n")
        out.write(f"**Hit-rate:** {m.get('hit_rate',0):.2%}\n")
        out.write(f"**ROI:** {m.get('roi',0):.2%}\n")
        out.write(f"**Max DD (units):** {m.get('max_drawdown',0):.2f}\n")
    print("Metrics appended to summary.md")
elif os.path.exists("results.csv"):
    with open("summary.md","a") as out:
        out.write("Backtest completed. See results.csv.\n")
    print("Results available, metrics missing")
else:
    print("No results or metrics found")
