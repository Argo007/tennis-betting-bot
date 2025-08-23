#!/usr/bin/env python3
import json
import pathlib
import sys

p = pathlib.Path("outputs/backtest_metrics.json")

print("## Matrix Backtest — Best by ROI")

if not p.exists() or p.stat().st_size == 0:
    print("No metrics json found.")
    sys.exit(0)

try:
    j = json.loads(p.read_text())
except Exception as e:
    print(f"Could not parse metrics: {e}")
    sys.exit(0)

best = (j or {}).get("best_by_roi") or {}

def fmt(x):
    if x is None:
        return "-"
    if isinstance(x, (int, float)):
        try:
            return f"{x:.4f}"
        except Exception:
            return str(x)
    return str(x)

print(f"- **Config**: `{best.get('config_id', '-')}`")
print(f"- **Band**: {best.get('label', '-')}")
print(f"- **ROI**: {fmt(best.get('roi'))}")
