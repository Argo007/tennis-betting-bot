#!/usr/bin/env python3
import os
from pathlib import Path
import pandas as pd

IN = Path("matrix_summary.csv")
OUT_DIR = Path("reports")
OUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_OUT = OUT_DIR / "matrix_summary_latest.csv"
MD_OUT  = OUT_DIR / "matrix_summary_latest.md"

if not IN.exists():
    raise FileNotFoundError(f"matrix_report.py: missing {IN}. Run the matrix workflow first.")

df = pd.read_csv(IN)

# Keep nice column order if present
cols = ['kelly_scale','min_edge','n_bets','total_staked','pnl','roi','end_bankroll']
df = df[[c for c in cols if c in df.columns]].copy()
df.sort_values(['kelly_scale','min_edge'], inplace=True)

# Save CSV
df.to_csv(CSV_OUT, index=False)

# Save Markdown (pretty if tabulate is available; safe fallback otherwise)
header = "# 📊 Matrix Backtest — Latest Summary\n\n"
try:
    md_table = df.to_markdown(index=False)
except Exception:
    md_table = "```\n" + df.to_string(index=False) + "\n```"

MD_OUT.write_text(header + md_table + "\n", encoding="utf-8")
print(f"Wrote {CSV_OUT} and {MD_OUT}")
