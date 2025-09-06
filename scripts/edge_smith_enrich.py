#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EdgeSmith Engine — Enrich live picks with edge, Kelly stake, and model confidence.
Optimized for Tennis Engine v3.2 (Sept 2025)
"""

import pandas as pd
import numpy as np
import os
from pathlib import Path

# ---------- SETTINGS ----------
KELLY_SCALE = float(os.getenv("KELLY_SCALE", 0.5))  # from YAML input, default 0.5
MAX_BANKROLL_PCT = float(os.getenv("MAX_BANKROLL_PCT", 0.02))  # cap bet size per pick

# ---------- FILE PATHS ----------
ROOT = Path(__file__).resolve().parent.parent
PICKS_FILE = ROOT / "picks_live.csv"

# ---------- HELPER FUNCTIONS ----------
def kelly_fraction(prob: float, odds: float) -> float:
    """Calculate Kelly fraction for given probability & odds."""
    b = odds - 1
    q = 1 - prob
    f = ((b * prob) - q) / b
    return max(0.0, f)

def enrich_picks(df: pd.DataFrame) -> pd.DataFrame:
    """Add edge, Kelly stake, and recommended bet size."""
    df["edge"] = df["model_conf"] - (1 / df["odds"])
    df["kelly_frac"] = df.apply(
        lambda row: kelly_fraction(row["model_conf"], row["odds"]), axis=1
    )
    df["kelly€"] = np.round(df["kelly_frac"] * KELLY_SCALE * 1000 * MAX_BANKROLL_PCT, 2)

    # Apply safe betting cap
    df["kelly€"] = np.where(df["kelly€"] < 0, 0, df["kelly€"])
    return df

# ---------- MAIN EXECUTION ----------
def main():
    if not PICKS_FILE.exists():
        print(f"[ERROR] Picks file not found: {PICKS_FILE}")
        return

    df = pd.read_csv(PICKS_FILE)

    if "model_conf" not in df.columns or df["model_conf"].isnull().all():
        print("[WARN] Missing model_conf — skipping enrichment.")
        return

    enriched = enrich_picks(df)
    enriched.to_csv(PICKS_FILE, index=False)
    print(f"[OK] Picks enriched → {PICKS_FILE}")

if __name__ == "__main__":
    main()
