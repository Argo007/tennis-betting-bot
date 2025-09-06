#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
EdgeSmith Dashboard — Build a live HTML dashboard for Tennis Engine picks.
"""

import pandas as pd
from pathlib import Path

# ---------- FILES ----------
ROOT = Path(__file__).resolve().parent.parent
PICKS_FILE = ROOT / "picks_live.csv"
DASHBOARD_FILE = ROOT / "docs" / "index.html"

# ---------- DASHBOARD BUILDER ----------
def build_dashboard():
    if not PICKS_FILE.exists():
        print(f"[ERROR] Picks file not found: {PICKS_FILE}")
        return

    df = pd.read_csv(PICKS_FILE)

    # Order by edge, highest first
    if "edge" in df.columns:
        df = df.sort_values(by="edge", ascending=False)

    # Build HTML
    html = f"""
    <html>
    <head>
        <title>Tennis Engine Dashboard</title>
        <meta charset="UTF-8">
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1 {{ color: #222; }}
            table {{
                width: 100%;
                border-collapse: collapse;
                margin-top: 20px;
            }}
            th, td {{
                border: 1px solid #ccc;
                padding: 8px;
                text-align: center;
            }}
            th {{
                background-color: #f5f5f5;
                cursor: pointer;
            }}
            tr:hover {{ background-color: #f9f9f9; }}
            .positive {{ color: green; font-weight: bold; }}
            .negative {{ color: red; font-weight: bold; }}
        </style>
    </head>
    <body>
        <h1>🎾 Tennis Engine Dashboard</h1>
        <p>Updated automatically after every run.</p>
        {df.to_html(index=False, classes="picks")}
    </body>
    </html>
    """

    DASHBOARD_FILE.write_text(html, encoding="utf-8")
    print(f"[OK] Dashboard generated → {DASHBOARD_FILE}")

# ---------- MAIN ----------
if __name__ == "__main__":
    build_dashboard()
