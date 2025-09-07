#!/usr/bin/env python3
"""
Generate a simple HTML report for the latest backtest.

- Reads results/backtests/summary.csv
- Picks a recommended config (highest Sharpe, break ties by ROI, then n_bets)
- Writes docs/backtests/index.html (and a tiny JSON with the winner)
"""

from __future__ import annotations
import csv, json
from pathlib import Path
from datetime import datetime, timezone

REPO_ROOT = Path(__file__).resolve().parents[1]
RES_DIR   = REPO_ROOT / "results" / "backtests"
DOCS_DIR  = REPO_ROOT / "docs" / "backtests"

def log(m): print(f"[report] {m}", flush=True)

def read_csv(path: Path) -> list[dict]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def pick_winner(rows: list[dict]) -> dict | None:
    if not rows: return None
    # cast numerics
    def num(x): 
        try: return float(x)
        except: return 0.0
    rows2 = [{
        **r,
        "roi": num(r.get("roi")),
        "sharpe": num(r.get("sharpe")),
        "n_bets": int(float(r.get("n_bets",0))),
    } for r in rows]
    rows2.sort(key=lambda r: (r["sharpe"], r["roi"], r["n_bets"]), reverse=True)
    return rows2[0]

def render_table(rows: list[dict]) -> str:
    if not rows:
        return "<p>No backtest results available.</p>"
    # Show top 25 by Sharpe
    rows = sorted(rows, key=lambda r: (float(r.get('sharpe',0)), float(r.get('roi',0))), reverse=True)[:25]
    cols = ["cfg_id","n_bets","total_staked","pnl","roi","hitrate","sharpe","end_bankroll"]
    th = "".join(f"<th>{c}</th>" for c in cols)
    trs = []
    for r in rows:
        tds = "".join(f"<td>{r.get(c,'')}</td>" for c in cols)
        trs.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{th}</tr></thead><tbody>{''.join(trs)}</tbody></table>"

def build_html(summary: list[dict], winner: dict | None) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    win_block = "<p>No winner found.</p>"
    if winner:
        cfg_id = winner["cfg_id"]
        win_block = f"""
        <h3>Recommended Config (cfg {cfg_id})</h3>
        <pre>{json.dumps(winner, indent=2)}</pre>
        <p>Params: see <code>results/backtests/params_cfg{cfg_id}.json</code></p>
        <p>Picks:  <code>results/backtests/logs/picks_cfg{cfg_id}.csv</code></p>
        """
    return f"""
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Tennis Bot Backtest</title>
  <style>
    body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial; margin: 24px; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: right; }}
    th {{ background: #f5f5f5; }}
    h1, h2, h3 {{ margin: 0.2em 0; }}
    pre {{ background: #f6f8fa; padding: 12px; overflow: auto; }}
  </style>
</head>
<body>
  <h1>Tennis Bot — Backtest Report</h1>
  <p><em>Generated {ts}</em></p>
  {win_block}
  <h3>Top Results</h3>
  {render_table(summary)}
</body>
</html>
"""

def main():
    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    summ = RES_DIR / "summary.csv"
    rows = read_csv(summ)
    winner = pick_winner(rows)
    # write winner json (optional)
    if winner:
        (RES_DIR / "winner.json").write_text(json.dumps(winner, indent=2))
    # write html
    html = build_html(rows, winner)
    (DOCS_DIR / "index.html").write_text(html, encoding="utf-8")
    log(f"Report written → {DOCS_DIR/'index.html'}")

if __name__ == "__main__":
    main()
