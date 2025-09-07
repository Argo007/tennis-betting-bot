#!/usr/bin/env python3
"""
Render backtest HTML with graceful empty-state + diagnostics.
"""

from __future__ import annotations
import csv, json
from pathlib import Path
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
BT   = ROOT / "results" / "backtests"
DOCS = ROOT / "docs" / "backtests"

def read_csv_rows(p: Path):
    if not p.exists() or p.stat().st_size==0:
        return []
    with p.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def num(x):
    try: return float(x)
    except: return 0.0

def pick_winner(rows):
    if not rows: return None
    rows2=[]
    for r in rows:
        rows2.append({
            **r,
            "cfg_id": int(float(r.get("cfg_id",0))),
            "n_bets": int(float(r.get("n_bets",0))),
            "roi": num(r.get("roi")),
            "sharpe": num(r.get("sharpe")),
        })
    rows2.sort(key=lambda r:(r["sharpe"], r["roi"], r["n_bets"]), reverse=True)
    return rows2[0] if rows2 else None

def render_table(rows):
    if not rows:
        return "<p>No backtest results available.</p>"
    cols = ["cfg_id","n_bets","total_staked","pnl","roi","hitrate","sharpe","end_bankroll"]
    head = "".join(f"<th>{c}</th>" for c in cols)
    body = []
    rows = sorted(rows, key=lambda r:(num(r.get("sharpe",0)), num(r.get("roi",0))), reverse=True)[:25]
    for r in rows:
        tds = "".join(f"<td>{r.get(c,'')}</td>" for c in cols)
        body.append(f"<tr>{tds}</tr>")
    return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body)}</tbody></table>"

def main():
    DOCS.mkdir(parents=True, exist_ok=True)
    summary = BT/"summary.csv"
    diags   = BT/"_diagnostics.json"

    rows = read_csv_rows(summary)
    winner = pick_winner(rows)
    diag_text = ""
    if diags.exists():
        d = json.loads(diags.read_text())
        diag_text = f"<pre>{json.dumps(d, indent=2)}</pre>"

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    win_html = "<p>No winner found.</p>" if not winner else f"""
      <h3>Recommended Config (cfg {winner['cfg_id']})</h3>
      <pre>{json.dumps(winner, indent=2)}</pre>
      <p>Params: results/backtests/params_cfg{winner['cfg_id']}.json</p>
      <p>Picks:  results/backtests/logs/picks_cfg{winner['cfg_id']}.csv</p>
    """

    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Tennis Bot — Backtest Report</title>
<style>
body{{font-family:system-ui,-apple-system,Segoe UI,Roboto,Arial;margin:24px}}
table{{border-collapse:collapse;width:100%}}
th,td{{border:1px solid #ddd;padding:8px;text-align:right}} th{{background:#f7f7f7}}
pre{{background:#f6f8fa;padding:10px;overflow:auto}}
</style></head><body>
<h1>Tennis Bot — Backtest Report</h1>
<p><em>Generated {ts}</em></p>
{win_html}
<h3>Top Results</h3>
{render_table(rows)}
<h3>Diagnostics</h3>
{diag_text or "<p>(none)</p>"}
</body></html>"""
    (DOCS/"index.html").write_text(html, encoding="utf-8")
    print(f"[report] wrote {DOCS/'index.html'}")

if __name__ == "__main__":
    main()

