- name: Write dashboard script
  shell: bash
  run: |
    mkdir -p scripts
    cat > scripts/make_dashboard.py <<'PY'
#!/usr/bin/env python3
import os, csv
from pathlib import Path
from datetime import datetime

STATE_DIR = os.environ.get("STATE_DIR",".state")
DOCS = os.environ.get("DOCS_DIR","docs")
HTML = os.path.join(DOCS,"index.html")
PICKS = os.environ.get("PICKS_FILE","picks_live.csv")
TRADE_LOG = os.path.join(STATE_DIR,"trade_log.csv")
SETTLED = os.path.join(STATE_DIR,"settled_trades.csv")
GOAL = float(os.environ.get("EDGE_GOAL","8.0") or 8.0)

def read_csv(p):
    try:
        if (not os.path.isfile(p)) or os.path.getsize(p)==0: return []
        return list(csv.DictReader(open(p)))
    except: return []

def fnum(x):
    try:
        s=str(x).strip()
        if s.endswith('%'): return float(s[:-1])/100.0
        return float(s)
    except: return None

def td(s): return f"<td>{s}</td>"

def table(rows, cols):
    if not rows: return "<i>(none)</i>"
    head="<tr>"+ "".join(f"<th>{c}</th>" for c in cols)+"</tr>"
    body=[]
    for r in rows:
        body.append("<tr>"+ "".join(td(r.get(c,"")) for c in cols)+"</tr>")
    return f"<table>{head}{''.join(body)}</table>"

def bucket(e):
    try:
        e=float(e)
        if e>=0.05: return "🟢"
        if e>=0.02: return "🟡"
        if e<0: return "🔴"
    except: pass
    return "⚪️"

def main():
    picks=read_csv(PICKS); trades=read_csv(TRADE_LOG); settled=read_csv(SETTLED)
    total_edge=0.0; total_kelly=0.0; top=[]
    for r in picks:
        e=fnum(r.get("edge")); k=fnum(r.get("kelly_stake")); o=fnum(r.get("odds")); ip=fnum(r.get("implied_p"))
        total_edge+=(e or 0.0); total_kelly+=(k or 0.0)
        top.append({
          "🏷": bucket(e),
          "match": r.get("match","—"),
          "selection": r.get("selection","—"),
          "odds": f"{o:.2f}" if o else (r.get("odds") or ""),
          "implied_p": f"{(ip*100):.1f}%" if ip else "",
          "edge": f"{e:.4f}" if e is not None else "",
          "kelly€": f"{k:.2f}" if k else "",
        })
    top=sorted(top, key=lambda x: float(x.get("edge") or -1e9), reverse=True)[:20]
    total_pts=total_edge*100; prog=int(max(0,min(100,(total_pts/GOAL)*100))) if GOAL>0 else 0
    bar="█"*(prog//10)+"░"*(10-prog//10)
    now=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%SZ")
    css="""
    body{font-family:system-ui,Segoe UI,Arial,sans-serif;max-width:950px;margin:2rem auto;padding:0 1rem}
    table{border-collapse:collapse;width:100%;margin:.5rem 0}
    th,td{border:1px solid #ddd;padding:6px 8px;font-size:14px}
    th{background:#f6f6f6;text-align:left}
    .progress{font-family:monospace}
    """
    html=f"""<!doctype html><meta charset="utf-8"><title>Tennis Engine — Dashboard</title>
    <style>{css}</style>
    <h1>Tennis Engine — Run Summary <span style='color:#666;font-size:14px'>({now})</span></h1>
    <p><b>Total Edge:</b> {total_pts:.2f} pts / Goal {GOAL:.2f}
       <span class="progress"> {bar} {prog}%</span> • <b>Total Kelly:</b> €{total_kelly:.2f}</p>
    <h2>🔥 Top Picks by Edge</h2>{table(top,["🏷","match","selection","odds","implied_p","edge","kelly€"])}
    <h2>Last 20 Trades</h2>{table(trades[-20:],["ts","match","selection","odds","edge","stake"])}
    <h2>Last 20 Settlements</h2>{table(settled[-20:],["ts","match","selection","odds","edge","stake","result","pnl","clv"])}
    """
    Path(DOCS).mkdir(parents=True, exist_ok=True)
    Path(HTML).write_text(html, encoding="utf-8")
    print(f"Wrote dashboard -> {HTML}")

if __name__=="__main__": main()
PY
    chmod +x scripts/make_dashboard.py
