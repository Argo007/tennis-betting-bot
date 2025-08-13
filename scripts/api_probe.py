#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, textwrap
from datetime import datetime, timezone
from pathlib import Path

import requests

API_KEY  = os.getenv("ODDS_API_KEY", "").strip()
REGIONS  = os.getenv("REGIONS", "eu,uk,us,au")
MARKETS  = os.getenv("MARKETS", "h2h,spreads,totals")
SPORTS   = ["tennis_atp", "tennis_wta"]  # probe ATP & WTA directly
OUT_DIR  = Path(os.getenv("OUT_DIR", "outputs"))
OUT_FILE = OUT_DIR / "api_probe.md"

def fetch(sport_key: str):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {
        "apiKey": API_KEY,
        "regions": REGIONS,
        "markets": MARKETS,
        "oddsFormat": "decimal",
        "dateFormat": "iso",
    }
    try:
        r = requests.get(url, params=params, timeout=30)
        return r
    except requests.RequestException as e:
        # return a fake-like response object
        class R:
            status_code = 0
            headers = {}
            def json(self): return {"error": str(e)}
            text = str(e)
        return R()

def summarize(events):
    h2h = spreads = totals = 0
    for ev in events:
        for bm in ev.get("bookmakers", []):
            for mk in bm.get("markets", []):
                k = mk.get("key")
                if k == "h2h":      h2h += 1
                elif k == "spreads": spreads += 1
                elif k == "totals":  totals += 1
    return h2h, spreads, totals

def main():
    if not API_KEY:
        print("ERROR: ODDS_API_KEY not set.", file=sys.stderr)
        sys.exit(1)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    lines = [f"# API Probe",
             f"",
             f"- Time (UTC): {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
             f"- Regions: `{REGIONS}`",
             f"- Markets: `{MARKETS}`",
             ""]

    for sport in SPORTS:
        r = fetch(sport)
        ok = (r.status_code == 200)
        try:
            data = r.json() if ok else []
        except Exception:
            data = []
        ev_count = len(data) if isinstance(data, list) else 0
        h2h, spr, tot = summarize(data if isinstance(data, list) else [])

        lines.append(f"## {sport}")
        lines.append(f"- status: **{r.status_code}**")
        lines.append(f"- events: **{ev_count}**")
        lines.append(f"- markets: h2h={h2h}, spreads={spr}, totals={tot}")

        # quota headers (if present)
        for k in ("x-requests-remaining", "x-requests-used", "x-requests-reset"):
            if k in r.headers:
                lines.append(f"- {k}: {r.headers[k]}")

        # first event snapshot
        if ev_count:
            ev = data[0]
            snap = {
                "sport_title": ev.get("sport_title"),
                "away_team": ev.get("away_team"),
                "home_team": ev.get("home_team"),
                "commence_time": ev.get("commence_time"),
                "bookmakers_count": len(ev.get("bookmakers", [])),
            }
            lines.append("")
            lines.append("### first_event")
            lines.append("```json")
            lines.append(json.dumps(snap, indent=2))
            lines.append("```")

        lines.append("")

        # if error, include body for visibility
        if not ok:
            lines.append("### error_body")
            body = r.text
            if len(body) > 2000:
                body = body[:2000] + "... (truncated)"
            lines.append("```")
            lines.append(body)
            lines.append("```")
            lines.append("")

    OUT_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("\n".join(lines))
    print(f"\nWrote {OUT_FILE}")

if __name__ == "__main__":
    main()
