#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Tennis Value Engine — H2H only (Realism-weighted)  ✅
- Only evaluates Head-to-Head (H2H) markets. Totals/Spreads removed.
- Uses Elo (ATP/WTA, 2023–2025) + per-player match counts to weight confidence.
- Confidence:
    * Elo-backed H2H rows: conf = min(1.0, min(matchesA, matchesB) / 30)
    * Market-only H2H rows: 0.6 if any sharp book used, else 0.3
- Rank score: score = EV * Kelly * Confidence
- YES bet if: Kelly >= KELLY_MIN and EV >= EV_MIN and Confidence >= MIN_CONF
- Auto-discovers active tennis tournament keys if generic keys return no events.
- Output: Markdown table (no "books" or "source" columns) + diagnostics.
"""

import os, io, unicodedata, requests, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# ---------------- ENV ----------------
API_KEY = os.getenv("ODDS_API_KEY", "").strip()
if not API_KEY:
    raise SystemExit("ODDS_API_KEY not set")

LOOKAHEAD_HOURS = int(os.getenv("LOOKAHEAD_HOURS", "48"))
REGIONS         = os.getenv("REGIONS", "eu,uk,us,au").replace(" ", "")
MARKETS         = os.getenv("MARKETS", "h2h").replace(" ", "")  # force default to H2H
SPORT_KEYS      = [s.strip() for s in os.getenv("SPORT_KEYS", "tennis,tennis_atp,tennis_wta").split(",") if s.strip()]

KELLY_MIN = float(os.getenv("KELLY_MIN", "0.05"))
EV_MIN    = float(os.getenv("EV_MIN", "0.00"))
MIN_CONF  = float(os.getenv("MIN_CONF", "0.40"))
TOP_ROWS  = int(os.getenv("TOP_ROWS", "25"))

DEFAULT_SHARP = "Pinnacle, Pinnacle Sports, bet365, Bet365, Unibet, Marathonbet, William Hill, Betfair Sportsbook, 888sport, 10Bet"
SHARP_BOOKS = [b.strip().lower() for b in os.getenv("SHARP_BOOKS", DEFAULT_SHARP).split(",") if b.strip()]

OUT_DIR        = Path(os.getenv("OUT_DIR", "outputs"))
SHORTLIST_FILE = os.getenv("SHORTLIST_FILE", "value_engine_shortlist.md")
LOCAL_TZ       = ZoneInfo(os.getenv("TZ", "Europe/Amsterdam"))

OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- Elo ----------------
START_ELO, K = 1500, 32
def _exp(a,b): return 1/(1+10**((b-a)/400))
def _upd(a,b,s): return a + K*(s-_exp(a,b))

SACK_ATP = [
 "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_2023.csv",
 "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_2024.csv",
 "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_2025.csv",
]
SACK_WTA = [
 "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_2023.csv",
 "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_2024.csv",
 "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_2025.csv",
]

def dl_csv(url):
    try:
        r = requests.get(url, timeout=30)
        if r.status_code==200 and r.text.strip():
            return pd.read_csv(io.StringIO(r.text))
    except requests.RequestException:
        pass
    return None

def build_elo_and_counts(urls):
    frames=[]
    for u in urls:
        df = dl_csv(u)
        if df is not None: frames.append(df)
    if not frames:
        return pd.DataFrame(columns=["player","elo"]), pd.DataFrame(columns=["player","matches"])
    df = pd.concat(frames, ignore_index=True)
    E={}
    C={}
    for _,r in df.iterrows():
        w,l = r.get("winner_name"), r.get("loser_name")
        if pd.isna(w) or pd.isna(l): continue
        ew, el = E.get(w,START_ELO), E.get(l,START_ELO)
        E[w] = _upd(ew,el,1); E[l] = _upd(el,ew,0)
        C[w] = C.get(w,0)+1; C[l] = C.get(l,0)+1
    elo_df = pd.DataFrame([{"player":k,"elo":v} for k,v in E.items()])
    cnt_df = pd.DataFrame([{"player":k,"matches":v} for k,v in C.items()])
    return elo_df, cnt_df

def ensure_elo():
    Path("data").mkdir(exist_ok=True)
    if not Path("data/atp_elo.csv").exists() or not Path("data/atp_matches.csv").exists():
        elo, cnt = build_elo_and_counts(SACK_ATP)
        if not elo.empty:
            elo.sort_values("elo", ascending=False).to_csv("data/atp_elo.csv", index=False)
            cnt.to_csv("data/atp_matches.csv", index=False)
    if not Path("data/wta_elo.csv").exists() or not Path("data/wta_matches.csv").exists():
        elo, cnt = build_elo_and_counts(SACK_WTA)
        if not elo.empty:
            elo.sort_values("elo", ascending=False).to_csv("data/wta_elo.csv", index=False)
            cnt.to_csv("data/wta_matches.csv", index=False)

def norm_name(s: str) -> str:
    if not s: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    keep = set("abcdefghijklmnopqrstuvwxyz -.'")
    s = "".join(ch for ch in s.lower() if ch in keep)
    return " ".join(s.split())

def load_index(path_csv, val_col):
    df = pd.read_csv(path_csv) if Path(path_csv).exists() else pd.DataFrame(columns=["player",val_col])
    return {norm_name(r["player"]): float(r[val_col]) for _,r in df.iterrows()}

def p_model(player_a: str, player_b: str, idx_elo: dict):
    e1 = idx_elo.get(norm_name(player_a))
    e2 = idx_elo.get(norm_name(player_b))
    if e1 is None or e2 is None: return None
    return _exp(e1, e2)

def conf_from_matches(pa: str, pb: str, idx_cnt: dict):
    a = idx_cnt.get(norm_name(pa), 0.0)
    b = idx_cnt.get(norm_name(pb), 0.0)
    return min(1.0, min(a,b)/30.0)

# ------------- Odds API + diagnostics -------------
def fetch_odds_with_diag(sport_key: str):
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = dict(
        apiKey=API_KEY,
        regions=REGIONS,
        markets=MARKETS,           # "h2h"
        oddsFormat="decimal",
        dateFormat="iso",
    )
    status = None
    headers = {}
    body_snippet = ""
    data = []
    try:
        r = requests.get(url, params=params, timeout=25)
        status = r.status_code
        headers = {k.lower(): v for k,v in r.headers.items()}
        if status == 200:
            j = r.json()
            data = j if isinstance(j, list) else []
        else:
            txt = r.text or ""
            body_snippet = (txt[:400] + "...") if len(txt) > 400 else txt
    except requests.RequestException as e:
        status = 0
        body_snippet = str(e)
    return data, status, headers, body_snippet

def list_tennis_keys(api_key: str):
    try:
        r = requests.get("https://api.the-odds-api.com/v4/sports", params={"apiKey": api_key}, timeout=30)
        if r.status_code != 200:
            return []
        out = []
        for s in r.json():
            key = str(s.get("key",""))
            if key.startswith("tennis_") and (("atp" in key) or ("wta" in key)) and s.get("active", True):
                out.append(key)
        return out
    except requests.RequestException:
        return []

def within_window(commence_iso: str) -> bool:
    if not commence_iso: return False
    start = datetime.fromisoformat(commence_iso.replace("Z","+00:00"))
    now = datetime.now(timezone.utc)
    return timedelta(0) <= (start - now) <= timedelta(hours=LOOKAHEAD_HOURS)

def fair_two_way(odds_a: float, odds_b: float):
    ia, ib = 1/odds_a, 1/odds_b
    s = ia+ib
    if s <= 0: return 0.5,0.5,0.5,0.5
    return ia, ib, ia/s, ib/s  # implied, implied, no-vig pA, pB

def kelly(p: float, o: float):
    b = o-1.0
    return max(0.0, (b*p - (1-p))/b) if b>0 else 0.0

# ---------------- helpers ----------------
def is_sharp_book(name: str) -> bool:
    n = (name or "").lower()
    return any(tag in n for tag in SHARP_BOOKS)

def add_row(rows, *, tour, market, selection, opponent, odds, p_model_val, p_fair, start_utc,
            conf, yes_rule=True):
    evu = ""
    kf  = ""
    bet = "NO"
    score = -1e9
    p_used = p_model_val if (p_model_val is not None) else p_fair
    if p_used is not None:
        evu = p_used * odds - 1.0
        kf  = kelly(p_used, odds)
        if yes_rule and (kf >= KELLY_MIN and evu >= EV_MIN and conf >= MIN_CONF):
            bet = "YES"
        score = evu * max(kf, 0.0) * conf
    rows.append({
        "tour": tour, "market": market, "selection": selection, "opponent": opponent or "",
        "odds": float(odds), "p_model": ("" if p_model_val is None else float(p_model_val)),
        "p_fair": float(p_fair) if p_fair is not None else "",
        "evu": ("" if evu=="" else float(evu)), "kelly": ("" if kf=="" else float(kf)),
        "bet": bet, "start_utc": start_utc, "score": score, "conf": conf
    })

def rowline(r):
    pm  = f"{r['p_model']:.3f}" if isinstance(r["p_model"], float) else (r["p_model"] or "")
    pf  = f"{r['p_fair']:.3f}"  if isinstance(r["p_fair"],  float) else (r["p_fair"]  or "")
    ev  = f"{r['evu']:.3f}"     if r["evu"]   != "" else ""
    kel = f"{r['kelly']:.3f}"   if r["kelly"] != "" else ""
    return (
        f"| {r['tour']} | {r['market']} | {r['selection']} | {r.get('opponent') or '—'} | "
        f"{r['odds']:.2f} | {pm} | {pf} | {ev} | {kel} | {r['conf']:.2f} | {r['bet']} | {r['start_utc']} |"
    )

def write_output(table_rows, diag_lines):
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    now_loc = datetime.now(timezone.utc).astimezone(LOCAL_TZ).strftime(f"%Y-%m-%d %H:%M {LOCAL_TZ.key.split('/')[-1]}")
    head = [
        "# Tennis Value Engine (H2H only, Realism-weighted)",
        "",
        f"Updated: {now_loc} ({now_utc})",
        "",
        "| Tour | Market | Selection | Opponent | Odds | p_model | p_fair | EV/u | Kelly | Conf | Bet | Start (UTC) |",
        "|---|---|---|---|---:|---:|---:|---:|---:|---:|:---:|---|",
    ]
    rows = head + (table_rows if table_rows else [
        "| – | – | – | – | – | – | – | – | – | – | – | – |",
        "",
        "_No markets returned by the API in your window/regions. See diagnostics below._",
    ])
    if diag_lines:
        rows += ["", "## API Diagnostics", *diag_lines]
    out_path = OUT_DIR / SHORTLIST_FILE
    out_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    if os.getenv("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write("\n".join(rows) + "\n")
    print(f"Wrote {out_path}")

# ---------------- main ----------------
def run():
    # Build/load Elo + match counts
    ensure_elo()
    elo_atp = load_index("data/atp_elo.csv", "elo")
    elo_wta = load_index("data/wta_elo.csv", "elo")
    cnt_atp = load_index("data/atp_matches.csv", "matches")
    cnt_wta = load_index("data/wta_matches.csv", "matches")

    # Fetch with diagnostics across configured sport keys
    diag = []
    events_all = []
    for key in SPORT_KEYS:
        data, status, hdrs, body = fetch_odds_with_diag(key)
        events_all += data
        line = f"- `{key}` status: **{status}**, events: **{len(data)}**"
        rem = hdrs.get("x-requests-remaining"); used = hdrs.get("x-requests-used")
        if rem or used:
            line += f"  | quota remaining={rem or '?'} used={used or '?'}"
        diag.append(line)
        if status != 200 and body:
            diag.append("  error body: " + body)

    # Fallback: if nothing from static keys, auto-discover active tennis_* keys
    if not events_all:
        auto_keys = list_tennis_keys(API_KEY)
        if auto_keys:
            diag.append(f"- Auto-discovered keys: {', '.join(auto_keys)}")
            for key in auto_keys:
                data, status, hdrs, body = fetch_odds_with_diag(key)
                events_all += data
                line = f"- `{key}` status: **{status}**, events: **{len(data)}**"
                rem = hdrs.get("x-requests-remaining"); used = hdrs.get("x-requests-used")
                if rem or used:
                    line += f"  | quota remaining={rem or '?'} used={used or '?'}"
                diag.append(line)
                if status != 200 and body:
                    diag.append("  error body: " + body)
        else:
            diag.append("- Auto-discovery found no active ATP/WTA keys from provider.")

    total_before_window = 0
    h2h_pairs_after_window = 0

    # Build candidates with realism weights — H2H only
    cands = []

    for ev in events_all:
        total_before_window += 1
        ct = ev.get("commence_time")
        if not ct or not within_window(ct):
            continue
        start_utc = datetime.fromisoformat(ct.replace("Z","+00:00"))
        start_utc_str = start_utc.strftime("%Y-%m-%d %H:%M UTC")

        stitle = (ev.get("sport_title") or "").upper()
        tour_hint = "ATP" if "ATP" in stitle else ("WTA" if "WTA" in stitle else "GEN")

        # Aggregate best H2H prices with sharp filter pref
        def best_prices_h2h():
            grid = {}
            sharp_any = False
            for bm in ev.get("bookmakers", []):
                bname = bm.get("title","?")
                bsharp = is_sharp_book(bname)
                for mk in bm.get("markets", []):
                    if mk.get("key") != "h2h": continue
                    for out in mk.get("outcomes", []):
                        nm = out.get("name")
                        pr = out.get("price")
                        if nm is None or pr is None: continue
                        cell = grid.get(nm)
                        if cell is None or pr > cell["price"] or (bsharp and pr == cell["price"] and not cell["sharp"]):
                            grid[nm] = {"price": float(pr), "sharp": bsharp}
                            if bsharp: sharp_any = True
                        elif abs(pr - cell["price"]) < 1e-9:
                            cell["sharp"] = cell["sharp"] or bsharp
                            if bsharp: sharp_any = True
            if sharp_any:
                grid = {k:v for k,v in grid.items() if v["sharp"]}
            return grid

        h2h = best_prices_h2h()
        if len(h2h) == 2:
            (n1, a), (n2, b) = list(h2h.items())[0], list(h2h.items())[1]
            h2h_pairs_after_window += 1

            # Guess model tour
            pmA = pmB = None; confE = 0.0; model_tour = None
            def try_tour(code):
                if code=="ATP":
                    p1 = p_model(n1, n2, elo_atp); p2 = p_model(n2, n1, elo_atp)
                    if p1 is not None and p2 is not None:
                        c = conf_from_matches(n1, n2, cnt_atp)
                        return p1, p2, c, "ATP"
                if code=="WTA":
                    p1 = p_model(n1, n2, elo_wta); p2 = p_model(n2, n1, elo_wta)
                    if p1 is not None and p2 is not None:
                        c = conf_from_matches(n1, n2, cnt_wta)
                        return p1, p2, c, "WTA"
                return None, None, 0.0, None

            for code in ([tour_hint] if tour_hint in ("ATP","WTA") else []) + ["ATP","WTA"]:
                pmA, pmB, confE, model_tour = try_tour(code)
                if model_tour: break

            # Market no-vig
            _,_,pfA,pfB = fair_two_way(a["price"], b["price"])
            conf_mkt = 0.6 if (a["sharp"] or b["sharp"]) else 0.3

            # Elo-backed rows
            if model_tour:
                add_row(cands, tour=model_tour, market="H2H", selection=n1, opponent=n2,
                        odds=a["price"], p_model_val=pmA, p_fair=pfA, start_utc=start_utc_str,
                        conf=confE)
                add_row(cands, tour=model_tour, market="H2H", selection=n2, opponent=n1,
                        odds=b["price"], p_model_val=pmB, p_fair=pfB, start_utc=start_utc_str,
                        conf=confE)

            # Market-only rows
            add_row(cands, tour=(model_tour or tour_hint), market="H2H", selection=n1, opponent=n2,
                    odds=a["price"], p_model_val=None, p_fair=pfA, start_utc=start_utc_str,
                    conf=conf_mkt)
            add_row(cands, tour=(model_tour or tour_hint), market="H2H", selection=n2, opponent=n1,
                    odds=b["price"], p_model_val=None, p_fair=pfB, start_utc=start_utc_str,
                    conf=conf_mkt)

    # Diagnostics summary
    diag.append(f"- Events fetched (all keys): {len(events_all)} | Before window filter: {total_before_window} | H2H pairs after window: {h2h_pairs_after_window}")

    # Sort & limit
    cands.sort(key=lambda r: r["score"], reverse=True)
    top = cands[:TOP_ROWS]
    rows = [rowline(r) for r in top]

    write_output(rows, diag)

if __name__ == "__main__":
    run()
