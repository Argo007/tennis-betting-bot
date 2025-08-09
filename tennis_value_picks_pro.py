#!/usr/bin/env python3
# Tennis value picks PRO: market odds + Bayesian-ish player model (surface Elo, recency, optional serve/return)
# Always writes CSV: value_picks_pro_YYYYMMDD.csv

import os, sys, argparse, json, math, io
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional

import pandas as pd
import requests

DEFAULT_REGION = "eu"

# Elo sources (best-effort; script keeps working if they fail)
DEFAULT_ELO_MEN = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_elo.csv"
DEFAULT_ELO_WOMEN = "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_elo.csv"

SURFACES = {"hard", "clay", "grass"}

def logit(p: float) -> float:
    p = min(max(p, 1e-6), 1 - 1e-6)
    return math.log(p / (1 - p))

def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))

def implied_prob(decimal_odds: float) -> Optional[float]:
    try:
        o = float(decimal_odds)
        return 1.0 / o if o > 1 else None
    except Exception:
        return None

def list_tennis_keys(api_key: str) -> List[str]:
    """Ask Odds API which tennis tournament keys are active (ATP & WTA)."""
    url = "https://api.the-odds-api.com/v4/sports"
    r = requests.get(url, params={"apiKey": api_key}, timeout=30)
    r.raise_for_status()
    sports = r.json()
    keys: List[str] = []
    for s in sports:
        key = str(s.get("key", ""))
        group = (s.get("group") or "").lower()
        if key.startswith("tennis_") and ("atp" in key or "wta" in key):
            if s.get("active", True):
                keys.append(key)
    return keys

def get_odds(api_key: str, sport_key: str, region: str) -> List[Dict[str, Any]]:
    url = f"https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
    params = {"apiKey": api_key, "regions": region, "markets": "h2h", "oddsFormat": "decimal"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    print(f"[DEBUG] fetched {len(data)} events for {sport_key} region={region}")
    return data

def load_csv(path_or_url: str) -> pd.DataFrame:
    if path_or_url.startswith("http"):
        r = requests.get(path_or_url, timeout=60)
        r.raise_for_status()
        return pd.read_csv(io.StringIO(r.text))
    return pd.read_csv(path_or_url)

def normalize_player_name(name: str) -> str:
    return " ".join(name.strip().replace("-", " ").split()).lower()

def latest_elo_by_player(df: pd.DataFrame, surface: str) -> pd.DataFrame:
    cols = {c.lower(): c for c in df.columns}
    name_col = cols.get("player") or cols.get("player_name") or list(df.columns)[0]
    date_col = cols.get("date") or "date"
    s_col = {"hard": "elo_hard", "clay": "elo_clay", "grass": "elo_grass"}.get(surface, "elo")
    if s_col not in df.columns:
        s_col = "elo"
    sdf = df[[name_col, date_col, s_col]].copy()
    sdf.columns = ["player", "date", "elo"]
    sdf["date"] = pd.to_datetime(sdf["date"], errors="coerce")
    sdf = sdf.sort_values(["player", "date"]).dropna(subset=["player"])
    last = sdf.groupby("player", as_index=False).tail(1)
    last["name_key"] = last["player"].map(normalize_player_name)
    return last[["name_key", "elo"]]

def recency_form(df_all: pd.DataFrame, surface: str, window_days: int = 120) -> pd.DataFrame:
    cols = {c.lower(): c for c in df_all.columns}
    name_col = cols.get("player") or cols.get("player_name") or list(df_all.columns)[0]
    date_col = cols.get("date") or "date"
    s_col = {"hard": "elo_hard", "clay": "elo_clay", "grass": "elo_grass"}.get(surface, "elo")
    if s_col not in df_all.columns:
        s_col = "elo"
    df = df_all[[name_col, date_col, s_col]].copy()
    df.columns = ["player", "date", "elo"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=window_days)
    df = df[df["date"] >= cutoff].dropna(subset=["date"])
    df = df.sort_values(["player", "date"])
    grp = df.groupby("player")
    momentum = (grp["elo"].last() - grp["elo"].min()).to_frame("form_raw")
    momentum["form"] = (momentum["form_raw"] / 100.0).clip(-1, 1)
    momentum = momentum.reset_index()
    momentum["name_key"] = momentum["player"].map(normalize_player_name)
    return momentum[["name_key", "form"]]

def load_serve_return(path: Optional[str]) -> pd.DataFrame:
    if not path:
        return pd.DataFrame(columns=["name_key", "surface", "srv", "ret"])
    df = load_csv(path)
    cols = {c.lower(): c for c in df.columns}
    name_col = cols.get("player") or cols.get("name") or list(df.columns)[0]
    surf_col = cols.get("surface", "surface")
    srv_col = cols.get("serve_pts_won") or cols.get("srv") or cols.get("first_serve_points_won")
    ret_col = cols.get("return_pts_won") or cols.get("ret") or cols.get("return_points_won")
    tmp = df[[name_col, surf_col, srv_col, ret_col]].copy()
    tmp.columns = ["player", "surface", "srv", "ret"]
    tmp["name_key"] = tmp["player"].map(normalize_player_name)
    out = []
    for s, g in tmp.groupby("surface"):
        if len(g) >= 10:
            g = g.copy()
            for col in ["srv", "ret"]:
                g[col] = (g[col] - g[col].mean()) / (g[col].std(ddof=0) + 1e-9)
            out.append(g)
    if out:
        tmp = pd.concat(out, ignore_index=True)
    return tmp[["name_key", "surface", "srv", "ret"]]

def consensus_from_books(bookmakers: List[Dict[str, Any]], preferred: List[str]) -> Tuple[Dict[str, float], Dict[str, float]]:
    probs: Dict[str, List[float]] = {}
    best_price: Dict[str, float] = {}
    for b in bookmakers or []:
        book_key = (b.get("key") or b.get("bookmaker", {}).get("key") or "").lower()
        weight = 1.15 if book_key in preferred else 1.0
        for m in b.get("markets", []):
            if m.get("key") != "h2h":
                continue
            for oc in m.get("outcomes", []):
                name = oc.get("name"); price = oc.get("price")
                if not (name and price):
                    continue
                ip = implied_prob(price)
                if ip:
                    probs.setdefault(name, []).append(ip * weight)
                best_price[name] = max(best_price.get(name, 0.0), float(price))
    consensus = {k: sum(v) / len(v) for k, v in probs.items() if v}
    if len(consensus) == 2:
        total = sum(consensus.values())
        if total > 0:
            for k in list(consensus.keys()):
                consensus[k] /= total
    return consensus, best_price

def model_prob_from_elo(p1_elo: float, p2_elo: float, form1: float, form2: float, srv1: float, srv2: float,
                        k_factor: float = 0.004) -> float:
    base = sigmoid(k_factor * (p1_elo - p2_elo))
    adj = 0.05 * (form1 - form2) + 0.05 * (srv1 - srv2)
    p = min(max(base + adj, 1e-4), 1 - 1e-4)
    return p

def blended_prob(p_market: float, p_model: float, w_market: float, w_model: float) -> float:
    z = w_market * logit(p_market) + w_model * logit(p_model)
    return sigmoid(z)

def kelly_fraction(p: float, odds: float, cap: float) -> float:
    b = max(0.0, odds - 1.0); q = 1.0 - p
    f = ((b * p) - q) / b if b > 0 else 0.0
    return max(0.0, min(cap, f))

def build_surface_map(path: Optional[str]) -> Dict[str, str]:
    if not path or not os.path.exists(path):
        return {
            "wimbledon": "grass", "roland garros": "clay", "french open": "clay",
            "madrid": "clay", "rome": "clay", "monte-carlo": "clay",
            "australian open": "hard", "us open": "hard",
            "indian wells": "hard", "miami": "hard", "cincinnati": "hard",
            "toronto": "hard", "montreal": "hard", "washington": "hard",
            "queens": "grass", "halle": "grass", "stuttgart": "grass", "eastbourne": "grass",
        }
    with open(path, "r", encoding="utf-8") as f:
        m = json.load(f)
        return {k.lower(): v.lower() for k, v in m.items()}

# ----------------------------- MAIN -----------------------------

def main():
    ap = argparse.ArgumentParser(description="Tennis value picks PRO (market + Bayesian player model)")
    ap.add_argument("--region", default=os.getenv("ODDS_REGION", DEFAULT_REGION))
    ap.add_argument("--min-fav", type=float, default=float(os.getenv("MIN_FAVORITE", 0.55)))
    ap.add_argument("--max-dog", type=float, default=float(os.getenv("MAX_DOG", 0.45)))
    ap.add_argument("--prefer-books", default=os.getenv("PREFER_BOOKS", "pinnacle,betfair"))
    ap.add_argument("--elo-men-url", default=os.getenv("ELO_MEN_URL", DEFAULT_ELO_MEN))
    ap.add_argument("--elo-women-url", default=os.getenv("ELO_WOMEN_URL", DEFAULT_ELO_WOMEN))
    ap.add_argument("--surface-map", default=os.getenv("SURFACE_MAP", ""))
    ap.add_argument("--default-surface", default=os.getenv("DEFAULT_SURFACE", "hard"), choices=list(SURFACES))
    ap.add_argument("--serve-file", default=os.getenv("SERVE_FILE", ""))
    ap.add_argument("--market-weight", type=float, default=float(os.getenv("MARKET_WEIGHT", 0.6)))
    ap.add_argument("--model-weight", type=float, default=float(os.getenv("MODEL_WEIGHT", 0.4)))
    ap.add_argument("--kelly", type=float, default=float(os.getenv("KELLY_CAP", 0.25)))
    ap.add_argument("--lookahead-h", type=int, default=int(os.getenv("LOOKAHEAD_H", 120)))
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        print("ERROR: Set ODDS_API_KEY.", file=sys.stderr)
        sys.exit(1)

    # Discover active tennis tournament keys (fixes UNKNOWN_SPORT)
    sport_keys = list_tennis_keys(api_key)
    if not sport_keys:
        out_path = args.out or f"value_picks_pro_{datetime.utcnow().strftime('%Y%m%d')}.csv"
        pd.DataFrame(columns=[
            "tour","surface","event_id","commence_time_utc","player","opponent",
            "market_prob","model_prob","blended_prob","best_odds",
            "ev_per_unit","kelly_fraction","confidence"
        ]).to_csv(out_path, index=False)
        print("No active ATP/WTA sport keys from Odds API. Wrote empty CSV.")
        return

    preferred = [b.strip().lower() for b in args.prefer_books.split(",") if b.strip()]

    # Load Elo & form (best-effort)
    try:
        elo_men_all = load_csv(args.elo_men_url)
    except Exception as e:
        print(f"WARNING: men Elo CSV load failed: {e}", file=sys.stderr)
        elo_men_all = pd.DataFrame()
    try:
        elo_women_all = load_csv(args.elo_women_url)
    except Exception as e:
        print(f"WARNING: women Elo CSV load failed: {e}", file=sys.stderr)
        elo_women_all = pd.DataFrame()

    surface_map = build_surface_map(args.surface_map)
    serve_df = load_serve_return(args.serve_file)

    def pack_elo(surface: str, tour: str):
        if (tour == "ATP" and not elo_men_all.empty) or (tour == "WTA" and not elo_women_all.empty):
            df_all = elo_men_all if tour == "ATP" else elo_women_all
            last = latest_elo_by_player(df_all, surface)
            form = recency_form(df_all, surface)
            ref = last.merge(form, on="name_key", how="left")
            ref["form"] = ref["form"].fillna(0.0)
            return ref
        return pd.DataFrame(columns=["name_key", "elo", "form"])

    rows = []
    now_utc = datetime.now(timezone.utc)

    for key in sport_keys:
        tour = "ATP" if "atp" in key else "WTA"
        try:
            raw = get_odds(api_key, key, args.region)
        except Exception as e:
            print(f"Error fetching odds for {tour} ({key}): {e}", file=sys.stderr)
            continue

        ref_cache: Dict[str, pd.DataFrame] = {}
        for ev in raw:
            league = (ev.get("sport_title") or ev.get("sport_key") or "")
            title = f"{ev.get('home_team','')} vs {ev.get('away_team','')}"
            commence = ev.get("commence_time")
            try:
                dt = datetime.fromisoformat(str(commence).replace("Z", "+00:00"))
            except Exception:
                dt = now_utc
            if (dt - now_utc).total_seconds() > args.lookahead_h * 3600:
                continue

            # Infer surface from text; fallback to default
            surface = None
            for k, v in surface_map.items():
                if k in (title + " " + league).lower():
                    surface = v; break
            if not surface:
                surface = args.default_surface

            if surface not in ref_cache:
                ref_cache[surface] = pack_elo(surface, tour)
            ref = ref_cache[surface]

            bookmakers = ev.get("bookmakers") or ev.get("sites") or []
            consensus, best_price = consensus_from_books(bookmakers, preferred)
            if len(consensus) != 2:
                continue

            p1, p2 = list(consensus.keys())
            p1_key, p2_key = normalize_player_name(p1), normalize_player_name(p2)

            p1_e = ref[ref["name_key"] == p1_key]["elo"]
            p2_e = ref[ref["name_key"] == p2_key]["elo"]
            p1_f = ref[ref["name_key"] == p1_key]["form"]
            p2_f = ref[ref["name_key"] == p2_key]["form"]

            if p1_e.empty or p2_e.empty:
                p_model = None
            else:
                srv1 = srv2 = 0.0
                if not serve_df.empty:
                    s1 = serve_df[(serve_df["name_key"] == p1_key) & (serve_df["surface"] == surface)]
                    s2 = serve_df[(serve_df["name_key"] == p2_key) & (serve_df["surface"] == surface)]
                    if not s1.empty:
                        srv1 = float(s1["srv"].iloc[0]) + float(s1["ret"].iloc[0]) * 0.5
                    if not s2.empty:
                        srv2 = float(s2["srv"].iloc[0]) + float(s2["ret"].iloc[0]) * 0.5
                p_model = sigmoid(0.004 * (float(p1_e.iloc[0]) - float(p2_e.iloc[0])))  # base
                if not p1_f.empty and not p2_f.empty:
                    p_model = min(max(p_model + 0.05 * (float(p1_f.iloc[0]) - float(p2_f.iloc[0])) , 1e-4), 1-1e-4)
                p_model = min(max(p_model + 0.05 * (srv1 - srv2) , 1e-4), 1-1e-4)

            p_mkt_1 = consensus[p1]
            p_blend_1 = sigmoid(0.6 * logit(p_mkt_1) + 0.4 * logit(p_model)) if p_model is not None else p_mkt_1
            p_blend_2 = 1.0 - p_blend_1
            best1, best2 = best_price.get(p1, 0.0), best_price.get(p2, 0.0)

            ev1 = best1 * p_blend_1 - (1 - p_blend_1) if best1 > 0 else None
            ev2 = best2 * p_blend_2 - (1 - p_blend_2) if best2 > 0 else None
            def kelly(p, o): 
                b = max(0.0, o-1.0); q = 1.0-p
                return max(0.0, min(0.25, ((b*p)-q)/b if b>0 else 0.0))
            f1, f2 = kelly(p_blend_1, best1), kelly(p_blend_2, best2)

            def conf(p_mkt, p_mod):
                if p_mod is None: return 50
                d = abs(p_mkt - p_mod); return int(max(10, min(95, 70 - 100*d)))

            rows.append({
                "tour": tour, "surface": surface, "event_id": ev.get("id"),
                "commence_time_utc": dt.isoformat(),
                "player": p1, "opponent": p2,
                "market_prob": round(p_mkt_1,4),
                "model_prob": round(p_model,4) if p_model is not None else None,
                "blended_prob": round(p_blend_1,4),
                "best_odds": round(best1,3),
                "ev_per_unit": round(ev1,4) if ev1 is not None else None,
                "kelly_fraction": round(f1,4), "confidence": conf(p_mkt_1, p_model)
            })
            rows.append({
                "tour": tour, "surface": surface, "event_id": ev.get("id"),
                "commence_time_utc": dt.isoformat(),
                "player": p2, "opponent": p1,
                "market_prob": round(1.0-p_mkt_1,4),
                "model_prob": round(1.0-p_model,4) if p_model is not None else None,
                "blended_prob": round(p_blend_2,4),
                "best_odds": round(best2,3),
                "ev_per_unit": round(ev2,4) if ev2 is not None else None,
                "kelly_fraction": round(f2,4), "confidence": conf(p_mkt_1, p_model)
            })

    out_path = args.out or f"value_picks_pro_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    if not rows:
        pd.DataFrame(columns=[
            "tour","surface","event_id","commence_time_utc","player","opponent",
            "market_prob","model_prob","blended_prob","best_odds",
            "ev_per_unit","kelly_fraction","confidence"
        ]).to_csv(out_path, index=False)
        print(f"No qualifying events in the next {args.lookahead_h}h. Wrote empty CSV to {out_path}.")
        return

    df = pd.DataFrame(rows)
    favs = df[df["blended_prob"] >= args.min_fav].sort_values(["ev_per_unit","confidence"], ascending=False)
    dogs = df[df["blended_prob"] <= args.max_dog].sort_values(["ev_per_unit","confidence"], ascending=False)
    def top_row(dfi): return dfi.head(1).to_dict(orient="records")

    picks = {
        "favorite_overall": top_row(favs),
        "underdog_overall": top_row(dogs),
        "ATP_favorite": top_row(favs[favs["tour"]=="ATP"]),
        "ATP_underdog": top_row(dogs[dogs["tour"]=="ATP"]),
        "WTA_favorite": top_row(favs[favs["tour"]=="WTA"]),
        "WTA_underdog": top_row(dogs[dogs["tour"]=="WTA"]),
    }

    df.to_csv(out_path, index=False)

    def fmt(r):
        if not r: return "None"
        r = r[0]
        return (f"{r['tour']} {r['surface']} | {r['player']} vs {r['opponent']} | "
                f"p_blend={r['blended_prob']:.3f} | best {r['best_odds']:.2f} | "
                f"EV/unit={r['ev_per_unit']:.3f} | Kelly={r['kelly_fraction']:.3f} | "
                f"conf={r['confidence']} | {r['commence_time_utc']}")

    print("\n=== Top Favorite (Overall) ===\n" + fmt(picks["favorite_overall"]))
    print("\n=== Top Underdog (Overall) ===\n" + fmt(picks["underdog_overall"]))
    print("\n=== ATP Favorite ===\n" + fmt(picks["ATP_favorite"]))
    print("\n=== ATP Underdog ===\n" + fmt(picks["ATP_underdog"]))
    print("\n=== WTA Favorite ===\n" + fmt(picks["WTA_favorite"]))
    print("\n=== WTA Underdog ===\n" + fmt(picks["WTA_underdog"]))
    print(f"\nSaved: {out_path}")

if __name__ == "__main__":
    main()
