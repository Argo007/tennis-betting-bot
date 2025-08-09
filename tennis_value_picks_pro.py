#!/usr/bin/env python3
import os, sys, argparse, json, math, io
from datetime import datetime, timezone
from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
import requests

ODDS_API_URL = "https://api.the-odds-api.com/v4/sports/{sport_key}/odds"
SPORT_KEYS = ["tennis_atp", "tennis_wta"]
DEFAULT_REGION = "eu"

# Default Elo sources (Jeff Sackmann)
DEFAULT_ELO_MEN = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_elo.csv"
DEFAULT_ELO_WOMEN = "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_elo.csv"

SURFACES = {"hard","clay","grass"}

def logit(p: float) -> float:
    p = min(max(p, 1e-6), 1-1e-6)
    return math.log(p/(1-p))

def sigmoid(x: float) -> float:
    return 1.0/(1.0+math.exp(-x))

def implied_prob(decimal_odds: float) -> Optional[float]:
    try:
        o = float(decimal_odds)
        return 1.0/o if o>1 else None
    except Exception:
        return None

def get_odds(api_key: str, sport_key: str, region: str) -> List[Dict[str, Any]]:
    params = {"apiKey": api_key, "regions": region, "markets": "h2h", "oddsFormat": "decimal"}
    url = ODDS_API_URL.format(sport_key=sport_key)
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def load_csv(path_or_url: str) -> pd.DataFrame:
    if path_or_url.startswith("http"):
        r = requests.get(path_or_url, timeout=60)
        r.raise_for_status()
        return pd.read_csv(io.StringIO(r.text))
    return pd.read_csv(path_or_url)

def normalize_player_name(name: str) -> str:
    return " ".join(name.strip().replace("-", " ").split()).lower()

def latest_elo_by_player(df: pd.DataFrame, surface: str) -> pd.DataFrame:
    # Expect columns: player, date, elo, elo_hard, elo_clay, elo_grass
    cols = {c.lower(): c for c in df.columns}
    name_col = cols.get("player") or cols.get("player_name") or list(df.columns)[0]
    date_col = cols.get("date") or "date"
    s_col = {"hard":"elo_hard","clay":"elo_clay","grass":"elo_grass"}.get(surface, "elo")
    if s_col not in df.columns:
        s_col = "elo"
    sdf = df[[name_col, date_col, s_col]].copy()
    sdf.columns = ["player","date", "elo"]
    sdf["date"] = pd.to_datetime(sdf["date"], errors="coerce")
    sdf = sdf.sort_values(["player","date"]).dropna(subset=["player"])
    last = sdf.groupby("player", as_index=False).tail(1)
    last["name_key"] = last["player"].map(normalize_player_name)
    return last[["name_key","elo"]]

def recency_form(df_all: pd.DataFrame, surface: str, window_days: int = 120) -> pd.DataFrame:
    cols = {c.lower(): c for c in df_all.columns}
    name_col = cols.get("player") or cols.get("player_name") or list(df_all.columns)[0]
    date_col = cols.get("date") or "date"
    s_col = {"hard":"elo_hard","clay":"elo_clay","grass":"elo_grass"}.get(surface, "elo")
    if s_col not in df_all.columns:
        s_col = "elo"
    df = df_all[[name_col,date_col,s_col]].copy()
    df.columns = ["player","date","elo"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=window_days)
    df = df[df["date"]>=cutoff].dropna(subset=["date"])
    df = df.sort_values(["player","date"])
    grp = df.groupby("player")
    momentum = (grp["elo"].last() - grp["elo"].min()).to_frame("form_raw")
    momentum["form"] = (momentum["form_raw"]/100.0).clip(-1,1)
    momentum = momentum.reset_index()
    momentum["name_key"] = momentum["player"].map(normalize_player_name)
    return momentum[["name_key","form"]]

def load_serve_return(path: Optional[str]) -> pd.DataFrame:
    if not path:
        return pd.DataFrame(columns=["name_key","surface","srv","ret"])
    df = load_csv(path)
    cols = {c.lower(): c for c in df.columns}
    name_col = cols.get("player") or cols.get("name") or list(df.columns)[0]
    surf_col = cols.get("surface","surface")
    srv_col = cols.get("serve_pts_won") or cols.get("srv") or cols.get("first_serve_points_won")
    ret_col = cols.get("return_pts_won") or cols.get("ret") or cols.get("return_points_won")
    tmp = df[[name_col, surf_col, srv_col, ret_col]].copy()
    tmp.columns = ["player","surface","srv","ret"]
    tmp["name_key"] = tmp["player"].map(normalize_player_name)
    out = []
    for s, g in tmp.groupby("surface"):
        if len(g) >= 10:
            g = g.copy()
            for col in ["srv","ret"]:
                g[col] = (g[col] - g[col].mean())/(g[col].std(ddof=0)+1e-9)
            out.append(g)
    if out:
        tmp = pd.concat(out, ignore_index=True)
    return tmp[["name_key","surface","srv","ret"]]

def infer_surface(event_title: str, league: str, surface_map: Dict[str,str], default_surface: str) -> str:
    title = (event_title or "" + " " + league or "").lower()
    for key, surf in surface_map.items():
        if key.lower() in title:
            return surf
    return default_surface

def consensus_from_books(bookmakers: List[Dict[str, Any]], preferred: List[str]) -> Tuple[Dict[str,float], Dict[str,float]]:
    probs: Dict[str, List[float]] = {}
    best_price: Dict[str, float] = {}
    for b in bookmakers or []:
        book_key = (b.get("key") or b.get("bookmaker",{}).get("key") or "").lower()
        weight = 1.15 if book_key in preferred else 1.0
        markets = b.get("markets", b.get("markets", []))
        for m in markets:
            if m.get("key") != "h2h":
                continue
            for oc in m.get("outcomes", []):
                name = oc.get("name")
                price = oc.get("price")
                if not (name and price):
                    continue
                ip = implied_prob(price)
                if ip:
                    probs.setdefault(name, []).append(ip*weight)
                best_price[name] = max(best_price.get(name,0.0), float(price))
    consensus = {k: sum(v)/len(v) for k,v in probs.items() if v}
    if len(consensus)==2:
        total = sum(consensus.values())
        if total>0:
            for k in list(consensus.keys()):
                consensus[k] /= total
    return consensus, best_price

def model_prob_from_elo(p1_elo: float, p2_elo: float, form1: float, form2: float, srv1: float, srv2: float,
                        k_factor: float = 0.004) -> float:
    base = sigmoid(k_factor * (p1_elo - p2_elo))
    adj = 0.0
    adj += 0.05 * (form1 - form2)
    adj += 0.05 * (srv1 - srv2)
    p = min(max(base + adj, 1e-4), 1-1e-4)
    return p

def blended_prob(p_market: float, p_model: float, w_market: float, w_model: float) -> float:
    z = w_market*logit(p_market) + w_model*logit(p_model)
    return sigmoid(z)

def kelly_fraction(p: float, odds: float, cap: float) -> float:
    b = max(0.0, odds-1.0)
    q = 1.0 - p
    f = ((b*p) - q) / b if b>0 else 0.0
    return max(0.0, min(cap, f))

def build_surface_map(path: Optional[str]) -> Dict[str, str]:
    if not path or not os.path.exists(path):
        # Minimal defaults; extend as needed
        return {
            "wimbledon": "grass",
            "roland garros": "clay",
            "french open": "clay",
            "madrid": "clay",
            "rome": "clay",
            "monte-carlo": "clay",
            "australian open": "hard",
            "us open": "hard",
            "indian wells": "hard",
            "miami": "hard",
            "cincinnati": "hard",
            "toronto": "hard",
            "montreal": "hard",
            "washington": "hard",
            "queens": "grass",
            "halle": "grass",
            "stuttgart": "grass",
            "eastbourne": "grass",
        }
    with open(path, "r", encoding="utf-8") as f:
        m = json.load(f)
        return {k.lower(): v.lower() for k, v in m.items()}

