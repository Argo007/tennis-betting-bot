# -*- coding: utf-8 -*-
import os, sys
import pandas as pd
from datetime import datetime, timedelta, timezone

# ---- Criteria ----
ODDS_FAV_MIN = 1.30
ODDS_FAV_MAX = 1.80
ODDS_DOG_MIN = 2.20
ODDS_DOG_MAX = 4.50
EDGE_MIN_PP = 0.03          # p_sane - p_mkt
EV_MIN_PER_UNIT = 0.015     # >= 1.5%
BET_KELLY_MIN_DOG = 0.05
BET_KELLY_MIN_FAV = 0.02
TOP_DOGS = 3
TOP_FAVS = 2

# Quietly exit if the model didn't produce the CSV (keeps workflow green)
if not os.path.exists("value_picks_pro.csv"):
    sys.exit(0)

df = pd.read_csv("value_picks_pro.csv")

# OPTIONAL: uncomment and map if your CSV uses different headers
# df.rename(columns={
#     "start_time_utc": "commence_time_utc",
#     "prob": "blended_prob",
# }, inplace=True)

# Normalize numeric types if present
for c in ["best_odds","blended_prob","model_prob","market_prob","ev_per_unit","kelly_fraction","confidence"]:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

# Require core columns
need = {"tour","player","opponent","best_odds","blended_prob","confidence","commence_time_utc"}
if not need.issubset(df.columns):
    sys.exit(0)

# Strict 24h window (UTC)
now = datetime.now(timezone.utc)
cutoff = now + timedelta(hours=24)
df["commence_time_utc"] = pd.to_datetime(df["commence_time_utc"], utc=True, errors="coerce")
df = df[df["commence_time_utc"].between(now, cutoff)]
if df.empty:
    sys.exit(0)

df = df.dropna(subset=["best_odds","blended_prob","confidence","commence_time_utc"])

# --- helpers ---
def fair_probs(o1, o2):
    r1, r2 = 1/float(o1), 1/float(o2)
    s = r1 + r2
    return (r1/s, r2/s) if s > 0 else (None, None)

def get_p_mkt(row):
    # Prefer vig-free from both sides if available
    if "best_odds_player" in row and "best_odds_opponent" in row:
        if pd.notna(row["best_odds_player"]) and pd.notna(row["best_odds_opponent"]):
            p_self, _ = fair_probs(row["best_odds_player"], row["best_odds_opponent"])
            if p_self is not None:
                return p_self
    # Else use provided market_prob if present (assumed vig-free)
    if "market_prob" in row and pd.notna(row["market_prob"]):
        return float(row["market_prob"])
    # Fallback: naive implied from offered price
    return 1/float(row["best_odds"])

def get_p_mdl(row):
    if "model_prob" in row and pd.notna(row["model_prob"]):
        return float(row["model_prob"])
    return float(row["blended_prob"])

def kelly(p, o):
    q = 1 - p
    return (o*p - q)/(o-1) if (o-1) > 0 else -1.0

def band(odds):
    if ODDS_FAV_MIN <= odds <= ODDS_FAV_MAX: return "FAV"
    if ODDS_DOG_MIN <= odds <= ODDS_DOG_MAX: return "DOG"
    return "OUT"

# Compute metrics and keep only YES picks
rows=[]
for _, r in df.iterrows():
    odds = float(r["best_odds"])
    p_mkt = get_p_mkt(r)
    p_mdl = get_p_mdl(r)
    conf = float(r.get("confidence", 50.0))
    lam = max(0.0, min(1.0, (conf/100.0)**2))  # confidence-weighted blend
    p_sane = (1 - lam) * p_mkt + lam * p_mdl
    delta_p = p_sane - p_mkt
    ev_sane = odds * p_sane - 1
    k = kelly(p_sane, odds)
    b = band(odds)

    yes = (
        (delta_p >= EDGE_MIN_PP) and
        (ev_sane >= EV_MIN_PER_UNIT) and
        (b != "OUT") and
        ((b == "DOG" and k >= BET_KELLY_MIN_DOG) or (b == "FAV" and k >= BET_KELLY_MIN_FAV))
    )
    if yes:
        rows.append({
            "tour": str(r["tour"]).upper(),
            "player": r["player"],
            "opponent": r["opponent"],
            "best_odds": odds,
            "p_sane": p_sane,
            "kelly": k,
            "when": r["commence_time_utc"],
            "band": b
        })

# No YES picks? Exit quietly.
if not rows:
    sys.exit(0)

res = pd.DataFrame(rows)

def dedup(d):
    d = d.copy()
    d.insert(0, "match_id", d.apply(lambda x: " :: ".join(sorted([str(x["player"]), str(x["opponent"])])), axis=1))
    return d.sort_values(["p_sane","kelly"], ascending=False).groupby("match_id", as_index=False).first()

out=[]
for tour in ["ATP","WTA"]:
    t = res[res["tour"] == tour]
    dogs = dedup(t[t["band"] == "DOG"]).head(TOP_DOGS)
    favs = dedup(t[t["band"] == "FAV"]).head(TOP_FAVS)

    if not dogs.empty:
        out.append(f"## {tour} Underdogs")
        for i, (_, rr) in enumerate(dogs.iterrows(), start=1):
            out.append(
                f"{i}. {rr['player']} vs *{rr['opponent']}* — {rr['best_odds']:.2f} "
                f"(p={rr['p_sane']:.2f}, Kelly={rr['kelly']:.3f}) "
                f"{pd.to_datetime(rr['when']).strftime('%Y-%m-%d %H:%M UTC')}"
            )
        out.append("")

    if not favs.empty:
        out.append(f"## {tour} Favorites")
        for i, (_, rr) in enumerate(favs.iterrows(), start=1):
            out.append(
                f"{i}. {rr['player']} vs *{rr['opponent']}* — {rr['best_odds']:.2f} "
                f"(p={rr['p_sane']:.2f}, Kelly={rr['kelly']:.3f}) "
                f"{pd.to_datetime(rr['when']).strftime('%Y-%m-%d %H:%M UTC')}"
            )
        out.append("")

with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as f:
    f.write("\n".join(out))
