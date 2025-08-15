import pandas as pd
import os
from datetime import datetime, timedelta, timezone

# Config
ODDS_DOG_MIN, ODDS_DOG_MAX = 2.20, 4.50
ODDS_FAV_MIN, ODDS_FAV_MAX = 1.30, 1.80
TE8_THRESHOLD_DOG, TE8_THRESHOLD_FAV = 0.6, 0.5
MIN_CONF = 50
LOOKAHEAD_H = 24

# Load Elo ratings
elo_atp = pd.read_csv("data/atp_elo.csv")
elo_wta = pd.read_csv("data/wta_elo.csv")

def get_elo_rating(player, tour):
    df = elo_atp if tour.upper() == "ATP" else elo_wta
    row = df[df["player"] == player]
    return float(row["elo"].iloc[0]) if not row.empty else 1500.0

# Kelly fraction
def kelly_fraction(prob, odds):
    return max((prob * odds - 1) / (odds - 1), 0)

# TrueEdge8 factors
def trueedge8(row):
    # Dummy simple scoring for now — replace with real data integration later
    form = 0.6   # last 5 matches win rate
    surface = 0.65
    h2h = 0.5
    rest = 0.7
    injury = 0.8
    stage = 0.55
    market = 0.6
    mental = 0.65
    return round((form + surface + h2h + rest + injury + stage + market + mental) / 8, 2)

# Load matches (replace with your match feed)
df = pd.read_csv("value_picks_pro.csv")
for c in ["blended_prob","best_odds","confidence","commence_time_utc"]:
    if c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce") if c != "commence_time_utc" else df[c]

# Filter by 24h upcoming
now = datetime.utcnow().replace(tzinfo=timezone.utc)
df["commence_dt"] = pd.to_datetime(df["commence_time_utc"], utc=True)
df = df[(df["commence_dt"] > now) & (df["commence_dt"] <= now + timedelta(hours=LOOKAHEAD_H))]

# Build output
out_lines = []

for tour in ["ATP", "WTA"]:
    sub = df[df["tour"].str.upper() == tour]
    out_lines.append(f"# {tour} Picks")
    if sub.empty:
        out_lines.append("_No matches in next 24h_")
        continue
    for _, r in sub.iterrows():
        p1_elo = get_elo_rating(r["player"], tour)
        p2_elo = get_elo_rating(r["opponent"], tour)
        prob = 1 / (1 + 10 ** ((p2_elo - p1_elo) / 400))
        kelly = kelly_fraction(prob, r["best_odds"])
        is_dog = r["best_odds"] >= ODDS_DOG_MIN
        if is_dog:
            kelly = min(kelly, kelly * 0.25)  # micro cap for dogs
        te8 = trueedge8(r)
        bet_it = (kelly > 0) and (te8 >= (TE8_THRESHOLD_DOG if is_dog else TE8_THRESHOLD_FAV))
        decision = "✅ Bet" if bet_it else "🚫 Pass"
        eta = r["commence_dt"] - now
        out_lines.append(
            f"{r['player']} vs {r['opponent']} — {r['best_odds']:.2f} | p={prob:.2f} | Kelly={kelly:.3f} | TE8={te8} | {decision}\n"
            f"🗓 {r['commence_time_utc']} UTC • ETA: {eta.components.hours}h {eta.components.minutes}m"
        )
    out_lines.append("")

# Write to GitHub Actions summary or local file
summary_path = os.environ.get("GITHUB_STEP_SUMMARY", "summary.md")
with open(summary_path, "w", encoding="utf-8") as f:
    f.write("\n".join(out_lines))

print("Summary written.")

    main()
