#!/usr/bin/env python3
import argparse, os, pandas as pd, datetime as dt

ap = argparse.ArgumentParser()
ap.add_argument("--out", required=True)
args = ap.parse_args()

os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)

now = dt.datetime.utcnow()
rows = [
    {"match_id":"L001","player_a":"Sinner","player_b":"Alcaraz","tournament":"ATP Toronto","start_time":(now+dt.timedelta(minutes=30)).isoformat()+"Z"},
    {"match_id":"L002","player_a":"Rybakina","player_b":"Gauff","tournament":"WTA Montreal","start_time":(now+dt.timedelta(minutes=45)).isoformat()+"Z"},
    {"match_id":"L003","player_a":"Djokovic","player_b":"Medvedev","tournament":"ATP Toronto","start_time":(now+dt.timedelta(hours=1)).isoformat()+"Z"},
]
pd.DataFrame(rows).to_csv(args.out, index=False)
print(f"Wrote live matches -> {args.out}")
