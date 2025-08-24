#!/usr/bin/env python3
import argparse, os, pandas as pd, numpy as np

parser = argparse.ArgumentParser()
parser.add_argument("--outdir", required=True)
args = parser.parse_args()
os.makedirs(args.outdir, exist_ok=True)

rng = np.random.default_rng(42)
n = 120
true_p = rng.uniform(0.35, 0.65, size=n)
odds = np.round(rng.uniform(1.9, 3.5, size=n), 2)
results = (rng.random(n) < true_p).astype(int)

df = pd.DataFrame({
    "match_id": [f"M{i:04d}" for i in range(n)],
    "player_a": rng.choice(["Osaka","Gauff","Rybakina","Sinner","Alcaraz","Djokovic"], size=n),
    "player_b": rng.choice(["Swiatek","Pegula","Sabalenka","Medvedev","Zverev","Rublev"], size=n),
    "odds": odds,
    "p": np.round(true_p, 3),
    "result": results
})
out = os.path.join(args.outdir, "tennis_data.csv")
df.to_csv(out, index=False)
print(f"Wrote {len(df)} rows -> {out}")
