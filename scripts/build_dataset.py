# scripts/build_dataset.py
import os, glob, pandas as pd

RAW_DIR = "raw"
OUT = "data/historical_matches.csv"
os.makedirs("data", exist_ok=True)

frames = []
for f in sorted(glob.glob(f"{RAW_DIR}/*.csv")):
    try:
        df = pd.read_csv(f)
        # normalize columns to the schema we use downstream
        rename = {
            'Date':'date','Tournament':'tournament','Round':'round',
            'Player1':'player1','Player2':'player2',
            'Odds1':'odds1','Odds2':'odds2','Result':'result'
        }
        # be forgiving with cases
        df.columns = [c.strip() for c in df.columns]
        for k in list(rename):
            if k not in df.columns and k.lower() in [c.lower() for c in df.columns]:
                # map by lower-case match
                src = [c for c in df.columns if c.lower()==k.lower()][0]
                rename[src] = rename.pop(k)
        df = df.rename(columns=rename)
        keep = ["date","tournament","round","player1","player2","odds1","odds2","result"]
        df = df[[c for c in keep if c in df.columns]]
        frames.append(df)
    except Exception as e:
        print(f"Skip {f}: {e}")

out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(
    columns=["date","tournament","round","player1","player2","odds1","odds2","result"]
)
# light cleaning
out = out.dropna(subset=["player1","player2"]).reset_index(drop=True)
out.to_csv(OUT, index=False)
print(f"Wrote {OUT} with {len(out)} rows")
