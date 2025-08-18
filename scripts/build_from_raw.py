import os, glob, pandas as pd

RAW_DIR = "data/raw/odds"
OUT = "data/historical_matches.csv"

# expected columns in *your* raw files (rename here to match reality)
CANDIDATE_MAPS = [
    {  # example schema A
        "date":"date","tournament":"tournament","round":"round",
        "player1":"player1","player2":"player2",
        "odds1":"odds1","odds2":"odds2","result":"result"
    },
    {  # example schema B
        "match_date":"date","tour":"tournament","rnd":"round",
        "p1":"player1","p2":"player2","p1_odds":"odds1","p2_odds":"odds2","winner":"result"
    },
]

def load_one(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    for cmap in CANDIDATE_MAPS:
        if set(cmap.keys()).issubset(df.columns):
            d = df[list(cmap.keys())].rename(columns=cmap)
            # normalize types
            d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.date.astype(str)
            for c in ["odds1","odds2"]:
                if c in d: d[c] = pd.to_numeric(d[c], errors="coerce")
            return d
    # if nothing matches, return empty
    return pd.DataFrame(columns=["date","tournament","round","player1","player2","odds1","odds2","result"])

def main():
    os.makedirs("data", exist_ok=True)
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))
    if not files:
        print("No raw files found; leaving OUT empty")
        pd.DataFrame(columns=["date","tournament","round","player1","player2","odds1","odds2","result"]).to_csv(OUT, index=False)
        return
    parts = [load_one(f) for f in files]
    df = pd.concat(parts, ignore_index=True).dropna(subset=["date","player1","player2"], how="any")
    df.to_csv(OUT, index=False)
    print(f"Wrote {OUT} with {len(df)} rows from {len(files)} raw files.")

if __name__ == "__main__":
    main()
