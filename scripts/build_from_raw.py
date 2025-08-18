# scripts/build_from_raw.py
import os
import glob
import pandas as pd

RAW_DIR = "data/raw/odds"
OUT = "data/historical_matches.csv"

# Map various raw schemas to the canonical columns we want
CANDIDATE_MAPS = [
    {  # Schema A (already aligned)
        "date": "date",
        "tournament": "tournament",
        "round": "round",
        "player1": "player1",
        "player2": "player2",
        "odds1": "odds1",
        "odds2": "odds2",
        "result": "result",
    },
    {  # Schema B (common alt names)
        "match_date": "date",
        "tour": "tournament",
        "rnd": "round",
        "p1": "player1",
        "p2": "player2",
        "p1_odds": "odds1",
        "p2_odds": "odds2",
        "winner": "result",
    },
]

CANON = ["date", "tournament", "round", "player1", "player2", "odds1", "odds2", "result"]


def load_one(path: str) -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
    except Exception:
        return pd.DataFrame(columns=CANON)

    for cmap in CANDIDATE_MAPS:
        if set(cmap.keys()).issubset(df.columns):
            d = df[list(cmap.keys())].rename(columns=cmap).copy()
            # normalize
            d["date"] = pd.to_datetime(d["date"], errors="coerce").dt.date.astype(str)
            for c in ("odds1", "odds2"):
                if c in d:
                    d[c] = pd.to_numeric(d[c], errors="coerce")
            # winner/result normalization: try to coerce to "P1"/"P2" if needed
            if "result" in d.columns:
                d["result"] = d["result"].astype(str).str.upper().str.replace("PLAYER1", "P1").str.replace("PLAYER2", "P2")
            # ensure all canon columns exist
            for c in CANON:
                if c not in d.columns:
                    d[c] = pd.NA
            return d[CANON]
    return pd.DataFrame(columns=CANON)


def main():
    os.makedirs("data", exist_ok=True)
    files = sorted(glob.glob(os.path.join(RAW_DIR, "*.csv")))
    if not files:
        # leave existing file alone if it exists; otherwise create empty canon file
        if not os.path.exists(OUT):
            pd.DataFrame(columns=CANON).to_csv(OUT, index=False)
        print("No raw files found; built empty/kept existing.")
        return

    parts = [load_one(f) for f in files]
    if not parts:
        pd.DataFrame(columns=CANON).to_csv(OUT, index=False)
        print("No parseable raw files; wrote empty dataset.")
        return

    df = pd.concat(parts, ignore_index=True)
    df.dropna(subset=["date", "player1", "player2"], inplace=True)
    df.to_csv(OUT, index=False)
    print(f"Wrote {OUT} with {len(df)} rows from {len(files)} raw files.")


if __name__ == "__main__":
    main()
