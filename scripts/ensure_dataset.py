# scripts/ensure_dataset.py
import os
import pandas as pd

os.makedirs("data", exist_ok=True)
path = "data/historical_matches.csv"

if not os.path.exists(path):
    cols = ["date","tournament","round","player1","player2","odds1","odds2","result"]
    pd.DataFrame(columns=cols).to_csv(path, index=False)
    print(f"Created empty {path}")
else:
    print(f"Found {path}")
