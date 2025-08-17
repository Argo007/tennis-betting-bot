import os
import pandas as pd

os.makedirs("data", exist_ok=True)
dataset_path = "data/historical_matches.csv"

if not os.path.exists(dataset_path):
    cols = ["date","tournament","round","player1","player2","odds1","odds2","result"]
    pd.DataFrame(columns=cols).to_csv(dataset_path, index=False)
    print(f"Created empty dataset at {dataset_path}")
else:
    print(f"Dataset already exists at {dataset_path}")
