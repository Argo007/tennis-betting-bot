#!/usr/bin/env python3
"""
Normalize local odds to Sackmann's player names (and align dates).
- Reads odds CSV/XLSX from data/raw/odds/*
- Fetches Sackmann ATP/WTA results for given years
- Fuzzy-maps player names to Sackmann canonical names
- Optionally snaps each odds row's date to the nearest Sackmann match
  between the two players within ±7 days
- Writes cleaned files to data/raw/odds_normalized/*.csv
- Also writes a report at data/normalize_report.md

Usage:
  python scripts/normalize_odds.py --years "2021 2022 2023 2024" --snap-dates yes
"""

import io, os, re, glob, sys, time, argparse
from pathlib import Path
from datetime import timedelta
import pandas as pd
import requests
from rapidfuzz import process, fuzz
from unidecode import unidecode

REQ_TIMEOUT = 12
RETRIES = 3
BACKOFF = [0, 1.5, 3.0]

ROOT = Path(".").resolve()
IN_DIR = ROOT / "data" / "raw" / "odds"
OUT_DIR = ROOT / "data" / "raw" / "odds_normalized"
REPORT = ROOT / "data" / "normalize_report.md"

def fetch_csv(url: str) -> pd.DataFrame:
    last = None
    for i in range(RETRIES):
        try:
            r = requests.get(url, timeout=REQ_TIMEOUT)
            r.raise_for_status()
            return pd.read_csv(io.StringIO(r.text))
        except Exception as e:
            last = e
            time.sleep(BACKOFF[min(i, len(BACKOFF)-1)])
    raise RuntimeError(f"Failed to fetch {url}: {last}")

def norm_name(s: str) -> str:
    s = unidecode(str(s)).lower()
    s = re.sub(r"[^a-z ]"," ", s)
    return re.sub(r"\s+"," ", s).strip()

def load_sackmann(years):
    frames=[]
    pulled={}
    for tour, repo in [("ATP","tennis_atp"),("WTA","tennis_wta")]:
        for y in years:
            url=f"https://raw.githubusercontent.com/JeffSackmann/{repo}/master/{tour.lower()}_matches_{y}.csv"
            df=fetch_csv(url)
            df["tour"]=tour
            frames.append(df[["tour","tourney_name","tourney_date","winner_name","loser_name"]].copy())
            pulled[(tour,y)] = len(df)
    res=pd.concat(frames, ignore_index=True)
    res["date"]=pd.to_datetime(res["tourney_date"].astype(str), format="%Y%m%d", errors="coerce").dt.normalize()
    res["w_norm"]=res["winner_name"].map(norm_name)
    res["l_norm"]=res["loser_name"].map(norm_name)
    # Canonical name set
    names = sorted(set(res["winner_name"].astype(str)) | set(res["loser_name"].astype(str)))
    names_norm = [norm_name(n) for n in names]
    canon_map = dict(zip(names_norm, names))  # exact normalized -> canonical
    # For fuzzy search
    name_index = {norm_name(n): n for n in names}
    return res, names, name_index, pulled

def best_match(q_norm: str, choices_norm_to_canon: dict, min_score=90):
    # direct exact first
    if q_norm in choices_norm_to_canon:
        return choices_norm_to_canon[q_norm], 100.0
    # otherwise fuzzy
    all_norms = list(choices_norm_to_canon.keys())
    match, score, _ = process.extractOne(q_norm, all_norms, scorer=fuzz.token_sort_ratio)
    if score < min_score:
        return None, score
    return choices_norm_to_canon[match], score

def load_any(path: Path) -> pd.DataFrame | None:
    try:
        if path.suffix.lower() in (".xlsx",".xls"):
            return pd.read_excel(path)
        return pd.read_csv(path)
    except Exception:
        try:
            return pd.read_csv(path, encoding="latin1")
        except Exception:
            return None

def pick_header(df, *opts):
    lower={c.lower():c for c in df.columns}
    for o in opts:
        if o in lower: return lower[o]
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--years", default="2021 2022 2023 2024")
    ap.add_argument("--snap-dates", default="yes", choices=["yes","no"], help="Snap odds dates to nearest Sackmann match in ±7 days")
    ap.add_argument("--min-name-score", type=int, default=90, help="Min fuzzy match score to accept")
    args = ap.parse_args()

    years = [int(y) for y in args.years.split() if y.strip().isdigit()]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    REPORT.parent.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] Years: {years}")
    sack, canon_names, name_index, pulled = load_sackmann(years)
    canon_norm_to_canon = {norm_name(n): n for n in canon_names}

    files = sorted(glob.glob(str(IN_DIR / "**/*.*"), recursive=True))
    files = [Path(p) for p in files if p.lower().endswith((".csv",".xlsx",".xls"))]
    if not files:
        print(f"[WARN] No odds files found in {IN_DIR}")
        sys.exit(0)

    lines=[]
    lines.append(f"# Normalize Odds Report\n")
    lines.append(f"- Years: {years}")
    lines.append("- Sackmann pulls:")
    for (tour,y),n in sorted(pulled.items()):
        lines.append(f"  - {tour} {y}: **{n:,}** rows")
    lines.append(f"- Input files: **{len(files)}**\n")

    total_rows_in=0
    total_rows_out=0
    total_unmatched=0

    for src in files:
        df = load_any(src)
        if df is None or df.empty:
            lines.append(f"## {src.name}\n- Skipped (could not read or empty)")
            continue

        c_date=pick_header(df,"date","event_date","match_date")
        c_pa  =pick_header(df,"player_a","home","player1","p1","selection","player")
        c_pb  =pick_header(df,"player_b","away","player2","p2","opponent")
        c_odds_a=pick_header(df,"odds_a","price_a","decimal_odds_a","odds1","home_odds","price1","best_odds_a")
        c_odds_b=pick_header(df,"odds_b","price_b","decimal_odds_b","odds2","away_odds","price2","best_odds_b")

        need = (c_date and c_pa and c_pb and c_odds_a and c_odds_b)
        if not need:
            lines.append(f"## {src.name}\n- Skipped (missing required headers)")
            continue

        tmp = pd.DataFrame({
            "date": pd.to_datetime(df[c_date], errors="coerce").dt.normalize(),
            "player_a": df[c_pa].astype(str),
            "player_b": df[c_pb].astype(str),
            "odds_a": pd.to_numeric(df[c_odds_a], errors="coerce"),
            "odds_b": pd.to_numeric(df[c_odds_b], errors="coerce"),
        }).dropna(subset=["date","odds_a","odds_b"]).copy()

        total_rows_in += len(tmp)
        unmatched_rows = 0

        # name normalization
        a_norm = tmp["player_a"].map(norm_name)
        b_norm = tmp["player_b"].map(norm_name)

        a_map = []
        b_map = []
        a_score=[]
        b_score=[]
        for an, bn in zip(a_norm, b_norm):
            can_a, sc_a = best_match(an, canon_norm_to_canon, min_score=args.min_name_score)
            can_b, sc_b = best_match(bn, canon_norm_to_canon, min_score=args.min_name_score)
            a_map.append(can_a); b_map.append(can_b)
            a_score.append(sc_a); b_score.append(sc_b)
            if can_a is None or can_b is None:
                unmatched_rows += 1

        tmp["player_a_norm"] = a_map
        tmp["player_b_norm"] = b_map
        tmp["a_score"] = a_score
        tmp["b_score"] = b_score

        # drop unmatched names
        cleaned = tmp.dropna(subset=["player_a_norm","player_b_norm"]).copy()

        # optional: snap date to nearest Sackmann match between the mapped players
        if args.snap_dates == "yes" and not cleaned.empty:
            sack_small = sack[["date","tour","winner_name","loser_name"]].copy()
            sack_small["pair_key"] = sack_small.apply(
                lambda r: " vs ".join(sorted([norm_name(r["winner_name"]), norm_name(r["loser_name"])])), axis=1
            )

            pair_dates = {}
            for pair, grp in sack_small.groupby("pair_key"):
                pair_dates[pair] = sorted(grp["date"].dropna().unique())

            def snap(row):
                pair = " vs ".join(sorted([norm_name(row["player_a_norm"]), norm_name(row["player_b_norm"])]))
                dates = pair_dates.get(pair, [])
                if not dates: return row["date"]
                # closest date
                d0 = row["date"]
                best = min(dates, key=lambda d: abs(d - d0))
                if abs(best - d0) <= pd.Timedelta(days=7):
                    return best
                return d0

            cleaned["date"] = cleaned.apply(snap, axis=1)

        out = cleaned.rename(columns={
            "player_a_norm":"player_a",
            "player_b_norm":"player_b"
        })[["date","player_a","player_b","odds_a","odds_b"]].copy()

        dst = OUT_DIR / (src.stem + "_normalized.csv")
        out.to_csv(dst, index=False)
        lines.append(f"## {src.name}")
        lines.append(f"- In rows: **{len(tmp):,}** | Out rows: **{len(out):,}**")
        lines.append(f"- Unmatched name rows removed: **{unmatched_rows:,}**")
        if not out.empty:
            lines.append("")
            lines.append("<details><summary>Preview (first 8 rows)</summary>\n")
            lines.append(out.head(8).to_markdown(index=False))
            lines.append("\n</details>\n")

        total_rows_out += len(out)
        total_unmatched += unmatched_rows

    summary = [
        "\n---\n",
        f"- TOTAL in: **{total_rows_in:,}** rows",
        f"- TOTAL out (normalized): **{total_rows_out:,}** rows",
        f"- TOTAL unmatched dropped: **{total_unmatched:,}** rows",
        f"- Output dir: `{OUT_DIR.as_posix()}`",
    ]
    lines += summary
    REPORT.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(summary))

if __name__ == "__main__":
    main()
