
#!/usr/bin/env python3
"""
Settle trades and update bankroll (safe, argument-optional).

Defaults (all paths are relative to repo root):
  --log           results/trade_log.csv          # optional; header-only if missing
  --close-odds    (latest) data/raw/odds/close_odds_*.csv  # optional
  --state-dir     state/
  --assume-random-if-missing  false              # don't change bankroll without results
  --out           results/settlements.csv

Trade log schema (flexible; we try to map common names):
  event_date, tournament, player, side, odds, stake, result
    - result ∈ {'W','L','V','void','push'} (case-insensitive)
    - If result is missing and --assume-random-if-missing=true, we settle
      by sampling a Bernoulli with p≈1/odds (still optional and off by default).

Outputs:
  results/settlements.csv with:
    event_date,tournament,player,odds,stake,result,payout,delta,
    bankroll_before,bankroll_after,source

Bankroll:
  Reads state/bankroll.json or uses env BANKROLL_START (default 1000.0).
  Writes updated bankroll.json only if at least one row was settled.
"""

import argparse, csv, json, os, random
from glob import glob
from pathlib import Path
from datetime import datetime, timezone

# ---------- paths & env ----------
REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR   = REPO_ROOT / "data" / "raw"
ODDS_DIR  = RAW_DIR / "odds"
RES_DIR   = REPO_ROOT / "results"

DEFAULT_LOG        = RES_DIR / "trade_log.csv"
DEFAULT_STATE_DIR  = REPO_ROOT / "state"
DEFAULT_OUT        = RES_DIR / "settlements.csv"
BANKROLL_FILE_ENV  = os.getenv("BANKROLL_FILE")  # pipeline exports this
BANKROLL_START     = float(os.getenv("BANKROLL_START", "1000.0"))

def log(msg: str):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[settle] {ts} {msg}", flush=True)

def latest_close_odds() -> Path | None:
    files = sorted(glob(str(ODDS_DIR / "close_odds_*.csv")))
    return Path(files[-1]) if files else None

def read_bankroll(state_dir: Path) -> float:
    f = Path(BANKROLL_FILE_ENV) if BANKROLL_FILE_ENV else state_dir / "bankroll.json"
    if f.exists():
        try:
            return float(json.loads(f.read_text()).get("bankroll", BANKROLL_START))
        except Exception:
            return BANKROLL_START
    return BANKROLL_START

def write_bankroll(state_dir: Path, value: float):
    f = Path(BANKROLL_FILE_ENV) if BANKROLL_FILE_ENV else state_dir / "bankroll.json"
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps({"bankroll": round(value, 2)}, indent=2))

def csv_has_rows(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size == 0: return False
        with path.open("r", encoding="utf-8") as fh:
            r = csv.reader(fh); _ = next(r, None); return next(r, None) is not None
    except Exception:
        return False

def norm_result(x: str | None) -> str | None:
    if not x: return None
    s = str(x).strip().lower()
    if s in ("w","win","won"): return "W"
    if s in ("l","loss","lost"): return "L"
    if s in ("push","void","v"): return "V"
    return None

def parse_float(x, default=None):
    try:
        return float(x)
    except Exception:
        return default

def choose(colnames: list[str], mapping: list[str]) -> str | None:
    s = {c.lower(): c for c in colnames}
    for m in mapping:
        if m in s: return s[m]
    return None

def settle_row(row: dict, bankroll: float, assume_random: bool) -> tuple[dict, float, bool]:
    # Map columns flexibly
    k_odds  = choose(list(row.keys()), ["odds","price","decimal_odds"])
    k_stake = choose(list(row.keys()), ["stake","amount"])
    k_res   = choose(list(row.keys()), ["result","outcome"])
    k_evt   = choose(list(row.keys()), ["event_date","date"])
    k_tour  = choose(list(row.keys()), ["tournament","event"])
    k_player= choose(list(row.keys()), ["player","selection","runner","team"])

    odds  = parse_float(row.get(k_odds) if k_odds else None)
    stake = parse_float(row.get(k_stake) if k_stake else None, 0.0)
    res   = norm_result(row.get(k_res) if k_res else None)

    # If no explicit result: either skip or (optionally) simulate
    if res is None:
        if assume_random and odds and odds > 1.0:
            p_win = 1.0 / odds
            res = "W" if random.random() < p_win else "L"
        else:
            # no settlement
            return {}, bankroll, False

    # Compute payout & delta
    payout = stake * odds if res == "W" else (stake if res == "V" else 0.0)
    delta  = payout - stake  # void => 0

    before = bankroll
    after  = bankroll + delta

    settled = {
        "event_date": row.get(k_evt) or "",
        "tournament": row.get(k_tour) or "",
        "player":     row.get(k_player) or "",
        "odds":       round(odds or 0.0, 3),
        "stake":      round(stake, 2),
        "result":     res,
        "payout":     round(payout, 2),
        "delta":      round(delta, 2),
        "bankroll_before": round(before, 2),
        "bankroll_after":  round(after, 2),
        "source":     "trade_log",
    }
    return settled, after, True

def main():
    ap = argparse.ArgumentParser(description="Settle trades and update bankroll (safe defaults)")
    ap.add_argument("--log", help="Trade log CSV", default=str(DEFAULT_LOG))
    ap.add_argument("--close-odds", help="Optional: path to close odds CSV", default="")
    ap.add_argument("--state-dir", help="Directory to store bankroll.json", default=str(DEFAULT_STATE_DIR))
    ap.add_argument("--assume-random-if-missing", type=str, default="false",
                    help="If true, simulate result when missing (p≈1/odds)")
    ap.add_argument("--out", help="Settlements output CSV", default=str(DEFAULT_OUT))
    args = ap.parse_args()

    log_csv  = Path(args.log)
    state_dir= Path(args.state_dir)
    out_csv  = Path(args.out)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    state_dir.mkdir(parents=True, exist_ok=True)

    # If --close-odds is empty, try detect latest (optional, not required)
    if not args.close_odds:
        lo = latest_close_odds()
        if lo: log(f"using latest close odds: {lo.name}")
    else:
        lo = Path(args.close_odds)
        if lo.exists(): log(f"using provided close odds: {lo.name}")
        else: log(f"provided close odds not found: {lo}")

    # Always write header first
    headers = ["event_date","tournament","player","odds","stake","result","payout","delta",
               "bankroll_before","bankroll_after","source"]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()

    if not csv_has_rows(log_csv):
        log(f"no trade log rows at {log_csv}; wrote header-only {out_csv.name}")
        return

    rows = list(csv.DictReader(log_csv.open("r", encoding="utf-8")))
    assume_random = str(args.assume_random_if_missing).strip().lower() in ("1","true","yes","y")
    br = read_bankroll(state_dir)

    settled_any = False
    with out_csv.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        for r in rows:
            s, br, ok = settle_row(r, br, assume_random)
            if ok:
                w.writerow(s)
                settled_any = True

    if settled_any:
        write_bankroll(state_dir, br)
        log(f"settled trades written → {out_csv.name}; bankroll updated to {br:.2f}")
    else:
        log(f"no trades settled; bankroll unchanged ({br:.2f})")

if __name__ == "__main__":
    main()
