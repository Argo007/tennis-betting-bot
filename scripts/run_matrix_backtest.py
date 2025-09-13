#!/usr/bin/env python3
import argparse, csv, json, math
from pathlib import Path
from typing import List, Tuple, Dict, Any

# ===== Debug-friendly defaults (wider bands, lower edge) =====
DEFAULT_BANDS = "1.0,10.0"     # wide open to force bets in debug
DEFAULT_MIN_EDGE = 0.005       # 0.5% minimum edge
DEFAULT_STAKING = "kelly"
DEFAULT_KELLY_SCALE = 0.5
DEFAULT_BANKROLL = 1000

def parse_bands(spec: str) -> List[Tuple[float, float]]:
    """
    Accepts either single range '1.0,10.0' or matrix format '1.2,2.0|2.0,2.6|...'
    Returns list of (low, high) inclusive of low, exclusive of high.
    """
    spec = (spec or "").strip()
    if "|" in spec:
        parts = spec.split("|")
    else:
        parts = [spec] if spec else []

    bands = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        lo, hi = p.split(",")
        bands.append((float(lo), float(hi)))
    return bands

def load_rows(input_csv: Path) -> List[Dict[str, Any]]:
    rows = []
    with input_csv.open(newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            rows.append(row)
    return rows

def to_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")

def pick_prob(row: Dict[str, Any], side: str) -> float:
    """
    Choose probability for side 'a' or 'b'. Prefer normalized 'pa'/'pb',
    fallback to vigfree columns if present.
    """
    if side == "a":
        if "pa" in row and row["pa"] != "":
            return to_float(row["pa"])
        if "prob_a_vigfree" in row and row["prob_a_vigfree"] != "":
            return to_float(row["prob_a_vigfree"])
    else:
        if "pb" in row and row["pb"] != "":
            return to_float(row["pb"])
        if "prob_b_vigfree" in row and row["prob_b_vigfree"] != "":
            return to_float(row["prob_b_vigfree"])
    return float("nan")

def side_ok(odds: float, bands: List[Tuple[float, float]]) -> bool:
    if not bands:
        return True
    for lo, hi in bands:
        if odds >= lo and odds < hi:
            return True
    return False

def kelly_fraction(p: float, o: float) -> float:
    """
    Kelly fraction for decimal odds o: b = o-1, f* = (bp - q)/b.
    Returns 0 if negative.
    """
    b = o - 1.0
    q = 1.0 - p
    if b <= 0:
        return 0.0
    f = (b * p - q) / b
    return max(0.0, f)

def run_backtest(
    rows: List[Dict[str, Any]],
    bands: List[Tuple[float, float]],
    min_edge: float,
    staking: str,
    kelly_scale: float,
    bankroll: float,
) -> Dict[str, Any]:
    bank = bankroll
    picks: List[Dict[str, Any]] = []

    for row in rows:
        oa = to_float(row.get("oa") or row.get("odds_a") or row.get("odds_a_vigfree"))
        ob = to_float(row.get("ob") or row.get("odds_b") or row.get("odds_b_vigfree"))
        pa = pick_prob(row, "a")
        pb = pick_prob(row, "b")

        # sanity
        if any(math.isnan(x) for x in [oa, ob, pa, pb]):
            continue

        # edges (value = p*odds - 1)
        edge_a = pa * oa - 1.0
        edge_b = pb * ob - 1.0

        # filter by bands + edge
        cand = []
        if edge_a >= min_edge and side_ok(oa, bands):
            cand.append(("A", oa, pa, edge_a))
        if edge_b >= min_edge and side_ok(ob, bands):
            cand.append(("B", ob, pb, edge_b))

        if not cand:
            continue

        # pick the better side
        side, o, p, edge = max(cand, key=lambda t: t[3])

        # stake
        if staking.lower() == "kelly":
            frac = kelly_fraction(p, o) * float(kelly_scale)
            stake = max(0.0, frac * bank)
        else:
            stake = 1.0  # flat unit

        if stake <= 0.0:
            continue

        # EV-only backtest (no realized results in sample): just log the pick + EV
        picks.append({
            "event_date": row.get("event_date", ""),
            "player_a": row.get("player_a", ""),
            "player_b": row.get("player_b", ""),
            "side": side,
            "odds": o,
            "p": p,
            "edge": edge,
            "stake": round(stake, 2),
        })

        # bankroll tracking as if we’re allocating (but not settling here)
        bank = max(0.0, bank - stake)  # reserve stake (for realism)
        # You can optionally “release” back after logging if you only want sizing not bankroll pressure
        bank += stake  # comment out this line to apply capital pressure

    # summary
    summary = {
        "cfg_id": 1,
        "n_bets": len(picks),
        "total_staked": round(sum(p["stake"] for p in picks), 2),
        "pnl": 0.0,            # no outcomes in this EV-only sample
        "roi": 0.0,
        "hitrate": 0.0,
        "sharpe": 0.0,
        "end_bankroll": round(bank, 2),
    }
    return {"picks": picks, "summary": summary}

def save_backtest(outdir: Path, result: Dict[str, Any]):
    outdir.mkdir(parents=True, exist_ok=True)
    # summary table
    with (outdir / "summary.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cfg_id","n_bets","total_staked","pnl","roi","hitrate","sharpe","end_bankroll"])
        s = result["summary"]
        w.writerow([s["cfg_id"], s["n_bets"], s["total_staked"], s["pnl"], s["roi"], s["hitrate"], s["sharpe"], s["end_bankroll"]])

    # picks log
    picks_path = outdir / "logs" / "picks_cfg1.csv"
    picks_path.parent.mkdir(parents=True, exist_ok=True)
    with picks_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_date","player_a","player_b","side","odds","p","edge","stake"])
        for p in result["picks"]:
            w.writerow([p["event_date"], p["player_a"], p["player_b"], p["side"], p["odds"], p["p"], p["edge"], p["stake"]])

    # params dump (so report can link it)
    with (outdir / "params_cfg1.json").open("w", encoding="utf-8") as f:
        json.dump({"cfg_id": 1}, f, indent=2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV with oa,ob, pa,pb (or prob_a_vigfree/prob_b_vigfree)")
    ap.add_argument("--bands", default=DEFAULT_BANDS, help="Odds bands: '1.0,10.0' or '1.2,2.0|2.0,2.6|...'")
    ap.add_argument("--staking", default=DEFAULT_STAKING, choices=["kelly","flat"])
    ap.add_argument("--kelly-scale", type=float, default=DEFAULT_KELLY_SCALE)
    ap.add_argument("--min-edge", type=float, default=DEFAULT_MIN_EDGE)
    ap.add_argument("--bankroll", type=float, default=DEFAULT_BANKROLL)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    bands = parse_bands(args.bands)
    rows = load_rows(Path(args.input))

    result = run_backtest(
        rows=rows,
        bands=bands,
        min_edge=float(args.min_edge),
        staking=args.staking,
        kelly_scale=float(args.kelly_scale),
        bankroll=float(args.bankroll),
    )
    save_backtest(Path(args.outdir), result)

if __name__ == "__main__":
    main()
