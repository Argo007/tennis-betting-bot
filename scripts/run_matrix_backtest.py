#!/usr/bin/env python3
import argparse, csv, json, math
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional

DEFAULT_BANDS = "1.0,10.0"
DEFAULT_MIN_EDGE = 0.005
DEFAULT_STAKING = "kelly"
DEFAULT_KELLY_SCALE = 0.5
DEFAULT_BANKROLL = 1000

def parse_bands(spec: str) -> List[Tuple[float, float]]:
    spec = (spec or "").strip()
    parts = spec.split("|") if spec and "|" in spec else ([spec] if spec else [])
    out: List[Tuple[float, float]] = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        lo, hi = p.split(",")
        out.append((float(lo), float(hi)))
    return out

def rd(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def to_f(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")

def side_ok(odds: float, bands: List[Tuple[float, float]]) -> bool:
    return True if not bands else any(lo <= odds < hi for lo, hi in bands)

def kelly_fraction(p: float, o: float) -> float:
    b = o - 1.0
    q = 1.0 - p
    if b <= 0: return 0.0
    f = (b*p - q) / b
    return max(0.0, f)

def pick_prob(row: dict[str, Any], side: str) -> float:
    # prefer normalized pa/pb, fallback to vigfree
    if side == "A":
        for k in ("pa","prob_a","probA","implied_prob_a","prob_a_vigfree","p_a"):
            if k in row and row[k] != "": return to_f(row[k])
    else:
        for k in ("pb","prob_b","probB","implied_prob_b","prob_b_vigfree","p_b"):
            if k in row and row[k] != "": return to_f(row[k])
    return float("nan")

def odds_for(row: dict[str,Any], side: str) -> float:
    if side == "A":
        for k in ("oa","odds_a","odds_a_vigfree","oddsA"):
            if k in row and row[k] != "": return to_f(row[k])
    else:
        for k in ("ob","odds_b","odds_b_vigfree","oddsB"):
            if k in row and row[k] != "": return to_f(row[k])
    return float("nan")

def te_for(row: dict[str,Any], side: str) -> Optional[float]:
    key = "te_a" if side == "A" else "te_b"
    return to_f(row[key]) if key in row and row[key] != "" else None

def value_edge(p: float, o: float) -> float:
    return p*o - 1.0

def run(
    rows: list[dict[str, Any]],
    bands: List[Tuple[float,float]],
    min_edge: float,
    staking: str,
    kelly_scale: float,
    bankroll: float
) -> dict[str, Any]:

    bank = bankroll
    picks: list[dict[str, Any]] = []

    for r in rows:
        # If EdgeSmith provided a recommended pick, use it; otherwise choose best side by edge.
        if "pick" in r and r["pick"] in ("A","B"):
            side = r["pick"]
            p = to_f(r.get("pick_prob", "")) if "pick_prob" in r else pick_prob(r, side)
            o = odds_for(r, side)

            # edge: prefer true-edge if present, else p*o-1
            te = te_for(r, side)
            edge = te if te is not None and not math.isnan(te) else value_edge(p, o)

            if math.isnan(p) or math.isnan(o): 
                continue
            if edge < min_edge or not side_ok(o, bands):
                continue

        else:
            # compute edges both sides
            oa = odds_for(r, "A"); ob = odds_for(r, "B")
            pa = pick_prob(r, "A"); pb = pick_prob(r, "B")
            if any(math.isnan(x) for x in (oa, ob, pa, pb)): 
                continue

            # prefer true-edge if present, else value edge
            tea = te_for(r, "A"); teb = te_for(r, "B")
            edge_a = tea if tea is not None and not math.isnan(tea) else value_edge(pa, oa)
            edge_b = teb if teb is not None and not math.isnan(teb) else value_edge(pb, ob)

            choices: list[tuple[str,float,float,float]] = []
            if edge_a >= min_edge and side_ok(oa, bands): choices.append(("A", oa, pa, edge_a))
            if edge_b >= min_edge and side_ok(ob, bands): choices.append(("B", ob, pb, edge_b))
            if not choices: 
                continue
            side, o, p, edge = max(choices, key=lambda t: t[3])

        # staking
        stake = 1.0 if staking.lower()=="flat" else max(0.0, kelly_fraction(p, o)*kelly_scale*bank)
        if stake <= 0.0: 
            continue

        picks.append({
            "date": r.get("event_date") or r.get("date") or "",
            "player_a": r.get("player_a",""),
            "player_b": r.get("player_b",""),
            "side": side,
            "odds": round(o,3),
            "p": round(p,6),
            "edge": round(edge,6),
            "stake": round(stake,2)
        })

        # reserve & release bankroll (sizing only)
        bank -= stake
        bank = max(0.0, bank)
        bank += stake

    summary = {
        "cfg_id": 1,
        "n_bets": len(picks),
        "total_staked": round(sum(p["stake"] for p in picks), 2),
        "pnl": 0.0,
        "roi": 0.0,
        "hitrate": 0.0,
        "sharpe": 0.0,
        "end_bankroll": round(bank, 2),
    }
    return {"picks": picks, "summary": summary}

def save(outdir: Path, result: dict[str,Any]) -> None:
    outdir.mkdir(parents=True, exist_ok=True)

    with (outdir/"summary.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cfg_id","n_bets","total_staked","pnl","roi","hitrate","sharpe","end_bankroll"])
        s = result["summary"]
        w.writerow([s["cfg_id"], s["n_bets"], s["total_staked"], s["pnl"], s["roi"], s["hitrate"], s["sharpe"], s["end_bankroll"]])

    logp = outdir/"logs"/"picks_cfg1.csv"
    logp.parent.mkdir(parents=True, exist_ok=True)
    with logp.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date","player_a","player_b","side","odds","p","edge","stake"])
        for p in result["picks"]:
            w.writerow([p[k] for k in ("date","player_a","player_b","side","odds","p","edge","stake")])

    with (outdir/"params_cfg1.json").open("w", encoding="utf-8") as f:
        json.dump({"cfg_id": 1}, f, indent=2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="CSV: prefer outputs/edge_enriched.csv; fallback to outputs/prob_enriched.csv")
    ap.add_argument("--bands", default=DEFAULT_BANDS, help="e.g. '1.2,2.0|2.0,2.6|3.2,4.0'")
    ap.add_argument("--staking", default=DEFAULT_STAKING, choices=["kelly","flat"])
    ap.add_argument("--kelly-scale", type=float, default=DEFAULT_KELLY_SCALE)
    ap.add_argument("--min-edge", type=float, default=DEFAULT_MIN_EDGE, help="Filter on true-edge if present; else value edge")
    ap.add_argument("--bankroll", type=float, default=DEFAULT_BANKROLL)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    rows = rd(Path(args.input))
    res = run(
        rows=rows,
        bands=parse_bands(args.bands),
        min_edge=float(args.min_edge),
        staking=args.staking,
        kelly_scale=float(args.kelly_scale),
        bankroll=float(args.bankroll),
    )
    save(Path(args.outdir), res)

if __name__ == "__main__":
    main()
