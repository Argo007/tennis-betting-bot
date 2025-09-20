#!/usr/bin/env python3
import sys, argparse
import pandas as pd
from pathlib import Path

def infer_cols(df, mapping):
    out = {}
    for want, cands in mapping.items():
        for c in cands:
            if c in df.columns:
                out[want] = c
                break
    return out

def vigfree_from_odds(oa: float, ob: float):
    ia, ib = 1.0/oa, 1.0/ob
    s = ia + ib
    return ia/s, ib/s

def main():
    ap = argparse.ArgumentParser(description="Normalize various inputs -> outputs/prob_enriched.csv")
    ap.add_argument("--input", required=True, help="Path to CSV (e.g., results/tennis_data.csv)")
    ap.add_argument("--output", required=True, help="Destination CSV (e.g., outputs/prob_enriched.csv)")
    args = ap.parse_args()

    inp = Path(args.input)
    if not inp.exists():
        print(f"[prepare] ERROR: input not found: {inp}", file=sys.stderr)
        sys.exit(2)

    df = pd.read_csv(inp)

    # Flexible column aliases
    name_map = infer_cols(df, {
        "date":     ["date","event_date","match_date"],
        "player_a": ["player_a","home","A","team_a"],
        "player_b": ["player_b","away","B","team_b"],
        "oa":       ["oa","odds_a","a_odds","oddsA","odds"],   # 'odds' = total odds if 2-way only
        "ob":       ["ob","odds_b","b_odds","oddsB"],
        "pa":       ["pa","prob_a","p_a","p"],                 # 'p' = model prob for player_a
        "pb":       ["pb","prob_b","p_b"],
        "pa_vf":    ["prob_a_vigfree","pa_vigfree"],
        "pb_vf":    ["prob_b_vigfree","pb_vigfree"],
    })

    # Players (optional—fill blanks for synthetic sets)
    if "player_a" not in name_map:
        df["player_a"] = ""
        name_map["player_a"] = "player_a"
    if "player_b" not in name_map:
        df["player_b"] = ""
        name_map["player_b"] = "player_b"

    # Need odds.
    if "oa" not in name_map or "ob" not in name_map:
        print("[prepare] ERROR: need oa/ob columns to proceed.", file=sys.stderr)
        sys.exit(2)

    out = pd.DataFrame({
        "date":     df[name_map["date"]] if "date" in name_map else "",
        "player_a": df[name_map["player_a"]],
        "player_b": df[name_map["player_b"]],
        "oa":       pd.to_numeric(df[name_map["oa"]], errors="coerce"),
        "ob":       pd.to_numeric(df[name_map["ob"]], errors="coerce"),
    })

    # Probability priority: model p -> explicit pa/pb -> vig-free
    if "pa" in name_map:
        pa = pd.to_numeric(df[name_map["pa"]], errors="coerce").clip(0.0, 1.0)
        if "pb" in name_map:
            pb = pd.to_numeric(df[name_map["pb"]], errors="coerce").clip(0.0, 1.0)
        else:
            pb = 1.0 - pa
        source = "model_p"
    elif "pb" in name_map:
        pb = pd.to_numeric(df[name_map["pb"]], errors="coerce").clip(0.0, 1.0)
        pa = 1.0 - pb
        source = "explicit_pa_pb"
    elif "pa_vf" in name_map and "pb_vf" in name_map:
        pa = pd.to_numeric(df[name_map["pa_vf"]], errors="coerce").clip(0.0, 1.0)
        pb = pd.to_numeric(df[name_map["pb_vf"]], errors="coerce").clip(0.0, 1.0)
        source = "vigfree_from_input"
    else:
        pa, pb = [], []
        for oa, ob in zip(out["oa"], out["ob"]):
            a, b = vigfree_from_odds(float(oa), float(ob))
            pa.append(a); pb.append(b)
        pa = pd.Series(pa); pb = pd.Series(pb)
        source = "vigfree_from_oa_ob"

    out["pa"] = pa
    out["pb"] = pb
    out["prob_source"] = source

    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    print(f"[prepare] OK -> {args.output} (prob_source={source})")

if __name__ == "__main__":
    main()
