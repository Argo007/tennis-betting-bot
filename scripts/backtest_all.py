#!/usr/bin/env python3
"""
CLI wrapper around backtest_core.simulate; writes artifacts and job summary.
"""

import argparse
import json
import os
from pathlib import Path
import sys

# --- FIRST REPAIR: make sure peer modules in scripts/ are importable ---
# This lets `from backtest_core import ...` work reliably inside GitHub Actions.
sys.path.insert(0, str(Path(__file__).parent))

from backtest_core import Config, simulate  # noqa: E402


def write_summary_md(cfg: dict, diagnostics: dict, summary_row: dict) -> str:
    md = []
    md.append("# Tennis Bot — Backtest Summary\n\n")
    md.append("## Params\n")
    md.append("```json\n" + json.dumps(cfg, indent=2) + "\n```\n")
    md.append("## Diagnostics\n")
    md.append("```json\n" + json.dumps(diagnostics, indent=2) + "\n```\n")
    md.append("## Results\n\n")
    md.append("| cfg_id | n_bets | total_staked | pnl | roi | end_bankroll |\n")
    md.append("|---:|---:|---:|---:|---:|---:|\n")
    md.append(
        f"| {summary_row['cfg_id']} | {summary_row['n_bets']} | "
        f"{summary_row['total_staked']:.2f} | {summary_row['pnl']:.2f} | "
        f"{summary_row['roi']:.4f} | {summary_row['end_bankroll']:.4f} |\n"
    )
    return "".join(md)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default="data/raw/odds/sample_odds_enriched.csv")
    ap.add_argument("--bands", default="[1.2,2.0]")
    ap.add_argument("--min_edge", type=float, default=0.0)
    ap.add_argument("--staking", default="kelly")
    ap.add_argument("--kelly_scale", type=float, default=0.5)
    ap.add_argument("--bankroll", type=float, default=1000.0)
    ap.add_argument("--cfg_id", type=int, default=1)
    ap.add_argument("--outdir", default="artifacts")
    args = ap.parse_args()

    # Build config dict for logging + a Config object for the simulator
    cfg = {
        "cfg_id": args.cfg_id,
        "dataset": args.dataset,
        "bands": json.loads(args.bands) if isinstance(args.bands, str) else args.bands,
        "min_edge": args.min_edge,
        "staking": args.staking,
        "kelly_scale": args.kelly_scale,
        "bankroll": args.bankroll,
    }

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Diagnostics (basic file stats)
    src = Path(args.dataset)
    total_rows = 0
    if src.exists():
        with open(src, "r", encoding="utf-8") as f:
            total_rows = sum(1 for _ in f) - 1  # minus header
    diagnostics = {
        "source": str(src),
        "total_rows": total_rows,
        "usable_rows": None,
        "skipped_missing": 0,
        "notes": [],
    }

    # Run simulation
    cfg_obj = Config(
        cfg_id=cfg["cfg_id"],
        dataset=cfg["dataset"],
        bands=tuple(cfg["bands"]),
        min_edge=cfg["min_edge"],
        staking=cfg["staking"],
        kelly_scale=cfg["kelly_scale"],
        bankroll=cfg["bankroll"],
    )
    bets_df, summary_df = simulate(cfg_obj)

    # Persist artifacts
    (outdir / "bets_log.csv").write_text(bets_df.to_csv(index=False), encoding="utf-8")
    (outdir / "summary.csv").write_text(summary_df.to_csv(index=False), encoding="utf-8")
    with open(outdir / f"params_cfg{cfg['cfg_id']}.json", "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)

    # Job summary (GH Actions)
    md = write_summary_md(cfg, diagnostics, summary_df.iloc[0].to_dict())
    print(md)  # always print to logs
    gh_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh_summary:
        with open(gh_summary, "a", encoding="utf-8") as f:
            f.write(md)


if __name__ == "__main__":
    main()

