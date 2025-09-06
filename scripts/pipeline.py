#!/usr/bin/env python3
"""
tennis-betting-bot unified runner (with visible metrics, robust I/O)
- daily: fetch -> build -> enrich -> picks -> dashboard/notify -> state
- live:  live fetch -> build -> enrich -> picks -> dashboard/notify
- backtest: build -> matrix/sweeps -> reports -> summary

Features:
- One source of truth for metrics (Kelly, TrueEdge8 weights, filters)
- Prints metrics to logs and saves results/metrics_config.json
- Exports metrics as env vars to every sub-process
- Explicit --outdir/--odds for fetchers, explicit I/O for processors
- Writes run meta at results/run_meta.json
"""

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ---------- repo paths ----------
REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS   = REPO_ROOT / "scripts"

DATA_DIR  = REPO_ROOT / "data"
RAW_DIR   = DATA_DIR / "raw"
ODDS_DIR  = RAW_DIR / "odds"

RESULTS   = REPO_ROOT / "results"
LIVE_RES  = REPO_ROOT / "live_results"
STATE_DIR = REPO_ROOT / "state"
DOTSTATE  = REPO_ROOT / ".state"
DOCS_DIR  = REPO_ROOT / "docs"
OUTPUTS   = REPO_ROOT / "outputs"

RUN_META      = RESULTS / "run_meta.json"
METRICS_JSON  = RESULTS / "metrics_config.json"

PY = sys.executable  # current Python interpreter

# ---------- central metrics ----------
METRICS = {
    # Kelly & staking
    "KELLY_FRACTION": 0.50,
    "KELLY_SCALE": 1.00,
    "STAKE_CAP_PCT": 0.04,
    "DAILY_RISK_BUDGET_PCT": 0.12,

    # Value thresholds
    "MIN_EDGE_EV": 0.02,
    "MIN_PROBABILITY": 0.05,

    # TrueEdge8 weights
    "WEIGHT_SURFACE_BOOST": 0.18,
    "WEIGHT_RECENT_FORM": 0.22,
    "WEIGHT_ELO_CORE": 0.28,
    "WEIGHT_SERVE_RETURN_SPLIT": 0.10,
    "WEIGHT_HEAD2HEAD": 0.06,
    "WEIGHT_TRAVEL_FATIGUE": -0.05,
    "WEIGHT_INJURY_PENALTY": -0.07,
    "WEIGHT_MARKET_DRIFT": 0.08,

    # Odds & vig handling
    "VIG_METHOD": "shin",                   # shin | proportional | none
    "ODDS_PRIORITY": "close,live,synthetic",

    # Bankroll & settlement
    "BANKROLL_START": 1000.0,
    "BANKROLL_FILE": str(STATE_DIR / "bankroll.json"),

    # Hygiene / filters
    "IGNORE_INPLAY_AFTER_MIN": 25,
    "MAX_MATCHES_PER_EVENT": 3,
}

# ---------- helpers ----------
def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)

def ensure_dirs() -> None:
    for p in [RESULTS, LIVE_RES, STATE_DIR, DOTSTATE, DOCS_DIR, RAW_DIR, ODDS_DIR, OUTPUTS]:
        p.mkdir(parents=True, exist_ok=True)

def write_meta(mode: str, status: str = "ok", extra: dict | None = None) -> None:
    meta = {
        "mode": mode,
        "status": status,
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "git_sha": os.getenv("GITHUB_SHA", ""),
        "runner": os.getenv("GITHUB_RUN_ID", ""),
    }
    if extra:
        meta.update(extra)
    RESULTS.mkdir(parents=True, exist_ok=True)
    RUN_META.write_text(json.dumps(meta, indent=2))
    log(f"meta → {RUN_META}")

def dump_metrics() -> None:
    METRICS_JSON.write_text(json.dumps(METRICS, indent=2))
    log("=== ACTIVE METRICS / PARAMETERS ===")
    for k, v in METRICS.items():
        log(f"{k} = {v}")
    log(f"metrics → {METRICS_JSON}")

def run(cmd: list[str], timeout: int = 900, extra_env: dict | None = None):
    """Run a command with merged env; raise on nonzero exit."""
    env = os.environ.copy()
    # export metrics to children
    for k, v in METRICS.items():
        env[k] = str(v)
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})

    log(f"→ {cmd}")
    t0 = time.time()
    proc = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=env,
    )
    dt = time.time() - t0
    if proc.stdout:
        print(proc.stdout.rstrip())
    if proc.returncode != 0:
        if proc.stderr:
            print(proc.stderr.rstrip(), file=sys.stderr)
        raise RuntimeError(f"Command failed ({dt:.1f}s): {' '.join(cmd)}")
    log(f"✓ done in {dt:.1f}s")
    return proc

def fresh_file(p: Path, max_age_min: int) -> bool:
    if not p.exists():
        return False
    age = time.time() - p.stat().st_mtime
    return age <= max_age_min * 60

# ---------- steps: fetch ----------
def step_fetch_daily() -> None:
    # Matches/schedule
    run([PY, str(SCRIPTS / "fetch_tennis_data.py"), "--outdir", str(RAW_DIR)])
    # Closing odds with explicit provider
    run([
        PY, str(SCRIPTS / "fetch_close_odds.py"),
        "--outdir", str(ODDS_DIR),
        "--odds", "oddsportal",
    ])
    # Optional synthetic filler (safe if nothing missing)
    if (SCRIPTS / "fill_with_synthetic_live.py").exists():
        run([PY, str(SCRIPTS / "fill_with_synthetic_live.py"), "--outdir", str(ODDS_DIR)])

def step_fetch_live() -> None:
    run([PY, str(SCRIPTS / "fetch_live_matches.py"), "--outdir", str(RAW_DIR)])
    run([
        PY, str(SCRIPTS / "fetch_live_odds.py"),
        "--outdir", str(ODDS_DIR),
        "--odds", "oddsportal",
    ])
    if (SCRIPTS / "fill_with_synthetic_live.py").exists():
        run([PY, str(SCRIPTS / "fill_with_synthetic_live.py"), "--outdir", str(ODDS_DIR)])

# ---------- steps: build/enrich ----------
def step_build_dataset() -> None:
    # If you have a custom builder from raw parts, run it (no-op if absent)
    if (SCRIPTS / "build_from_raw.py").exists():
        run([PY, str(SCRIPTS / "build_from_raw.py")])

    # Build the unified dataset (writes data/raw/historical_matches.csv)
    run([PY, str(SCRIPTS / "build_dataset.py")])

    # Guard/normalize raw dataset (header-only tolerated)
    if (SCRIPTS / "ensure_dataset.py").exists():
        run([PY, str(SCRIPTS / "ensure_dataset.py")])

    # Vig-free probs
    run([
        PY, str(SCRIPTS / "compute_prob_vigfree.py"),
        "--input",  str(RAW_DIR / "historical_matches.csv"),
        "--output", str(RAW_DIR / "vigfree_matches.csv"),
        "--method", os.getenv("VIG_METHOD", "shin"),
    ])

    # Sanity checks → outputs/prob_enriched.csv
    run([PY, str(SCRIPTS / "check_probabilities.py")])

    # EdgeSmith/TrueEdge8 → outputs/edge_enriched.csv
    run([PY, str(SCRIPTS / "edge_smith_enrich.py")])

    # Quick metrics → results/quick_metrics.csv (non-fatal if empty)
    run([PY, str(SCRIPTS / "append_metrics.py")])

# ---------- steps: engine/output ----------
def step_engine_daily() -> None:
    # Uses defaults: IN=outputs/edge_enriched.csv, OUT=picks_live.csv + results/picks_YYYYMMDD.csv
    run([PY, str(SCRIPTS / "tennis_value_picks_pro.py")])

def step_engine_live() -> None:
    run([PY, str(SCRIPTS / "tennis_value_picks_live.py")])
    if (SCRIPTS / "log_live_picks.py").exists():
        run([PY, str(SCRIPTS / "log_live_picks.py")])

def step_outputs_and_notify() -> None:
    if (SCRIPTS / "make_dashboard.py").exists():
        run([PY, str(SCRIPTS / "make_dashboard.py")])
    if (SCRIPTS / "notify_picks.py").exists():
        try:
            run([PY, str(SCRIPTS / "notify_picks.py")], timeout=120)
        except Exception as e:
            log(f"notify_picks soft-failed: {e}")

def step_state_rollup() -> None:
    if (SCRIPTS / "settle_trades.py").exists():
        run([PY, str(SCRIPTS / "settle_trades.py")])
    if (SCRIPTS / "update_bankroll_state.py").exists():
        run([PY, str(SCRIPTS / "update_bankroll_state.py")])
    if (SCRIPTS / "autocommit_state.py").exists():
        run([PY, str(SCRIPTS / "autocommit_state.py")])

def guard_daily_outputs() -> None:
    picks_csv = REPO_ROOT / "picks_live.csv"
    # Always write a header even if empty; still check freshness
    if not picks_csv.exists():
        raise RuntimeError("picks_live.csv missing — engine did not write any file.")
    if not fresh_file(picks_csv, 30):
        raise RuntimeError("picks_live.csv is stale (>30 min).")

# ---------- modes ----------
def mode_daily() -> None:
    step_fetch_daily()
    step_build_dataset()
    step_engine_daily()
    guard_daily_outputs()
    step_outputs_and_notify()
    step_state_rollup()

def mode_live() -> None:
    step_fetch_live()
    step_build_dataset()
    step_engine_live()
    step_outputs_and_notify()

def mode_backtest() -> None:
    step_build_dataset()
    if (SCRIPTS / "run_matrix_backtest.py").exists():
        run([PY, str(SCRIPTS / "run_matrix_backtest.py")])
    if (SCRIPTS / "parameter_sweep.py").exists():
        run([PY, str(SCRIPTS / "parameter_sweep.py")])
    if (SCRIPTS / "generate_report.py").exists():
        run([PY, str(SCRIPTS / "generate_report.py")])
    if (SCRIPTS / "merge_report.py").exists():
        run([PY, str(SCRIPTS / "merge_report.py")])
    if (SCRIPTS / "quick_summary.py").exists():
        run([PY, str(SCRIPTS / "quick_summary.py")])

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="tennis-betting-bot unified pipeline")
    parser.add_argument("--mode", required=True, choices=["daily", "live", "backtest"])
    args = parser.parse_args()

    ensure_dirs()
    dump_metrics()

    t0 = time.time()
    try:
        if args.mode == "daily":
            mode_daily()
        elif args.mode == "live":
            mode_live()
        else:
            mode_backtest()
        elapsed = round(time.time() - t0, 1)
        write_meta(args.mode, status="ok", extra={"elapsed_sec": elapsed})
        log("ALL GOOD.")
    except Exception as e:
        write_meta(args.mode, status="error", extra={"error": str(e)})
        log(f"FATAL: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

