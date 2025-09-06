#!/usr/bin/env python3
"""
tennis-betting-bot unified runner (with visible metrics)
- daily: data -> dataset -> engine -> dashboard/notify -> state
- live:  live data -> dataset -> live engine -> dashboard/notify
- backtest: dataset -> matrix/sweeps -> reports -> summary

This version:
- Centralizes METRICS (Kelly + TrueEdge8 + risk controls)
- Prints metrics at start and writes results/metrics_config.json
- Exports metrics as env vars for every sub-process
- Explicit --outdir and --odds for fetchers to avoid CLI errors
"""

import argparse, json, os, subprocess, sys, time
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
RUN_META  = RESULTS / "run_meta.json"
METRICS_JSON = RESULTS / "metrics_config.json"

PY = sys.executable  # current python

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
    "VIG_METHOD": "shin",
    "ODDS_PRIORITY": "close,live,synthetic",

    # Bankroll & settlement
    "BANKROLL_START": 1000.0,
    "BANKROLL_FILE": str(STATE_DIR / "bankroll.json"),

    # Hygiene / filters
    "IGNORE_INPLAY_AFTER_MIN": 25,
    "MAX_MATCHES_PER_EVENT": 3,
}

# ---------- utils ----------
def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)

def ensure_dirs():
    for p in [RESULTS, LIVE_RES, STATE_DIR, DOTSTATE, DOCS_DIR, RAW_DIR, ODDS_DIR]:
        p.mkdir(parents=True, exist_ok=True)

def write_meta(mode, status="ok", extra=None):
    meta = {
        "mode": mode,
        "status": status,
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "git_sha": os.getenv("GITHUB_SHA", ""),
        "runner": os.getenv("GITHUB_RUN_ID", ""),
    }
    if extra: meta.update(extra)
    RESULTS.mkdir(parents=True, exist_ok=True)
    RUN_META.write_text(json.dumps(meta, indent=2))
    log(f"meta → {RUN_META}")

def dump_metrics():
    METRICS_JSON.write_text(json.dumps(METRICS, indent=2))
    log("=== ACTIVE METRICS / PARAMETERS ===")
    for k, v in METRICS.items():
        log(f"{k} = {v}")
    log(f"metrics → {METRICS_JSON}")

def run(cmd, timeout=900, extra_env=None):
    """Run a command with merged env; raise on nonzero."""
    env = os.environ.copy()
    for k, v in METRICS.items():
        env[k] = str(v)  # export metrics to children
    if extra_env:
        env.update({k: str(v) for k, v in extra_env.items()})
    log(f"→ {cmd}")
    t0 = time.time()
    proc = subprocess.run(
        cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout, env=env
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
    if not p.exists(): return False
    age = time.time() - p.stat().st_mtime
    return age <= max_age_min * 60

# ---------- steps ----------
def step_fetch_daily():
    run([PY, str(SCRIPTS / "fetch_tennis_data.py"), "--outdir", str(RAW_DIR)])
    run([
        PY, str(SCRIPTS / "fetch_close_odds.py"),
        "--outdir", str(ODDS_DIR),
        "--odds", "oddsportal"
    ])
    if (SCRIPTS / "fill_with_synthetic_live.py").exists():
        run([PY, str(SCRIPTS / "fill_with_synthetic_live.py"), "--outdir", str(ODDS_DIR)])

def step_fetch_live():
    run([PY, str(SCRIPTS / "fetch_live_matches.py"), "--outdir", str(RAW_DIR)])
    run([
        PY, str(SCRIPTS / "fetch_live_odds.py"),
        "--outdir", str(ODDS_DIR),
        "--odds", "oddsportal"
    ])
    if (SCRIPTS / "fill_with_synthetic_live.py").exists():
        run([PY, str(SCRIPTS / "fill_with_synthetic_live.py"), "--outdir", str(ODDS_DIR)])

def step_build_dataset():
    run([PY, str(SCRIPTS / "build_from_raw.py")])
    run([PY, str(SCRIPTS / "build_dataset.py")])
    run([PY, str(SCRIPTS / "ensure_dataset.py")])
    run([PY, str(SCRIPTS / "compute_prob_vigfree.py")])
    run([PY, str(SCRIPTS / "check_probabilities.py")])
    run([PY, str(SCRIPTS / "edge_smith_enrich.py")])
    run([PY, str(SCRIPTS / "append_metrics.py")])

def step_engine_daily():
    run([PY, str(SCRIPTS / "tennis_value_picks_pro.py")])

def step_engine_live():
    run([PY, str(SCRIPTS / "tennis_value_picks_live.py")])
    if (SCRIPTS / "log_live_picks.py").exists():
        run([PY, str(SCRIPTS / "log_live_picks.py")])

def step_outputs_and_notify():
    if (SCRIPTS / "make_dashboard.py").exists():
        run([PY, str(SCRIPTS / "make_dashboard.py")])
    if (SCRIPTS / "notify_picks.py").exists():
        try:
            run([PY, str(SCRIPTS / "notify_picks.py")], timeout=120)
        except Exception as e:
            log(f"notify_picks soft-failed: {e}")

def step_state_rollup():
    if (SCRIPTS / "settle_trades.py").exists():
        run([PY, str(SCRIPTS / "settle_trades.py")])
    if (SCRIPTS / "update_bankroll_state.py").exists():
        run([PY, str(SCRIPTS / "update_bankroll_state.py")])
    if (SCRIPTS / "autocommit_state.py").exists():
        run([PY, str(SCRIPTS / "autocommit_state.py")])

def guard_daily_outputs():
    picks_csv = REPO_ROOT / "picks_live.csv"
    if not picks_csv.exists() or picks_csv.stat().st_size == 0:
        raise RuntimeError("picks_live.csv missing or empty — engine produced no picks.")
    if not fresh_file(picks_csv, 30):
        raise RuntimeError("picks_live.csv is stale (>30 min).")

# ---------- modes ----------
def mode_daily():
    step_fetch_daily()
    step_build_dataset()
    step_engine_daily()
    guard_daily_outputs()
    step_outputs_and_notify()
    step_state_rollup()

def mode_live():
    step_fetch_live()
    step_build_dataset()
    step_engine_live()
    step_outputs_and_notify()

def mode_backtest():
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
