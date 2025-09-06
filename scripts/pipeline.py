#!/usr/bin/env python3
"""
tennis-betting-bot unified runner
- daily: full E2E (data -> picks -> dashboard -> notifications -> state)
- live: minimal loop for live picks & notifications
- backtest: dataset -> matrix runs -> reports -> summary

Design:
- subprocess calls to existing repo scripts (no fragile imports)
- clear logging, fail-fast, JSON run meta
- standard IO locations
"""
import argparse, json, os, subprocess, sys, time, shutil
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS   = REPO_ROOT / "scripts"
RESULTS   = REPO_ROOT / "results"
LIVE_RES  = REPO_ROOT / "live_results"
STATE_DIR = REPO_ROOT / "state"
DOTSTATE  = REPO_ROOT / ".state"
DOCS_DIR  = REPO_ROOT / "docs"
RUN_META  = RESULTS / "run_meta.json"

PY = sys.executable  # current python

def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{ts}] {msg}", flush=True)

def run(cmd, timeout=600):
    """Run a command list; raise on nonzero."""
    log(f"→ {cmd}")
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout)
    dt = time.time() - t0
    if proc.stdout:
        print(proc.stdout.strip())
    if proc.returncode != 0:
        if proc.stderr:
            print(proc.stderr.strip(), file=sys.stderr)
        raise RuntimeError(f"Command failed ({dt:.1f}s): {' '.join(cmd)}")
    log(f"✓ done in {dt:.1f}s")
    return proc

def ensure_dirs():
    for p in [RESULTS, LIVE_RES, STATE_DIR, DOTSTATE, DOCS_DIR, REPO_ROOT / "data" / "raw" / "odds"]:
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

def fresh_file(p: Path, max_age_min: int) -> bool:
    if not p.exists(): return False
    age = time.time() - p.stat().st_mtime
    return age <= max_age_min * 60

def step_fetch_daily():
    # Prefer close odds for daily; backfill with synthetic if needed
    run([PY, str(SCRIPTS / "fetch_tennis_data.py")])
    run([PY, str(SCRIPTS / "fetch_close_odds.py")])
    run([PY, str(SCRIPTS / "fill_with_synthetic_live.py")])  # harmless if not needed

def step_fetch_live():
    run([PY, str(SCRIPTS / "fetch_live_matches.py")])
    run([PY, str(SCRIPTS / "fetch_live_odds.py")])
    run([PY, str(SCRIPTS / "fill_with_synthetic_live.py")])

def step_build_dataset():
    run([PY, str(SCRIPTS / "build_from_raw.py")])
    run([PY, str(SCRIPTS / "build_dataset.py")])
    run([PY, str(SCRIPTS / "ensure_dataset.py")])
    run([PY, str(SCRIPTS / "compute_prob_vigfree.py")])
    run([PY, str(SCRIPTS / "check_probabilities.py")])
    run([PY, str(SCRIPTS / "edge_smith_enrich.py")])
    run([PY, str(SCRIPTS / "append_metrics.py")])

def step_engine_daily():
    # Use your pro engine as daily default
    run([PY, str(SCRIPTS / "tennis_value_picks_pro.py")])

def step_engine_live():
    run([PY, str(SCRIPTS / "tennis_value_picks_live.py")])
    # optional log of live decisions
    if (SCRIPTS / "log_live_picks.py").exists():
        run([PY, str(SCRIPTS / "log_live_picks.py")])

def step_outputs_and_notify():
    # dashboard
    if (SCRIPTS / "make_dashboard.py").exists():
        run([PY, str(SCRIPTS / "make_dashboard.py")])
        # docs/ is the GH Pages / dashboard output target
    # notifications (safe to skip if not configured)
    if (SCRIPTS / "notify_picks.py").exists():
        try:
            run([PY, str(SCRIPTS / "notify_picks.py")], timeout=120)
        except Exception as e:
            log(f"notify_picks soft-failed: {e}")

def step_state_rollup():
    # settle and update bankroll (skip silently if scripts not present)
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
    # optional freshness: ensure recent write
    if not fresh_file(picks_csv, 30):
        raise RuntimeError("picks_live.csv is stale (>30 min).")

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

def mode_backtest(args):
    # assumes dataset exists; but we can ensure build to be safe
    step_build_dataset()
    # matrix & sweeps if present
    if (SCRIPTS / "run_matrix_backtest.py").exists():
        run([PY, str(SCRIPTS / "run_matrix_backtest.py")])
    if (SCRIPTS / "parameter_sweep.py").exists():
        run([PY, str(SCRIPTS / "parameter_sweep.py")])
    # reports
    if (SCRIPTS / "generate_report.py").exists():
        run([PY, str(SCRIPTS / "generate_report.py")])
    if (SCRIPTS / "merge_report.py").exists():
        run([PY, str(SCRIPTS / "merge_report.py")])
    if (SCRIPTS / "quick_summary.py").exists():
        run([PY, str(SCRIPTS / "quick_summary.py")])

def main():
    parser = argparse.ArgumentParser(description="tennis-betting-bot unified pipeline")
    parser.add_argument("--mode", required=True, choices=["daily", "live", "backtest"])
    args = parser.parse_args()

    ensure_dirs()
    t0 = time.time()
    try:
        if args.mode == "daily":
            mode_daily()
        elif args.mode == "live":
            mode_live()
        else:
            mode_backtest(args)
        write_meta(args.mode, status="ok", extra={"elapsed_sec": round(time.time()-t0, 1)})
        log("ALL GOOD.")
    except Exception as e:
        write_meta(args.mode, status="error", extra={"error": str(e)})
        log(f"FATAL: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
