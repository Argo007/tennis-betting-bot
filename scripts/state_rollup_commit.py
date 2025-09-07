#!/usr/bin/env python3
"""
State rollup + auto-commit for tennis-betting-bot.

Actions (idempotent, safe):
  1) Read state/bankroll.json (or BANKROLL_START if absent).
  2) If results/settlements.csv has rows, adopt the last bankroll_after.
  3) Append a line to results/bankroll_history.csv.
  4) Write updated state/bankroll.json.
  5) Git add/commit state/ + results/ (only if there are changes).

No required args. Always exits 0.

ENV (optional):
  BANKROLL_START          default 1000.0
  BANKROLL_FILE           path to bankroll.json (pipeline already sets this)
  GITHUB_ACTOR            used for commit author
  GITHUB_EMAIL            used for commit email (fallback "actions@github.com")
"""

from __future__ import annotations
import csv, json, os, subprocess, sys
from pathlib import Path
from datetime import datetime, timezone

# ---------- paths & env ----------
REPO_ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = REPO_ROOT / "state"
RES_DIR   = REPO_ROOT / "results"

BANKROLL_FILE_ENV = os.getenv("BANKROLL_FILE", str(STATE_DIR / "bankroll.json"))
BANKROLL_FILE     = Path(BANKROLL_FILE_ENV)
BANKROLL_START    = float(os.getenv("BANKROLL_START", "1000.0"))

AUTHOR_NAME  = os.getenv("GITHUB_ACTOR", "github-actions[bot]")
AUTHOR_EMAIL = os.getenv("GITHUB_EMAIL", "actions@github.com")
COMMIT_MSG   = "Auto-update state & results [skip ci]"

SETTLEMENTS_CSV = RES_DIR / "settlements.csv"
HISTORY_CSV     = RES_DIR / "bankroll_history.csv"

def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[state_rollup] {ts} {msg}", flush=True)

# ---------- bankroll helpers ----------
def read_bankroll() -> float:
    if BANKROLL_FILE.exists():
        try:
            data = json.loads(BANKROLL_FILE.read_text())
            return float(data.get("bankroll", BANKROLL_START))
        except Exception:
            return BANKROLL_START
    return BANKROLL_START

def write_bankroll(value: float) -> None:
    BANKROLL_FILE.parent.mkdir(parents=True, exist_ok=True)
    BANKROLL_FILE.write_text(json.dumps({"bankroll": round(value, 2)}, indent=2))

def settlements_last_bankroll() -> float | None:
    """Return last bankroll_after from settlements.csv, or None if not present."""
    if not SETTLEMENTS_CSV.exists() or SETTLEMENTS_CSV.stat().st_size == 0:
        return None
    try:
        with SETTLEMENTS_CSV.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            last = None
            for row in reader:
                last = row
            if not last:
                return None
            val = last.get("bankroll_after") or last.get("bankroll")
            return float(val) if val not in (None, "") else None
    except Exception:
        return None

def append_history(value: float) -> None:
    RES_DIR.mkdir(parents=True, exist_ok=True)
    header = ["time_utc", "bankroll"]
    write_header = not HISTORY_CSV.exists() or HISTORY_CSV.stat().st_size == 0
    with HISTORY_CSV.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(header)
        w.writerow([datetime.now(timezone.utc).isoformat(), round(value, 2)])

# ---------- git helpers ----------
def run(cmd: str, check: bool=False):
    proc = subprocess.run(cmd, shell=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=REPO_ROOT)
    if check and proc.returncode != 0:
        log(f"FAILED: {cmd}\n{proc.stderr}")
        sys.exit(0)  # never fail CI
    return proc

def has_changes() -> bool:
    res = run("git status --porcelain")
    return bool(res.stdout.strip())

def commit_paths(paths: list[str]) -> None:
    run(f'git config user.name "{AUTHOR_NAME}"')
    run(f'git config user.email "{AUTHOR_EMAIL}"')
    for p in paths:
        if Path(p).exists():
            run(f"git add {p}")
    if has_changes():
        run(f'git commit -m "{COMMIT_MSG}"', check=True)
        log("Committed state/results changes")
    else:
        log("No changes to commit")

# ---------- main ----------
def main():
    # 1) current bankroll
    current = read_bankroll()
    # 2) pick last from settlements if any
    settled = settlements_last_bankroll()
    final_val = settled if settled is not None else current
    # 3) append history (always)
    append_history(final_val)
    # 4) write bankroll.json (only if changed or file missing)
    if (not BANKROLL_FILE.exists()) or (abs(final_val - current) > 1e-9):
        write_bankroll(final_val)
        log(f"Updated bankroll: {current:.2f} → {final_val:.2f}")
    else:
        log(f"Bankroll unchanged: {final_val:.2f}")

    # 5) commit state + results (never fails)
    commit_paths([str(STATE_DIR), str(RES_DIR)])
    sys.exit(0)

if __name__ == "__main__":
    main()
