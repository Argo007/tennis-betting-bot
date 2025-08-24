#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Auto-commit state/results to the current git branch.
Usage:
  python scripts/autocommit_state.py --paths state results live_results --message "update state"
Notes:
  - Requires the repo to be a git checkout with write permission.
  - In GitHub Actions, GITHUB_TOKEN must allow workflow to push (repo settings).
"""
import argparse, os, subprocess, sys, shlex

def run(cmd):
    print("+", cmd)
    return subprocess.run(cmd, shell=True, check=False, capture_output=True, text=True)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--paths", nargs="+", required=True)
    ap.add_argument("--message", default="auto: update state")
    ap.add_argument("--user-name", default="tennis-bot")
    ap.add_argument("--user-email", default="tennis-bot@users.noreply.github.com")
    args = ap.parse_args()

    # Basic git config (safe if already configured)
    run(f'git config user.name {shlex.quote(args.user_name)}')
    run(f'git config user.email {shlex.quote(args.user_email)}')

    # Add paths that exist
    any_added = False
    for p in args.paths:
        if os.path.exists(p):
            r = run(f"git add {shlex.quote(p)}")
            if r.returncode == 0:
                any_added = True

    if not any_added:
        print("Nothing to add.")
        sys.exit(0)

    # Commit if there are changes
    diff = run("git diff --cached --name-only")
    if diff.stdout.strip() == "":
        print("No staged changes.")
        sys.exit(0)

    r = run(f'git commit -m {shlex.quote(args.message)}')
    if r.returncode != 0:
        print("Commit failed:", r.stderr)
        sys.exit(1)

    # Push
    r = run("git push")
    if r.returncode != 0:
        print("Push failed:", r.stderr)
        sys.exit(1)

    print("State committed and pushed.")
  
