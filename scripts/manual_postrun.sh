#!/usr/bin/env bash
# Manual post-run: CLV + settlement + state + dashboard (+ optional alerts)
# Usage:
#   bash scripts/manual_postrun.sh
# Optional env:
#   TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DISCORD_WEBHOOK_URL

set -euo pipefail

OUT="results"
LIVE="live_results"
STATE="state"
DOCS="docs"

echo "== Step 0: Ensure Python deps =="
python - <<'PY'
import sys, subprocess
try:
    import pandas  # noqa
except Exception:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
print("Python OK")
PY

echo "== Step 1: Sanity check for inputs =="
test -d "$LIVE" || { echo "Missing $LIVE (run workflow first)."; exit 1; }
test -f "$LIVE/live_matches.csv" || { echo "Missing $LIVE/live_matches.csv"; exit 1; }
test -f "$LIVE/live_odds.csv" || { echo "Missing $LIVE/live_odds.csv"; exit 1; }
mkdir -p "$STATE" "$DOCS"

echo "== Step 2: Produce close odds for CLV =="
python scripts/fetch_close_odds.py \
  --matches "$LIVE/live_matches.csv" \
  --odds "$LIVE/live_odds.csv" \
  --out "$LIVE/close_odds.csv"

echo "== Step 3: Settle open trades & update bankroll =="
# If you don’t have real results yet, we simulate outcome from p using --assume-random-if-missing
python scripts/settle_trades.py \
  --log "$STATE/trade_log.csv" \
  --close-odds "$LIVE/close_odds.csv" \
  --state-dir "$STATE" \
  --assume-random-if-missing

echo "== Step 4: Rebuild dashboard =="
python scripts/make_dashboard.py \
  --state-dir "$STATE" \
  --results "$OUT" \
  --live "$LIVE" \
  --out "$DOCS"

echo "== Step 5 (optional): Notify picks (if env vars exist) =="
python scripts/notify_picks.py \
  --live-outdir "$LIVE" \
  --backtest-outdir "$OUT" \
  --min-rows 1 || true

echo "== Done =="
echo "Outputs:"
echo " - $STATE/trade_log.csv (now with close_odds, clv, pnl, status=settled)"
echo " - $STATE/bankroll.json and $STATE/bankroll_history.csv (updated)"
echo " - $LIVE/close_odds.csv"
echo " - $DOCS/index.html (dashboard)"
