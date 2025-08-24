#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Notify value picks via Telegram and/or Discord.
Env:
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
  DISCORD_WEBHOOK_URL
Usage:
  python scripts/notify_picks.py --live-outdir live_results --backtest-outdir results --min-rows 1
"""
import os, argparse, pandas as pd, textwrap, json, time
import requests

def read_csv_safe(path):
    try:
        if os.path.isfile(path):
            df = pd.read_csv(path)
            return df if not df.empty else pd.DataFrame()
    except Exception as e:
        print(f"[warn] failed reading {path}: {e}")
    return pd.DataFrame()

def fmt_block(df, title, topn=8):
    if df.empty:
        return f"*{title}*\nNo picks.\n"
    cols = [c for c in ["match_id","sel","player_a","player_b","odds","p","edge","price","p_model"] if c in df.columns]
    df2 = df.copy()
    if "edge" in df2.columns:
        df2["edge"] = (df2["edge"]*100).round(1)
    if "p" in df2.columns:
        df2["p"] = (df2["p"]*100).round(1)
    df2 = df2[cols].head(topn)
    lines = [f"*{title}* (top {len(df2)})"]
    for _, r in df2.iterrows():
        parts = []
        if "match_id" in r: parts.append(f"`{r['match_id']}`")
        if "player_a" in r and "player_b" in r:
            parts.append(f"{r['player_a']} vs {r['player_b']}")
        if "sel" in r:
            parts.append(f"Pick: {r['sel']}")
        if "odds" in r:
            parts.append(f"odds {r['odds']}")
        if "p" in r:
            parts.append(f"p {r['p']}%")
        if "edge" in r:
            parts.append(f"edge {r['edge']}%")
        lines.append("• " + " | ".join(parts))
    return "\n".join(lines) + "\n"

def send_telegram(text):
    tok = os.getenv("TELEGRAM_BOT_TOKEN")
    chat = os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not chat:
        return False
    url = f"https://api.telegram.org/bot{tok}/sendMessage"
    payload = {"chat_id": chat, "text": text, "parse_mode": "Markdown"}
    r = requests.post(url, json=payload, timeout=15)
    print("[telegram]", r.status_code, r.text[:200])
    return r.ok

def send_discord(text):
    hook = os.getenv("DISCORD_WEBHOOK_URL")
    if not hook:
        return False
    payload = {"content": text}
    r = requests.post(hook, json=payload, timeout=15)
    print("[discord]", r.status_code, r.text[:200])
    return r.ok

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--live-outdir", default="live_results")
    ap.add_argument("--backtest-outdir", default="results")
    ap.add_argument("--min-rows", type=int, default=1, help="Minimum rows to trigger an alert")
    args = ap.parse_args()

    live_df = read_csv_safe(os.path.join(args.live_outdir, "picks_live.csv"))
    hist_df = read_csv_safe(os.path.join(args.backtest_outdir, "picks_final.csv"))

    msg = []
    ts = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime())
    msg.append(f"*Tennis Value Engine* — {ts}")
    msg.append(fmt_block(live_df, "LIVE PICKS"))
    msg.append(fmt_block(hist_df, "HISTORICAL PICKS"))
    text = "\n".join(msg)

    total_rows = len(live_df) + len(hist_df)
    if total_rows < args.min_rows:
        print("No alert sent (below min rows).")
        print(text)
        raise SystemExit(0)

    sent = send_telegram(text)
    sent |= send_discord(text)
    if not sent:
        print(text)  # fallback to stdout
