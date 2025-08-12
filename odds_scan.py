def write_markdown(hits):
    now_utc = datetime.now(timezone.utc)
    now_local = now_utc + timedelta(hours=2)  # CEST naive

    header = (
        "# Tennis Kelly Sweet Spots (>= {thr:.2f})\n\n"
        "Last updated: {local}  \n"
        "({utc})\n\n"
    ).format(
        thr=KELLY_THRESHOLD,
        local=now_local.strftime('%Y-%m-%d %H:%M CEST'),
        utc=now_utc.strftime('%Y-%m-%d %H:%M UTC'),
    )

    cols = ["Tournament","Match","Market","Line","Odds","p_mkt","p_adj","EV/u","Kelly","Start (CEST)","Start (UTC)","Book"]
    lines = ["| " + " | ".join(cols) + " |",
             "| " + " | ".join(["---"]*len(cols)) + " |"]

    if not hits:
        empty_row = ["–"]*len(cols)
        empty_row[0] = "No qualifying pre-match opportunities in the next {} hours".format(LOOKAHEAD_HOURS)
        lines.append("| " + " | ".join(empty_row) + " |")
        content = header + "\n".join(lines) + "\n"
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"Wrote {OUTPUT_FILE} (no qualifiers).")
        return

    for r in hits:
        row = [str(r.get(c,"")) for c in cols]
        lines.append("| " + " | ".join(row) + " |")

    content = header + "\n".join(lines) + "\n"
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"Wrote {OUTPUT_FILE} with {len(hits)} qualifiers.")
