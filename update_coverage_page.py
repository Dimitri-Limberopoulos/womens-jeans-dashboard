#!/usr/bin/env python3
"""
update_coverage_page.py — Regenerate the Data Coverage page table on
index.html from RAW. Adds Target 3P + Walmart NB columns, fixes the
stale Target NB column (was 2578 from pre-split, should be 67), and
adds a "Scraped" row showing capture dates per retailer.

Idempotent: replaces the existing coverage table in place.
"""

import json
import os
import re
import shutil
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML = os.path.join(HERE, "index.html")

# Canonical group order + display labels for the table header
GROUPS = [
    ("Target OB",  "Target OB"),
    ("Target NB",  "Target NB"),
    ("Target 3P",  "Target 3P"),
    ("Walmart OB", "Walmart OB"),
    ("Walmart NB", "Walmart NB"),
    ("Amazon OB",  "Amazon OB"),
    ("AE",         "American Eagle"),
    ("Old Navy",   "Old Navy"),
    ("Macys OB",   "Macy's OB"),
    ("Kohls OB",   "Kohl's OB"),
    ("Levis",      "Levi's"),
]

# Per-field coverage definitions. Each: (label, sub-description, predicate)
FIELDS = [
    ("Price",           "Market observed price > 0",
        lambda r: isinstance(r.get("p"), (int, float)) and r.get("p", 0) > 0),
    ("Color / Wash",    "Color classified (not Unclassified/blank)",
        lambda r: bool(r.get("w")) and r.get("w") != "Unclassified"),
    ("Rise",            "Rise categorized (Low/Mid/High/Super High)",
        lambda r: bool(r.get("ri"))),
    ("Leg Shape",       "Leg shape identified (Skinny/Straight/Wide Leg etc.)",
        lambda r: bool(r.get("le"))),
    ("Fit Style",       "Fit mapped (Slim/Contemporary, Regular, Relaxed, Curvy)",
        lambda r: bool(r.get("fi"))),
    ("Cotton %",        "Numeric cotton percentage available",
        lambda r: isinstance(r.get("cp"), (int, float)) or isinstance(r.get("cot"), (int, float))),
    ("Fabric Weight",   "Fabric weight category (Lightweight/Midweight/Heavyweight)",
        lambda r: bool(r.get("fw"))),
]

# Scrape date per group — sourced from progress files / file timestamps
SCRAPE_DATES = {
    "Target OB":  "Apr 15-17, 2026",
    "Target NB":  "Apr 15-17, 2026",
    "Target 3P":  "Apr 15-17, 2026",
    "Walmart OB": "Apr 16, 2026",
    "Walmart NB": "Apr 28, 2026",
    "Amazon OB":  "Apr 16, 2026",
    "AE":         "Apr 16, 2026",
    "Old Navy":   "Apr 16, 2026",
    "Macys OB":   "Apr 16, 2026",
    "Kohls OB":   "Apr 17, 2026",
    "Levis":      "Apr 17, 2026",
}


def load_raw():
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()
    i = html.find("var RAW = [")
    if i < 0:
        raise RuntimeError("Could not find var RAW in index.html")
    arr_open = i + len("var RAW = ")
    arr_end = html.find("];", arr_open)
    return html, json.loads(html[arr_open:arr_end + 1])


def color_for_pct(pct):
    """Traffic-light: green ≥80%, amber 40-79, red <40."""
    if pct >= 80:
        return ("#22c55e", "rgba(34,197,94,0.12)")
    if pct >= 40:
        return ("#f59e0b", "rgba(245,158,11,0.12)")
    return ("#ef4444", "rgba(239,68,68,0.12)")


def render_coverage_table(raw):
    # Group rows
    by_group = {g: [] for g, _ in GROUPS}
    for r in raw:
        if r.get("g") in by_group:
            by_group[r["g"]].append(r)

    # Build header
    header = "<thead><tr>"
    header += '<th style="text-align:left;padding:10px 12px;border-bottom:2px solid var(--bg3);font-size:.72rem;font-weight:700;color:var(--fg2);text-transform:uppercase;letter-spacing:.04em">Field</th>'
    for g, lbl in GROUPS:
        header += (
            '<th style="text-align:center;padding:10px 6px;border-bottom:2px solid var(--bg3);'
            'font-size:.7rem;font-weight:700;color:var(--fg);min-width:78px">'
            + lbl + '</th>'
        )
    header += "</tr></thead>"

    # Scraped row
    scraped_row = (
        '<tr style="border-bottom:1px solid var(--bg3);background:rgba(0,0,0,0.02)">'
        '<td style="padding:10px 12px;font-weight:700;color:var(--fg)">Scraped'
        '<div style="font-size:.58rem;color:var(--fg3);font-weight:400;margin-top:2px">Date data was captured from PDPs</div>'
        '</td>'
    )
    for g, _ in GROUPS:
        scraped_row += (
            '<td style="text-align:center;padding:8px 6px">'
            '<div style="font-size:.68rem;font-weight:600;color:var(--fg2);line-height:1.3">'
            + SCRAPE_DATES.get(g, "—") + '</div></td>'
        )
    scraped_row += "</tr>"

    # CC count row
    cc_row = (
        '<tr style="border-bottom:1px solid var(--bg3)">'
        '<td style="padding:10px 12px;font-weight:700;color:var(--fg)">CCs (n)'
        '<div style="font-size:.58rem;color:var(--fg3);font-weight:400;margin-top:2px">Total color combinations captured</div>'
        '</td>'
    )
    for g, _ in GROUPS:
        n = len(by_group[g])
        cc_row += (
            '<td style="text-align:center;padding:8px 6px">'
            '<div style="font-size:1rem;font-weight:800;color:var(--fg)">' + f'{n:,}' + '</div></td>'
        )
    cc_row += "</tr>"

    # Field coverage rows
    field_rows = []
    for label, sub, pred in FIELDS:
        row = (
            '<tr style="border-bottom:1px solid var(--bg3)">'
            '<td style="padding:10px 12px;font-weight:700;color:var(--fg)">' + label +
            '<div style="font-size:.58rem;color:var(--fg3);font-weight:400;margin-top:2px">' + sub + '</div></td>'
        )
        for g, _ in GROUPS:
            rows = by_group[g]
            n_total = len(rows)
            n_ok = sum(1 for r in rows if pred(r))
            pct = (100 * n_ok / n_total) if n_total else 0
            text_color, bg_color = color_for_pct(pct)
            row += (
                '<td style="text-align:center;padding:8px 6px">'
                '<div style="background:' + bg_color + ';border-radius:8px;padding:6px 4px">'
                '<div style="font-size:.9rem;font-weight:800;color:' + text_color + '">'
                + f'{pct:.0f}%' + '</div>'
                '<div style="font-size:.55rem;color:var(--fg3);margin-top:1px">'
                + f'{n_ok}/{n_total}' + '</div>'
                '</div></td>'
            )
        row += "</tr>"
        field_rows.append(row)

    body = "<tbody>" + scraped_row + cc_row + "".join(field_rows) + "</tbody>"

    table = (
        '<table style="width:100%;border-collapse:collapse;font-family:Montserrat,sans-serif;font-size:.78rem;background:var(--bg);border-radius:var(--radius);overflow:hidden">'
        + header + body + "</table>"
    )
    return table


# Find the existing coverage <table> within the page-coverage section and replace
def replace_table(html):
    start = html.find('<div id="page-coverage"')
    if start < 0:
        raise RuntimeError("page-coverage div not found")
    # Find the <table> element after this anchor (the first one in the section)
    tbl_start = html.find('<table', start)
    if tbl_start < 0:
        raise RuntimeError("No <table> found in page-coverage")
    tbl_end = html.find('</table>', tbl_start)
    if tbl_end < 0:
        raise RuntimeError("No </table> close found")
    tbl_end += len('</table>')

    raw_html, raw = load_raw()
    new_table = render_coverage_table(raw)
    return html[:tbl_start] + new_table + html[tbl_end:]


def main():
    dry = "--dry-run" in sys.argv
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()
    orig = len(html)
    new_html = replace_table(html)
    delta = len(new_html) - orig
    print(f"index.html: {orig:,} -> {len(new_html):,} chars ({delta:+,})")
    if dry:
        print("[dry-run] no write")
        return 0
    backup = INDEX_HTML + ".bak_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(INDEX_HTML, backup)
    print(f"Backup: {backup}")
    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(new_html)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
