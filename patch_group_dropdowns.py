#!/usr/bin/env python3
"""
patch_group_dropdowns.py — Rebuild the inner contents of every group
dropdown in index.html so they include all 11 groups in canonical order:

  Target OB, Target NB, Target 3P, Walmart OB, Walmart NB, Amazon OB,
  AE, Old Navy, Macys OB, Kohls OB, Levis

The dropdowns were hardcoded with the original 9 groups; Target 3P and
Walmart NB never made it into the option lists, even though the GROUPS
JS array and KPI tiles were updated.

Targets:
  - hm-g1   (Heatmap Group 1)
  - hm-g2   (Heatmap Group 2)
  - sbs-gA  (Side-by-Side Group A)
  - sbs-gB  (Side-by-Side Group B)

Idempotent: each select's existing `selected` value is preserved so the
default A/B comparison doesn't get reset.
"""

import os
import re
import shutil
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML = os.path.join(HERE, "index.html")

# Canonical group order + display labels. Two label variants because the
# heatmap selects use shorter labels than side-by-side.
GROUPS = [
    "Target OB", "Target NB", "Target 3P",
    "Walmart OB", "Walmart NB", "Amazon OB",
    "AE", "Old Navy", "Macys OB", "Kohls OB", "Levis",
]

LABELS_LONG = {
    "Target OB":  "Target Owned Brands",
    "Target NB":  "Target National Brands",
    "Target 3P":  "Target 3P (Marketplace)",
    "Walmart OB": "Walmart Owned Brands",
    "Walmart NB": "Walmart National Brands",
    "Amazon OB":  "Amazon Owned Brands",
    "AE":         "American Eagle",
    "Old Navy":   "Old Navy",
    "Macys OB":   "Macy's OB",
    "Kohls OB":   "Kohl's OB",
    "Levis":      "Levi's",
}

LABELS_SHORT = {
    "Target OB":  "Target Owned",
    "Target NB":  "Target National",
    "Target 3P":  "Target 3P",
    "Walmart OB": "Walmart Owned",
    "Walmart NB": "Walmart National",
    "Amazon OB":  "Amazon Owned",
    "AE":         "American Eagle",
    "Old Navy":   "Old Navy",
    "Macys OB":   "Macy's OB",
    "Kohls OB":   "Kohl's OB",
    "Levis":      "Levi's",
}

# Which select uses which label set + default selection
SELECTS = [
    # (id, label_map, default_value)
    ("hm-g1",  LABELS_SHORT, "Target OB"),
    ("hm-g2",  LABELS_SHORT, "Walmart OB"),
    ("sbs-gA", LABELS_LONG,  "Target OB"),
    ("sbs-gB", LABELS_LONG,  "Walmart OB"),
]


def detect_default(select_inner_html, fallback):
    """Pull the currently-selected value from existing options, if any."""
    m = re.search(r'<option[^>]*\bvalue\s*=\s*"([^"]+)"[^>]*\bselected', select_inner_html)
    return m.group(1) if m else fallback


def render_options(label_map, selected):
    parts = []
    for g in GROUPS:
        sel = ' selected=""' if g == selected else ''
        parts.append(f'<option value="{g}"{sel}>{label_map[g]}</option>')
    return "\n        ".join(parts)


def rewrite_select(html, sel_id, label_map, default):
    """Find <select id="sel_id" ...> ... </select> and replace inner."""
    pattern = re.compile(
        r'(<select[^>]*\bid\s*=\s*"' + re.escape(sel_id) + r'"[^>]*>)(.*?)(</select>)',
        re.DOTALL,
    )
    m = pattern.search(html)
    if not m:
        print(f"  WARN: could not find <select id=\"{sel_id}\">")
        return html, False
    open_tag, inner, close_tag = m.group(1), m.group(2), m.group(3)
    selected_val = detect_default(inner, default)
    if selected_val not in label_map:
        selected_val = default
    new_inner = "\n        " + render_options(label_map, selected_val) + "\n      "
    new_select = open_tag + new_inner + close_tag
    return html[:m.start()] + new_select + html[m.end():], True


def main():
    dry = "--dry-run" in sys.argv
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()
    orig = len(html)
    for sel_id, label_map, default in SELECTS:
        html, ok = rewrite_select(html, sel_id, label_map, default)
        if ok:
            print(f"  rewrote <select id=\"{sel_id}\"> ({len(GROUPS)} options, default = '{default}')")
    new_len = len(html)
    print(f"\nindex.html: {orig:,} -> {new_len:,}  ({new_len - orig:+,} chars)")
    if dry:
        print("[dry-run] no write")
        return 0
    backup = INDEX_HTML + ".bak_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(INDEX_HTML, backup)
    print(f"Backup: {backup}")
    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
