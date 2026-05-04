#!/usr/bin/env python3
"""
rename_price_terminology.py — Rename pricing terminology dashboard-wide:
  "Original Price" -> "List Price"
  "Current Price"  -> "Market Observed Price"

Targeted patterns only — does not touch product names that contain the
word "Original" (e.g., Levi's "501 Original Fit") because we match
explicit "<Original|Current> Price" / button label patterns.

Idempotent: safe to re-run; nothing matches the second time.
"""

import os
import re
import shutil
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML = os.path.join(HERE, "index.html")

# Replacements as (regex, substitution). Order matters — apply more
# specific patterns first so we don't double-rename.
RULES = [
    # Phrases first
    (r'\bOriginal\s+Price\b',                    'List Price'),
    (r'\boriginal\s+price\b',                    'list price'),
    (r'\bCurrent\s+Price\b',                     'Market Observed Price'),
    (r'\bcurrent\s+price\b',                     'market observed price'),
    # KPI tile / chart titles  "Current vs Original"
    (r'\bCurrent\s+vs\s+Original\b',             'Market Observed vs List'),
    (r'\bcurrent\s+vs\s+original\b',             'market observed vs list'),
    # Inverted phrasing some places use
    (r'\bOriginal\s+vs\s+Current\b',             'List vs Market Observed'),
    (r'\boriginal\s+vs\s+current\b',             'list vs market observed'),
    # Toggle phrasing  "Current/Original toggle"
    (r'Current/Original\s+toggle',               'Market Observed/List toggle'),
    # Standalone button labels — only inside the price-toggle context
    # ('original' / 'current' as visible button text between > and <)
    (r'>Original</button>',                      '>List</button>'),
    (r'>Current</button>',                       '>Market Observed</button>'),
    # Standalone in column header (Box Plot / IQR labels) — phrases like
    # "Original price (Q1-Q3 box, ...)" or "Current price (...)"
    # Already handled by Original Price / Current Price rules above.
]


def main():
    dry = "--dry-run" in sys.argv
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()
    orig = len(html)

    counts = []
    for pat, repl in RULES:
        new_html, n = re.subn(pat, repl, html)
        counts.append((pat, repl, n))
        html = new_html

    print("Rename summary:")
    total = 0
    for pat, repl, n in counts:
        if n:
            print(f'  {n:>3}  {pat}  ->  {repl}')
        total += n
    print(f'Total replacements: {total}')
    print(f'index.html: {orig:,} -> {len(html):,} chars  ({len(html) - orig:+,})')

    if dry:
        print("[dry-run] no write")
        return 0
    if total == 0:
        print("Nothing to rename — already converted.")
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
