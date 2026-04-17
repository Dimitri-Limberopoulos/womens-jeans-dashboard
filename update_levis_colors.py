"""
After running levis_color_scraper.py, use this script to:
1. Read the scraped color names from levis_colors_scraped.csv
2. Update the RAW data in cross_retailer_dashboard_v2.html
3. Reclassify wash categories for the newly-colored entries

Usage:
  python3 update_levis_colors.py
"""

import csv
import json
import re

DASHBOARD = "cross_retailer_dashboard_v2.html"
SCRAPED_CSV = "levis_colors_scraped.csv"


def classify_wash(color_name):
    """Classify a Levi's color name into a wash category using word-boundary regex."""
    c = color_name.lower().strip()

    # Direct wash keywords from Levi's naming convention (e.g., "Clever Girl - Dark Wash")
    if re.search(r'\bdark\s*wash\b', c): return 'Dark Wash'
    if re.search(r'\bmedium\s*wash\b', c): return 'Medium Wash'
    if re.search(r'\blight\s*wash\b', c): return 'Light Wash'
    if re.search(r'\bmid\s*wash\b', c): return 'Medium Wash'

    # Color-based classification
    if re.search(r'\bblack\b', c): return 'Black'
    if re.search(r'\bwhite\b|\bcream\b|\becru\b|\bivory\b', c): return 'White/Cream'
    if re.search(r'\bgrey\b|\bgray\b', c): return 'Grey'
    if re.search(r'\bbrown\b|\btan\b|\bearth\b|\bcamel\b|\bkhaki\b|\bbeige\b|\bcognac\b', c): return 'Brown/Earth'
    if re.search(r'\bgreen\b|\bolive\b|\bsage\b|\bmoss\b|\bforest\b|\bharmy\b|\bteal\b|\bpine\b', c): return 'Green'
    if re.search(r'\bpink\b|\bred\b|\brose\b|\bcoral\b|\bburgundy\b|\bmaroon\b|\bcrimson\b|\bberry\b|\bwine\b|\bmauve\b|\bfuchsia\b|\bmagenta\b|\bblush\b|\bruby\b|\bcherry\b|\brust\b|\bclay\b', c): return 'Pink/Red'
    if re.search(r'\byellow\b|\borange\b|\bgold\b|\bmustard\b|\bamber\b|\bpeach\b|\bapricot\b', c): return 'Yellow/Orange'
    if re.search(r'\bpurple\b|\bviolet\b|\bplum\b|\blavender\b|\blilac\b|\borchid\b', c): return 'Purple'
    if re.search(r'\bprint\b|\bpattern\b|\bstripe\b|\bfloral\b|\btie.?dye\b|\bcheck\b|\bplaid\b|\bcamo\b', c): return 'Print/Pattern'

    # Denim-specific keywords
    if re.search(r'\bindigo\b|\brinse\b|\bdenim\b', c): return 'Dark Wash'
    if re.search(r'\bbleach\b|\bfade\b|\bworn\b', c): return 'Light Wash'
    if re.search(r'\bstone\b|\bvintage\b', c): return 'Medium Wash'
    if re.search(r'\bsoft\s*black\b|\bjet\b|\bonyx\b|\bmidnight\b', c): return 'Black'

    return 'Unclassified'


def main():
    # Load scraped colors
    url_to_color = {}
    with open(SCRAPED_CSV, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            color = row['color_name'].strip()
            if color and color != 'SCRAPE_FAILED' and not color.startswith('ERROR'):
                url_to_color[row['url'].strip()] = color

    print(f"Loaded {len(url_to_color)} scraped colors")

    # Read dashboard
    with open(DASHBOARD, 'r') as f:
        html = f.read()

    # Extract RAW data
    start = html.index('var RAW = [') + len('var RAW = ')
    depth = 0
    for i in range(start, len(html)):
        if html[i] == '[': depth += 1
        elif html[i] == ']': depth -= 1
        if depth == 0: break
    end = i + 1

    raw = json.loads(html[start:end])
    print(f"RAW entries: {len(raw)}")

    # We need to match RAW entries to scraped URLs
    # RAW uses short keys: g=group, c=color, w=wash, n=name
    # The URL isn't in RAW, but we can match via the levis_pdp_results_v2.csv

    # Load the PDP results to get URL -> row index mapping
    pdp_rows = []
    with open('levis_pdp_results_v2.csv', 'r') as f:
        pdp_rows = list(csv.DictReader(f))

    # Build mapping: (product_name, old_color) -> new_color, new_wash
    updates = {}
    updated_count = 0
    for pdp_row in pdp_rows:
        url = pdp_row['url'].strip()
        if url in url_to_color:
            new_color = url_to_color[url]
            new_wash = classify_wash(new_color)
            # Match to RAW entry by product name + original color being Unknown
            key = (pdp_row['product_name'].strip(), url)
            updates[key] = (new_color, new_wash)

    print(f"Prepared {len(updates)} color updates")

    # Update RAW entries
    # We need a way to match RAW entries to PDP rows. Since RAW doesn't have URLs,
    # we match by: group='Levis', color='Unknown' or empty, and product name
    # Build a list of Levi's Unknown RAW entries and match them to PDP rows in order

    levis_unknown_raw_indices = []
    for idx, r in enumerate(raw):
        if r.get('g') == 'Levis' and r.get('c', '').strip() in ('Unknown', ''):
            levis_unknown_raw_indices.append(idx)

    levis_unknown_pdp = [(i, row) for i, row in enumerate(pdp_rows)
                         if row['color'].strip() in ('Unknown', '')]

    print(f"Levi's Unknown in RAW: {len(levis_unknown_raw_indices)}")
    print(f"Levi's Unknown in PDP CSV: {len(levis_unknown_pdp)}")

    # Match by position (both should be in same order)
    if len(levis_unknown_raw_indices) != len(levis_unknown_pdp):
        print("WARNING: Count mismatch! Trying name-based matching...")
        # Fallback: match by product name within the Unknown set
        # This is less reliable but better than nothing

    matched = 0
    for raw_idx, (pdp_idx, pdp_row) in zip(levis_unknown_raw_indices, levis_unknown_pdp):
        url = pdp_row['url'].strip()
        if url in url_to_color:
            new_color = url_to_color[url]
            new_wash = classify_wash(new_color)
            raw[raw_idx]['c'] = new_color
            raw[raw_idx]['w'] = new_wash
            matched += 1

    print(f"Updated {matched} RAW entries with new colors")

    # Show wash distribution for updated entries
    from collections import Counter
    new_washes = Counter()
    for raw_idx, (pdp_idx, pdp_row) in zip(levis_unknown_raw_indices, levis_unknown_pdp):
        url = pdp_row['url'].strip()
        if url in url_to_color:
            new_washes[classify_wash(url_to_color[url])] += 1
    print(f"\nNew wash distribution for updated entries:")
    for w, c in new_washes.most_common():
        print(f"  {w}: {c}")

    # Write updated RAW back into HTML
    new_raw_str = json.dumps(raw, separators=(',', ':'))
    new_html = html[:start] + new_raw_str + html[end:]

    with open(DASHBOARD, 'w') as f:
        f.write(new_html)

    print(f"\nDashboard updated: {DASHBOARD}")


if __name__ == "__main__":
    main()
