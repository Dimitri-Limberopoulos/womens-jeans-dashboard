#!/usr/bin/env python3
"""Extract color data from saved Levi's HTML pages."""
import json
import re
import os
import glob

INPUT_DIR = "/sessions/gallant-upbeat-allen/mnt/Womens jeans scraper/levis_html"
OUTPUT_FILE = "/sessions/gallant-upbeat-allen/mnt/Womens jeans scraper/levis_colors_remaining_1.json"

results = {}
errors = []

for filepath in sorted(glob.glob(os.path.join(INPUT_DIR, "*.html"))):
    code = os.path.basename(filepath).replace(".html", "")
    with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
        html = f.read()
    
    # Try to find __LSCO_INITIAL_STATE__
    m = re.search(r'window\.__LSCO_INITIAL_STATE__\s*=\s*(\{.+?\});\s*</script>', html, re.DOTALL)
    if m:
        try:
            state = json.loads(m.group(1))
            product = state.get('ssrViewStoreProduct', {})
            swatches = product.get('swatches', [])
            for s in swatches:
                swatch_code = s.get('code', '')
                color_name = s.get('colorName', '')
                if swatch_code and color_name:
                    results[swatch_code] = color_name
                elif swatch_code and not color_name:
                    # Try to get from title pattern
                    pass
            if not swatches:
                errors.append(f"{code}: no swatches in state")
        except json.JSONDecodeError as e:
            errors.append(f"{code}: JSON parse error: {e}")
    else:
        # Try colorName pattern directly
        color_matches = re.findall(r'"code"\s*:\s*"([^"]+)"[^}]*?"colorName"\s*:\s*"([^"]*)"', html)
        if color_matches:
            for c, cn in color_matches:
                if cn:
                    results[c] = cn
        else:
            # Try title pattern
            title_match = re.search(r'<title>([^<]+)</title>', html)
            if title_match:
                title = title_match.group(1)
                tm = re.match(r'.+ - (.+?)\s*\|', title)
                if tm:
                    results[code] = tm.group(1).strip()
                else:
                    errors.append(f"{code}: no color patterns found, title={title[:60]}")
            else:
                errors.append(f"{code}: no state, no color patterns, no title")

print(f"Extracted {len(results)} color entries")
if errors:
    print(f"Errors ({len(errors)}):")
    for e in errors:
        print(f"  {e}")

with open(OUTPUT_FILE, 'w') as f:
    json.dump(results, f, indent=2)
print(f"Saved to {OUTPUT_FILE}")
