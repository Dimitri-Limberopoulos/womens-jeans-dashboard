#!/usr/bin/env python3
"""
enrich_dashboard.py
Extracts material/cotton% from all retailer source XLSX files and merges
into the dashboard RAW data. Also parses cotton% from Walmart long_description
and Old Navy product_details.
"""
import json, re, openpyxl, csv
from collections import defaultdict

BASE = "/sessions/hopeful-tender-dijkstra/mnt/Womens jeans scraper"

def parse_cotton_pct(text):
    """Extract cotton percentage from a text string."""
    if not text:
        return None
    m = re.search(r'(\d+)\s*%\s*(?:Organic\s*)?Cotton', str(text), re.I)
    if m:
        return int(m.group(1))
    return None

def cotton_pct_range(cp):
    if cp is None: return None
    if cp <= 25: return "0-25%"
    if cp <= 50: return "26-50%"
    if cp <= 75: return "51-75%"
    return "76-100%"

# ─── Build lookup tables from source XLSX files ────────────────────────────

# Key: (group_name, product_name_lower, color_lower) → cotton_pct
lookups = {}

# === TARGET ===
print("Building Target lookup...")
wb = openpyxl.load_workbook(f"{BASE}/target_pdp_results.xlsx", read_only=True)
ws = wb.active
headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
title_idx = headers.index('Title')
color_idx = headers.index('Color')
mat_idx = headers.index('Material')
ob_idx = headers.index('Owned Brand')
cotton_idx = headers.index('% Cotton') if '% Cotton' in headers else -1

target_count = 0
for row in ws.iter_rows(min_row=2, values_only=True):
    title = str(row[title_idx] or '').strip()
    color = str(row[color_idx] or '').strip()
    is_ob = str(row[ob_idx] or '').strip().lower() in ('true', '1', 'yes')
    group = 'Target OB' if is_ob else 'Target NB'
    
    cp = None
    # Try % Cotton column first
    if cotton_idx >= 0 and row[cotton_idx] is not None:
        try:
            cp = int(float(row[cotton_idx]))
        except (ValueError, TypeError):
            pass
    # Fallback: parse from Material
    if cp is None:
        cp = parse_cotton_pct(row[mat_idx])
    
    if cp is not None:
        key = (group, title.lower()[:80], color.lower())
        lookups[key] = cp
        target_count += 1
wb.close()
print(f"  Target: {target_count} entries with cotton %")

# === WALMART ===
print("Building Walmart lookup...")
wb = openpyxl.load_workbook(f"{BASE}/walmart_pdp_results.xlsx", read_only=True)
ws = wb.active
headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
name_idx = headers.index('product_name')
color_idx = headers.index('color')
fm_idx = headers.index('fabric_material')
fp_idx = headers.index('fabric_pct')
ld_idx = headers.index('long_description')

walmart_count = 0
for row in ws.iter_rows(min_row=2, values_only=True):
    name = str(row[name_idx] or '').strip()
    color = str(row[color_idx] or '').strip()
    
    cp = None
    # Try fabric_pct first (structured field)
    if fp_idx >= 0 and row[fp_idx] is not None:
        try:
            cp = int(float(row[fp_idx]))
        except (ValueError, TypeError):
            pass
    # Try fabric_material
    if cp is None:
        cp = parse_cotton_pct(row[fm_idx])
    # Try long_description
    if cp is None:
        cp = parse_cotton_pct(row[ld_idx])
    
    if cp is not None:
        key = ('Walmart OB', name.lower()[:80], color.lower())
        lookups[key] = cp
        walmart_count += 1
wb.close()
print(f"  Walmart: {walmart_count} entries with cotton %")

# === AMERICAN EAGLE ===
print("Building AE lookup...")
wb = openpyxl.load_workbook(f"{BASE}/ae_pdp_results.xlsx", read_only=True)
ws = wb.active
headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
name_idx = headers.index('product_name')
color_idx = headers.index('color')
cp_idx = headers.index('cotton_pct') if 'cotton_pct' in headers else -1
fm_idx = headers.index('fabric_material') if 'fabric_material' in headers else -1

ae_count = 0
for row in ws.iter_rows(min_row=2, values_only=True):
    name = str(row[name_idx] or '').strip()
    color = str(row[color_idx] or '').strip()
    
    cp = None
    if cp_idx >= 0 and row[cp_idx] is not None:
        try:
            val = str(row[cp_idx]).replace('%','').strip()
            cp = int(float(val))
        except (ValueError, TypeError):
            pass
    if cp is None and fm_idx >= 0:
        cp = parse_cotton_pct(row[fm_idx])
    
    if cp is not None:
        key = ('AE', name.lower()[:80], color.lower())
        lookups[key] = cp
        ae_count += 1
wb.close()
print(f"  AE: {ae_count} entries with cotton %")

# === OLD NAVY ===
print("Building Old Navy lookup...")
wb = openpyxl.load_workbook(f"{BASE}/oldnavy_pdp_results.xlsx", read_only=True)
ws = wb.active
headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
name_idx = headers.index('product_name')
color_idx = headers.index('color')
pd_idx = headers.index('product_details') if 'product_details' in headers else -1

on_count = 0
for row in ws.iter_rows(min_row=2, values_only=True):
    name = str(row[name_idx] or '').strip()
    color = str(row[color_idx] or '').strip()
    
    cp = None
    if pd_idx >= 0:
        cp = parse_cotton_pct(row[pd_idx])
    
    if cp is not None:
        key = ('Old Navy', name.lower()[:80], color.lower())
        lookups[key] = cp
        on_count += 1
wb.close()
print(f"  Old Navy: {on_count} entries with cotton %")

# === AMAZON ===
# Already confirmed: no material data available
print("  Amazon: 0 entries (no material data in source)")

print(f"\nTotal lookup entries: {len(lookups)}")

# ─── Now merge into dashboard RAW ──────────────────────────────────────────
print("\nReading dashboard...")
with open(f"{BASE}/cross_retailer_dashboard_v2.html", "r", encoding="utf-8") as f:
    html = f.read()

start = html.find('var RAW = [')
arr_start = html.find('[', start)
line_start = html.rfind('\n', 0, arr_start) + 1
line_end = html.find('\n', arr_start)
raw_line = html[line_start:line_end]
j_start = raw_line.find('[')
j_end = raw_line.rfind(']')
prefix = raw_line[:j_start]
suffix = raw_line[j_end+1:]

entries = json.loads(raw_line[j_start:j_end+1])
print(f"Total RAW entries: {len(entries)}")

# Merge cotton % from lookups
enriched = 0
already_had = 0
groups_enriched = defaultdict(int)
groups_total = defaultdict(int)

for e in entries:
    g = e.get('g', '')
    name = e.get('n', '')
    color = e.get('c', '')
    groups_total[g] += 1
    
    # Skip if already has cotton %
    if e.get('cp') is not None and e.get('cp') != '':
        already_had += 1
        continue
    
    # Try exact match
    key = (g, name.lower()[:80], color.lower())
    if key in lookups:
        cp = lookups[key]
        e['cp'] = cp
        e['cpr'] = cotton_pct_range(cp)
        enriched += 1
        groups_enriched[g] += 1
        continue
    
    # Try product-name-only match (for entries where color might differ slightly)
    # Build a name-only lookup on first pass
    pass

# Build name-only fallback (same group + product name, take most common cotton %)
name_lookup = defaultdict(list)
for (g, n, c), cp in lookups.items():
    name_lookup[(g, n)].append(cp)

# Second pass: try name-only match
for e in entries:
    if e.get('cp') is not None and e.get('cp') != '':
        continue
    g = e.get('g', '')
    name = e.get('n', '').lower()[:80]
    key = (g, name)
    if key in name_lookup:
        # Use the most common value
        vals = name_lookup[key]
        cp = max(set(vals), key=vals.count)
        e['cp'] = cp
        e['cpr'] = cotton_pct_range(cp)
        enriched += 1
        groups_enriched[g] += 1

print(f"\nAlready had cotton %: {already_had}")
print(f"Newly enriched: {enriched}")
print(f"\nEnrichment by group:")
for g in ['Target OB', 'Target NB', 'Walmart OB', 'Amazon OB', 'AE', 'Old Navy', 'Macys OB', 'Kohls OB', 'Levis']:
    total = groups_total.get(g, 0)
    new = groups_enriched.get(g, 0)
    existing = sum(1 for e in entries if e.get('g') == g and e.get('cp') is not None and e.get('cp') != '')
    print(f"  {g:15s}: {existing:4d}/{total:4d} now have cotton % (+{new} new)")

# Write back
new_json = json.dumps(entries, ensure_ascii=False, separators=(',', ':'))
new_raw_line = prefix + new_json + suffix
new_html = html[:line_start] + new_raw_line + html[line_end:]

with open(f"{BASE}/cross_retailer_dashboard_v2.html", "w", encoding="utf-8") as f:
    f.write(new_html)

print(f"\nDashboard updated!")
