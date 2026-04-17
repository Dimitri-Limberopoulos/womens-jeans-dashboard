#!/usr/bin/env python3
"""Fix wash classifications in recovered dashboard using word-boundary regex."""
import json, re

BASE = "/sessions/gallant-upbeat-allen/mnt/Womens jeans scraper"

def classify_wash(color):
    if not color:
        return 'Unclassified'
    cl = color.lower().strip()
    if not cl or cl == 'unknown':
        return 'Unclassified'

    # 1. Explicit wash descriptors (highest priority)
    if re.search(r'\blight\s+wash\b', cl): return 'Light Wash'
    if re.search(r'\bmedium\s+wash\b|\bmid\s+wash\b', cl): return 'Medium Wash'
    if re.search(r'\bdark\s+wash\b', cl): return 'Dark Wash'

    # 2. Light/Medium/Dark modifiers (check before hue)
    has_light = bool(re.search(r'\blight\b|\bbright\b|\bpale\b|\bfaded\b|\bbleach\b|\bsun\b', cl))
    has_dark = bool(re.search(r'\bdark\b|\bdeep\b|\bmidnight\b', cl))
    has_medium = bool(re.search(r'\bmedium\b|\bmid\b', cl))

    # 3. Check for specific hue/color words
    if re.search(r'\bblack\b', cl): return 'Black'
    if re.search(r'\bwhite\b|\bcream\b|\bivory\b|\becru\b|\bbone\b', cl): return 'White/Cream'

    # Grey family
    if re.search(r'\bgrey\b|\bgray\b|\bcharcoal\b|\bslate\b|\bsilver\b|\bsmoke\b|\bheather\b|\bash\b|\bfog\b|\bpewter\b|\bgranite\b|\bgunmetal\b', cl):
        if has_light: return 'Light Wash'
        if has_dark: return 'Dark Wash'
        return 'Grey'

    # Brown/Earth family
    if re.search(r'\bbrown\b|\btan\b|\bkhaki\b|\bcamel\b|\bbeige\b|\bsand\b|\bcognac\b|\bchestnut\b|\bmocha\b|\btaupe\b|\bcinnamon\b|\bcopper\b|\bbronze\b|\brust\b|\bearth\b|\bclay\b|\bsienna\b|\bcoffee\b|\btoffee\b|\bcocoa\b|\bespresso\b|\bcaramel\b|\bwalnut\b|\bmahogany\b|\bumber\b|\bmushroom\b', cl):
        if has_light: return 'Light Wash'
        if has_dark: return 'Dark Wash'
        return 'Brown/Earth'

    # Green family
    if re.search(r'\bgreen\b|\bolive\b|\bmint\b|\bsage\b|\bforest\b|\bemerald\b|\blime\b|\bjuniper\b|\bteal\b|\bmoss\b|\bfern\b|\bjade\b|\bpine\b|\bhunter\b|\bkelly\b|\bceladon\b|\bpistachio\b|\bseafoam\b|\bbasil\b|\balpine\b', cl):
        if has_light: return 'Light Wash'
        if has_dark: return 'Dark Wash'
        return 'Green'

    # Pink/Red family
    if re.search(r'\bpink\b|\bred\b|\brose\b|\bcoral\b|\bsalmon\b|\bberry\b|\bfuchsia\b|\bmagenta\b|\bcrimson\b|\bburgundy\b|\bmaroon\b|\bwine\b|\bruby\b|\bcherry\b|\bcranberry\b|\bblush\b|\bmauve\b|\bmerlot\b|\bgarnet\b|\braspberry\b|\bscarlet\b', cl):
        if has_light: return 'Light Wash'
        if has_dark: return 'Dark Wash'
        return 'Pink/Red'

    # Yellow/Orange family
    if re.search(r'\byellow\b|\bgold\b|\bmustard\b|\blemon\b|\bsunflower\b|\bhoney\b|\bturmeric\b|\bamber\b|\bsaffron\b|\bmarigold\b|\bbutter\b|\bcanary\b|\borange\b|\btangerine\b|\bapricot\b|\bpeach\b|\bmango\b|\bpumpkin\b|\bcitrus\b|\bmelon\b', cl):
        if has_light: return 'Light Wash'
        if has_dark: return 'Dark Wash'
        return 'Yellow/Orange'

    # Purple family
    if re.search(r'\bpurple\b|\bviolet\b|\blilac\b|\blavender\b|\bplum\b|\borchid\b|\beggplant\b|\bamethyst\b|\bwisteria\b|\bfig\b|\bgrape\b|\bphlox\b|\bhydrangea\b', cl):
        if has_light: return 'Light Wash'
        if has_dark: return 'Dark Wash'
        return 'Purple'

    # Blue/Denim family → map to wash categories
    if re.search(r'\bblue\b|\bnavy\b|\bindigo\b|\bcobalt\b|\bazure\b|\bsapphire\b|\bdenim\b|\bocean\b|\bsky\b|\bmarine\b|\bcerulean\b', cl):
        if has_light: return 'Light Wash'
        if has_dark: return 'Dark Wash'
        return 'Medium Wash'

    # Wash-related words without explicit color
    if re.search(r'\brinse\b|\braw\b', cl): return 'Dark Wash'
    if re.search(r'\bwash\b|\bwashed\b|\bstone\b|\bstonewash\b', cl):
        if has_light: return 'Light Wash'
        if has_dark: return 'Dark Wash'
        return 'Medium Wash'

    # Print/Pattern
    if re.search(r'\bprint\b|\bpattern\b|\bstripe\b|\bfloral\b|\bplaid\b|\bcheck\b|\bcamo\b|\btie.?dye\b|\bpaisley\b|\bpolka\b|\bdot\b|\bgingham\b|\bleopard\b|\bsnake\b|\banimal\b|\bzebra\b|\bembroidery\b|\bembroidered\b', cl):
        return 'Print/Pattern'

    # Specific denim terms
    if re.search(r'\bonyx\b|\bnoir\b|\bjet\b', cl): return 'Black'
    if re.search(r'\bice\b|\bfrost\b', cl): return 'Light Wash'
    if re.search(r'\bsteel\b', cl): return 'Grey'
    if re.search(r'\bvanilla\b', cl): return 'White/Cream'

    return 'Unclassified'

# Read dashboard
with open(f"{BASE}/cross_retailer_dashboard_v2.html", "r") as f:
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
print(f"Total entries: {len(entries)}")

# Reclassify
changed = 0
old_dist = {}
new_dist = {}
for e in entries:
    old_w = e.get('w', '')
    old_dist[old_w] = old_dist.get(old_w, 0) + 1
    new_w = classify_wash(e.get('c', ''))
    new_dist[new_w] = new_dist.get(new_w, 0) + 1
    if old_w != new_w:
        changed += 1
    e['w'] = new_w

print(f"Changed: {changed}")
print(f"\nOLD distribution:")
for w, c in sorted(old_dist.items(), key=lambda x: -x[1]):
    print(f"  {w:20s}: {c:5d}")
print(f"\nNEW distribution:")
for w, c in sorted(new_dist.items(), key=lambda x: -x[1]):
    print(f"  {w:20s}: {c:5d}")

# Write back
new_json = json.dumps(entries, ensure_ascii=False, separators=(',', ':'))
new_raw_line = prefix + new_json + suffix
new_html = html[:line_start] + new_raw_line + html[line_end:]

with open(f"{BASE}/cross_retailer_dashboard_v2.html", "w") as f:
    f.write(new_html)
print("\nDashboard updated!")
