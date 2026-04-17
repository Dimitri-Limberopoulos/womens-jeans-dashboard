#!/usr/bin/env python3
"""
update_dashboard.py
Reads 3 new CSV files (Macy's, Kohl's, Levi's), transforms them into
the dashboard's RAW array format, injects into the existing cross-retailer
dashboard HTML, applies 9 modifications, and writes the updated file.

Output: cross_retailer_dashboard_v2.html
"""

import csv
import re
import os
import json
from collections import defaultdict

# ─── Paths ────────────────────────────────────────────────────────────────────
BASE = "/sessions/gallant-upbeat-allen/mnt/Womens jeans scraper"
DASHBOARD_IN = os.path.join(BASE, "cross_retailer_dashboard.html")
DASHBOARD_OUT = os.path.join(BASE, "cross_retailer_dashboard_v2.html")
MACYS_CSV = os.path.join(BASE, "macys_pdp_results_v2.csv")
KOHLS_CSV = os.path.join(BASE, "kohls_pdp_results_v2.csv")
LEVIS_CSV = os.path.join(BASE, "levis_pdp_results_v2.csv")

# ─── Constants ────────────────────────────────────────────────────────────────
NEW_GROUPS = [
    ("Macys OB", "Macy's Owned Brands", "#E21A2C", "rgba(226,26,44,0.18)"),
    ("Kohls OB", "Kohl's Owned Brands", "#6B2D8B", "rgba(107,45,139,0.18)"),
    ("Levis", "Levi's", "#C41230", "rgba(196,18,48,0.18)"),
]

TARGET_DASHBOARD = os.path.join(BASE, "jeans_dashboard.html")

LEG_SHAPE_KEYWORDS = [
    ("wide leg", "Wide Leg"), ("wide-leg", "Wide Leg"),
    ("skinny", "Skinny"), ("jegging", "Jegging"),
    ("slim", "Slim"), ("straight", "Straight"),
    ("bootcut", "Bootcut"), ("boot-cut", "Bootcut"), ("boot cut", "Bootcut"),
    ("flare", "Flare"),
    ("baggy", "Baggy"), ("barrel", "Barrel"),
    ("boyfriend", "Boyfriend"), ("relaxed", "Relaxed"),
    ("crop", "Crop"), ("cropped", "Crop"),
    ("tapered", "Tapered"), ("taper", "Tapered"),
    ("mom", "Mom"), ("trouser", "Trouser"),
    ("loose", "Loose"),
]

LEG_TO_FIT = {
    "Skinny": "Slim/Contemporary",
    "Slim": "Slim/Contemporary",
    "Jegging": "Slim/Contemporary",
    "Straight": "Regular",
    "Bootcut": "Regular",
    "Flare": "Regular",
    "Barrel": "Regular",
    "Tapered": "Regular",
    "Mom": "Regular",
    "Trouser": "Regular",
    "Wide Leg": "Relaxed",
    "Baggy": "Relaxed",
    "Boyfriend": "Relaxed",
    "Relaxed": "Relaxed",
    "Loose": "Relaxed",
    "Crop": None,  # keep existing or infer from context
}

OLD_FIT_COLLAPSE = {
    "Slim/Contemporary": "Slim/Contemporary",
    "Regular": "Regular",
    "Relaxed": "Relaxed",
    "Curvy": "Curvy",
    "Loose": "Relaxed",
    "Baggy": "Relaxed",
    "Rigid": "Regular",
}


# ─── Color → Wash classification ─────────────────────────────────────────────

def _word_match(keyword, text):
    """True if keyword appears as a whole word (word-boundary match) in text."""
    return bool(re.search(r'\b' + re.escape(keyword) + r'\b', text))


def _any_kw(keywords, text):
    """True if any keyword is a substring of text (for unambiguous long keywords)."""
    for kw in keywords:
        if kw in text:
            return True
    return False


def _match_list(safe_kw, boundary_kw, text):
    """Check safe keywords with substring, boundary keywords with word-boundary."""
    for kw in safe_kw:
        if kw in text:
            return True
    for kw in boundary_kw:
        if _word_match(kw, text):
            return True
    return False


def classify_wash(color_str):
    """Map a raw color name to a wash category.
    Returns a specific wash category or 'Unclassified' for unknowns.
    Never returns 'Other' — that legacy label is eliminated."""
    if not color_str:
        return "Unclassified"
    c = color_str.lower().strip()

    # Unknown / blank → Unclassified
    if c in ("unknown", "none", "n/a", "na", ""):
        return "Unclassified"

    # ── Levi's "Name - Wash" format (e.g. "Jazz Pop - Medium Wash") ──
    if " - " in c:
        wash_part = c.split(" - ", 1)[1].strip()
        result = classify_wash(wash_part)
        if result not in ("Other", "Unclassified"):
            return result

    # Print/Pattern first (high priority)
    pattern_kw = ["print", "pattern", "stripe", "floral", "camo", "plaid",
                   "tie-dye", "tie dye", "patchwork", "colorblock", "color block",
                   "polka", "check", "gingham", "leopard", "animal", "bandana",
                   "embroidery", "embriodary", "embriodery", "motif"]
    if _any_kw(pattern_kw, c):
        return "Print/Pattern"

    # "washed black" → Dark Wash (before Black check)
    if "washed black" in c or "faded black" in c:
        return "Dark Wash"

    # Black — "jet" needs word boundary to avoid matching inside other words
    black_safe = ["black", "noir", "onyx", "licorice"]
    black_boundary = ["jet"]
    if _match_list(black_safe, black_boundary, c):
        return "Black"
    if "charcoal" in c:
        return "Black"

    # White/Cream — "lily" needs boundary to avoid matching "amily" etc.
    white_safe = ["white", "cream", "ivory", "ecru", "bone", "vanilla", "natural",
                  "snow", "frost", "blanc", "pearl", "alabaster", "coconut",
                  "wht", "eyelet"]
    white_boundary = ["lily"]
    if _match_list(white_safe, white_boundary, c):
        return "White/Cream"

    # Grey — "ash" must NOT match inside "wash"; "stone", "fog", "mist",
    #         "moon", "steel", "dove" need word boundaries for safety
    grey_safe = ["grey", "gray", "silver", "slate", "pewter", "graphite",
                 "smoke", "cement", "concrete", "titanium", "nickel",
                 "heather", "potassium"]
    grey_boundary = ["ash", "stone", "fog", "mist", "moon", "steel", "dove"]
    if _match_list(grey_safe, grey_boundary, c):
        return "Grey"

    # Light Wash — "light", "faded", "cloud", "acid", "sky" need word boundary
    light_safe = ["light wash", "lt wash", "lt ", "lite", "bleach",
                  "pale blue", "baby blue", "ice blue", "sun wash"]
    light_boundary = ["light", "faded", "cloud", "acid", "sky"]
    if _match_list(light_safe, light_boundary, c):
        return "Light Wash"

    # Dark Wash — "dark", "deep", "night", "ink" need word boundary
    #             "ink" was matching inside "pink" → critical fix
    dark_safe = ["dark wash", "dk wash", "indigo", "rinse", "midnight",
                 "navy", "lapis"]
    dark_boundary = ["dark", "deep", "night", "ink"]
    if _match_list(dark_safe, dark_boundary, c):
        return "Dark Wash"

    # Medium Wash — "medium", "blue", "sea", "water", "red" need word boundary
    med_safe = ["medium wash", "med wash", "med ", "mid wash", "vintage",
                "classic", "denim", "destroy", "distress", "ocean",
                "authentic", "heritage", "traditional", "standard",
                "regular wash", "summit", "moddy", "perry", "river",
                "scout", "shale", "dream", "vega", "cruz", "cori", "marfa",
                "fresno", "alexis", "tint"]
    med_boundary = ["medium", "blue", "sea", "water"]
    if _match_list(med_safe, med_boundary, c):
        return "Medium Wash"

    # ── Macy's / Kohl's proprietary wash names (manually mapped) ──
    PROPRIETARY_MEDIUM = {
        "fortress", "render", "gates", "ashby", "hudson", "stockton",
        "essex", "mercer", "chester", "ames", "northern", "crosby",
        "zach", "logan", "scottie", "thorne", "lorene", "arvyn",
        "allison wa", "telluride", "pierre", "cradle pin", "rain song"
    }
    PROPRIETARY_DARK = {
        "kato", "bell"
    }
    PROPRIETARY_LIGHT = {
        "quick dip"
    }
    # Embroidery variants → Print/Pattern (catch typos)
    if any(x in c for x in ["embriodary", "embriodery", "emb "]):
        return "Print/Pattern"
    if c in PROPRIETARY_MEDIUM or any(c.startswith(p) for p in PROPRIETARY_MEDIUM):
        return "Medium Wash"
    if c in PROPRIETARY_DARK:
        return "Dark Wash"
    if c in PROPRIETARY_LIGHT:
        return "Light Wash"

    # Purple (before Pink/Red since plum can be ambiguous)
    purple_kw = ["purple", "lavender", "violet", "lilac", "mauve",
                 "eggplant", "amethyst", "plum", "orchid", "iris", "phlox"]
    if _any_kw(purple_kw, c):
        return "Purple"

    # Pink/Red — "red", "rose" need word boundary to avoid false positives
    pink_safe = ["pink", "coral", "berry", "blush", "wine",
                 "burgundy", "cranberry", "crimson", "scarlet", "maroon",
                 "cherry", "merlot", "raspberry", "fuchsia", "magenta",
                 "garnet", "ruby", "cardinal", "cerise", "poppy", "watermelon",
                 "lipstick", "petal", "ballerina", "phlox", "flamingo"]
    pink_boundary = ["red", "rose"]
    if _match_list(pink_safe, pink_boundary, c):
        return "Pink/Red"

    # Green
    green_kw = ["green", "teal", "olive", "sage", "moss", "forest",
                "emerald", "jade", "army", "hunter", "fern", "pine",
                "mint", "shamrock", "kelly", "spruce", "ivy", "basil",
                "seaweed", "eucalyptus", "pistachio"]
    if _any_kw(green_kw, c):
        return "Green"

    # Brown/Earth
    brown_safe = ["brown", "khaki", "camel", "earth", "rust",
                  "bronze", "sand", "cargo", "mushroom", "cognac",
                  "clay", "canyon", "mocha", "coffee", "espresso",
                  "chestnut", "cinnamon", "cocoa", "copper", "amber",
                  "sienna", "umber", "toffee", "walnut", "pecan",
                  "mahogany", "terracotta", "terra cotta", "hazel",
                  "wheat", "latte", "biscuit", "caramel", "nutmeg",
                  "tobacco", "saddle", "leather", "bark",
                  "beige", "shiitake"]
    brown_boundary = ["tan", "nut"]
    if _match_list(brown_safe, brown_boundary, c):
        return "Brown/Earth"

    # Yellow/Orange
    yellow_kw = ["yellow", "orange", "mustard", "gold", "peach",
                 "sunset", "honey", "lemon", "tangerine", "apricot",
                 "marigold", "saffron", "turmeric", "butterscotch",
                 "mango", "papaya", "pumpkin", "ginger", "sunrise"]
    if _any_kw(yellow_kw, c):
        return "Yellow/Orange"

    # Check for standalone "med" abbreviation
    if re.match(r'.*\bmed\b', c):
        return "Medium Wash"

    # Fallback: if the color string contains "wash", default to Medium Wash
    if "wash" in c:
        return "Medium Wash"

    # Truly unclassifiable proprietary names (Old Navy's "Tessa", "Nina", etc.)
    return "Unclassified"


# ─── Rise normalization ───────────────────────────────────────────────────────
def normalize_rise(rise_str, product_name="", description=""):
    """Normalize rise string to one of: Low, Mid, High, Super High."""
    combined = " ".join([
        (rise_str or ""),
        (product_name or ""),
        (description or "")
    ]).lower()

    # Check rise_str first if it exists, then fall back to name/desc
    sources = [(rise_str or "").lower()]
    if not rise_str or not rise_str.strip():
        sources = [combined]
    else:
        sources = [(rise_str or "").lower(), combined]

    for src in sources:
        if any(x in src for x in ["super high", "ultra high", "extra high"]):
            return "Super High"
        if "high" in src and "thigh" not in src:
            return "High"
        if any(x in src for x in ["mid", "regular rise", "classic rise"]):
            return "Mid"
        if "low" in src and "below" not in src and "flow" not in src:
            return "Low"
        if src != combined:
            continue
        break

    # If rise_str was provided but didn't match, try just name
    name_lower = (product_name or "").lower()
    if "super high" in name_lower or "ultra high" in name_lower:
        return "Super High"
    if "high rise" in name_lower or "high-rise" in name_lower:
        return "High"
    if "mid rise" in name_lower or "mid-rise" in name_lower:
        return "Mid"
    if "low rise" in name_lower or "low-rise" in name_lower:
        return "Low"

    return ""


# ─── Leg shape parsing ───────────────────────────────────────────────────────
def parse_leg_shape(product_name="", fit_field="", description=""):
    """Parse leg shape from product name, fit field, or description."""
    # Try fit field first (for Kohl's/Levi's)
    if fit_field and fit_field.strip():
        fit_lower = fit_field.strip().lower()
        for kw, shape in LEG_SHAPE_KEYWORDS:
            if kw in fit_lower:
                return shape

    # Try product name
    name_lower = (product_name or "").lower()
    for kw, shape in LEG_SHAPE_KEYWORDS:
        if kw in name_lower:
            return shape

    # Try description
    desc_lower = (description or "").lower()
    for kw, shape in LEG_SHAPE_KEYWORDS:
        if kw in desc_lower:
            return shape

    return ""


# ─── Fit style mapping ───────────────────────────────────────────────────────
def map_fit_style(leg_shape, product_name="", description=""):
    """Map leg shape to fit style. Also check for curvy keyword."""
    combined = ((product_name or "") + " " + (description or "")).lower()
    if "curvy" in combined:
        return "Curvy"

    if leg_shape and leg_shape in LEG_TO_FIT:
        mapped = LEG_TO_FIT[leg_shape]
        if mapped is not None:
            return mapped
        # Crop: try to infer
        if "curvy" in combined:
            return "Curvy"
        return "Regular"

    return ""


# ─── Cotton % range bucket ───────────────────────────────────────────────────
def cotton_pct_range(cp):
    """Bucket cotton percentage into a range string."""
    if cp is None:
        return None
    if cp <= 25:
        return "0-25%"
    if cp <= 50:
        return "26-50%"
    if cp <= 75:
        return "51-75%"
    return "76-100%"


# ─── Parse Target dashboard for fabric_weight / cotton_percent lookup ────────
def build_target_lookup():
    """Parse the original Target dashboard HTML to build a lookup map for
    fabric_weight and cotton_percent by (brand, color, current_price)."""
    lookup = {}
    if not os.path.exists(TARGET_DASHBOARD):
        print("  WARNING: Target dashboard not found at %s" % TARGET_DASHBOARD)
        return lookup

    with open(TARGET_DASHBOARD, "r", encoding="utf-8") as f:
        content = f.read()

    # Find window.DATA = { "rows": [...] }
    data_start = content.find("window.DATA")
    if data_start < 0:
        print("  WARNING: Could not find window.DATA in Target dashboard")
        return lookup

    # Find the opening { after window.DATA =
    brace_start = content.find("{", data_start)
    if brace_start < 0:
        return lookup

    # Find matching closing brace (simple approach: find "rows" array)
    # The structure is: window.DATA = { "rows": [...] };
    # We need to extract the JSON object
    # Find the end: look for }; after the data
    # Use a bracket counter
    depth = 0
    i = brace_start
    while i < len(content):
        if content[i] == '{':
            depth += 1
        elif content[i] == '}':
            depth -= 1
            if depth == 0:
                break
        i += 1

    json_str = content[brace_start:i + 1]
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        print("  WARNING: Could not parse Target dashboard JSON: %s" % str(e))
        return lookup

    rows = data.get("rows", [])
    for row in rows:
        brand = row.get("brand", "")
        color = row.get("color", "")
        price = row.get("current_price")
        fw = row.get("fabric_weight")
        cp = row.get("cotton_percent")

        if brand and color and price is not None:
            key = (brand.strip(), color.strip().lower(), round(float(price), 2))
            entry = {}
            if fw:
                entry["fw"] = fw
            if cp is not None:
                entry["cp"] = cp
                entry["cpr"] = cotton_pct_range(cp)
            if entry:
                lookup[key] = entry

    print("  Built Target lookup with %d entries" % len(lookup))
    return lookup


# ─── Cotton % parsing ────────────────────────────────────────────────────────
def parse_cotton_pct(material_str):
    """Extract cotton percentage from material string like '65% Cotton, 35% Lyocell'."""
    if not material_str:
        return None
    m = re.search(r'(\d+)\s*%\s*cotton', material_str, re.IGNORECASE)
    if m:
        return int(m.group(1))
    # Check if cotton is listed without percentage
    if re.search(r'\bcotton\b', material_str, re.IGNORECASE):
        return None  # has cotton but unknown %
    return None


# ─── Stretch parsing ─────────────────────────────────────────────────────────
def parse_stretch(stretch_str, material_str=""):
    """Determine Stretch vs No Stretch."""
    if stretch_str:
        s = stretch_str.strip().lower()
        if "no" in s or "none" in s or s == "false":
            return "No Stretch"
        if s in ("yes", "true") or "stretch" in s:
            return "Stretch"

    # Check material for elastane/spandex/lycra
    if material_str:
        mat_lower = material_str.lower()
        if any(x in mat_lower for x in ["elastane", "spandex", "lycra", "stretch"]):
            return "Stretch"

    return ""


# ─── Price parsing helpers ────────────────────────────────────────────────────
def safe_float(val, default=0.0):
    """Parse a float from string, handling currency symbols and commas."""
    if val is None:
        return default
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace("$", "").replace(",", "")
    try:
        return float(s)
    except (ValueError, TypeError):
        return default


def parse_bool(val):
    """Parse boolean from string."""
    if isinstance(val, bool):
        return val
    if val is None:
        return False
    return str(val).strip().lower() in ("true", "1", "yes", "t")


# ─── CSV reading ──────────────────────────────────────────────────────────────
def read_csv(path):
    """Read a CSV file and return list of dicts."""
    rows = []
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


# ─── Transform Macy's ────────────────────────────────────────────────────────
def transform_macys(rows):
    entries = []
    for row in rows:
        name = row.get("product_name", "")
        brand = row.get("brand", "")
        color = row.get("color", "")
        sale_price = safe_float(row.get("sale_price"))
        reg_price = safe_float(row.get("regular_price"))
        on_sale = parse_bool(row.get("on_sale"))
        pct_off = safe_float(row.get("percent_off"))
        desc = row.get("description", "")

        if reg_price <= 0:
            reg_price = sale_price
        cur_price = sale_price if on_sale and sale_price > 0 else reg_price
        if cur_price <= 0:
            cur_price = reg_price

        if pct_off <= 0 and on_sale and reg_price > 0 and sale_price > 0:
            pct_off = round((reg_price - sale_price) / reg_price * 100, 1)
        if not on_sale:
            pct_off = 0

        wash = classify_wash(color)
        rise = normalize_rise("", name, desc)
        leg = parse_leg_shape(name, "", desc)
        fit = map_fit_style(leg, name, desc)
        cot = parse_cotton_pct(desc)

        entry = {
            "g": "Macys OB",
            "n": name,
            "b": brand,
            "p": round(cur_price, 2),
            "o": round(reg_price, 2),
            "s": 1 if on_sale else 0,
            "d": round(pct_off, 1),
            "w": wash,
            "c": color,
            "ri": rise,
            "le": leg,
            "fi": fit,
            "ln": "",
        }
        if cot is not None:
            entry["cot"] = cot
        entries.append(entry)
    return entries


# ─── Transform Kohl's ────────────────────────────────────────────────────────
def transform_kohls(rows):
    entries = []
    for row in rows:
        name = row.get("product_name", "")
        brand = row.get("brand", "")
        color = row.get("color", "")
        sale_price = safe_float(row.get("sale_price"))
        reg_price = safe_float(row.get("regular_price"))
        on_sale = parse_bool(row.get("on_sale"))
        material = row.get("material", "")
        rise_field = row.get("rise", "")
        fit_field = row.get("fit", "")
        desc = row.get("description", "")

        if reg_price <= 0:
            reg_price = sale_price
        cur_price = sale_price if on_sale and sale_price > 0 else reg_price
        if cur_price <= 0:
            cur_price = reg_price

        pct_off = 0
        if on_sale and reg_price > 0 and sale_price > 0 and sale_price < reg_price:
            pct_off = round((reg_price - sale_price) / reg_price * 100, 1)

        wash = classify_wash(color)
        rise = normalize_rise(rise_field, name, desc)
        leg = parse_leg_shape(name, fit_field, desc)
        fit = map_fit_style(leg, name, desc)
        cot = parse_cotton_pct(material)

        entry = {
            "g": "Kohls OB",
            "n": name,
            "b": brand,
            "p": round(cur_price, 2),
            "o": round(reg_price, 2),
            "s": 1 if on_sale else 0,
            "d": round(pct_off, 1),
            "w": wash,
            "c": color,
            "ri": rise,
            "le": leg,
            "fi": fit,
            "ln": "",
        }
        if material:
            entry["mat"] = material
        if cot is not None:
            entry["cot"] = cot
        entries.append(entry)
    return entries


# ─── Transform Levi's ────────────────────────────────────────────────────────
def transform_levis(rows):
    entries = []
    for row in rows:
        name = row.get("product_name", "")
        brand = row.get("brand", "")
        color = row.get("color", "")
        sale_price = safe_float(row.get("sale_price"))
        reg_price = safe_float(row.get("regular_price"))
        on_sale = parse_bool(row.get("on_sale"))
        material = row.get("material", "")
        fit_field = row.get("fit", "")
        rise_field = row.get("rise", "")
        desc = row.get("description", "")

        if reg_price <= 0:
            reg_price = sale_price
        cur_price = sale_price if on_sale and sale_price > 0 else reg_price
        if cur_price <= 0:
            cur_price = reg_price

        pct_off = 0
        if on_sale and reg_price > 0 and sale_price > 0 and sale_price < reg_price:
            pct_off = round((reg_price - sale_price) / reg_price * 100, 1)

        wash = classify_wash(color)
        rise = normalize_rise(rise_field, name, desc)
        leg = parse_leg_shape(name, fit_field, desc)
        fit = map_fit_style(leg, name, desc)
        cot = parse_cotton_pct(material)

        entry = {
            "g": "Levis",
            "n": name,
            "b": brand,
            "p": round(cur_price, 2),
            "o": round(reg_price, 2),
            "s": 1 if on_sale else 0,
            "d": round(pct_off, 1),
            "w": wash,
            "c": color,
            "ri": rise,
            "le": leg,
            "fi": fit,
            "ln": "",
        }
        if material:
            entry["mat"] = material
        if cot is not None:
            entry["cot"] = cot
        entries.append(entry)
    return entries


# ─── Convert entry dict to JS object literal ─────────────────────────────────
def entry_to_js(e):
    """Convert a Python dict to a JSON-style object string (double quotes).
    The existing dashboard RAW data uses JSON format: {"g":"Target OB","n":"...","p":39.99,...}
    """
    obj = {}
    for key in ["g", "n", "b", "p", "o", "s", "d", "w", "c", "ri", "le", "fi", "ln", "mat", "cot", "fw", "cp", "cpr"]:
        if key not in e or e[key] is None:
            continue
        val = e[key]
        if key == "s":
            # Ensure s (on_sale) is numeric 1/0, not boolean
            obj[key] = 1 if val else 0
        elif isinstance(val, bool):
            obj[key] = 1 if val else 0
        elif isinstance(val, (int, float)):
            obj[key] = val
        elif isinstance(val, str):
            obj[key] = val.replace("\n", " ").replace("\r", "")
    return json.dumps(obj, ensure_ascii=False)


# ─── Compute insights from all RAW data ──────────────────────────────────────
def compute_insights(all_entries):
    """Compute the 8 key insights from the combined data."""
    insights = {}

    # Group data
    groups = defaultdict(list)
    for e in all_entries:
        groups[e["g"]].append(e)

    # Helper: count unique products
    def unique_products(entries):
        return len(set(e["n"] for e in entries))

    # Helper: median
    def median(vals):
        if not vals:
            return 0
        s = sorted(vals)
        n = len(s)
        if n % 2 == 0:
            return (s[n // 2 - 1] + s[n // 2]) / 2
        return s[n // 2]

    # ── Insight 1: Style selection breadth ──
    style_counts = {}
    for g, ents in groups.items():
        style_counts[g] = unique_products(ents)
    target_ob_styles = style_counts.get("Target OB", 0)
    # Find top 2 non-Target-OB
    others = sorted([(g, c) for g, c in style_counts.items() if g != "Target OB"],
                    key=lambda x: -x[1])
    comp1 = others[0] if len(others) > 0 else ("N/A", 0)
    comp2 = others[1] if len(others) > 1 else ("N/A", 0)
    insights["i1"] = {
        "target_styles": target_ob_styles,
        "comp1_name": comp1[0], "comp1_styles": comp1[1],
        "comp2_name": comp2[0], "comp2_styles": comp2[1],
    }

    # ── Insight 2: Colors per style ──
    avg_colors = {}
    for g, ents in groups.items():
        np = unique_products(ents)
        avg_colors[g] = round(len(ents) / np, 1) if np > 0 else 0
    target_ob_avg = avg_colors.get("Target OB", 0)
    all_avgs = [v for v in avg_colors.values()]
    cross_avg = round(sum(all_avgs) / len(all_avgs), 1) if all_avgs else 0
    insights["i2"] = {
        "target_avg": target_ob_avg,
        "cross_avg": cross_avg,
    }

    # ── Insight 3: Pricing band ──
    def price_range(ents):
        prices = [e["p"] for e in ents if e["p"] > 0]
        if not prices:
            return 0, 0, 0
        return min(prices), max(prices), max(prices) - min(prices)

    ranges = {}
    for g, ents in groups.items():
        lo, hi, span = price_range(ents)
        ranges[g] = {"lo": lo, "hi": hi, "span": span}

    target_r = ranges.get("Target OB", {"lo": 0, "hi": 0, "span": 0})
    levis_r = ranges.get("Levis", {"lo": 0, "hi": 0, "span": 0})
    ae_r = ranges.get("AE", {"lo": 0, "hi": 0, "span": 0})
    insights["i3"] = {
        "target_span": round(target_r["span"], 2),
        "target_lo": round(target_r["lo"], 2),
        "target_hi": round(target_r["hi"], 2),
        "levis_span": round(levis_r["span"], 2),
        "ae_span": round(ae_r["span"], 2),
    }

    # ── Insight 4: OB brands compete against themselves ──
    target_ob_ents = groups.get("Target OB", [])
    ob_brands = defaultdict(list)
    for e in target_ob_ents:
        ob_brands[e["b"]].append(e["p"])
    # Check overlap: for each CC, does its current price fall within another brand's range?
    brand_ranges = {}
    for b, prices in ob_brands.items():
        if prices:
            brand_ranges[b] = (min(prices), max(prices), round(median(prices), 2))
    overlap_count = 0
    total_ob = len(target_ob_ents)
    brand_names = list(brand_ranges.keys())
    for e in target_ob_ents:
        price = e["p"]
        my_brand = e["b"]
        for other_b in brand_names:
            if other_b == my_brand:
                continue
            lo, hi, _ = brand_ranges[other_b]
            if lo <= price <= hi:
                overlap_count += 1
                break
    overlap_pct = round(overlap_count / total_ob * 100) if total_ob > 0 else 0

    # Find two most overlapping brands
    ut_median = brand_ranges.get("Universal Thread", (0, 0, 0))[2]
    wf_median = brand_ranges.get("Wild Fable", (0, 0, 0))[2]
    insights["i4"] = {
        "overlap_pct": overlap_pct,
        "ut_median": ut_median,
        "wf_median": wf_median,
    }

    # ── Insight 5: OB vs NB overlap ──
    target_nb_ents = groups.get("Target NB", [])
    ob_prices_orig = [e["o"] for e in target_ob_ents if e["o"] > 0]
    ob_prices_cur = [e["p"] for e in target_ob_ents if e["p"] > 0]
    nb_prices_orig = [e["o"] for e in target_nb_ents if e["o"] > 0]
    nb_prices_cur = [e["p"] for e in target_nb_ents if e["p"] > 0]

    def range_overlap_pct(prices_a, prices_b):
        if not prices_a or not prices_b:
            return 0
        lo_a, hi_a = min(prices_a), max(prices_a)
        # Count how many of B fall within A's range
        count = sum(1 for p in prices_b if lo_a <= p <= hi_a)
        return round(count / len(prices_b) * 100) if prices_b else 0

    orig_overlap = range_overlap_pct(ob_prices_orig, nb_prices_orig)
    cur_overlap = range_overlap_pct(ob_prices_cur, nb_prices_cur)
    insights["i5"] = {
        "orig_overlap": orig_overlap,
        "cur_overlap": cur_overlap,
    }

    # ── Insight 6: Ceding entry-level ──
    def med_price(ents):
        prices = [e["p"] for e in ents if e["p"] > 0]
        return round(median(prices), 2) if prices else 0

    target_med = med_price(groups.get("Target OB", []))
    amazon_med = med_price(groups.get("Amazon OB", []))
    walmart_med = med_price(groups.get("Walmart OB", []))

    amazon_diff = round((target_med - amazon_med) / target_med * 100) if target_med > 0 and amazon_med > 0 else 0
    walmart_diff = round((target_med - walmart_med) / target_med * 100) if target_med > 0 and walmart_med > 0 else 0
    insights["i6"] = {
        "target_med": target_med,
        "amazon_med": amazon_med,
        "walmart_med": walmart_med,
        "amazon_diff": abs(amazon_diff),
        "walmart_diff": abs(walmart_diff),
    }

    # ── Insight 7: Rise distribution ──
    def rise_pct(ents, rise_val):
        with_rise = [e for e in ents if e.get("ri")]
        if not with_rise:
            return 0
        count = sum(1 for e in with_rise if e.get("ri") == rise_val)
        return round(count / len(with_rise) * 100)

    target_low = rise_pct(groups.get("Target OB", []), "Low")
    # Market average (all groups)
    all_with_rise = [e for e in all_entries if e.get("ri")]
    low_count = sum(1 for e in all_with_rise if e["ri"] == "Low")
    market_low = round(low_count / len(all_with_rise) * 100) if all_with_rise else 0
    insights["i7"] = {
        "target_low": target_low,
        "market_low": market_low,
    }

    # ── Insight 8: Wash mix ──
    def wash_pct(ents, wash_val):
        with_wash = [e for e in ents if e.get("w")]
        if not with_wash:
            return 0
        count = sum(1 for e in with_wash if e.get("w") == wash_val)
        return round(count / len(with_wash) * 100)

    target_light = wash_pct(groups.get("Target OB", []), "Light Wash")
    all_with_wash = [e for e in all_entries if e.get("w")]
    light_count = sum(1 for e in all_with_wash if e["w"] == "Light Wash")
    market_light = round(light_count / len(all_with_wash) * 100) if all_with_wash else 0
    over_exposed = target_light > market_light + 3
    insights["i8"] = {
        "target_light": target_light,
        "market_light": market_light,
        "over_exposed": over_exposed,
    }

    return insights


def build_insights_html(ins):
    """Build the Key Insights page HTML."""
    # Helper to get group labels
    gl = {
        "Target OB": "Target Owned Brands", "Target NB": "Target National Brands",
        "Walmart OB": "Walmart Owned Brands", "Amazon OB": "Amazon Owned Brands",
        "AE": "American Eagle", "Old Navy": "Old Navy",
        "Macys OB": "Macy's Owned Brands", "Kohls OB": "Kohl's Owned Brands", "Levis": "Levi's",
    }

    i1 = ins["i1"]
    i2 = ins["i2"]
    i3 = ins["i3"]
    i4 = ins["i4"]
    i5 = ins["i5"]
    i6 = ins["i6"]
    i7 = ins["i7"]
    i8 = ins["i8"]

    wash_commentary = ""
    if i8["over_exposed"]:
        wash_commentary = "This over-indexes vs market and limits appeal to customers preferring medium and dark washes."
    else:
        wash_commentary = "Wash mix is roughly in line with market."

    cards = [
        {
            "num": 1,
            "title": "Style Selection Too Narrow",
            "body": "Target Owned Brands offer only <strong>%d unique jean styles</strong> compared to %s at <strong>%d styles</strong> and %s at <strong>%d styles</strong>. This limits our ability to capture customer consideration across different silhouettes."
                     % (i1["target_styles"], gl.get(i1["comp1_name"], i1["comp1_name"]), i1["comp1_styles"],
                        gl.get(i1["comp2_name"], i1["comp2_name"]), i1["comp2_styles"]),
        },
        {
            "num": 2,
            "title": "Too Few Color/Wash Variations",
            "body": "Target OB averages <strong>%.1f colors per style</strong> vs the cross-retailer average of <strong>%.1f</strong>. Limited wash options reduce shelf impact and restrict customer choice."
                     % (i2["target_avg"], i2["cross_avg"]),
        },
        {
            "num": 3,
            "title": "Tightest Pricing Band",
            "body": "Target OB pricing spans just <strong>$%.0f</strong> (from $%.0f to $%.0f), the narrowest range of any retailer studied. Even premium brands like Levi&rsquo;s maintain a <strong>$%.0f range</strong>, and American Eagle spans <strong>$%.0f</strong>. There are no graduation tiers for customers to trade up."
                     % (i3["target_span"], i3["target_lo"], i3["target_hi"], i3["levis_span"], i3["ae_span"]),
        },
        {
            "num": 4,
            "title": "OB Brands Compete Against Themselves",
            "body": "After discounting, <strong>%d%%</strong> of Target OB CCs overlap in price with another Target OB brand. Universal Thread at current price (<strong>$%.2f median</strong>) overlaps directly with Wild Fable (<strong>$%.2f median</strong>)."
                     % (i4["overlap_pct"], i4["ut_median"], i4["wf_median"]),
        },
        {
            "num": 5,
            "title": "OB Brands Overlap with National Brands",
            "body": "At original pricing, Target OB overlaps with <strong>%d%%</strong> of NB price points. At current (discounted) price, overlap increases to <strong>%d%%</strong>. Discounting erodes planned differentiation."
                     % (i5["orig_overlap"], i5["cur_overlap"]),
        },
        {
            "num": 6,
            "title": "Ceding Entry-Level",
            "body": "Target OB median current price is <strong>$%.2f</strong> vs Amazon OB at <strong>$%.2f</strong> (%d%% lower) and Walmart OB at <strong>$%.2f</strong> (%d%% lower). Price-conscious customers have clear lower-cost alternatives."
                     % (i6["target_med"], i6["amazon_med"], i6["amazon_diff"], i6["walmart_med"], i6["walmart_diff"]),
        },
        {
            "num": 7,
            "title": "Over-Exposed on Low Rise",
            "body": "<strong>%d%%</strong> of Target OB CCs are Low Rise vs market average of <strong>%d%%</strong>. This exposes Target to trend risk if the low-rise cycle shifts."
                     % (i7["target_low"], i7["market_low"]),
        },
        {
            "num": 8,
            "title": "Wash Mix Assessment",
            "body": "Target OB indexes at <strong>%d%% Light Wash</strong> vs market average of <strong>%d%%</strong>. %s"
                     % (i8["target_light"], i8["market_light"], wash_commentary),
        },
    ]

    html = '\n<div id="page-insights" style="display:none">\n'
    html += '<div class="hero">\n'
    html += '  <span class="label">Strategic Analysis</span>\n'
    html += '  <h1>Key Insights</h1>\n'
    html += '  <p>Data-driven findings from the cross-retailer assortment analysis. Each insight is calculated from the full dataset.</p>\n'
    html += '</div>\n'
    html += '<div class="section">\n'
    html += '<div style="display:grid;grid-template-columns:1fr;gap:20px;max-width:960px">\n'

    for card in cards:
        html += '<div style="background:var(--bg);border:1px solid var(--bg3);border-radius:var(--radius);padding:24px 28px;transition:box-shadow .2s">\n'
        html += '  <div style="display:flex;align-items:baseline;gap:14px;margin-bottom:10px">\n'
        html += '    <span style="font-size:.7rem;font-weight:700;color:#002855;letter-spacing:.1em;padding:3px 10px;border:1.5px solid #002855;border-radius:18px">%02d</span>\n' % card["num"]
        html += '    <div style="font-size:1rem;font-weight:800;color:var(--fg);letter-spacing:-.02em">%s</div>\n' % card["title"]
        html += '  </div>\n'
        html += '  <div style="font-size:.85rem;color:var(--fg2);line-height:1.7">%s</div>\n' % card["body"]
        html += '</div>\n'

    html += '</div>\n</div>\n</div><!-- end page-insights -->\n'
    return html


# ─── Dynamic coverage page generator ─────────────────────────────────────────
GROUP_ORDER = [
    ("Target OB", "Target OB"),
    ("Target NB", "Target NB"),
    ("Walmart OB", "Walmart OB"),
    ("Amazon OB", "Amazon OB"),
    ("AE", "American Eagle"),
    ("Old Navy", "Old Navy"),
    ("Macys OB", "Macy's OB"),
    ("Kohls OB", "Kohl's OB"),
    ("Levis", "Levi's"),
]

COVERAGE_FIELDS = [
    ("Price", "Current price > 0", lambda e: e.get("p", 0) > 0),
    ("Color", "Color name populated", lambda e: bool(e.get("c", "").strip())),
    ("Wash Category", "Classified wash (excl. Unclassified)",
     lambda e: e.get("w", "") not in ("", "Unclassified", "Other")),
    ("Rise", "Rise categorized (Low/Mid/High/Super High)",
     lambda e: bool(e.get("ri", "").strip())),
    ("Leg Shape", "Leg shape identified (Skinny/Straight/Wide Leg etc.)",
     lambda e: bool(e.get("le", "").strip())),
    ("Fit Style", "Fit mapped (Slim/Contemporary, Regular, Relaxed, Curvy)",
     lambda e: bool(e.get("fi", "").strip())),
    ("Cotton %", "Numeric cotton percentage available",
     lambda e: e.get("cp") is not None and e.get("cp") != ""),
    ("Fabric Weight", "Fabric weight category (Lightweight/Midweight/Heavyweight)",
     lambda e: bool(e.get("fw", "").strip()) if isinstance(e.get("fw"), str) else e.get("fw") is not None),
    ("Brand", "Brand name populated",
     lambda e: bool(e.get("b", "").strip())),
]


def build_coverage_html(all_entries):
    """Dynamically generate the Data Coverage page HTML from actual data."""
    from collections import defaultdict

    # Group entries
    groups = defaultdict(list)
    for e in all_entries:
        g = e.get("g", "")
        if g:
            groups[g].append(e)

    # CC count cards
    cards_html = ""
    for gkey, glabel in GROUP_ORDER:
        count = len(groups.get(gkey, []))
        cards_html += (
            '<div style="background:var(--bg);border:1px solid var(--bg3);border-radius:var(--radius-sm);'
            'padding:10px 16px;min-width:100px;text-align:center">'
            '<div style="font-size:.65rem;color:var(--fg3);letter-spacing:.05em;text-transform:uppercase;'
            'font-weight:700">%s</div>'
            '<div style="font-size:1.3rem;font-weight:800;color:var(--fg)">%d</div>'
            '<div style="font-size:.6rem;color:var(--fg3)">CCs</div></div>\n' % (glabel, count)
        )

    # Table header
    header_cells = '<th style="text-align:left;padding:10px 12px;font-weight:800;color:var(--fg);font-size:.72rem">Field</th>'
    for gkey, glabel in GROUP_ORDER:
        header_cells += (
            '<th style="text-align:center;padding:10px 8px;font-weight:700;color:var(--fg2);'
            'font-size:.62rem;min-width:72px">%s</th>' % glabel
        )

    # Table rows
    rows_html = ""
    for field_name, field_desc, field_fn in COVERAGE_FIELDS:
        row = '<tr style="border-bottom:1px solid var(--bg3)">'
        row += (
            '<td style="padding:10px 12px;font-weight:700;color:var(--fg)">%s'
            '<div style="font-size:.58rem;color:var(--fg3);font-weight:400;margin-top:2px">%s</div></td>'
            % (field_name, field_desc)
        )
        for gkey, glabel in GROUP_ORDER:
            ents = groups.get(gkey, [])
            total = len(ents)
            if total == 0:
                pct = 0
                filled = 0
            else:
                filled = sum(1 for e in ents if field_fn(e))
                pct = round(filled / total * 100)

            if pct >= 80:
                color = "#22c55e"
                bg = "rgba(34,197,94,0.12)"
            elif pct >= 40:
                color = "#f59e0b"
                bg = "rgba(245,158,11,0.12)"
            else:
                color = "#ef4444"
                bg = "rgba(239,68,68,0.12)"

            row += (
                '<td style="text-align:center;padding:8px 6px">'
                '<div style="background:%s;border-radius:8px;padding:6px 4px">'
                '<div style="font-size:.9rem;font-weight:800;color:%s">%d%%</div>'
                '<div style="font-size:.55rem;color:var(--fg3);margin-top:1px">%d/%d</div>'
                '</div></td>' % (bg, color, pct, filled, total)
            )
        row += '</tr>\n'
        rows_html += row

    # Assemble full page
    parts = []
    parts.append('<div id="page-coverage" style="display:none">\n')
    parts.append('<div class="hero">\n')
    parts.append('  <span class="label">Data Quality</span>\n')
    parts.append('  <h1>Data Coverage</h1>\n')
    parts.append('  <p>Traffic light view of data completeness across all retailer groups. ')
    parts.append('Green = 80%+, Amber = 40&ndash;79%, Red = &lt;40%. ')
    parts.append('Wash Category excludes Unclassified (proprietary color names).</p>\n')
    parts.append('</div>\n')
    parts.append('<div class="section" id="sec-coverage">\n')
    parts.append('<div style="display:flex;flex-wrap:wrap;gap:12px;margin-bottom:24px">\n')
    parts.append(cards_html)
    parts.append('</div>\n')
    parts.append('<div style="overflow-x:auto">\n')
    parts.append('<table style="width:100%;border-collapse:collapse;font-size:.72rem;font-family:Montserrat,sans-serif">\n')
    parts.append('<thead><tr style="border-bottom:2px solid var(--bg3)">')
    parts.append(header_cells)
    parts.append('</tr></thead>\n<tbody>\n')
    parts.append(rows_html)
    parts.append('</tbody></table>\n</div>\n')
    parts.append('<div style="display:flex;gap:20px;margin-top:16px;font-size:.65rem;color:var(--fg2)">\n')
    parts.append('<div style="display:flex;align-items:center;gap:6px"><span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#22c55e"></span> 80%+ Complete</div>\n')
    parts.append('<div style="display:flex;align-items:center;gap:6px"><span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#f59e0b"></span> 40&ndash;79% Partial</div>\n')
    parts.append('<div style="display:flex;align-items:center;gap:6px"><span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#ef4444"></span> &lt;40% Sparse</div>\n')
    parts.append('</div>\n</div>\n</div><!-- end page-coverage -->\n')

    return "".join(parts)


# ─── Main pipeline ────────────────────────────────────────────────────────────
def main():
    print("Reading dashboard HTML...")
    with open(DASHBOARD_IN, "r", encoding="utf-8") as f:
        html = f.read()

    # Split into lines for line-based operations
    lines = html.split("\n")

    # ── Read CSVs ──
    print("Reading CSV files...")
    macys_rows = read_csv(MACYS_CSV)
    kohls_rows = read_csv(KOHLS_CSV)
    levis_rows = read_csv(LEVIS_CSV)
    print("  Macy's: %d rows" % len(macys_rows))
    print("  Kohl's: %d rows" % len(kohls_rows))
    print("  Levi's: %d rows" % len(levis_rows))

    # ── Transform CSVs ──
    print("Transforming CSV data...")
    macys_entries = transform_macys(macys_rows)
    kohls_entries = transform_kohls(kohls_rows)
    levis_entries = transform_levis(levis_rows)
    new_entries = macys_entries + kohls_entries + levis_entries
    print("  Total new entries: %d" % len(new_entries))

    # ── Add cotton percent range (cpr) to Levi's entries that have cotton % ──
    for e in new_entries:
        if e.get("cot") is not None:
            cpr = cotton_pct_range(e["cot"])
            if cpr:
                e["cp"] = e["cot"]
                e["cpr"] = cpr

    # ── Build Target lookup for fabric_weight / cotton_percent ──
    print("Building Target dashboard lookup for fabric_weight/cotton_percent...")
    target_lookup = build_target_lookup()

    # ── Parse existing RAW data ──
    # Find line 345 (0-indexed: 344) which contains var RAW = [...]
    raw_line_idx = None
    for i, line in enumerate(lines):
        if "var RAW" in line and "[" in line:
            raw_line_idx = i
            break

    if raw_line_idx is None:
        print("ERROR: Could not find 'var RAW' line!")
        return

    print("Found RAW data on line %d" % (raw_line_idx + 1))

    raw_line = lines[raw_line_idx]

    # ── Re-classify existing "Other" wash entries and collapse fit categories ──
    # We need to modify the existing RAW line to:
    # 1. Reclassify "Other" colors
    # 2. Collapse fit categories (Loose/Baggy/Rigid → Relaxed/Regular)
    # We'll use regex to find and replace within the RAW line

    # Strategy: parse as JSON, reclassify, re-serialize
    def reclassify_and_collapse_in_raw(raw_str):
        """Parse the RAW JSON array, reclassify Other colors, collapse fit categories,
        merge fabric_weight/cotton_percent from Target lookup, and re-serialize."""
        # Extract the JSON array
        arr_start = raw_str.find("[")
        arr_end = raw_str.rfind("]")
        if arr_start < 0 or arr_end < 0:
            print("  ERROR: Cannot find JSON array brackets")
            return raw_str

        prefix = raw_str[:arr_start]
        suffix = raw_str[arr_end + 1:]
        json_str = raw_str[arr_start:arr_end + 1]

        try:
            entries = json.loads(json_str)
        except json.JSONDecodeError as e:
            print("  ERROR parsing JSON for reclassification: %s" % str(e))
            return raw_str

        wash_count = 0
        fit_count = 0
        fw_count = 0
        for entry in entries:
            # Reclassify "Other" entries — legacy label eliminated
            if entry.get("w") == "Other":
                if entry.get("c"):
                    new_wash = classify_wash(entry["c"])
                    entry["w"] = new_wash
                    wash_count += 1
                else:
                    entry["w"] = "Unclassified"
                    wash_count += 1
            # Collapse fit categories
            if entry.get("fi") in OLD_FIT_COLLAPSE:
                entry["fi"] = OLD_FIT_COLLAPSE[entry["fi"]]
                fit_count += 1
            # Ensure s field is numeric 1/0
            if "s" in entry:
                if isinstance(entry["s"], bool):
                    entry["s"] = 1 if entry["s"] else 0
            # Merge fabric_weight / cotton_percent from Target lookup
            if entry.get("g") in ("Target OB", "Target NB") and target_lookup:
                brand = entry.get("b", "")
                color = entry.get("c", "")
                price = entry.get("p", 0)
                key = (brand.strip(), color.strip().lower(), round(float(price), 2))
                if key in target_lookup:
                    tgt = target_lookup[key]
                    if "fw" in tgt and "fw" not in entry:
                        entry["fw"] = tgt["fw"]
                        fw_count += 1
                    if "cp" in tgt and "cp" not in entry:
                        entry["cp"] = tgt["cp"]
                    if "cpr" in tgt and "cpr" not in entry:
                        entry["cpr"] = tgt["cpr"]

        print("  Reclassified %d 'Other' entries to specific wash categories" % wash_count)
        print("  Collapsed %d fit entries (Loose/Baggy/Rigid)" % fit_count)
        print("  Merged fabric_weight for %d Target entries" % fw_count)

        # Re-serialize as compact JSON
        new_json = json.dumps(entries, ensure_ascii=False, separators=(",", ":"))
        return prefix + new_json + suffix

    print("Reclassifying 'Other' colors and collapsing fit categories in existing data...")
    raw_line = reclassify_and_collapse_in_raw(raw_line)

    # ── Append new entries to RAW ──
    print("Appending new entries to RAW array...")
    new_js = ",".join(entry_to_js(e) for e in new_entries)

    # The RAW line format is: <script>var RAW = [{...},{...},...]; </script>
    # Find the closing ]; and insert before it, preserving any trailing </script> tag
    raw_line = raw_line.rstrip()

    # Look for ];</script> pattern (script tag on same line)
    script_suffix = ""
    if "</script>" in raw_line:
        script_idx = raw_line.rfind("</script>")
        script_suffix = raw_line[script_idx:]
        raw_line = raw_line[:script_idx].rstrip()
        print("  Found </script> suffix, handling separately")

    if raw_line.endswith("];"):
        raw_line = raw_line[:-2] + "," + new_js + "];"
    elif raw_line.endswith("]"):
        raw_line = raw_line[:-1] + "," + new_js + "]"
    else:
        print("WARNING: Unexpected RAW line ending: ...%s" % raw_line[-30:])
        last_bracket = raw_line.rfind("]")
        if last_bracket > 0:
            raw_line = raw_line[:last_bracket] + "," + new_js + raw_line[last_bracket:]

    raw_line = raw_line + script_suffix
    lines[raw_line_idx] = raw_line

    # ── Now parse all entries for insights calculation ──
    # The RAW data is in JSON format: [{"g":"Target OB","n":"...","p":39.99,...},...]
    # Use json.loads to parse the array from the original file
    print("Parsing existing RAW for insights calculation...")

    existing_entries = []
    with open(DASHBOARD_IN, "r", encoding="utf-8") as f:
        orig_html = f.read()
    orig_lines = orig_html.split("\n")
    orig_raw_line = orig_lines[raw_line_idx]

    # Extract the JSON array from: <script>var RAW = [...]; </script>
    # or: var RAW = [...];
    array_start = orig_raw_line.find("[")
    array_end = orig_raw_line.rfind("]")
    if array_start >= 0 and array_end > array_start:
        json_str = orig_raw_line[array_start:array_end + 1]
        try:
            parsed = json.loads(json_str)
            for entry in parsed:
                if entry.get("g"):
                    # Apply fit collapse to existing entries for insights
                    if entry.get("fi") in OLD_FIT_COLLAPSE:
                        entry["fi"] = OLD_FIT_COLLAPSE[entry["fi"]]
                    # Reclassify "Other" entries — legacy label eliminated
                    if entry.get("w") == "Other":
                        if entry.get("c"):
                            entry["w"] = classify_wash(entry["c"])
                        else:
                            entry["w"] = "Unclassified"
                    # Ensure s field is numeric
                    if "s" in entry and isinstance(entry["s"], bool):
                        entry["s"] = 1 if entry["s"] else 0
                    # Merge fabric_weight / cotton_percent from Target lookup
                    if entry.get("g") in ("Target OB", "Target NB") and target_lookup:
                        brand = entry.get("b", "")
                        color = entry.get("c", "")
                        price = entry.get("p", 0)
                        key = (brand.strip(), color.strip().lower(), round(float(price), 2))
                        if key in target_lookup:
                            tgt = target_lookup[key]
                            if "fw" in tgt:
                                entry["fw"] = tgt["fw"]
                            if "cp" in tgt:
                                entry["cp"] = tgt["cp"]
                            if "cpr" in tgt:
                                entry["cpr"] = tgt["cpr"]
                    existing_entries.append(entry)
            print("  Parsed %d existing entries via JSON" % len(existing_entries))
        except json.JSONDecodeError as e:
            print("  ERROR parsing JSON: %s" % str(e))
            print("  First 200 chars of array: %s" % json_str[:200])
            print("  Last 200 chars of array: %s" % json_str[-200:])
    else:
        print("  ERROR: Could not find [ ] brackets in RAW line")

    all_entries = existing_entries + new_entries

    # ── Compute insights ──
    print("Computing insights...")
    insights = compute_insights(all_entries)

    # ═══════════════════════════════════════════════════════════════════════════
    # Now rebuild the HTML with all modifications
    # ═══════════════════════════════════════════════════════════════════════════
    html = "\n".join(lines)

    # ── CHANGE 1: Update GROUPS, GROUP_LABELS, GC ──
    print("Updating GROUPS, GROUP_LABELS, GC...")

    html = html.replace(
        "var GROUPS = ['Target OB','Target NB','Walmart OB','Amazon OB','AE','Old Navy'];",
        "var GROUPS = ['Target OB','Target NB','Walmart OB','Amazon OB','AE','Old Navy','Macys OB','Kohls OB','Levis'];"
    )

    html = html.replace(
        "'AE':'American Eagle','Old Navy':'Old Navy'\n};",
        "'AE':'American Eagle','Old Navy':'Old Navy',\n  'Macys OB':\"Macy's Owned Brands\",'Kohls OB':\"Kohl's Owned Brands\",'Levis':\"Levi's\"\n};"
    )

    html = html.replace(
        "'Old Navy':   {bg:'#003B5C', light:'rgba(0,59,92,0.18)', border:'#003B5C'}\n};",
        "'Old Navy':   {bg:'#003B5C', light:'rgba(0,59,92,0.18)', border:'#003B5C'},\n"
        "  'Macys OB':  {bg:'#E21A2C', light:'rgba(226,26,44,0.18)', border:'#E21A2C'},\n"
        "  'Kohls OB':  {bg:'#6B2D8B', light:'rgba(107,45,139,0.18)', border:'#6B2D8B'},\n"
        "  'Levis':     {bg:'#C41230', light:'rgba(196,18,48,0.18)', border:'#C41230'}\n};"
    )

    # ── CHANGE 4: Fix description width — remove max-width:760px ──
    print("Fixing description box width...")
    html = html.replace("margin-top:16px;max-width:760px", "margin-top:16px")
    html = html.replace("margin-top:10px;max-width:760px", "margin-top:10px")

    # ── CHANGE 2: Remove Length charts ──
    print("Removing length/garment length charts...")

    # Remove overview length chart card
    html = html.replace(
        '<div class="chart-card"><div class="chart-title">Garment Length <button onclick="toggleInfo(\'length\')" style="width:22px;height:22px;border-radius:50%;border:1.5px solid var(--bg4);background:transparent;color:var(--fg3);font-family:Montserrat,sans-serif;font-size:.7rem;font-weight:700;cursor:pointer;margin-left:6px;line-height:1" title="Details">?</button></div>\n<div id="info-length" style="display:none;background:var(--bg2);border:1px solid var(--bg3);border-radius:var(--radius-sm);padding:12px 16px;margin:8px 0 12px;font-size:.68rem;color:var(--fg2);line-height:1.6">From Target &#8220;Garment Length&#8221; or &#8220;hits at&#8221; text. Short, Knee, Crop, Ankle, Full Length.</div>\n<div class="chart-subtitle" id="sub-length"></div><div class="chart-wrap"><canvas id="lengthCanvas"></canvas></div></div>',
        ''
    )

    # Remove SBS length chart card
    html = html.replace(
        '<div class="chart-card"><div class="chart-title">Garment Length</div>\n<div class="chart-subtitle" id="sbs-sub-length"></div><div class="chart-wrap"><canvas id="sbs-lengthCanvas"></canvas></div></div>',
        ''
    )

    # Remove renderStackedAttr for length
    html = html.replace(
        "  renderStackedAttr('lengthCanvas','ln','sub-length',['Short','Knee','Crop','Ankle','Full Length']);\n",
        ""
    )

    # Remove SBS length render call
    html = html.replace(
        "  renderSBSGroupedBar('sbs-lengthCanvas', 'ln', gA, gB, dA, dB, 'sbs-sub-length');\n",
        ""
    )

    # ── CHANGE 3: Collapse fit categories + update tooltip ──
    print("Updating fit categories and tooltip...")

    # Update the fit renderStackedAttr call
    html = html.replace(
        "renderStackedAttr('fitCanvas','fi','sub-fit',['Slim/Contemporary','Regular','Relaxed','Curvy','Loose','Baggy','Rigid']);",
        "renderStackedAttr('fitCanvas','fi','sub-fit',['Slim/Contemporary','Regular','Relaxed','Curvy']);"
    )

    # Update the fit info tooltip
    html = html.replace(
        '<div id="info-fit" style="display:none;background:var(--bg2);border:1px solid var(--bg3);border-radius:var(--radius-sm);padding:12px 16px;margin:8px 0 12px;font-size:.68rem;color:var(--fg2);line-height:1.6">From Target &#8220;with a X Fit&#8221; or Walmart &#8220;clothing fit&#8221;. Slim/Contemporary, Regular, Relaxed, Curvy, Loose, Baggy, Rigid.</div>',
        '<div id="info-fit" style="display:none;background:var(--bg2);border:1px solid var(--bg3);border-radius:var(--radius-sm);padding:12px 16px;margin:8px 0 12px;font-size:.68rem;color:var(--fg2);line-height:1.6"><b>Fit Style Taxonomy</b> &mdash; Derived from leg shape:<br><b>Slim/Contemporary:</b> Skinny, Slim, Jegging<br><b>Regular:</b> Straight, Bootcut, Flare, Barrel, Tapered, Mom, Trouser<br><b>Relaxed:</b> Wide Leg, Baggy, Boyfriend, Relaxed, Loose<br><b>Curvy:</b> Curvy-designated styles</div>'
    )

    # ── (Stretch chart removed per user request) ──

    # ── Add Fabric Weight and Cotton Content charts ──
    print("Adding Fabric Weight and Cotton Content charts...")

    # Add chart cards to overview section (before the Wash chart card)
    fw_cp_cards = (
        '<div class="chart-card"><div class="chart-title">Fabric Weight</div>\n'
        '<div class="chart-subtitle" id="sub-fw"></div><div class="chart-wrap"><canvas id="fwCanvas"></canvas></div></div>\n'
        '<div class="chart-card"><div class="chart-title">Cotton Content</div>\n'
        '<div class="chart-subtitle" id="sub-cp"></div><div class="chart-wrap"><canvas id="cpCanvas"></canvas></div></div>\n'
    )

    # Insert before the wash chart card (full width) in overview
    html = html.replace(
        '<div class="chart-card full"><div class="chart-title">Wash / Color Mix',
        fw_cp_cards + '<div class="chart-card full"><div class="chart-title">Wash / Color Mix',
        1  # only first occurrence (overview section)
    )

    # Add Fabric Weight and Cotton Content render calls
    html = html.replace(
        "  renderStackedAttr('washCanvas','w','sub-wash', WASH_ORDER);",
        "  renderStackedAttr('fwCanvas','fw','sub-fw',['Extra Lightweight','Lightweight','Midweight','Heavyweight']);\n"
        "  renderStackedAttr('cpCanvas','cpr','sub-cp',['0-25%','26-50%','51-75%','76-100%']);\n"
        "  renderStackedAttr('washCanvas','w','sub-wash', WASH_ORDER);"
    )

    # Add SBS Fabric Weight and Cotton Content chart cards
    sbs_fw_cp_cards = (
        '<div class="chart-card"><div class="chart-title">Fabric Weight</div>\n'
        '<div class="chart-subtitle" id="sbs-sub-fw"></div><div class="chart-wrap"><canvas id="sbs-fwCanvas"></canvas></div></div>\n'
        '<div class="chart-card"><div class="chart-title">Cotton Content</div>\n'
        '<div class="chart-subtitle" id="sbs-sub-cp"></div><div class="chart-wrap"><canvas id="sbs-cpCanvas"></canvas></div></div>\n'
    )

    # Insert before SBS wash chart
    html = html.replace(
        '<div class="chart-card full"><div class="chart-title">Wash / Color Mix</div>\n<div class="chart-subtitle" id="sbs-sub-wash">',
        sbs_fw_cp_cards +
        '<div class="chart-card full"><div class="chart-title">Wash / Color Mix</div>\n<div class="chart-subtitle" id="sbs-sub-wash">'
    )

    # Add SBS Fabric Weight and Cotton Content render calls
    html = html.replace(
        "  renderSBSGroupedBar('sbs-fitCanvas', 'fi', gA, gB, dA, dB, 'sbs-sub-fit');",
        "  renderSBSGroupedBar('sbs-fitCanvas', 'fi', gA, gB, dA, dB, 'sbs-sub-fit');\n"
        "  renderSBSGroupedBar('sbs-fwCanvas', 'fw', gA, gB, dA, dB, 'sbs-sub-fw');\n"
        "  renderSBSGroupedBar('sbs-cpCanvas', 'cpr', gA, gB, dA, dB, 'sbs-sub-cp');"
    )

    # ── CHANGES 7-9: Add new retailer options to all dropdowns ──
    print("Adding new retailer options to dropdowns...")

    new_opts = (
        '<option value="Macys OB">Macy\'s OB</option>'
        '<option value="Kohls OB">Kohl\'s OB</option>'
        '<option value="Levis">Levi\'s</option>'
    )

    # Use regex to find all <select> blocks that contain retailer group options
    # and append the new options before the closing </select>
    # Pattern: find option value="Old Navy" followed eventually by </select>
    # We want to insert new options after the Old Navy option in each relevant select

    def add_options_to_selects(html_str):
        """Add new retailer options to all select dropdowns that have the existing retailer options."""
        # Find all occurrences of Old Navy option followed by newline+</select>
        # Match: <option value="Old Navy">Old Navy</option> ... </select>
        # We match the last existing option before </select>
        pattern = r'(<option value="Old Navy">Old Navy</option>)(\s*</select>)'
        replacement = r'\1' + new_opts + r'\2'
        result = re.sub(pattern, replacement, html_str)
        return result

    html = add_options_to_selects(html)

    # Remove Length from heatmap dimension selects, add Fabric Weight and Cotton Content
    html = html.replace(
        '<option value="ln">Length</option><option value="fi">Fit Style</option>',
        '<option value="fi">Fit Style</option><option value="fw">Fabric Weight</option><option value="cpr">Cotton Content</option>'
    )

    # ── CHANGE 5: Already handled by the reclassify_other_in_raw and the classify_wash improvements ──

    # ── Update hero text to mention new retailers ──
    html = html.replace(
        "across six retailer groups: Target (Owned &amp; National), Walmart Owned Brands, Amazon Owned Brands, American Eagle, and Old Navy.",
        "across nine retailer groups: Target (Owned &amp; National), Walmart Owned Brands, Amazon Owned Brands, American Eagle, Old Navy, Macy&rsquo;s, Kohl&rsquo;s, and Levi&rsquo;s."
    )

    # Update coverage text
    coverage_addition = (
        "\n      <strong>Macy&rsquo;s</strong> &#8212; %d CCs across product pages.\n"
        "      <strong>Kohl&rsquo;s</strong> &#8212; %d CCs across product pages.\n"
        "      <strong>Levi&rsquo;s</strong> &#8212; %d CCs across product pages."
        % (len(macys_entries), len(kohls_entries), len(levis_entries))
    )

    html = html.replace(
        "<strong>Amazon</strong> &#8212; 78 CCs across 2 owned brands (Amazon Essentials, The Drop).\n    </div>",
        "<strong>Amazon</strong> &#8212; 78 CCs across 2 owned brands (Amazon Essentials, The Drop)." + coverage_addition + "\n    </div>"
    )

    # ── CHANGE 6: Add Key Insights tab ──
    print("Adding Key Insights tab...")

    # Build insights HTML
    insights_html = build_insights_html(insights)

    # Insert the insights page div before the footer
    html = html.replace(
        '<div class="footer">Cross-Retailer Women',
        insights_html + '\n<div class="footer">Cross-Retailer Women'
    )

    # Add the insights button to the side nav
    html = html.replace(
        '    <button id="pt-sidebyside" class="view-toggle-btn" onclick="showPage(\'sidebyside\')" style="width:100%;text-align:left;padding:7px 10px;border-radius:6px;font-size:.7rem;font-weight:700;border:1.5px solid var(--bg4);background:transparent;color:var(--fg2);cursor:pointer;font-family:Montserrat">Side-by-Side</button>\n  </div>',
        '    <button id="pt-sidebyside" class="view-toggle-btn" onclick="showPage(\'sidebyside\')" style="width:100%;text-align:left;padding:7px 10px;border-radius:6px;font-size:.7rem;font-weight:700;border:1.5px solid var(--bg4);background:transparent;color:var(--fg2);cursor:pointer;font-family:Montserrat">Side-by-Side</button>\n'
        '    <button id="pt-insights" class="view-toggle-btn" onclick="showPage(\'insights\')" style="width:100%;text-align:left;padding:7px 10px;border-radius:6px;font-size:.7rem;font-weight:700;border:1.5px solid var(--bg4);background:transparent;color:var(--fg2);cursor:pointer;font-family:Montserrat">Key Insights</button>\n  </div>'
    )

    # Add insights nav section
    html = html.replace(
        '  <div id="nav-sidebyside" style="display:none">',
        '  <div id="nav-insights" style="display:none">\n'
        '    <a class="nav-link" href="#page-insights"><span class="nav-num">00</span> All Insights</a>\n'
        '  </div>\n'
        '  <div id="nav-sidebyside" style="display:none">'
    )

    # ── Update showPage function to handle 3 pages ──
    print("Updating showPage function...")

    old_showpage = """function showPage(page) {
  currentPage = page;
  var po = document.getElementById('page-overview');
  var ps = document.getElementById('page-sidebyside');
  var no = document.getElementById('nav-overview');
  var ns = document.getElementById('nav-sidebyside');
  var ptOverview = document.getElementById('pt-overview');
  var ptSidebyside = document.getElementById('pt-sidebyside');

  if (page === 'overview') {
    po.style.display = 'block';
    ps.style.display = 'none';
    no.style.display = 'block';
    ns.style.display = 'none';
    ptOverview.style.background = '#002855';
    ptOverview.style.color = '#fff';
    ptOverview.style.borderColor = '#002855';
    ptSidebyside.style.background = 'transparent';
    ptSidebyside.style.color = 'var(--fg2)';
    ptSidebyside.style.borderColor = 'var(--bg4)';
  } else {
    po.style.display = 'none';
    ps.style.display = 'block';
    no.style.display = 'none';
    ns.style.display = 'block';
    ptSidebyside.style.background = '#002855';
    ptSidebyside.style.color = '#fff';
    ptSidebyside.style.borderColor = '#002855';
    ptOverview.style.background = 'transparent';
    ptOverview.style.color = 'var(--fg2)';
    ptOverview.style.borderColor = 'var(--bg4)';
    renderSideBySide();
  }
  window.scrollTo(0, 0);
}"""

    new_showpage = """function showPage(page) {
  currentPage = page;
  var pc = document.getElementById('page-coverage');
  var po = document.getElementById('page-overview');
  var ps = document.getElementById('page-sidebyside');
  var pi = document.getElementById('page-insights');
  var nc = document.getElementById('nav-coverage');
  var no = document.getElementById('nav-overview');
  var ns = document.getElementById('nav-sidebyside');
  var ni = document.getElementById('nav-insights');
  var ptCoverage = document.getElementById('pt-coverage');
  var ptOverview = document.getElementById('pt-overview');
  var ptSidebyside = document.getElementById('pt-sidebyside');
  var ptInsights = document.getElementById('pt-insights');

  var allPages = [pc, po, ps, pi];
  var allNavs = [nc, no, ns, ni];
  var allBtns = [ptCoverage, ptOverview, ptSidebyside, ptInsights];
  for (var i = 0; i < allPages.length; i++) {
    if (allPages[i]) allPages[i].style.display = 'none';
    if (allNavs[i]) allNavs[i].style.display = 'none';
    if (allBtns[i]) {
      allBtns[i].style.background = 'transparent';
      allBtns[i].style.color = 'var(--fg2)';
      allBtns[i].style.borderColor = 'var(--bg4)';
    }
  }

  if (page === 'coverage') {
    pc.style.display = 'block';
    nc.style.display = 'block';
    ptCoverage.style.background = '#002855';
    ptCoverage.style.color = '#fff';
    ptCoverage.style.borderColor = '#002855';
  } else if (page === 'overview') {
    po.style.display = 'block';
    no.style.display = 'block';
    ptOverview.style.background = '#002855';
    ptOverview.style.color = '#fff';
    ptOverview.style.borderColor = '#002855';
  } else if (page === 'sidebyside') {
    ps.style.display = 'block';
    ns.style.display = 'block';
    ptSidebyside.style.background = '#002855';
    ptSidebyside.style.color = '#fff';
    ptSidebyside.style.borderColor = '#002855';
    renderSideBySide();
  } else if (page === 'insights') {
    pi.style.display = 'block';
    ni.style.display = 'block';
    ptInsights.style.background = '#002855';
    ptInsights.style.color = '#fff';
    ptInsights.style.borderColor = '#002855';
  }
  window.scrollTo(0, 0);
}"""

    # Try the old 2-page version first, then the 3-page version
    if old_showpage in html:
        html = html.replace(old_showpage, new_showpage)
        print("  Replaced 2-page showPage with 4-page version")
    else:
        # The input already has a 3-page showPage (with insights but no coverage)
        # Inject coverage handling into the existing function
        print("  Injecting coverage into existing showPage...")

        # Add coverage variables after the insights variable declarations
        html = html.replace(
            "  var pi = document.getElementById('page-insights');\n"
            "  var no = document.getElementById('nav-overview');",
            "  var pi = document.getElementById('page-insights');\n"
            "  var pc = document.getElementById('page-coverage');\n"
            "  var no = document.getElementById('nav-overview');"
        )
        html = html.replace(
            "  var ni = document.getElementById('nav-insights');\n"
            "  var ptOverview = document.getElementById('pt-overview');",
            "  var ni = document.getElementById('nav-insights');\n"
            "  var nc = document.getElementById('nav-coverage');\n"
            "  var ptOverview = document.getElementById('pt-overview');"
        )
        html = html.replace(
            "  var ptInsights = document.getElementById('pt-insights');\n\n"
            "  var allPages = [po, ps, pi];",
            "  var ptInsights = document.getElementById('pt-insights');\n"
            "  var ptCoverage = document.getElementById('pt-coverage');\n\n"
            "  var allPages = [pc, po, ps, pi];"
        )
        html = html.replace(
            "  var allNavs = [no, ns, ni];\n"
            "  var allBtns = [ptOverview, ptSidebyside, ptInsights];",
            "  var allNavs = [nc, no, ns, ni];\n"
            "  var allBtns = [ptCoverage, ptOverview, ptSidebyside, ptInsights];"
        )

        # Add null checks for the loop
        html = html.replace(
            "    allPages[i].style.display = 'none';\n"
            "    allNavs[i].style.display = 'none';\n"
            "    allBtns[i].style.background = 'transparent';",
            "    if (allPages[i]) allPages[i].style.display = 'none';\n"
            "    if (allNavs[i]) allNavs[i].style.display = 'none';\n"
            "    if (!allBtns[i]) continue;\n"
            "    allBtns[i].style.background = 'transparent';"
        )

        # Add coverage page handling before overview
        html = html.replace(
            "  if (page === 'overview') {",
            "  if (page === 'coverage') {\n"
            "    pc.style.display = 'block';\n"
            "    nc.style.display = 'block';\n"
            "    ptCoverage.style.background = '#002855';\n"
            "    ptCoverage.style.color = '#fff';\n"
            "    ptCoverage.style.borderColor = '#002855';\n"
            "  } else if (page === 'overview') {"
        )

    # ── Remove "Other" from WASH_ORDER (legacy label eliminated) ──
    print("Removing 'Other' from WASH_ORDER...")
    html = html.replace(
        "var WASH_ORDER = ['Light Wash','Medium Wash','Dark Wash','Black','White/Cream','Grey','Brown/Earth','Green','Pink/Red','Yellow/Orange','Purple','Print/Pattern','Other'];",
        "var WASH_ORDER = ['Light Wash','Medium Wash','Dark Wash','Black','White/Cream','Grey','Brown/Earth','Green','Pink/Red','Yellow/Orange','Purple','Print/Pattern'];"
    )

    # ── Inject coverage page dynamically ──
    print("Injecting Data Coverage page from actual data...")
    new_coverage_html = build_coverage_html(all_entries)

    # Replace if it already exists, otherwise insert before page-overview
    coverage_start = html.find('<div id="page-coverage"')
    coverage_end = html.find('<!-- end page-coverage -->')
    if coverage_start >= 0 and coverage_end >= 0:
        coverage_end = coverage_end + len('<!-- end page-coverage -->')
        if coverage_end < len(html) and html[coverage_end] == '\n':
            coverage_end += 1
        html = html[:coverage_start] + new_coverage_html + html[coverage_end:]
        print("  Coverage page replaced successfully")
    else:
        # Insert before page-overview
        overview_marker = '<div id="page-overview"'
        pos = html.find(overview_marker)
        if pos >= 0:
            html = html[:pos] + new_coverage_html + '\n' + html[pos:]
            print("  Coverage page inserted before page-overview")
        else:
            print("  WARNING: Could not find insertion point for coverage page")

    # ── Add coverage button to side nav (if not already present) ──
    if 'pt-coverage' not in html:
        print("Adding coverage button to side nav...")
        # The overview button may have 'active' class — match with regex
        import re as re_mod
        coverage_btn = (
            '    <button id="pt-coverage" class="view-toggle-btn" onclick="showPage(\'coverage\')" '
            'style="width:100%;text-align:left;padding:7px 10px;border-radius:6px;font-size:.7rem;'
            'font-weight:700;border:1.5px solid var(--bg4);background:transparent;color:var(--fg2);'
            'cursor:pointer;font-family:Montserrat">Data Coverage</button>\n'
        )
        html = re_mod.sub(
            r'(    <button id="pt-overview" class="view-toggle-btn)',
            coverage_btn + r'\1',
            html,
            count=1
        )

    # ── Add coverage nav section (if not already present) ──
    if 'nav-coverage' not in html:
        print("Adding coverage nav section...")
        html = html.replace(
            '  <div id="nav-overview"',
            '  <div id="nav-coverage" style="display:none">\n'
            '    <a class="nav-link" href="#sec-coverage"><span class="nav-num">00</span> Coverage Matrix</a>\n'
            '  </div>\n'
            '  <div id="nav-overview"'
        )

    # ── Update footer ──
    html = html.replace(
        "Cross-Retailer Women&#8217;s Jeans Dashboard &#8212; Test Data Preview &#8212; April 2026",
        "Cross-Retailer Women&#8217;s Jeans Dashboard &#8212; 9 Retailer Groups &#8212; April 2026"
    )

    # ── Update data collection text ──
    html = html.replace(
        "across all five retailers",
        "across all nine retailers"
    )

    # ── Write output ──
    print("Writing updated dashboard to %s..." % DASHBOARD_OUT)
    with open(DASHBOARD_OUT, "w", encoding="utf-8") as f:
        f.write(html)

    # ── Summary stats ──
    print("\n=== Summary ===")
    print("New entries added: %d (Macy's: %d, Kohl's: %d, Levi's: %d)"
          % (len(new_entries), len(macys_entries), len(kohls_entries), len(levis_entries)))
    print("Existing entries parsed: %d" % len(existing_entries))
    print("Total entries in dashboard: %d" % len(all_entries))
    print("\nInsights computed:")
    for key in sorted(insights.keys()):
        print("  %s: %s" % (key, insights[key]))
    print("\nDashboard written to: %s" % DASHBOARD_OUT)
    print("Done!")


if __name__ == "__main__":
    main()
