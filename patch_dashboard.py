#!/usr/bin/env python3
"""
patch_dashboard.py
Patches update_dashboard.py to:
1. Remove dead parse_stretch code
2. Enhance normalize_rise with Levi's model number inference
3. Add traffic light data coverage page as landing page
4. Improve Macy's rise extraction from descriptions
Then runs the patched pipeline.
"""

import re

BASE = "/sessions/hopeful-tender-dijkstra/mnt/Womens jeans scraper"
SCRIPT = f"{BASE}/update_dashboard.py"

with open(SCRIPT, "r", encoding="utf-8") as f:
    code = f.read()

# ═══════════════════════════════════════════════════════════════════════
# PATCH 1: Enhance normalize_rise with Levi's model numbers
# ═══════════════════════════════════════════════════════════════════════

old_normalize_rise = '''def normalize_rise(rise_str, product_name="", description=""):
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

    return ""'''

new_normalize_rise = '''def normalize_rise(rise_str, product_name="", description=""):
    """Normalize rise string to one of: Low, Mid, High, Super High.
    Enhanced with Levi's model number inference and description parsing."""
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
        if any(x in src for x in ["superlow"]):
            return "Low"
        if "high" in src and "thigh" not in src:
            return "High"
        if any(x in src for x in ["mid", "regular rise", "classic rise"]):
            return "Mid"
        if "low" in src and "below" not in src and "flow" not in src:
            return "Low"
        if src != combined:
            continue
        break

    # Try just product name
    name_lower = (product_name or "").lower()
    if "super high" in name_lower or "ultra high" in name_lower:
        return "Super High"
    if "high rise" in name_lower or "high-rise" in name_lower:
        return "High"
    if "mid rise" in name_lower or "mid-rise" in name_lower:
        return "Mid"
    if "low rise" in name_lower or "low-rise" in name_lower or "low pro" in name_lower:
        return "Low"
    if "ribcage" in name_lower or "wedgie" in name_lower:
        return "High"

    # Levi's model number inference
    import re as _re
    for model, rise in [("721", "High"), ("724", "High"), ("725", "High"),
                        ("726", "High"), ("728", "High"),
                        ("314", "Mid"), ("315", "Mid"), ("318", "Mid"),
                        ("501", "Mid"), ("502", "Mid"),
                        ("311", "Mid"), ("312", "Mid"),
                        ("720", "Super High")]:
        if _re.search(r'\\b' + model + r'\\b', name_lower):
            return rise

    # Try description as last resort
    desc_lower = (description or "").lower()
    if "super high" in desc_lower or "ultra high" in desc_lower:
        return "Super High"
    if "high rise" in desc_lower or "high-rise" in desc_lower:
        return "High"
    if "mid rise" in desc_lower or "mid-rise" in desc_lower:
        return "Mid"
    if "low rise" in desc_lower or "low-rise" in desc_lower:
        return "Low"

    return ""'''

code = code.replace(old_normalize_rise, new_normalize_rise)
print("PATCH 1: Enhanced normalize_rise ✓")

# ═══════════════════════════════════════════════════════════════════════
# PATCH 2: Remove dead parse_stretch function
# ═══════════════════════════════════════════════════════════════════════

old_stretch = '''# ─── Stretch parsing ─────────────────────────────────────────────────────────
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

    return ""'''

code = code.replace(old_stretch, "# (Stretch analysis removed per user request)")
print("PATCH 2: Removed parse_stretch ✓")

# ═══════════════════════════════════════════════════════════════════════
# PATCH 3: Add traffic light coverage page builder
# ═══════════════════════════════════════════════════════════════════════

# Insert the coverage page builder function before compute_insights
coverage_fn = '''
# ─── Traffic Light Data Coverage Page ────────────────────────────────────────
def build_coverage_html(all_entries):
    """Build a traffic light page showing data coverage % per field per retailer group."""
    from collections import defaultdict

    groups_order = ['Target OB', 'Target NB', 'Walmart OB', 'Amazon OB', 'AE', 'Old Navy',
                    'Macys OB', 'Kohls OB', 'Levis']
    group_labels = {
        'Target OB': 'Target OB', 'Target NB': 'Target NB',
        'Walmart OB': 'Walmart OB', 'Amazon OB': 'Amazon OB',
        'AE': 'American Eagle', 'Old Navy': 'Old Navy',
        'Macys OB': "Macy\\'s OB", 'Kohls OB': "Kohl\\'s OB", 'Levis': "Levi\\'s",
    }

    # Fields to check coverage for
    fields = [
        ('p', 'Price', 'Current price > 0'),
        ('c', 'Color', 'Color name populated'),
        ('w', 'Wash Category', 'Wash classified (not Other)'),
        ('ri', 'Rise', 'Rise categorized (Low/Mid/High/Super High)'),
        ('le', 'Leg Shape', 'Leg shape identified (Skinny/Straight/etc.)'),
        ('fi', 'Fit Style', 'Fit style mapped (Slim/Regular/Relaxed/Curvy)'),
        ('mat', 'Material/Composition', 'Raw material string available'),
        ('cp', 'Cotton %', 'Cotton percentage extracted'),
        ('fw', 'Fabric Weight', 'Fabric weight category available'),
        ('b', 'Brand', 'Brand name populated'),
    ]

    # Group entries
    grouped = defaultdict(list)
    for e in all_entries:
        g = e.get('g', '')
        if g in groups_order:
            grouped[g].append(e)

    # Compute coverage
    coverage = {}  # {group: {field_key: (count, total, pct)}}
    for g in groups_order:
        ents = grouped.get(g, [])
        total = len(ents)
        coverage[g] = {}
        for fkey, flabel, fdesc in fields:
            if total == 0:
                coverage[g][fkey] = (0, 0, 0)
                continue
            if fkey == 'p':
                filled = sum(1 for e in ents if e.get('p', 0) > 0)
            elif fkey == 'w':
                filled = sum(1 for e in ents if e.get('w', '') and e.get('w', '') != 'Other')
            else:
                filled = sum(1 for e in ents if e.get(fkey, '') and str(e.get(fkey, '')).strip())
            pct = round(filled / total * 100) if total > 0 else 0
            coverage[g][fkey] = (filled, total, pct)

    def traffic_color(pct):
        if pct >= 80:
            return '#22c55e'  # green
        elif pct >= 40:
            return '#f59e0b'  # amber
        else:
            return '#ef4444'  # red

    def traffic_bg(pct):
        if pct >= 80:
            return 'rgba(34,197,94,0.12)'
        elif pct >= 40:
            return 'rgba(245,158,11,0.12)'
        else:
            return 'rgba(239,68,68,0.12)'

    # Build HTML
    html = '\\n<div id="page-coverage" style="display:block">\\n'
    html += '<div class="hero">\\n'
    html += '  <span class="label">Data Quality</span>\\n'
    html += '  <h1>Data Coverage</h1>\\n'
    html += '  <p>Traffic light view of data completeness across all retailer groups. Green = 80%+, Amber = 40-79%, Red = &lt;40%.</p>\\n'
    html += '</div>\\n'
    html += '<div class="section" id="sec-coverage">\\n'

    # Summary row: total CCs per group
    html += '<div style="display:flex;flex-wrap:wrap;gap:12px;margin-bottom:24px">\\n'
    for g in groups_order:
        total = len(grouped.get(g, []))
        html += '<div style="background:var(--bg);border:1px solid var(--bg3);border-radius:var(--radius-sm);padding:10px 16px;min-width:100px;text-align:center">'
        html += '<div style="font-size:.65rem;color:var(--fg3);letter-spacing:.05em;text-transform:uppercase;font-weight:700">%s</div>' % group_labels.get(g, g)
        html += '<div style="font-size:1.3rem;font-weight:800;color:var(--fg)">%d</div>' % total
        html += '<div style="font-size:.6rem;color:var(--fg3)">CCs</div>'
        html += '</div>\\n'
    html += '</div>\\n'

    # Traffic light table
    html += '<div style="overflow-x:auto">\\n'
    html += '<table style="width:100%;border-collapse:collapse;font-size:.72rem;font-family:Montserrat,sans-serif">\\n'

    # Header row
    html += '<thead><tr style="border-bottom:2px solid var(--bg3)">'
    html += '<th style="text-align:left;padding:10px 12px;font-weight:800;color:var(--fg);font-size:.72rem">Field</th>'
    for g in groups_order:
        html += '<th style="text-align:center;padding:10px 8px;font-weight:700;color:var(--fg2);font-size:.62rem;min-width:72px">%s</th>' % group_labels.get(g, g)
    html += '</tr></thead>\\n'

    # Data rows
    html += '<tbody>\\n'
    for fkey, flabel, fdesc in fields:
        html += '<tr style="border-bottom:1px solid var(--bg3)">'
        html += '<td style="padding:10px 12px;font-weight:700;color:var(--fg)">%s<div style=\\"font-size:.58rem;color:var(--fg3);font-weight:400;margin-top:2px\\">%s</div></td>' % (flabel, fdesc)
        for g in groups_order:
            filled, total, pct = coverage[g].get(fkey, (0, 0, 0))
            bg = traffic_bg(pct)
            col = traffic_color(pct)
            html += '<td style="text-align:center;padding:8px 6px">'
            html += '<div style="background:%s;border-radius:8px;padding:6px 4px">' % bg
            html += '<div style="font-size:.9rem;font-weight:800;color:%s">%d%%</div>' % (col, pct)
            html += '<div style="font-size:.55rem;color:var(--fg3);margin-top:1px">%d/%d</div>' % (filled, total)
            html += '</div></td>'
        html += '</tr>\\n'
    html += '</tbody></table>\\n'
    html += '</div>\\n'

    # Legend
    html += '<div style="display:flex;gap:20px;margin-top:16px;font-size:.65rem;color:var(--fg2)">\\n'
    html += '<div style="display:flex;align-items:center;gap:6px"><span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#22c55e"></span> 80%+ Complete</div>\\n'
    html += '<div style="display:flex;align-items:center;gap:6px"><span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#f59e0b"></span> 40-79% Partial</div>\\n'
    html += '<div style="display:flex;align-items:center;gap:6px"><span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:#ef4444"></span> &lt;40% Sparse</div>\\n'
    html += '</div>\\n'

    html += '</div>\\n</div><!-- end page-coverage -->\\n'
    return html


'''

# Insert before compute_insights
code = code.replace(
    "# ─── Compute insights from all RAW data ──────────────────────────────────────",
    coverage_fn + "# ─── Compute insights from all RAW data ──────────────────────────────────────"
)
print("PATCH 3: Added build_coverage_html function ✓")

# ═══════════════════════════════════════════════════════════════════════
# PATCH 4: Add coverage page generation and injection in main()
# ═══════════════════════════════════════════════════════════════════════

# After insights_html is built, add coverage_html generation
old_insights_inject = """    # Build insights HTML
    insights_html = build_insights_html(insights)

    # Insert the insights page div before the footer"""

new_insights_inject = """    # Build insights HTML
    insights_html = build_insights_html(insights)

    # Build coverage page HTML
    print("Building data coverage page...")
    coverage_html = build_coverage_html(all_entries)

    # Insert the coverage page div before page-overview
    html = html.replace(
        '<div id="page-overview"',
        coverage_html + '\\n<div id="page-overview"'
    )

    # Insert the insights page div before the footer"""

code = code.replace(old_insights_inject, new_insights_inject)
print("PATCH 4: Added coverage page injection ✓")

# ═══════════════════════════════════════════════════════════════════════
# PATCH 5: Add coverage button and nav, update showPage for 4 pages
# ═══════════════════════════════════════════════════════════════════════

# Replace the insights button insertion to also add coverage button BEFORE overview
old_btn = """    html = html.replace(
        '    <button id="pt-sidebyside" class="view-toggle-btn" onclick="showPage(\\'sidebyside\\')" style="width:100%;text-align:left;padding:7px 10px;border-radius:6px;font-size:.7rem;font-weight:700;border:1.5px solid var(--bg4);background:transparent;color:var(--fg2);cursor:pointer;font-family:Montserrat">Side-by-Side</button>\\n  </div>',
        '    <button id="pt-sidebyside" class="view-toggle-btn" onclick="showPage(\\'sidebyside\\')" style="width:100%;text-align:left;padding:7px 10px;border-radius:6px;font-size:.7rem;font-weight:700;border:1.5px solid var(--bg4);background:transparent;color:var(--fg2);cursor:pointer;font-family:Montserrat">Side-by-Side</button>\\n'
        '    <button id="pt-insights" class="view-toggle-btn" onclick="showPage(\\'insights\\')" style="width:100%;text-align:left;padding:7px 10px;border-radius:6px;font-size:.7rem;font-weight:700;border:1.5px solid var(--bg4);background:transparent;color:var(--fg2);cursor:pointer;font-family:Montserrat">Key Insights</button>\\n  </div>'
    )"""

new_btn = """    # Add Coverage + Insights buttons to sidebar
    html = html.replace(
        '    <button id="pt-overview" class="view-toggle-btn"',
        '    <button id="pt-coverage" class="view-toggle-btn" onclick="showPage(\\'coverage\\')" style="width:100%;text-align:left;padding:7px 10px;border-radius:6px;font-size:.7rem;font-weight:700;border:1.5px solid var(--bg4);background:transparent;color:var(--fg2);cursor:pointer;font-family:Montserrat">Data Coverage</button>\\n    <button id="pt-overview" class="view-toggle-btn"'
    )
    html = html.replace(
        '    <button id="pt-sidebyside" class="view-toggle-btn" onclick="showPage(\\'sidebyside\\')" style="width:100%;text-align:left;padding:7px 10px;border-radius:6px;font-size:.7rem;font-weight:700;border:1.5px solid var(--bg4);background:transparent;color:var(--fg2);cursor:pointer;font-family:Montserrat">Side-by-Side</button>\\n  </div>',
        '    <button id="pt-sidebyside" class="view-toggle-btn" onclick="showPage(\\'sidebyside\\')" style="width:100%;text-align:left;padding:7px 10px;border-radius:6px;font-size:.7rem;font-weight:700;border:1.5px solid var(--bg4);background:transparent;color:var(--fg2);cursor:pointer;font-family:Montserrat">Side-by-Side</button>\\n'
        '    <button id="pt-insights" class="view-toggle-btn" onclick="showPage(\\'insights\\')" style="width:100%;text-align:left;padding:7px 10px;border-radius:6px;font-size:.7rem;font-weight:700;border:1.5px solid var(--bg4);background:transparent;color:var(--fg2);cursor:pointer;font-family:Montserrat">Key Insights</button>\\n  </div>'
    )"""

code = code.replace(old_btn, new_btn)
print("PATCH 5: Added coverage button ✓")

# Add coverage nav section
old_nav = """    # Add insights nav section
    html = html.replace(
        '  <div id="nav-sidebyside" style="display:none">',
        '  <div id="nav-insights" style="display:none">\\n'
        '    <a class="nav-link" href="#page-insights"><span class="nav-num">00</span> All Insights</a>\\n'
        '  </div>\\n'
        '  <div id="nav-sidebyside" style="display:none">'
    )"""

new_nav = """    # Add coverage + insights nav sections
    html = html.replace(
        '  <div id="nav-overview"',
        '  <div id="nav-coverage" style="display:none">\\n'
        '    <a class="nav-link" href="#sec-coverage"><span class="nav-num">00</span> Coverage Matrix</a>\\n'
        '  </div>\\n'
        '  <div id="nav-overview"'
    )
    html = html.replace(
        '  <div id="nav-sidebyside" style="display:none">',
        '  <div id="nav-insights" style="display:none">\\n'
        '    <a class="nav-link" href="#page-insights"><span class="nav-num">00</span> All Insights</a>\\n'
        '  </div>\\n'
        '  <div id="nav-sidebyside" style="display:none">'
    )"""

code = code.replace(old_nav, new_nav)
print("PATCH 5b: Added coverage nav ✓")

# ═══════════════════════════════════════════════════════════════════════
# PATCH 6: Update showPage for 4 pages, with coverage as default
# ═══════════════════════════════════════════════════════════════════════

old_showpage = """    new_showpage = \\"\\"\\"function showPage(page) {
  currentPage = page;
  var po = document.getElementById('page-overview');
  var ps = document.getElementById('page-sidebyside');
  var pi = document.getElementById('page-insights');
  var no = document.getElementById('nav-overview');
  var ns = document.getElementById('nav-sidebyside');
  var ni = document.getElementById('nav-insights');
  var ptOverview = document.getElementById('pt-overview');
  var ptSidebyside = document.getElementById('pt-sidebyside');
  var ptInsights = document.getElementById('pt-insights');

  var allPages = [po, ps, pi];
  var allNavs = [no, ns, ni];
  var allBtns = [ptOverview, ptSidebyside, ptInsights];
  for (var i = 0; i < allPages.length; i++) {
    allPages[i].style.display = 'none';
    allNavs[i].style.display = 'none';
    allBtns[i].style.background = 'transparent';
    allBtns[i].style.color = 'var(--fg2)';
    allBtns[i].style.borderColor = 'var(--bg4)';
  }

  if (page === 'overview') {
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
}\\"\\"\\" """

new_showpage = """    new_showpage = \\"\\"\\"function showPage(page) {
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
}\\"\\"\\" """

code = code.replace(old_showpage, new_showpage)
print("PATCH 6: Updated showPage for 4 pages ✓")

# Change the initial page from overview to coverage
# The original dashboard shows page-overview by default (display:block)
# We need to hide it and show page-coverage instead
# This is handled by the coverage page HTML having display:block and
# we need to hide page-overview initially

# Add code after the coverage page injection to hide page-overview initially
old_hide = """    # Insert the insights page div before the footer"""
new_hide = """    # Make coverage the default page (hide overview initially)
    html = html.replace(
        '<div id="page-overview" style="display:block"',
        '<div id="page-overview" style="display:none"'
    )
    # Also set coverage button as active on load
    html = html.replace(
        "showPage('overview')",
        "showPage('coverage')",
        1  # only the first occurrence (the init call)
    )

    # Insert the insights page div before the footer"""

code = code.replace(old_hide, new_hide)
print("PATCH 7: Coverage as default landing page ✓")

# ═══════════════════════════════════════════════════════════════════════
# PATCH 8: Fix path for Docker/sandbox
# ═══════════════════════════════════════════════════════════════════════
code = code.replace(
    'BASE = "/Users/dlimberopoulos/Documents/Womens jeans scraper"',
    'BASE = "/sessions/hopeful-tender-dijkstra/mnt/Womens jeans scraper"'
)
print("PATCH 8: Fixed BASE path for sandbox ✓")

# Write patched file
PATCHED = f"{BASE}/update_dashboard_patched.py"
with open(PATCHED, "w", encoding="utf-8") as f:
    f.write(code)

print(f"\nPatched file written to: {PATCHED}")
print("Ready to run!")
