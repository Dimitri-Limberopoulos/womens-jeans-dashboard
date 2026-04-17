#!/usr/bin/env python3
"""
Build script: combines data.json + app.js into single HTML dashboard file.
Uses the consulting-grade CSS theme with side nav and chart cards.
"""

import json, os, re, subprocess, sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASH_DIR = '/sessions/eloquent-kind-feynman/mnt/Womens jeans scraper'

# Detect environment
if os.path.exists(BASH_DIR):
    BASE = BASH_DIR
else:
    BASE = SCRIPT_DIR

def read_file(path):
    with open(path, 'r') as f:
        return f.read()

def lb(chart_id, default_on=True):
    """Generate a label toggle pill button."""
    state = 'active' if default_on else ''
    return (f'<button id="lb-{chart_id}" class="filter-btn {state}" '
            f"onclick=\"toggleLabels('{chart_id}',!labelState['{chart_id}'])\" "
            f'style="font-size:.6rem;padding:3px 9px;margin-left:8px">Labels</button>')

def main():
    data_path = os.path.join(BASE, 'data.json')
    js_path = os.path.join(BASE, 'app.js')

    with open(data_path, 'r') as f:
        data_json = f.read()
    app_js = read_file(js_path)

    # ── Chart card helper ─────────────────────────────────────
    INFO_TEXT = {
        'priceBox': 'Prices are extracted per color combination from Target PDPs. <b>Current Price</b> = selling price at time of scrape. <b>Original Price</b> = pre-sale price; if a product is not on sale, original price = current price. Box shows IQR (Q1–Q3), whiskers extend to 1.5&times;IQR, vertical line = median, ◆ = mean.',
        'rise': '<b>Rise</b> is standardized from Target&rsquo;s raw rise field. <b>Low Rise</b>, <b>Mid Rise</b> (includes Target&rsquo;s "Classic" and "Regular" labels), <b>High Rise</b>, and <b>Ultra-High Rise</b>. Rows with no rise data are excluded.',
        'legShape': '<b>Leg shape</b> is parsed from the first part of Target&rsquo;s Fit field (e.g. "Straight Leg with a Regular Fit" &rarr; Straight). Categories: Straight, Skinny, Wide, Bootcut, Slim, Tapered, Flare, Jegging, Barrel, Boyfriend, Relaxed. <b>"Other"</b> = unrecognized leg shapes (&lt;2% of data).',
        'fitStyle': '<b>Fit style</b> is the second part of Target&rsquo;s Fit field (e.g. "...with a Contemporary Fit"). <b>Regular</b> (includes Classic, Standard). <b>Contemporary/Slim</b> (Contemporary + Slim + Tailored &mdash; modern/updated fits slimmer than Regular). <b>Casual/Relaxed</b> (Casual + Relaxed + Easy &mdash; comfort-oriented looser fits). <b>Curvy</b>. <b>Loose</b> (includes Boyfriend/Girlfriend). <b>Straight</b>.',
        'length': '<b>Garment length</b> from Target specs, grouped into 5 buckets: <b>Short</b> = above knee. <b>Capri</b> = at/below knee (includes "At Knee" and "Below Knee"). <b>Crop</b> = mid-calf (includes "At Calf", "Low Calf", "7/8"). <b>Ankle</b> = ankle-length. <b>Full</b> = full-length.',
        'weight': '<b>Fabric weight</b> from Target specs. 4 buckets from lightest to heaviest: Extra Lightweight &rarr; Lightweight &rarr; <b>Midweight</b> (includes Target&rsquo;s "Year Round Fabric Construction" label) &rarr; Heavyweight.',
        'wash': '<b>Wash/Color</b> mapped from the raw color name. <b>Light Wash</b> (light, bleach, faded). <b>Medium Wash</b> (medium, stonewash, plain "blue"/"denim"). <b>Dark Wash</b> (dark, indigo, rinse, navy, midnight). <b>Black</b>. <b>White/Cream</b>. <b>Grey</b> (grey, charcoal, gunmetal). <b>Brown/Tan</b> (chocolate, toffee, bourbon). <b>Earth Tones</b> (khaki, olive, pine, sage). <b>Color</b> = all other non-denim colors (pink, red, emerald, etc.).',
        'inseam': '<b>Inseam</b> in inches, parsed from Target specs. Only rows with a numeric inseam value are included (27% of all CCs have this data).',
        'cotton': '<b>Cotton %</b> parsed from the "% Cotton" field, or extracted from the Material description (e.g. "98% Cotton, 2% Spandex" &rarr; 98). Only rows with identifiable cotton content are included (31% of all CCs).',
    }

    def info_btn(chart_id):
        return (f'<button onclick="toggleInfo(\'{chart_id}\')" '
                f'style="width:22px;height:22px;border-radius:50%;border:1.5px solid var(--bg4);'
                f'background:transparent;color:var(--fg3);font-family:Montserrat,sans-serif;'
                f'font-size:.7rem;font-weight:700;cursor:pointer;margin-left:6px;line-height:1" '
                f'title="How this data was interpreted">?</button>')

    def info_panel(chart_id):
        text = INFO_TEXT.get(chart_id, '')
        return (f'<div id="info-{chart_id}" style="display:none;background:var(--bg2);border:1px solid var(--bg3);'
                f'border-radius:var(--radius-sm);padding:12px 16px;margin-bottom:12px;font-size:.68rem;'
                f'color:var(--fg2);line-height:1.6">{text}</div>')

    def chart_card(chart_id, canvas_id, title, subtitle_id, full=False):
        cls = 'chart-card full' if full else 'chart-card'
        return f'''<div class="{cls}">
  <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap">
    <div><div class="chart-title">{title}{info_btn(chart_id)}</div>
    <div class="chart-subtitle" id="{subtitle_id}">Loading...</div></div>
    <div>{lb(chart_id)}</div>
  </div>
  {info_panel(chart_id)}
  <div class="chart-wrap"><canvas id="{canvas_id}"></canvas></div>
</div>'''

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Target Women's Jeans — Assortment Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=Montserrat:wght@400;500;600;700;800&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels@2.2.0/dist/chartjs-plugin-datalabels.min.js"></script>
<style>
:root{{--bg:#fff;--bg2:#f8f8fa;--bg3:#eeeef2;--bg4:#dddde3;--fg:#1a1a2e;--fg2:#555568;--fg3:#8a8a9a;--accent:#1a1a2e;--red:#002855;--purple:#0072CE;--radius:10px;--radius-sm:7px}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Montserrat',sans-serif;background:var(--bg);color:var(--fg);line-height:1.6;-webkit-font-smoothing:antialiased}}
.page-wrapper{{display:flex;min-height:100vh}}
.side-nav{{position:fixed;left:0;top:0;bottom:0;width:220px;z-index:100;display:flex;flex-direction:column;gap:2px;background:var(--bg);border-right:1px solid var(--bg3);padding:20px 10px;overflow-y:auto}}
.side-nav .nav-title{{font-size:.62rem;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:var(--fg3);padding:0 10px 12px;border-bottom:1px solid var(--bg3);margin-bottom:8px}}
.side-nav a{{display:flex;align-items:center;gap:8px;padding:7px 10px;border-radius:6px;text-decoration:none;color:var(--fg3);font-size:.68rem;font-weight:600;transition:all .15s;border:1px solid transparent;white-space:nowrap}}
.side-nav a:hover{{color:var(--fg);background:var(--bg2)}}
.side-nav a.active{{background:var(--purple);color:#fff;border-color:var(--purple)}}
.side-nav a .nav-num{{font-weight:700;min-width:20px;font-size:.62rem;opacity:.7}}
.main-area{{margin-left:220px;flex:1;min-width:0}}
.container{{max-width:1340px;margin:0 auto;padding:0 28px}}
h1,h2{{font-weight:800;letter-spacing:-.03em}}
h1{{font-size:clamp(2.2rem,4.5vw,3.4rem);line-height:1.08}}
h2{{font-size:clamp(1.4rem,2.8vw,2rem);line-height:1.2}}
.hero{{padding:52px 0 36px;border-bottom:2px solid var(--bg3)}}
.hero .label{{font-size:.68rem;font-weight:700;letter-spacing:.18em;text-transform:uppercase;color:var(--fg3);margin-bottom:14px;display:block}}
.hero p{{font-size:1rem;color:var(--fg2);max-width:700px;margin-top:14px}}
.section{{padding:44px 0;border-bottom:1px solid var(--bg3)}}
.section-header{{display:flex;align-items:baseline;gap:14px;margin-bottom:24px;flex-wrap:wrap}}
.section-header .num{{font-size:.7rem;font-weight:700;color:var(--red);letter-spacing:.1em;padding:3px 10px;border:1.5px solid var(--red);border-radius:18px}}
.chart-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(480px,1fr));gap:18px}}
.chart-card{{background:var(--bg);border:1px solid var(--bg3);border-radius:var(--radius);padding:22px;position:relative;transition:box-shadow .2s}}
.chart-card:hover{{box-shadow:0 2px 12px rgba(0,0,0,.06)}}
.chart-card.full{{grid-column:1/-1}}
.chart-title{{font-size:.88rem;font-weight:700;color:var(--fg);margin-bottom:2px}}
.chart-subtitle{{font-size:.72rem;color:var(--fg3);margin-bottom:14px;font-weight:500}}
.chart-wrap{{position:relative;width:100%;min-height:300px}}
.chart-wrap canvas{{width:100%!important}}
.filter-btn{{font-family:'Montserrat',sans-serif;font-size:.7rem;font-weight:600;padding:5px 12px;border:1.5px solid var(--bg4);border-radius:18px;background:transparent;color:var(--fg2);cursor:pointer;transition:all .15s}}
.filter-btn:hover{{border-color:var(--fg3);color:var(--fg)}}
.filter-btn.active{{background:var(--fg);color:#fff;border-color:var(--fg)}}
.brand-color-dot{{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:4px}}
.global-filter{{background:var(--bg2);border:1px solid var(--bg3);border-radius:var(--radius);padding:18px 22px;margin-top:20px}}
.global-filter .gf-label{{font-size:.68rem;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--fg3);margin-bottom:8px}}
.global-filter .gf-row{{display:flex;gap:7px;flex-wrap:wrap;align-items:center}}
.kpi-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:22px}}
.kpi{{background:var(--bg2);border:1px solid var(--bg3);border-radius:var(--radius-sm);padding:16px}}
.kpi-value{{font-size:1.6rem;font-weight:800}}
.kpi-label{{font-size:.72rem;color:var(--fg3);font-weight:500;margin-top:1px}}
.view-toggle{{display:flex;gap:3px;margin-bottom:10px;background:var(--bg3);border-radius:7px;padding:3px;width:fit-content}}
.view-toggle-btn{{font-family:'Montserrat',sans-serif;font-size:.68rem;font-weight:600;padding:4px 14px;border:none;border-radius:5px;background:transparent;color:var(--fg3);cursor:pointer;transition:all .15s}}
.view-toggle-btn.active{{background:#fff;color:var(--fg);box-shadow:0 1px 3px rgba(0,0,0,.1)}}
.footer{{padding:32px 0;text-align:center;color:var(--fg3);font-size:.72rem;font-weight:500}}
::-webkit-scrollbar{{width:7px}}
::-webkit-scrollbar-track{{background:var(--bg2)}}
::-webkit-scrollbar-thumb{{background:var(--bg4);border-radius:4px}}
@media(max-width:768px){{.chart-grid{{grid-template-columns:1fr}}.container{{padding:0 14px}}.side-nav{{display:none}}.main-area{{margin-left:0}}}}
@media(max-width:1024px){{.side-nav{{width:180px}}.main-area{{margin-left:180px}}}}
</style>
</head>
<body>

<div class="page-wrapper">

<!-- Side Nav -->
<nav class="side-nav">
  <div class="nav-title">Target Jeans Dashboard</div>
  <a href="#sec-overview"><span class="nav-num">00</span> Overview</a>
  <a href="#sec-price"><span class="nav-num">01</span> Price Architecture</a>
  <a href="#sec-attr"><span class="nav-num">02</span> Attribute Comparisons</a>
  <a href="#sec-continuous"><span class="nav-num">03</span> Continuous Attributes</a>
  <a href="#sec-heatmap"><span class="nav-num">04</span> Cross-Tab Heatmap</a>
</nav>

<!-- Main Area -->
<div class="main-area">
<div class="container">

<!-- Hero -->
<div class="hero" id="sec-overview">
  <span class="label">Assortment Intelligence</span>
  <h1>Target Women's Jeans</h1>
  <p>Interactive analysis of 2,673 color combinations (CCs) across 58 brands. Toggle brands below to compare Owned vs National brand assortments.</p>
  <div style="background:var(--bg2);border:1px solid var(--bg3);border-radius:var(--radius-sm);padding:12px 16px;margin-top:14px;max-width:700px">
    <div style="font-size:.72rem;font-weight:700;color:var(--fg);margin-bottom:4px">Unit of Analysis: Color Combination (CC)</div>
    <div style="font-size:.68rem;color:var(--fg2);line-height:1.5">Each row represents one <strong>product &times; color</strong> combination. A single jean style available in 5 colors = 5 CCs. All counts, distributions, and n= values in this dashboard are measured in CCs. Brands are ordered by CC count (most to least) throughout.</div>
  </div>

  <div class="global-filter" id="brandSelector">
    <!-- populated by JS -->
  </div>
</div>

<!-- Section 0: KPIs -->
<div class="section" id="sec-kpis">
  <div class="kpi-row">
    <div class="kpi"><div class="kpi-value" id="kpi-total-ccs">—</div><div class="kpi-label">Total Color Combos</div></div>
    <div class="kpi"><div class="kpi-value" id="kpi-ob-brands">—</div><div class="kpi-label">Owned Brands</div></div>
    <div class="kpi"><div class="kpi-value" id="kpi-nb-brands">—</div><div class="kpi-label">National Brands</div></div>
    <div class="kpi"><div class="kpi-value" id="kpi-ob-ccs">—</div><div class="kpi-label">Owned CCs</div></div>
    <div class="kpi"><div class="kpi-value" id="kpi-nb-ccs">—</div><div class="kpi-label">National CCs</div></div>
    <div class="kpi"><div class="kpi-value" id="kpi-ob-avg">—</div><div class="kpi-label">Owned Avg Price</div></div>
    <div class="kpi"><div class="kpi-value" id="kpi-nb-avg">—</div><div class="kpi-label">National Avg Price</div></div>
  </div>
</div>

<!-- Section 1: Price Architecture -->
<div class="section" id="sec-price">
  <div class="section-header">
    <span class="num">01</span>
    <h2>Price Architecture</h2>
  </div>

  <div class="view-toggle" style="margin-bottom:16px">
    <button class="view-toggle-btn active" id="btn-price-current" onclick="setPriceType('current')">Current Price</button>
    <button class="view-toggle-btn" id="btn-price-original" onclick="setPriceType('original')">Original Price</button>
  </div>

  <div class="chart-card full">
    <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap">
      <div>
        <div class="chart-title">Price Distribution by Brand{info_btn('priceBox')}</div>
        <div class="chart-subtitle">Owned Brands (top) → OB TOTAL | NB TOTAL → National Brands (bottom). Sorted by median price. Line = median, ◆ = mean.</div>
      </div>
      <div>{lb('priceBox')}</div>
    </div>
    {info_panel('priceBox')}
    <div class="chart-wrap" id="priceBoxWrap" style="min-height:500px">
      <canvas id="priceBoxCanvas"></canvas>
    </div>
  </div>
</div>

<!-- Section 2: Attribute Comparisons (Categorical) -->
<div class="section" id="sec-attr">
  <div class="section-header">
    <span class="num">02</span>
    <h2>Attribute Comparisons</h2>
  </div>
  <p style="font-size:.82rem;color:var(--fg2);margin-bottom:20px">Each chart <strong>excludes</strong> rows without data for that attribute. The n= shows how many color combinations were included per group.</p>

  <div class="chart-grid">
    {chart_card('rise', 'riseCanvas', 'Rise Distribution', 'sub-rise')}
    {chart_card('legShape', 'legShapeCanvas', 'Leg Shape Distribution', 'sub-legShape')}
    {chart_card('length', 'lengthCanvas', 'Garment Length Distribution', 'sub-length')}
    {chart_card('weight', 'weightCanvas', 'Fabric Weight Distribution', 'sub-weight')}
    {chart_card('wash', 'washCanvas', 'Wash / Color Distribution', 'sub-wash')}
    {chart_card('fitStyle', 'fitStyleCanvas', 'Fit Style Distribution', 'sub-fitStyle')}
  </div>
</div>

<!-- Section 3: Continuous Attributes -->
<div class="section" id="sec-continuous">
  <div class="section-header">
    <span class="num">03</span>
    <h2>Continuous Attributes</h2>
  </div>
  <p style="font-size:.82rem;color:var(--fg2);margin-bottom:20px">Histogram distributions for numeric attributes. Only rows with valid data are included.</p>

  <div class="chart-grid">
    {chart_card('inseam', 'inseamCanvas', 'Inseam Distribution', 'sub-inseam')}
    {chart_card('cotton', 'cottonCanvas', 'Cotton Content Distribution', 'sub-cotton')}
  </div>
</div>

<!-- Section 4: Interactive Heatmap -->
<div class="section" id="sec-heatmap">
  <div class="section-header">
    <span class="num">04</span>
    <h2>Cross-Tab Heatmap</h2>
  </div>
  <p style="font-size:.82rem;color:var(--fg2);margin-bottom:16px">Select any two attributes to see how Owned Brands and National Brands distribute across combinations. Cell intensity = % of each group's total CCs. Only rows with data for <em>both</em> selected dimensions are included.</p>

  <div class="chart-card full">
    <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;margin-bottom:16px">
      <div style="font-size:.72rem;font-weight:700;color:var(--fg2)">X-Axis (columns):</div>
      <select id="hm-dim-x" style="font-family:Montserrat,sans-serif;font-size:.72rem;font-weight:600;padding:5px 10px;border:1.5px solid var(--bg4);border-radius:6px;background:#fff;color:var(--fg);cursor:pointer"></select>
      <div style="font-size:.72rem;font-weight:700;color:var(--fg2);margin-left:12px">Y-Axis (rows):</div>
      <select id="hm-dim-y" style="font-family:Montserrat,sans-serif;font-size:.72rem;font-weight:600;padding:5px 10px;border:1.5px solid var(--bg4);border-radius:6px;background:#fff;color:var(--fg);cursor:pointer"></select>
    </div>
    <div id="heatmapContainer" style="min-height:300px">
      <!-- populated by JS -->
    </div>
  </div>
</div>

<div class="footer">
  Target Women's Jeans Dashboard &middot; 2,673 color combinations &middot; 58 brands &middot; Data current as of April 2026
</div>

</div><!-- /.container -->
</div><!-- /.main-area -->
</div><!-- /.page-wrapper -->

<script>
window.DATA = {data_json};
Chart.register(ChartDataLabels);
</script>
<script>
{app_js}
</script>

</body>
</html>'''

    output_path = os.path.join(BASE, 'jeans_dashboard.html')
    with open(output_path, 'w') as f:
        f.write(html)

    print('Built jeans_dashboard.html')
    print('File size: ' + str(len(html)) + ' bytes (' + str(round(len(html)/1024/1024, 2)) + ' MB)')

    # Validate embedded scripts
    scripts = re.findall(r'<script>(.*?)</script>', html, re.DOTALL)
    print('Found ' + str(len(scripts)) + ' script blocks')
    for i, s in enumerate(scripts):
        tmp = os.path.join('/tmp', 'check_' + str(i) + '.js')
        with open(tmp, 'w') as f:
            f.write(s)
        result = subprocess.run(['node', '--check', tmp], capture_output=True, text=True)
        if result.returncode == 0:
            print('  Script ' + str(i) + ': OK')
        else:
            print('  Script ' + str(i) + ': ERROR')
            print('    ' + result.stderr[:300])

    # Verify DOM element references
    js_ids = re.findall(r"getElementById\(['\"]([^'\"]+)['\"]\)", html)
    html_ids = re.findall(r'id="([^"]+)"', html)
    html_id_set = set(html_ids)
    missing = [jid for jid in set(js_ids) if jid not in html_id_set
               and not jid.startswith('lb-') and not jid.startswith('sub-')]
    if missing:
        print('WARNING: JS references missing DOM IDs: ' + str(missing))
    else:
        print('All DOM ID references verified OK')

if __name__ == '__main__':
    main()
