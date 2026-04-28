# Cross-Retailer Women's Jeans Dashboard

An interactive single-file HTML dashboard comparing women's jeans assortments across 9 retailer groups and 50+ brands — covering price architecture, promotional activity, color/wash distribution, fit attributes, and data coverage.

**Live dashboard:** [View on GitHub Pages](https://dlimberopoulos.github.io/Womens-jeans-scraper/) (or open `index.html` locally)

---

## What This Project Does

This project scrapes product detail pages (PDPs) from major US retailers, extracts 15+ attributes per product, classifies and normalizes the data, and renders it as a self-contained interactive HTML dashboard. The entire pipeline runs locally — no server, no database, no API keys.

**Retailers covered:** Target (Owned + National Brands), Walmart, Amazon, American Eagle, Old Navy, Macy's, Kohl's, Levi's

**Data points per product:** Name, brand, current price, original price, color, wash category, fit style, leg shape, rise, inseam, fabric composition, cotton %, fabric weight, pack size, URL

---

## End-to-End Pipeline

The full build has five phases. Each phase can be run independently — you only need to re-run from the phase where something changed.

```
Phase 1          Phase 2          Phase 3           Phase 4          Phase 5
URL Collection → PDP Scraping  → Data Cleaning   → Dashboard Build → Deployment
(Chrome plugin)  (Playwright/     (Python)          (Python + JS)    (GitHub Pages)
                  web_fetch)
```

---

## Phase 1: URL Collection (Instant Data Scraper)

### What You Need

- Google Chrome
- [Instant Data Scraper](https://chrome.google.com/webstore/detail/instant-data-scraper/ofaokhiedipichpaobibbnahnkdoiiah) extension (free)

### How It Works

Instant Data Scraper auto-detects product listing grids on retailer websites and extracts all visible product URLs. You paginate through the full listing, and it captures every page.

### Step-by-Step

1. Navigate to the retailer's women's jeans category page (e.g., `target.com/c/womens-jeans`)
2. Click the Instant Data Scraper extension icon — it highlights the detected data table
3. If it detected the wrong table, click "Try another table" until it finds the product grid
4. Click "Start crawling" — it paginates automatically
5. When finished, click "Download" → save as CSV

### Output

One CSV per retailer, saved to this project folder:

| File | Retailer | Typical URL Count |
|------|----------|-------------------|
| `target.csv` | Target | ~800 |
| `walmart.csv` | Walmart | ~600 |
| `amazon.csv` | Amazon | ~400 |
| `ae.csv` | American Eagle | ~300 |
| `oldnavy.csv` | Old Navy | ~250 |
| `macys.csv` | Macy's | ~400 |
| `kohls.csv` | Kohl's | ~350 |
| `levi.csv` | Levi's | ~250 |

### Tips

- Make sure you're on the **full category page**, not a filtered subset — check that the page title says something like "All Women's Jeans" and the product count looks right
- Some retailers (Target, Walmart) show 24–48 products per page and have 20+ pages — let the crawler run through all of them
- If a retailer paginates via infinite scroll instead of numbered pages, scroll to the bottom first to load all products, then run the scraper
- The CSV will have many columns — the scraper only needs the **URL column** (usually column 1 or a column named "href" / "link" / "url")
- De-duplicate URLs before scraping: retailers sometimes list the same product in multiple category views

---

## Phase 2: PDP Scraping

### What You Need

```bash
pip3 install playwright openpyxl beautifulsoup4 --break-system-packages
python3 -m playwright install chromium
```

### How It Works

Each retailer has a dedicated scraper (`{retailer}_pdp_scraper.py`) that visits every URL from Phase 1, waits for the page to load, and extracts product attributes from the DOM. Some retailers render content server-side (Target, Old Navy, Macy's) and can use the faster `web_fetch` approach; others require a full Playwright browser (Walmart, Amazon, Levi's).

### Running a Scraper

```bash
# Run a single retailer
python3 target_pdp_scraper.py

# Each scraper reads its CSV (e.g., target.csv) and writes results to:
#   {retailer}_pdp_results.xlsx   — Excel output
#   {retailer}_pdp_progress.json  — Progress checkpoint (for crash recovery)
```

### Crash Recovery

All scrapers save progress every 100 URLs. If a run crashes or you stop it:

- The scraper checks `{retailer}_pdp_progress.json` on startup
- Already-scraped URLs are skipped automatically
- Just re-run the same command — it picks up where it left off

### Concurrency and Throttling

| Retailer | Method | Concurrency | Typical Speed |
|----------|--------|-------------|---------------|
| Target | web_fetch | 5 parallel | ~1.5s/page |
| Walmart | Playwright | 3 tabs | ~3s/page |
| Amazon | Playwright | 2 tabs | ~4s/page |
| AE | web_fetch | 5 parallel | ~1.5s/page |
| Old Navy | web_fetch | 5 parallel | ~1.5s/page |
| Macy's | web_fetch | 3 parallel | ~2s/page |
| Kohl's | Playwright | 3 tabs | ~3s/page |
| Levi's | Playwright | 2 tabs | ~3.5s/page |

If you see many consecutive errors, the site may be rate-limiting. Reduce concurrency or increase sleep time in the scraper config.

### Multi-Pass Strategy

Expect ~5–10% of URLs to fail on the first pass (timeouts, CAPTCHAs, 503 errors). Run a second pass on just the failures:

1. First pass scrapes all URLs → results + error log
2. Extract failed URLs from the progress JSON
3. Re-run with lower concurrency and longer timeouts
4. Merge results

For Levi's specifically, color data required a separate scraping pass because colors are loaded dynamically via JavaScript and aren't in the initial page HTML.

### Long Runs

Scraping 4,000+ URLs takes 2–4 hours depending on retailer mix. During the run:

- Keep your Mac plugged into power
- System Settings → Battery → "Prevent automatic sleeping"
- Don't close the terminal or browser window
- You can use other apps freely while it runs

---

## Phase 3: Data Cleaning and Classification

### What You Need

```bash
pip3 install pandas openpyxl --break-system-packages
```

### Unifying Retailer Data

Each retailer scraper outputs slightly different column names and formats. The `unify_retailers.py` script normalizes everything into a single CSV:

```bash
python3 unify_retailers.py
# Reads: all *_pdp_results.xlsx files
# Writes: unified_retailer_data.csv
```

This script handles: price parsing (stripping "$" and commas), retailer group assignment (Target OB vs Target NB, etc.), pack size normalization for Walmart multi-packs (divides price by pack count), and brand name standardization.

### Color → Wash Classification

Raw color names from retailers are a mix of standard ("Dark Wash", "Black") and creative/brand-specific ("Ink Well", "Honey Crisp", "Salsa Verde"). Classification happens in two stages:

**Stage 1 — Regex with word boundaries** (`fix_wash.py`):

Patterns like `\b(dark|dk\b|rinse|indigo)\b` map ~85% of colors to categories: Light Wash, Medium Wash, Dark Wash, Black, White/Cream, Grey, Colored.

Word boundaries (`\b`) are critical — without them, "light" matches "moonlight" and "grey" matches "greyhound."

**Stage 2 — Web search for creative names:**

The remaining ~15% are brand-specific names that regex can't classify. Batch them into groups of 20 and search "What color is [name] in jeans?" to determine the actual color family. Build a lookup table from the results.

### Fit Style Normalization

Some retailers label the same concept differently. "Straight" can appear as a fit style or a leg shape. When one field is missing, fall back to the other using a mapping table (see `enrich_dashboard.py`).

### Preparing Dashboard Data

```bash
python3 prep_data.py
# Reads: unified_retailer_data.csv
# Writes: data.json (compact JSON with abbreviated field names)
```

This produces `data.json` with single-character keys (`b` for brand, `p` for price, `w` for wash, etc.) to minimize file size. A typical 4,000-product dataset produces ~1.8 MB of JSON.

---

## Phase 4: Dashboard Build

### How It Works

The dashboard is a single self-contained HTML file with all data, CSS, and JavaScript embedded. No server, no CDN dependencies at runtime (fonts and Chart.js are loaded from CDN but the dashboard degrades gracefully without them).

The build uses three source files that get combined:

| File | Purpose |
|------|---------|
| `app.js` | All chart logic, filters, toggles (~36 KB). Must use ES5 syntax. |
| `data.json` | Compact product data (~1.8 MB) |
| `build.py` | Assembles HTML + CSS + JS + data into one file |

### Building

```bash
python3 build.py
# Reads: app.js, data.json
# Writes: index.html (~1.9 MB)
```

### Why ES5?

The JavaScript gets embedded into a Python f-string during the build. ES6 features like template literals (`${x}`), arrow functions (`=>`), and spread operators (`{...obj}`) conflict with Python's `{...}` f-string syntax and cause build failures. All JavaScript must use ES5 equivalents: string concatenation, `function(){}`, `Object.assign()`.

### Dashboard Sections

| # | Section | What It Shows |
|---|---------|---------------|
| — | Coverage Traffic Light | Data completeness per retailer (% of fields populated) |
| — | KPI Overview | Median prices, total CCs, promotional rates across retailers |
| 01 | Price Architecture | Box plots by retailer group + bubble chart price tier distribution |
| 02 | Brand Drill-Down | Click a retailer to see individual brand price distributions with reference bars |
| 03 | Promotional Activity | % on sale, discount depth distribution |
| 04 | Color & Wash | Stacked bars showing wash distribution per retailer |
| 05 | Fit & Rise | Stacked bars for fit style, leg shape, rise |
| 06 | Side-by-Side Comparison | Pick two retailers and compare everything head-to-head |

### Interactive Controls

| Control | What It Does |
|---------|-------------|
| Retailer group pills | Toggle groups on/off across all charts |
| Current / Original Price | Switch all price charts between current and MSRP |
| Labels button | Show/hide value labels on any chart |
| Band size dropdown | Change bubble chart price band width ($10/$20/$25/$50) |
| Size slider | Adjust bubble chart bubble density |
| Drill-down pills | Select a retailer group to see its individual brands |

---

## Phase 5: Deployment

### GitHub Pages

The dashboard is served via GitHub Pages from the `main` branch root.

```bash
git add index.html
git commit -m "Update dashboard with latest scrape data"
git push origin main
```

GitHub Pages is configured in repo Settings → Pages → Source: "Deploy from a branch" → Branch: `main`, folder: `/ (root)`.

Changes typically appear within 1–2 minutes of pushing.

### Important: Single Source of Truth

There is one file that matters: **`index.html`** in the repo root. Old dashboard versions are archived in `archive/`. Never edit files in `archive/` — they're historical snapshots only.

---

## Project Structure

```
Womens jeans scraper/
│
├── index.html                    ← THE LIVE DASHBOARD (edit this one)
├── README.md                     ← This file
│
├── Phase 1 inputs (from Instant Data Scraper)
│   ├── target.csv
│   ├── walmart.csv
│   ├── amazon.csv
│   ├── ae.csv
│   ├── oldnavy.csv
│   ├── macys.csv
│   ├── kohls.csv
│   └── levi.csv
│
├── Phase 2 scrapers
│   ├── target_pdp_scraper.py
│   ├── walmart_pdp_scraper.py
│   ├── amazon_pdp_scraper.py
│   ├── ae_pdp_scraper.py
│   ├── oldnavy_pdp_scraper.py
│   ├── macys_pdp_scraper.py
│   ├── kohls_pdp_scraper.py
│   ├── levis_pdp_scraper.py
│   └── *_pdp_progress.json       (crash recovery checkpoints)
│
├── Phase 2 outputs
│   ├── target_pdp_results.xlsx
│   ├── walmart_pdp_results.xlsx
│   ├── amazon_pdp_results.xlsx
│   ├── ae_pdp_results.xlsx
│   ├── oldnavy_pdp_results.xlsx
│   ├── kohls_pdp_results.xlsx
│   └── *_pdp_analysis.json       (scrape summaries)
│
├── Phase 3 cleaning
│   ├── unify_retailers.py         (merge all retailer data)
│   ├── unified_retailer_data.csv  (merged output)
│   ├── fix_wash.py                (color → wash classification)
│   ├── enrich_dashboard.py        (fit style normalization + enrichment)
│   ├── prep_data.py               (CSV → compact JSON)
│   └── data.json                  (dashboard-ready data)
│
├── Phase 4 build
│   ├── app.js                     (dashboard JavaScript — ES5 only)
│   ├── build.py                   (assembles final HTML)
│   └── boxplot-plugin.js          (Chart.js whisker plugin)
│
├── Levi's color resolution (multi-pass)
│   ├── levis_color_scraper.py
│   ├── levis_colors_batch_*.json  (batch scrape results)
│   ├── levis_colors_remaining_*.json
│   ├── levis_colors_final_*.json
│   └── extract_levis_colors.py
│
├── archive/                       (old dashboard versions — DO NOT EDIT)
│   ├── cross_retailer_dashboard.html
│   ├── cross_retailer_dashboard_v2.html
│   └── jeans_dashboard.html       (original Target-only version)
│
└── Skills (for Claude)
    ├── create-assortment-dashboard.skill
    └── pdp-scraper.skill
```

---

## Refreshing the Data

To update with fresh scrape data, run these phases in order:

```bash
# 1. Collect new URLs via Instant Data Scraper (save CSVs to this folder)

# 2. Run scrapers (one per retailer, or all)
python3 target_pdp_scraper.py
python3 walmart_pdp_scraper.py
python3 amazon_pdp_scraper.py
python3 ae_pdp_scraper.py
python3 oldnavy_pdp_scraper.py
python3 macys_pdp_scraper.py
python3 kohls_pdp_scraper.py
python3 levis_pdp_scraper.py

# 3. Unify and clean
python3 unify_retailers.py
python3 fix_wash.py
python3 enrich_dashboard.py
python3 prep_data.py

# 4. Rebuild dashboard
python3 build.py

# 5. Deploy
git add index.html
git commit -m "Refresh data — $(date +%Y-%m-%d)"
git push origin main
```

---

## Common Issues

| Problem | Cause | Fix |
|---------|-------|-----|
| Scraper gets many timeouts | Rate limiting | Reduce concurrency, increase sleep times |
| Bubble chart — all bubbles same size | Radius capped with `Math.min` | Use relative scaling: `Math.sqrt(count/maxCount) * maxR` |
| Stacked bars don't sum to 100% | Category order array missing a category | Ensure WASH_ORDER includes every category in the data |
| Toggle button looks unclicked | White active state on white background | Use high-contrast active CSS: `background: #002855; color: #fff` |
| Price toggle only works in one section | Handler only updates one section's buttons | Query both sections and use `i % 2` for index reset |
| Traffic light table shows stale data | Table is hardcoded HTML | Regenerate from RAW data or make it dynamic |
| `git push` fails with index.lock | Stale lock file from crashed git process | `rm -f .git/index.lock` then retry |
| Dashboard looks old on GitHub Pages | Browser cache | Hard refresh (Cmd+Shift+R) or wait 2 minutes |
| Levi's colors show as "Unknown" | Colors loaded via JS, not in initial HTML | Run dedicated color scraper (`levis_color_scraper.py`) |

---

## Tech Stack

- **Scraping:** Python 3, Playwright (async), BeautifulSoup, web_fetch
- **Data processing:** pandas, openpyxl
- **Dashboard:** Chart.js 4.4.4, D3.js 7, Montserrat font, pure ES5 JavaScript
- **Deployment:** GitHub Pages (static HTML, no server)
- **URL collection:** Instant Data Scraper (Chrome extension)

---

Built: April 2026
Data Sources: Target.com, Walmart.com, Amazon.com, AE.com, OldNavy.com, Macys.com, Kohls.com, Levi.com
