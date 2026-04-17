# RUN_ME — Finishing the Scrape + Dashboard Refresh

**Status as of this session (Apr 17, 2026):**

| Retailer | Source | Already scraped | Missing |
|---|---|---|---|
| Target | 981 | 981 | 0 ✓ |
| Walmart | 312 | 312 | 0 ✓ |
| Amazon | 18 | 18 | 0 ✓ |
| AE | 171 | 171 | 0 ✓ |
| Old Navy | 70 | 70 | 0 ✓ |
| **Macy's** | 191 | 188 (1.78 colors/product) | 3 products + full colorway backfill |
| **Kohl's** | 150 | 125 (3.96 colors/product) | **25 products** |
| **Levi's** | 189 / 636 colorway URLs | 50 / 121 colorways | **515 colorway URLs** |

Cowork's sandbox can't reach kohls.com / macys.com / levi.com, and it has no Playwright. **Run the Python scrapers on your Mac** — they work there (Playwright is set up, `.levis_browser_profile` is already primed).

---

## Part A — Run the scrapers on your Mac

Working directory: `/Users/dlimberopoulos/Documents/Womens jeans scraper` (adjust as needed).

### A0. One-time setup (only if needed)
```bash
pip3 install playwright pandas openpyxl
python3 -m playwright install chromium
```

### A1. Kohl's (25 products, ~5–15 min) — start here
```bash
mv kohls.csv kohls_full.csv
cp kohls_missing.csv kohls.csv
python3 kohls_pdp_scraper.py       # writes kohls_pdp_results.xlsx
mv kohls.csv kohls_missing_ran.csv
mv kohls_full.csv kohls.csv
```

### A2. Macy's (191 products, ~30–90 min) — backfills every colorway
```bash
rm -f macys_pdp_progress.json macys_pdp_results.xlsx
python3 macys_pdp_scraper.py       # writes macys_pdp_results.xlsx
```
If the internal 1-hour queue timeout hits, just re-run — it resumes from the progress file.

### A3. Levi's (515 colorway URLs, ~2–6 hours) — the big one
```bash
mv levis_all_color_urls.json levis_all_color_urls_full.json
cp levis_urls_to_scrape.json levis_all_color_urls.json
rm -f levis_progress.json
python3 levis_scraper.py           # writes levis_pdp_results.xlsx

# When done:
mv levis_all_color_urls.json levis_urls_to_scrape_ran.json
mv levis_all_color_urls_full.json levis_all_color_urls.json
```
Run this in its own terminal. If Levi's 403-blocks you heavily, take a 30-min break then re-run (progress is saved every 10 URLs).

---

## Part B — After scrapes complete, come back to Cowork

Tell me **"scrapes done"** (or run the steps yourself). Everything below runs inside the Cowork sandbox — no network needed.

### B1. Merge new scrape output into the v2 CSVs
```bash
python3 merge_new_scrape.py        # merges all three if their xlsx exist
```
This reads `macys_pdp_results.xlsx` / `kohls_pdp_results.xlsx` / `levis_pdp_results.xlsx`, dedupes against the existing v2 CSVs, and appends new rows. Backups (`*.bak_<timestamp>`) are saved first.

### B2. Rebuild the unified all-retailers CSV
```bash
python3 unify_retailers.py         # writes unified_retailer_data.csv
```
Single CSV with 20 columns across all 8 retailers: identity, pricing, and construction fields (rise, leg shape, fit, inseam, fabric, % cotton, % natural fiber, stretch, closure). This is what the Target owned-brand team can work with.

### B3. Rebuild the cross-retailer dashboard
```bash
python3 rebuild_dashboard.py       # writes cross_retailer_dashboard_v2.html
```
Wraps the existing `update_dashboard.py` and patches its hardcoded path so it works from any machine. The output dashboard HTML now reflects the fresh v2 CSVs.

---

## Files I prepared in this session

**Scraper inputs (for Part A)**
- `kohls_missing.csv` — 25 missing Kohl's URLs in the CSV format the scraper expects
- `levis_urls_to_scrape.json` — 515 missing Levi's colorway URLs
- `macys_urls_to_scrape.json` — reference list of all 191 Macy's product URLs (scraper uses `macys.csv` directly)
- `kohls_urls_to_scrape.json` — reference JSON of the same 25 Kohl's URLs

**Post-scrape scripts (for Part B)**
- `merge_new_scrape.py` — merges new `*_pdp_results.xlsx` into `*_pdp_results_v2.csv`
- `unify_retailers.py` — builds `unified_retailer_data.csv` (all 8 retailers, 20 columns)
- `rebuild_dashboard.py` — portable wrapper around `update_dashboard.py` that rebuilds `cross_retailer_dashboard_v2.html`

**Outputs already generated this session (without running any scrapes)**
- `unified_retailer_data.csv` — 4,988 rows across all 8 retailers, current data
- `cross_retailer_dashboard_v2.html` — 1.26 MB, 4,567 entries, 9 retailer groups

After you finish Part A + B, those two files will be refreshed with the new Macy's / Kohl's / Levi's rows.

---

## If things go wrong

**Scraper**
- Akamai 403 on Macy's or Levi's → wait 20–30 min, re-run. Progress is saved.
- Playwright browser keeps crashing → the scraper auto-restarts up to 20× before giving up. Re-running resumes.
- `.levis_browser_profile` is too big or corrupt → safe to delete; you'll lose cached cookies but scraping still works.

**Merge / dashboard**
- `ModuleNotFoundError: openpyxl` in Cowork → `pip3 install openpyxl --break-system-packages`
- Dashboard missing a retailer group → check that the corresponding `*_pdp_results_v2.csv` or `*_pdp_results.xlsx` exists and has rows.

---

## Current dashboard composition (for reference)

Entries per retailer group in `cross_retailer_dashboard_v2.html` as of this session:

| Group | Rows |
|---|---|
| Target OB + Target NB | ~2,673 (Target only) |
| Macys OB | 335 |
| Kohls OB | 495 |
| Levis | 121 |
| Walmart OB | (injected from existing data.json / dashboard base) |
| Amazon OB, AE, Old Navy | (likewise) |
| **Grand total** | **4,567** |

After Part A + B: Macy's climbs toward ~500–800, Kohl's climbs to ~600+, Levi's climbs to ~500–600. Total will land around **5,500–6,000 rows**.
