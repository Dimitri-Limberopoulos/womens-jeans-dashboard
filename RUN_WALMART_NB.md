# Walmart NB — run instructions

## What changed
- `walmart_pdp_scraper.py` now reads `WALMART_INPUT` and `WALMART_OUTPUT_PREFIX`
  env vars. Default behavior (no env vars) is unchanged: reads `walmart.csv`,
  writes `walmart_pdp_results.xlsx` + `walmart_pdp_progress.json`.
- The CSV reader picks up URLs from the primary `w-100 href` column **and**
  the variant columns `z-2 href`, `z-2 href 2`, `z-2 href 3`. Dedupes on the
  canonical `/ip/<slug>/<id>` path before queueing.
- New `walmart_nb_urls.json` — 562 unique walmart.com/ip URLs distilled from
  `walmart(NEW).csv` (252 primary + 343 variant URLs after dedupe).
- New `add_walmart_nb.py` — post-scrape integrator. Reads the new XLSX +
  the held-back NB rows from the existing `walmart_pdp_results.xlsx`,
  dedupes by `(url, color)`, maps to RAW schema, and patches `index.html`.

## Step 1 — Run the scraper

From the workspace folder:

```bash
cd "/Users/dlimberopoulos/Documents/Womens jeans scraper"

WALMART_INPUT=walmart_nb_urls.json \
WALMART_OUTPUT_PREFIX=walmart_nb_pdp \
python3 walmart_pdp_scraper.py
```

Outputs:
- `walmart_nb_pdp_results.xlsx`
- `walmart_nb_pdp_progress.json`

It does **not** touch the original `walmart_pdp_results.xlsx` (legacy held-back
data). The legacy file stays put — `add_walmart_nb.py` will pull NB rows from
it and merge.

Ballpark: 562 URLs, ~30–45 min on a Mac. Crash-safe + resumable like the
other scrapers (re-run with the same env vars to pick up where you left off).

## Step 2 — Inject Walmart NB into the dashboard

After the scrape finishes:

```bash
cd "/Users/dlimberopoulos/Documents/Womens jeans scraper"
python3 add_walmart_nb.py --dry-run    # see what'll change
python3 add_walmart_nb.py              # do the injection
```

It will:
1. Add `'Walmart NB'` to `GROUPS`
2. Add `'Walmart NB':'Walmart National Brands'` to `GROUP_LABELS`
3. Add Walmart-yellow color (`#FFC220`) to `GC` so the new group has its own
   filter-pill / chart color
4. Insert a "Walmart NB" KPI tile right after Walmart OB
5. Append all transformed rows to the `var RAW = [...]` array
6. Save `index.html` with a timestamped `.bak_…` backup alongside it

Re-running is safe — it detects prior injection and refuses unless you pass
`--force` (which rebuilds the Walmart NB block fresh).

## What rolls into "Walmart NB"

From the **legacy** `walmart_pdp_results.xlsx` (already on disk, never made
it to the dashboard), everything except Walmart-owned brands:

| Brand                        | Rows |
|------------------------------|-----:|
| Levi Strauss Signature       |  179 |
| Levi's                       |  162 |
| Sofia Vergara                |   77 |
| Tinseltown                   |    1 |
| **Subtotal (legacy NB)**     |  **419** |

OB exclusions (kept in `Walmart OB` group, untouched): Time and Tru,
No Boundaries, Terra & Sky, Free Assembly.

From the **new scrape** of `walmart(NEW).csv` (562 URLs queued), brand mix
based on the seed CSV:

| Brand                         | Rows in seed |
|-------------------------------|-------------:|
| Lee                           |          82  |
| Sofia Vergara                 |          24  |
| Scoop                         |          23  |
| Gloria Vanderbilt             |          15  |
| Wrangler                      |          14  |
| Rock & Republic               |          13  |
| DARING DIVA                   |          12  |
| Jessica Simpson               |          10  |
| Celebrity Pink                |           9  |
| Madden NYC                    |           8  |
| White Mark                    |           7  |
| (others)                      |          35  |

Sofia Vergara overlaps with the legacy 77 rows — `add_walmart_nb.py` dedupes
by `(canonical url, color)`, so duplicates collapse.

## Sanity check (already validated dry-run, no scrape data yet)

Running `add_walmart_nb.py --dry-run` against just the legacy XLSX produces:

```
Legacy walmart_pdp_results.xlsx       :   825 rows
New    walmart_nb_pdp_results.xlsx    :     0 rows  (not yet scraped)
Legacy NB-eligible (not Walmart OB)   :   419 rows
After dedupe by (url, color)          :   416 entries
RAW: 5107 -> 5523 entries (+416)
KPI tile inserted after Walmart OB (count = 416)
```

Once the new scrape lands, the post-scrape totals will be ~416 + ~600–700 net
new color-variants → ballpark **~1,000–1,100 rows in Walmart NB**, putting it
between Macy's OB and Kohl's OB in size and giving you a full national-brand
view at Walmart for direct comparison vs Target NB.

## Known minor issue (pre-existing)

The wash classifier in `update_dashboard.py` mis-classifies "Black Rinse" /
"Indigo Rinse" as Light Wash for a small number of Sofia Vergara and
Tinseltown rows. This is upstream of the Walmart-NB integration (same
classifier all retailers use) and affects ~4% of rows. Worth fixing in a
later pass — not blocking this rollout.
