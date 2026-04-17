# Target Women's Jeans Dashboard

An interactive single-file HTML dashboard analyzing 2,673 women's jeans from Target.com across 58 brands.

## Quick Start

Simply open `jeans_dashboard.html` in any modern web browser. No installation or dependencies required.

## Features

### Brand Filtering
- Toggle any of 58 brands on/off with interactive pills
- Owned brands (red) separated from National brands (navy)
- Real-time chart updates as you filter

### Price Analysis (Box & Whisker Plot)
- Compare price distributions across all brands
- Toggle between **Current Price** and **Original Price**
- See median prices with optional labels
- OB TOTAL and NB TOTAL show aggregate Owned/National performance

### Attribute Comparisons (7 Charts)
Compare how Owned Brands vs National Brands differ across:
1. **Rise** - High Rise, Standard Rise, Low Rise, Ultra-High Rise
2. **Leg Shape** - Straight, Wide, Skinny, Slim, Flare, Bootcut, etc.
3. **Garment Length** - Ankle, Capri, Petite, etc.
4. **Fabric Weight** - Lightweight, Medium, Heavyweight
5. **Wash Category** - Black, Dark Wash, Medium Wash, Light Wash, White, Color
6. **Inseam** - Distribution histogram (inches)
7. **Cotton Content** - Distribution histogram (percentage)

All charts show percentages of assortment for each group.

## Data Overview

**Source:** target_pdp_results.xlsx (2,673 rows)

**Brands:**
- 2 Owned Brands: Universal Thread (44 items), Wild Fable (37 items)
- 56 National Brands: Woman Within (729 items), Roaman's (243 items), and 54 others

**Price Range:** $19.99 - $150+

**Color Distribution:**
- Color (non-denim): 1,025 items
- Dark Wash: 472 items
- Black: 333 items
- Light Wash: 266 items
- Medium Wash: 270 items
- White: 154 items
- Other: 153 items

## Data Cleaning Applied

✓ **Prices** - Parsed numeric values from "$XX.XX" format
✓ **Fit** - Split into leg_shape (Straight, Wide, Skinny, etc.) and fit_style (Regular, Slim, Stretch)
✓ **Rise** - Standardized to Standard Rise, High Rise, Low Rise, Ultra-High Rise
✓ **Colors** - Mapped 500+ values into 6 wash categories
✓ **Inseam** - Parsed numeric inches values
✓ **Cotton %** - Extracted from % Cotton field or Material description

## Technical Details

**Single-file delivery:**
- No external dependencies (fully offline-capable)
- ~1.33 MB total (includes all data and application logic)
- Responsive design (desktop, tablet, mobile)

**Technologies:**
- Chart.js 4.4.4 for interactive charts
- chartjs-plugin-datalabels 2.2.0 for chart labels
- Montserrat font for typography
- Pure ES5 JavaScript (works everywhere)
- Fully inline CSS

**Browser Support:**
- Chrome/Edge 60+
- Firefox 55+
- Safari 12+
- Mobile browsers (iOS Safari, Chrome Mobile)

## Interactions

| Control | Action |
|---------|--------|
| Brand Pills | Click to toggle brand on/off |
| Current Price | Show box plot for current prices |
| Original Price | Show box plot for original/list prices |
| Toggle Labels | Show/hide median price labels on box plot |
| Show Labels Checkbox | Same as toggle labels (alternative control) |

All charts update instantly as you change filters.

## File Structure

```
jeans_dashboard.html    (1.33 MB) - Final dashboard [USE THIS FILE]
data.json               (1.8 MB)  - Cleaned data (embedded in HTML)
app.js                  (16 KB)   - Dashboard logic (embedded in HTML)
build.py                (11 KB)   - Build script (for reference)
prep_data.py            (12 KB)   - Data cleaning script (for reference)
BUILD_SUMMARY.txt       (6.7 KB)  - Detailed build report
README.md               (this file)
```

## Building the Dashboard

To rebuild from source:

```bash
python3 prep_data.py   # Cleans Excel → data.json
python3 build.py       # Combines data.json + app.js → jeans_dashboard.html
```

## Color Scheme

- **Owned Brands:** Target Red #CC0000
- **National Brands:** Navy #002855
- **OB TOTAL / NB TOTAL:** Thicker borders, higher opacity for emphasis

## Performance

- Fast initial load: ~200-500ms depending on browser
- Smooth filtering: Real-time chart updates within 100ms
- No lag during interaction
- Optimized for 1400px+ displays (responsive on smaller screens)

## Notes

- Rise categories were standardized (Classic + Regular → Standard Rise)
- Wash categories map 500+ color values to 6 buckets
- Owned brands are Universal Thread and Wild Fable only (as marked in data)
- "Other" rise category includes rare/unclassified values
- Inseam/Cotton distributions use automatic histogram binning

---

Built: April 15, 2026
Data Source: Target.com Product Data Pages (PDPs)
