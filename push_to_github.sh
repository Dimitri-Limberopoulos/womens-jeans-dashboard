#!/usr/bin/env bash
# One-shot script to push current state to origin/main.
# Adds a .gitignore, untracks junk that was accidentally tracked,
# stages all meaningful changes, commits, and pushes.
#
# Safe to re-run: skips already-staged operations, re-stages anything
# that's still modified, and only commits if there's something to commit.

set -euo pipefail

cd "$(dirname "$0")"

# Clean up any stale git lock from prior sandbox attempts
rm -f .git/index.lock || true

echo "==> Untracking junk files (keeping them on disk)..."
git rm --cached -q -f .DS_Store 2>/dev/null || true
git rm --cached -q -f '~$walmart_pdp_results.xlsx' 2>/dev/null || true
git rm --cached -rq -f __pycache__/ 2>/dev/null || true

echo "==> Staging meaningful changes..."
# Core dashboard files
[ -f .gitignore ] && git add .gitignore
[ -f README.md ] && git add README.md
[ -f index.html ] && git add index.html
[ -f walmart_debug_json.json ] && git add walmart_debug_json.json

# Scrapers + scrape inputs / outputs
[ -f walmart_pdp_scraper.py ] && git add walmart_pdp_scraper.py
[ -f walmart_nb_urls.json ] && git add walmart_nb_urls.json
[ -f 'walmart(NEW).csv' ] && git add 'walmart(NEW).csv'
[ -f 'Sold by Target.csv' ] && git add 'Sold by Target.csv'
[ -f walmart_nb_pdp_progress.json ] && git add walmart_nb_pdp_progress.json
[ -f walmart_nb_pdp_results.xlsx ] && git add walmart_nb_pdp_results.xlsx

# Dashboard build / patch scripts
[ -f add_walmart_nb.py ] && git add add_walmart_nb.py
[ -f split_target_nb.py ] && git add split_target_nb.py
[ -f patch_group_dropdowns.py ] && git add patch_group_dropdowns.py
[ -f rename_price_terminology.py ] && git add rename_price_terminology.py
[ -f rebuild_insights_page.py ] && git add rebuild_insights_page.py
[ -f update_insights.py ] && git add update_insights.py
[ -f update_coverage_page.py ] && git add update_coverage_page.py
[ -f add_chart_export.py ] && git add add_chart_export.py
[ -f add_legend_rebalance.py ] && git add add_legend_rebalance.py
[ -f add_access_gate.py ] && git add add_access_gate.py

# Slide deck builder + outputs + style assets
[ -f build_deck.py ] && git add build_deck.py
[ -f 'Target OB - Cross-Retailer Jeans Insights.pptx' ] && git add 'Target OB - Cross-Retailer Jeans Insights.pptx'
[ -d bharat-slides ] && git add bharat-slides/
[ -f levis_posture_boxplot.svg ] && git add levis_posture_boxplot.svg

# Docs / runbooks
[ -f RUN_WALMART_NB.md ] && git add RUN_WALMART_NB.md
[ -f GATE_SETUP.md ] && git add GATE_SETUP.md
[ -f pipeline-guide.html ] && git add pipeline-guide.html
[ -f create-assortment-dashboard.skill ] && git add create-assortment-dashboard.skill
[ -f pdp-scraper.skill ] && git add pdp-scraper.skill

# Cross-retailer xlsx deliverable
[ -f Cross_Retailer_Jeans_Data.xlsx ] && git add Cross_Retailer_Jeans_Data.xlsx

# The push script itself (so the repo records how it was deployed)
[ -f push_to_github.sh ] && git add push_to_github.sh

echo
echo "==> Pending commit:"
git status --short
echo

# Bail cleanly if nothing's staged
if git diff --cached --quiet; then
  echo "==> Nothing staged — nothing to commit. Done."
  exit 0
fi

# Write commit message to a temp file so quoting issues don't trip up bash 3.2
COMMIT_MSG_FILE=$(mktemp -t commit_msg.XXXXXX)
trap 'rm -f "$COMMIT_MSG_FILE"' EXIT

cat > "$COMMIT_MSG_FILE" <<'COMMIT_MSG_EOF'
Sync dashboard, build scripts, deck, and access gate

Dashboard (index.html):
- 6-insight Key Findings page rewritten with softer client-facing tone
- Target NB / 3P / Walmart NB columns added across coverage table,
  side-by-side dropdowns, and group filters
- Insight detail charts use shared dashboard helpers (drawHBoxPlot,
  GC, GROUP_LABELS) and rebalance to 100% on legend toggle
- Wash chart legend syncs to the sunburst grid
- Target OB rows highlighted red+bold across all stacked bars
- Pricing terminology renamed: "Original" -> "List", "Current" ->
  "Market Observed"
- Per-chart CSV export buttons
- Access gate (email domain + password) injected at body open;
  see GATE_SETUP.md for Apps Script logging setup

Build / patch scripts (additive):
- rebuild_insights_page.py, update_coverage_page.py,
  patch_group_dropdowns.py, rename_price_terminology.py,
  add_chart_export.py, add_legend_rebalance.py, add_access_gate.py,
  add_walmart_nb.py, split_target_nb.py

Slide deck:
- build_deck.py + Target OB - Cross-Retailer Jeans Insights.pptx
- bharat-slides/ skill
- levis_posture_boxplot.svg

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
COMMIT_MSG_EOF

git commit -F "$COMMIT_MSG_FILE"

echo
echo "==> Pushing to origin/main..."
git push origin main

echo
echo "==> Done. Latest commit:"
git log --oneline -1
