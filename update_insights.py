#!/usr/bin/env python3
"""
update_insights.py — Rewrite the Key Insights section of index.html.

Changes:
  - Drop old Insight 4 (Curvy Fit)
  - Number fixes in #2 (Ava & Viv median), #5 (Wide Leg %s), #7 (Kohl's
    Low Rise %), #9 (wash mix %s)
  - Full rewrite of #8 (now: OB overlap with NB AND 3P)
  - Add 4 new insights:
      - Plus-size customer served by 3P, not OB
      - Target NB curated vs Walmart NB broad
      - Levi's positioning across retailers
      - 3P teaches Target customer to expect 30%+ off
  - Renumber to 01..13

Patches index.html in place with a timestamped backup. Idempotent — replaces
the entire grid content, so re-running just refreshes from the latest text.
"""

import os
import re
import shutil
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML = os.path.join(HERE, "index.html")

GRID_OPEN_MARKER = '<div style="display:grid;grid-template-columns:1fr;gap:20px;max-width:960px">'

# ── Insight cards (number, title, body HTML) ──────────────────────────────

INSIGHTS = [
    (
        "01",
        "Smallest OB Assortment by a Wide Margin",
        "Target OB fields just <strong>43 styles / 95 CCs</strong> across 3 brands — "
        "the smallest owned-brand jeans assortment of any retailer in this study. "
        "Walmart OB offers <strong>114 styles / 406 CCs</strong> across 4 brands "
        "(4.3× the CCs), Kohl's OB runs <strong>520 CCs across 7 brands</strong>, "
        "and Macy's OB delivers <strong>335 CCs across 5 brands</strong>. Even "
        "Amazon's nascent OB program (2 brands) has <strong>78 CCs</strong>. "
        "Target OB's depth per style is also thin at <strong>2.2 colors/style</strong> "
        "vs Walmart OB at <strong>3.6</strong>, Old Navy at <strong>4.2</strong>, "
        "and Amazon OB at <strong>4.6</strong>."
    ),
    (
        "02",
        "Three Brands, One Price Cluster",
        "<strong>58% of Target OB CCs</strong> sit in a single $28–$35 band at "
        "current price. Universal Thread (median <strong>$28</strong>), Wild Fable "
        "(median <strong>$32</strong>), and Ava &amp; Viv (median <strong>$33</strong>) "
        "stack on top of each other with no breathing room. The result: three brands "
        "that compete for the same basket instead of laddering the customer up or down."
    ),
    (
        "03",
        "OB Price Architecture vs Competitors",
        "<strong>At original price</strong>, Target OB's IQR is <strong>$28–$36</strong> "
        "(median $32). This already sits below Kohl's OB at <strong>$35–$45</strong> "
        "(med $40) and far below Macy's OB at <strong>$50–$70</strong> (med $60), "
        "while pricing above Walmart OB at <strong>$17–$25</strong> (med $20).<br><br>"
        "<strong>At current price</strong>, promotional activity reshuffles the "
        "landscape. Macy's OB collapses from a $60 median to <strong>$36</strong> "
        "(90% of CCs on sale, avg 40% off), dropping into direct overlap with Target "
        "OB's <strong>$28–$35</strong> band. Kohl's OB compresses to "
        "<strong>$30–$40</strong> (71% on sale). Target OB's planned mid-price "
        "positioning erodes as competitors discount into the same band."
    ),
    (
        "04",
        "Wide Leg Under-Indexed",
        "Wide Leg is the dominant silhouette trend across OB programs — it accounts "
        "for <strong>26% at Walmart OB</strong>, <strong>27% at Macy's OB</strong>, "
        "<strong>21% at Old Navy</strong>, and <strong>18% at AE</strong>. Target "
        "OB sits at just <strong>12%</strong>, roughly half the competitor average. "
        "Combined with Barrel (10.5%) and Baggy (9.5%), Target OB's relaxed-silhouette "
        "total is <strong>32%</strong>, but the wide-leg gap specifically signals a "
        "missed opportunity in the fastest-growing sub-silhouette."
    ),
    (
        "05",
        "Promo Strategy Creates Asymmetric Exposure",
        "Target OB runs <strong>20% of CCs on sale</strong> with a steep "
        "<strong>43% avg discount</strong> when it does. Compare this to the two "
        "dominant strategies competitors use: <em>low promo frequency</em> (Walmart "
        "OB 14% on sale, Old Navy 4%) or <em>high frequency / shallow discount</em> "
        "(Kohl's OB 71% on sale at just 18% off). Target OB's approach — infrequent "
        "but deep — risks training the customer to wait for markdowns rather than "
        "building consistent value perception."
    ),
    (
        "06",
        "Over-Exposed on Low Rise",
        "<strong>23%</strong> of Target OB CCs are Low Rise (driven entirely by Wild "
        "Fable), vs <strong>1.5%</strong> at Walmart OB, <strong>0%</strong> at "
        "Macy's OB, and <strong>11%</strong> at Kohl's OB. Only American Eagle "
        "indexes higher at <strong>26%</strong>. This heavy low-rise bet concentrates "
        "risk: if the trend cycle shifts, nearly a quarter of the OB assortment "
        "becomes difficult to move."
    ),
    (
        "07",
        "OB Price Overlap with Both NB and 3P",
        "Target OB's $28–$36 IQR sits well below the original-price IQRs of both "
        "national-brand bands at Target — Target NB (1P: Levi's + KBB) at "
        "<strong>$60–$75</strong> and Target 3P (marketplace) at "
        "<strong>$50–$75</strong>. But the two NB streams behave very differently "
        "at current price.<br><br>"
        "<strong>Target NB (1P) holds price firmly</strong> — only <strong>13% on "
        "sale</strong>, current median <strong>$65</strong>. The price gap to OB "
        "stays a clean ~$30, preserving the premium read on Levi's at Target.<br><br>"
        "<strong>Target 3P collapses into the OB band</strong> — <strong>87% on "
        "sale at avg 34% off</strong>, current IQR <strong>$35–$50</strong>. "
        "<strong>30% of 3P (747 CCs) now sits at or below Target OB's Q3 ($35)</strong>, "
        "directly overlapping the OB price band. Customers see KanCan, Coolmee, and "
        "WallFlower at near-OB price points — eroding the OB value proposition while "
        "Target NB stays clean."
    ),
    (
        "08",
        "Wash Mix Assessment",
        "Target OB indexes at <strong>23% Light Wash</strong> — the highest OB share "
        "among competitors (Walmart OB 18%, Kohl's OB 18%, Macy's OB 8%). Meanwhile, "
        "Target OB under-indexes on <strong>Dark Wash at 16%</strong> vs Walmart OB "
        "at <strong>20%</strong>, Kohl's OB at <strong>28%</strong>, and Macy's OB "
        "at <strong>17%</strong>. Rebalancing toward dark and medium washes could "
        "improve cross-shopping appeal and year-round sell-through."
    ),
    (
        "09",
        "Ava &amp; Viv Lacks Scale",
        "Ava &amp; Viv carries just <strong>14 CCs (10 styles)</strong> with only "
        "<strong>1.4 colors/style</strong> — the thinnest offering of any OB brand "
        "across all retailers studied. By comparison, Walmart's plus-focused Terra "
        "&amp; Sky runs <strong>93 CCs</strong>, and Kohl's SO alone carries "
        "<strong>166 CCs</strong>. The limited depth makes it difficult for Ava "
        "&amp; Viv to present a credible brand destination on the floor or online."
    ),
    # ── New insights ──
    (
        "10",
        "Plus-Size Customer Is Being Served by 3P, Not OB",
        "<strong>50% of Target 3P (1,261 of 2,511 CCs) is plus-size brands</strong> "
        "— Woman Within (729), Roaman's (243), Jessica London (106), AVENUE (67), "
        "Catherines (66). Ava &amp; Viv has just <strong>14 CCs</strong> to compete "
        "with this assortment. Walmart's Terra &amp; Sky carries <strong>93 OB plus "
        "CCs</strong> backed by another <strong>~600 NB plus CCs</strong> (Just My "
        "Size 214, Catherines, Roaman's, Woman Within). At Target, the plus customer "
        "is being routed to 3P margin instead of OB margin — the single largest "
        "white space for the OB team to address."
    ),
    (
        "11",
        "Target's National-Brand Shelf Is Hyper-Curated; Walmart's Is 40× Broader",
        "<strong>Target NB: 67 CCs across 2 brands</strong> (Levi's + KBB by "
        "Kahlana). <strong>Walmart NB: 2,666 CCs across 36 brands</strong> — Lee "
        "(732), Sofia Vergara (274), Just My Size (214), Levi Strauss Signature "
        "(177), Gloria Vanderbilt (162), Levi's (161), Wrangler (111), Jessica "
        "Simpson (126), Rock &amp; Republic (144). Target's strategy keeps the "
        "1P NB shelf premium and narrow, leaving breadth to 3P. The trade-off: "
        "3P fills the brand-name gap at the cost of brand-trust signal that a "
        "curated NB shelf carries."
    ),
    (
        "12",
        "Levi's Plays Three Different Roles by Retailer",
        "Same brand, three distinct postures:<br>"
        "&bull; <strong>Target NB:</strong> 59 CCs, current median <strong>$70</strong> "
        "(IQR $60–$75), only <strong>15% on sale</strong> — premium denim destination<br>"
        "&bull; <strong>Walmart NB:</strong> 161 CCs, current median <strong>$40</strong> "
        "(IQR $32–$43), <strong>14% on sale</strong> — value Levi's<br>"
        "&bull; <strong>Levi's standalone:</strong> 636 CCs, current median <strong>$75</strong> "
        "(IQR $53–$110), <strong>54% on sale</strong> — full breadth + heavy promo<br><br>"
        "For OB pricing strategy this matters: Target's $70 Levi's sets the practical "
        "ceiling that Target OB can ladder up against. Walmart's $40 Levi's sets the "
        "competitive floor for any OB program trying to claim heritage credibility."
    ),
    (
        "13",
        "3P Trains the Target Customer to Expect 30%+ Off",
        "<strong>87% of Target 3P CCs are on sale at avg 34% off</strong>. By "
        "contrast, Target OB shows up at only <strong>20% on sale</strong>, and "
        "Target NB (1P) at just <strong>13%</strong>. When customers shop Target's "
        "broader denim assortment online, the dominant visual signal is markdown "
        "tags — and OB without them can feel premium-priced even though OB's "
        "current-price IQR ($28–$35) is below 3P's ($35–$50). The OB team should "
        "weigh how visible this contrast is on PLP/PDP and whether OB needs more "
        "value-tier signaling to avoid being read as 'the expensive option' inside "
        "the Target denim landscape."
    ),
]


def card_html(num, title, body):
    """Render one insight card matching the existing visual style."""
    return (
        '<div style="background:var(--bg);border:1px solid var(--bg3);'
        'border-radius:var(--radius);padding:24px 28px;transition:box-shadow .2s">\n'
        '  <div style="display:flex;align-items:baseline;gap:14px;margin-bottom:10px">\n'
        '    <span style="font-size:.7rem;font-weight:700;color:#002855;'
        'letter-spacing:.1em;padding:3px 10px;border:1.5px solid #002855;'
        f'border-radius:18px">{num}</span>\n'
        '    <div style="font-size:1rem;font-weight:800;color:var(--fg);'
        f'letter-spacing:-.02em">{title}</div>\n'
        '  </div>\n'
        '  <div style="font-size:.85rem;line-height:1.65;color:var(--fg2)">'
        f'{body}</div>\n'
        '</div>'
    )


def build_grid_inner():
    """Render the inner grid HTML (cards joined)."""
    cards = "\n\n".join(card_html(n, t, b) for (n, t, b) in INSIGHTS)
    return cards


def patch_html(html):
    grid_start = html.find(GRID_OPEN_MARKER)
    if grid_start < 0:
        raise RuntimeError("Could not locate insights grid opening div")
    # Walk forward to find matching </div> for the grid
    i = grid_start
    depth = 0
    while i < len(html):
        if html[i:i+5] == "<div " or html[i:i+4] == "<div>":
            depth += 1
            i = html.find(">", i) + 1
        elif html[i:i+6] == "</div>":
            depth -= 1
            i += 6
            if depth == 0:
                break
        else:
            i += 1
    grid_end = i
    if depth != 0:
        raise RuntimeError("Grid div nesting did not balance")

    new_grid = (
        GRID_OPEN_MARKER + "\n\n"
        + build_grid_inner() + "\n\n"
        + "</div>"
    )
    return html[:grid_start] + new_grid + html[grid_end:], grid_end - grid_start, len(new_grid)


def main():
    dry_run = "--dry-run" in sys.argv
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()
    new_html, old_size, new_size = patch_html(html)

    print(f"Insight grid: {old_size:,} chars -> {new_size:,} chars")
    print(f"Insights rendered: {len(INSIGHTS)}")
    for n, t, _ in INSIGHTS:
        print(f"  {n}  {t}")

    if dry_run:
        print("\n[dry-run] no changes written")
        return 0

    backup = INDEX_HTML + ".bak_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(INDEX_HTML, backup)
    print(f"\nBackup written: {backup}")
    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(new_html)
    print(f"index.html updated: {len(html):,} -> {len(new_html):,} chars")
    return 0


if __name__ == "__main__":
    sys.exit(main())
