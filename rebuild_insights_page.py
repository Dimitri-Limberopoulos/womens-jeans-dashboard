#!/usr/bin/env python3
"""
rebuild_insights_page.py — Replace the existing Key Insights page on
index.html with the 6-insight, 2-column layout matching the deck PDF.
Each insight has a "View details" button that expands one or more
in-line charts in the dashboard's existing visual style.

Chart design notes (per latest spec):
  01 — TWO charts: # of styles by retailer + CCs per style
  02 — THREE charts: Wide Leg %, Slim/Contemporary %, Low Rise % each on
       its own bar chart across the OB peer set
  03 — wash-mix 100% stacked bar, with rows rescaled to 100% AFTER
       excluding 'Unclassified'
  04 — Target OB price by brand box plot, with a shaded $28-$35 band
       and a callout showing % of OB CCs that fall in that band
  05 — min-max range bars per retailer group, List Price vs Market
       Observed Price (no boxes/medians/whiskers — just ranges)
  06 — Levi's price posture box plot across the four channels
       (Target NB, Target OB, Walmart NB, Levi's standalone) plus
       Walmart's licensed Levi Strauss Signature

All charts use the dashboard's existing helpers (byGroup, bS,
drawHBoxPlot, GC, GROUP_LABELS, fmt$, destroyChart, charts{}) and
Montserrat typography.

Idempotent — replaces the entire page-insights div in place.
"""

import os
import re
import shutil
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML = os.path.join(HERE, "index.html")

# ── New page content (HTML) ────────────────────────────────────────────────

NEW_PAGE_HTML = '''<div id="page-insights" style="display: block;">
<div class="hero">
  <span class="label">Strategic Analysis</span>
  <h1>Key Findings</h1>
  <p>Six findings across two themes: assortment structure and pricing &amp; promotional posture. Click "View details" under any insight to expand the supporting chart inline.</p>
  <p style="font-size:.78rem;color:var(--fg3);margin-top:8px"><strong>Data scraped:</strong> Target OB / NB / 3P (Apr 15-17, 2026) · Amazon OB, AE, Old Navy, Walmart OB, Macy's OB (Apr 16, 2026) · Kohl's OB, Levi's (Apr 17, 2026) · Walmart NB (Apr 28, 2026). 7,773 colorways across 11 retailer groups.</p>
</div>

<div class="section">
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:32px;max-width:1400px">

    <!-- COLUMN A — ASSORTMENT -->
    <div>
      <div class="ki-col-header">
        <span class="ki-col-letter">A.</span>
        <span class="ki-col-title">ASSORTMENT</span>
      </div>

      <div class="ki-card" data-insight="01">
        <div class="ki-row"><span class="ki-num">01</span>
          <div class="ki-title">Limited assortment</div></div>
        <div class="ki-body">
          Target OB has the <strong>smallest owned-brand jeans assortment</strong> across physical retailers analyzed — 95 CCs across 3 brands, vs. Walmart 406 (4.3×), Kohl's 520, Macy's 335. Depth is also thin: <strong>2.2 colors per style</strong> vs. Walmart's 3.6. Limiting shopper choice and basket potential.
        </div>
        <button class="ki-toggle" onclick="toggleInsight('01')" id="ki-toggle-01">View details ▾</button>
        <div class="ki-detail" id="ki-detail-01"></div>
      </div>

      <div class="ki-card" data-insight="02">
        <div class="ki-row"><span class="ki-num">02</span>
          <div class="ki-title">Mix may not align with trend cycle</div></div>
        <div class="ki-body">
          Target <strong>under-indexes in rapidly growing segments</strong> like Wide Leg (12% vs ~20% OB peer avg weighted by CCs) and Slim (7% vs ~20%), and <strong>over-indexes in Low Rise</strong> (23% vs ~5%). Concentrates trend-cycle risk and downstream markdown exposure.
        </div>
        <button class="ki-toggle" onclick="toggleInsight('02')" id="ki-toggle-02">View details ▾</button>
        <div class="ki-detail" id="ki-detail-02"></div>
      </div>

      <div class="ki-card" data-insight="03">
        <div class="ki-row"><span class="ki-num">03</span>
          <div class="ki-title">Wash mix may not align with how the category shops</div></div>
        <div class="ki-body">
          <strong>Light Wash 23%</strong> — the highest of any OB — and <strong>Dark Wash only 16%</strong>, below Walmart's 20% and Kohl's 28%. Pattern is <strong>inverted vs. how the category shops</strong>. Rebalancing toward dark and medium could lift year-round sell-through and cross-basket pull.
        </div>
        <button class="ki-toggle" onclick="toggleInsight('03')" id="ki-toggle-03">View details ▾</button>
        <div class="ki-detail" id="ki-detail-03"></div>
      </div>
    </div>

    <!-- COLUMN B — PRICING & PROMO -->
    <div>
      <div class="ki-col-header">
        <span class="ki-col-letter">B.</span>
        <span class="ki-col-title">PRICING &amp; PROMO</span>
      </div>

      <div class="ki-card" data-insight="04">
        <div class="ki-row"><span class="ki-num">04</span>
          <div class="ki-title">Brand architecture is compressed in mid-tier</div></div>
        <div class="ki-body">
          Target OB brands <strong>cluster in a narrow ~$28–$35 band (~58% of CCs)</strong>, with no clear price ladder — leaving both value and premium segments unaddressed. Universal Thread, Wild Fable, and Ava &amp; Viv stack on top of each other rather than laddering up or down.
        </div>
        <button class="ki-toggle" onclick="toggleInsight('04')" id="ki-toggle-04">View details ▾</button>
        <div class="ki-detail" id="ki-detail-04"></div>
      </div>

      <div class="ki-card" data-insight="05">
        <div class="ki-row"><span class="ki-num">05</span>
          <div class="ki-title">Nat'l brand competitors are encroaching into Target's price bands</div></div>
        <div class="ki-body">
          Assortment and style options become even more important given that <strong>many NBs compete at OB price point when on promo</strong>. At the time we scraped data (April 15-17, 2026), Macy's OB collapsed $60→$36 (90% on sale, 40% off) and AE collapsed $60→$45 (88% on sale). Only Target's own NB ($65) remains meaningfully anchored higher.
        </div>
        <button class="ki-toggle" onclick="toggleInsight('05')" id="ki-toggle-05">View details ▾</button>
        <div class="ki-detail" id="ki-detail-05"></div>
      </div>

      <div class="ki-card" data-insight="06">
        <div class="ki-row"><span class="ki-num">06</span>
          <div class="ki-title">Levi's brand undercuts Target OB at other retailers</div></div>
        <div class="ki-body">
          The same Levi's styles that Target carries in NB are available at <strong>equal or lower prices at Walmart and Levi.com</strong> — frequently landing at Target OB price points. Walmart's Levi Strauss Signature ($27) sits below Target OB's $30 median. Erodes the value rationale for choosing OB over a national brand.
        </div>
        <button class="ki-toggle" onclick="toggleInsight('06')" id="ki-toggle-06">View details ▾</button>
        <div class="ki-detail" id="ki-detail-06"></div>
      </div>
    </div>

  </div>
</div>

</div>'''


# ── CSS ────────────────────────────────────────────────────────────────────

CSS_BLOCK = '''<style id="key-insights-css">
.ki-col-header { display: flex; align-items: baseline; gap: 10px;
  border-bottom: 2px solid var(--bg3, #d1d5db); padding-bottom: 8px; margin-bottom: 18px; }
.ki-col-letter { font-size: 1.4rem; font-weight: 800; color: #CC0000; letter-spacing: -.01em; font-family: Montserrat, sans-serif; }
.ki-col-title { font-size: 1.1rem; font-weight: 800; color: var(--fg, #1a1a1a); letter-spacing: .04em; font-family: Montserrat, sans-serif; }
.ki-card { background: var(--bg); border: 1px solid var(--bg3); border-radius: var(--radius); padding: 20px 24px; margin-bottom: 16px; transition: box-shadow .2s; }
.ki-card:hover { box-shadow: 0 2px 12px rgba(0,0,0,.06); }
.ki-row { display: flex; align-items: baseline; gap: 12px; margin-bottom: 10px; }
.ki-num { font-size: .9rem; font-weight: 800; color: #CC0000; letter-spacing: .04em; font-family: Montserrat, sans-serif; min-width: 22px; }
.ki-title { font-size: 1rem; font-weight: 800; color: var(--fg); letter-spacing: -.02em; line-height: 1.3; }
.ki-body { font-size: .85rem; line-height: 1.55; color: var(--fg2); margin-bottom: 14px; }
.ki-toggle { background: transparent; border: 1.5px solid var(--bg4, #b5b5b5); color: var(--fg2, #4b5563);
  padding: 6px 14px; font-size: .72rem; font-weight: 700; letter-spacing: .06em; text-transform: uppercase;
  border-radius: 6px; cursor: pointer; font-family: Montserrat, sans-serif; transition: all .15s; }
.ki-toggle:hover { border-color: #CC0000; color: #CC0000; }
.ki-toggle.active { background: #CC0000; border-color: #CC0000; color: #fff; }
.ki-detail { display: none; margin-top: 18px; padding-top: 18px; border-top: 1px solid var(--bg3, #e5e5e5); }
.ki-detail.open { display: block; }
.ki-detail .ki-chart-wrap { position: relative; width: 100%; min-height: 320px; }
.ki-detail-title { font-size: .82rem; font-weight: 700; color: var(--fg, #1a1a1a); margin-bottom: 4px; font-family: Montserrat, sans-serif; }
.ki-detail-sub   { font-size: .72rem; color: var(--fg3, #6b7280); margin-bottom: 14px; font-family: Montserrat, sans-serif; }
.ki-detail-note  { font-size: .7rem;  color: var(--fg3, #6b7280); margin-top: 12px; font-style: italic; line-height: 1.5; }
.ki-callout { display: inline-block; background: rgba(204,0,0,0.10); color: #CC0000; padding: 6px 12px;
  border-radius: 4px; font-family: Montserrat, sans-serif; font-size: .78rem; font-weight: 700;
  letter-spacing: .03em; margin-bottom: 12px; }
.ki-grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }
.ki-grid-3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; }
.ki-stack  { display: flex; flex-direction: column; gap: 28px; }
@media (max-width: 1024px) {
  .ki-grid-2, .ki-grid-3 { grid-template-columns: 1fr; }
}
</style>'''


# ── JS — the chart renderers ──────────────────────────────────────────────

JS_BLOCK = r'''<script id="key-insights-js">
(function () {
  // Reuses the dashboard's globals/helpers:
  //   byGroup(g), bS(prices), drawHBoxPlot(), GC[g], GROUP_LABELS[g],
  //   destroyChart(id), fmt$(v), charts{}, RAW[]
  var FONT = 'Montserrat';
  var GRID_X = '#eee';

  function pctOf(rows, predicate) {
    if (!rows.length) return 0;
    var n = 0;
    for (var i = 0; i < rows.length; i++) if (predicate(rows[i])) n++;
    return 100 * n / rows.length;
  }
  function countStyles(rows) {
    var names = {};
    for (var i = 0; i < rows.length; i++) names[rows[i].n] = 1;
    return Object.keys(names).length;
  }
  // Reusable tick styling that bolds + reds the Target Owned Brands row
  function isTargetOBLabel(label) {
    if (typeof label !== 'string') return false;
    // Match "Target Owned Brands" with or without trailing " (n=..)" suffix
    return /^Target Owned Brand/.test(label);
  }
  function targetTickStyle() {
    return {
      color: function (ctx) { return isTargetOBLabel(ctx.tick && ctx.tick.label) ? '#CC0000' : '#1a1a1a'; },
      font:  function (ctx) {
        var bold = isTargetOBLabel(ctx.tick && ctx.tick.label);
        return { family: FONT, size: 11, weight: bold ? 800 : 600 };
      },
    };
  }
  // After-the-fact y-tick-styling for charts created via drawHBoxPlot
  function applyTargetTickStyle(canvasId) {
    var ch = charts[canvasId];
    if (!ch || !ch.options || !ch.options.scales || !ch.options.scales.y) return;
    var t = targetTickStyle();
    ch.options.scales.y.ticks = Object.assign(ch.options.scales.y.ticks || {}, t);
    ch.update('none');
  }

  // ── Insight 01: THREE charts stacked — styles + CCs + depth ───────────
  function renderInsight01(detail) {
    detail.innerHTML =
      '<div class="ki-stack">' +
        '<div>' +
          '<div class="ki-detail-title">Number of styles by retailer</div>' +
          '<div class="ki-detail-sub">Distinct product styles per retailer group, sorted descending.</div>' +
          '<div class="ki-chart-wrap" style="min-height:380px"><canvas id="ki-canvas-01a"></canvas></div>' +
        '</div>' +
        '<div>' +
          '<div class="ki-detail-title">Number of color combos (CCs) by retailer</div>' +
          '<div class="ki-detail-sub">Total color combinations (1 row per product × color), sorted descending.</div>' +
          '<div class="ki-chart-wrap" style="min-height:380px"><canvas id="ki-canvas-01b"></canvas></div>' +
        '</div>' +
        '<div>' +
          '<div class="ki-detail-title">Color combos per style (depth)</div>' +
          '<div class="ki-detail-sub">CCs ÷ styles. Higher = more colors offered per silhouette.</div>' +
          '<div class="ki-chart-wrap" style="min-height:380px"><canvas id="ki-canvas-01c"></canvas></div>' +
        '</div>' +
      '</div>' +
      '<div class="ki-detail-note">Source: Public retailer PDPs scraped Apr 15–28, 2026 · 7,773 colorways across 11 retailer groups.</div>';

    var groups = (window.GROUPS || []).slice();
    var styleData = groups.map(function (g) {
      var rs = byGroup(g);
      var styles = countStyles(rs);
      return { g: g, styles: styles, ccs: rs.length, depth: styles ? rs.length / styles : 0 };
    });
    var sortedByStyles = styleData.slice().sort(function (a, b) { return b.styles - a.styles; });
    var sortedByCCs    = styleData.slice().sort(function (a, b) { return b.ccs - a.ccs; });
    var sortedByDepth  = styleData.slice().sort(function (a, b) { return b.depth - a.depth; });

    function makeBar(canvasId, sorted, valKey, axisTitle, labelFmt) {
      destroyChart(canvasId);
      var ctx = document.getElementById(canvasId);
      if (!ctx) return;
      var labels  = sorted.map(function (d) { return GROUP_LABELS[d.g] || d.g; });
      var values  = sorted.map(function (d) { return Math.round(d[valKey] * 10) / 10; });
      var bgs     = sorted.map(function (d) { return GC[d.g] ? GC[d.g].light  : 'rgba(150,150,150,.3)'; });
      var borders = sorted.map(function (d) { return GC[d.g] ? GC[d.g].border : '#999'; });
      charts[canvasId] = new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: { labels: labels, datasets: [{
          label: axisTitle, data: values,
          backgroundColor: bgs, borderColor: borders, borderWidth: 1.5, borderRadius: 3,
        }] },
        options: {
          indexAxis: 'y', responsive: true, maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            datalabels: { anchor: 'end', align: 'right', offset: 4,
              font: { family: FONT, size: 10, weight: 700 }, color: '#333',
              formatter: labelFmt || function (v) { return v.toLocaleString(); } },
            tooltip: { callbacks: { label: function (c) {
              return labelFmt ? labelFmt(c.parsed.x) : c.parsed.x.toLocaleString(); } } },
          },
          scales: {
            x: { beginAtZero: true,
              title: { display: true, text: axisTitle, font: { family: FONT, size: 11, weight: 600 } },
              grid: { color: GRID_X }, ticks: { font: { family: FONT, size: 10 } } },
            y: { grid: { display: false }, ticks: targetTickStyle() },
          },
        },
        plugins: [ChartDataLabels],
      });
    }
    makeBar('ki-canvas-01a', sortedByStyles, 'styles', 'Number of styles');
    makeBar('ki-canvas-01b', sortedByCCs,    'ccs',    'Color combos (CCs)');
    makeBar('ki-canvas-01c', sortedByDepth,  'depth',  'Color combos per style',
            function (v) { return v.toFixed(1); });
  }

  // ── Insight 02: THREE charts stacked — Wide Leg, Slim, Low Rise ────────
  function renderInsight02(detail) {
    detail.innerHTML =
      '<div class="ki-stack">' +
        '<div>' +
          '<div class="ki-detail-title">Wide Leg %</div>' +
          '<div class="ki-detail-sub">Share of CCs tagged Wide Leg silhouette. Sorted least → most.</div>' +
          '<div class="ki-chart-wrap" style="min-height:440px"><canvas id="ki-canvas-02a"></canvas></div>' +
        '</div>' +
        '<div>' +
          '<div class="ki-detail-title">Slim / Contemporary %</div>' +
          '<div class="ki-detail-sub">Share of CCs in Slim/Contemporary fit. Sorted least → most.</div>' +
          '<div class="ki-chart-wrap" style="min-height:440px"><canvas id="ki-canvas-02b"></canvas></div>' +
        '</div>' +
        '<div>' +
          '<div class="ki-detail-title">Low Rise %</div>' +
          '<div class="ki-detail-sub">Share of CCs tagged Low Rise. Sorted least → most.</div>' +
          '<div class="ki-chart-wrap" style="min-height:440px"><canvas id="ki-canvas-02c"></canvas></div>' +
        '</div>' +
      '</div>' +
      '<div class="ki-detail-note">Target OB at 11.6% Wide Leg vs ~20% OB peer avg (CC-weighted). Low Rise +18pp vs peer avg, driven by Wild Fable.</div>';

    // All 11 retailer groups (the original chart set)
    var groups = (window.GROUPS || []).slice();

    function makeBar(canvasId, predicate, axisTitle) {
      destroyChart(canvasId);
      var ctx = document.getElementById(canvasId);
      if (!ctx) return;
      // Compute pct per group, then sort ascending (least → most)
      var pairs = groups.map(function (g) {
        return { g: g, v: Math.round(pctOf(byGroup(g), predicate) * 10) / 10 };
      });
      pairs.sort(function (a, b) { return a.v - b.v; });
      var labels  = pairs.map(function (p) { return GROUP_LABELS[p.g] || p.g; });
      var values  = pairs.map(function (p) { return p.v; });
      var bgs     = pairs.map(function (p) { return GC[p.g] ? GC[p.g].bg     : '#999'; });
      var borders = pairs.map(function (p) { return GC[p.g] ? GC[p.g].border : '#999'; });
      charts[canvasId] = new Chart(ctx.getContext('2d'), {
        type: 'bar',
        data: { labels: labels, datasets: [{
          label: axisTitle, data: values,
          backgroundColor: bgs, borderColor: borders, borderWidth: 1.5, borderRadius: 3,
        }] },
        options: {
          indexAxis: 'y', responsive: true, maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            datalabels: { anchor: 'end', align: 'right', offset: 4,
              font: { family: FONT, size: 10, weight: 700 }, color: '#333',
              formatter: function (v) { return v.toFixed(1) + '%'; } },
            tooltip: { callbacks: { label: function (c) { return c.parsed.x.toFixed(1) + '%'; } } },
          },
          scales: {
            x: { beginAtZero: true,
              title: { display: true, text: axisTitle, font: { family: FONT, size: 11, weight: 600 } },
              grid: { color: GRID_X }, ticks: { callback: function (v) { return v + '%'; }, font: { family: FONT, size: 10 } } },
            y: { grid: { display: false }, ticks: targetTickStyle() },
          },
        },
        plugins: [ChartDataLabels],
      });
    }
    makeBar('ki-canvas-02a', function (r) { return r.le === 'Wide Leg'; }, 'Wide Leg %');
    makeBar('ki-canvas-02b', function (r) { return r.fi === 'Slim/Contemporary'; }, 'Slim/Contemporary %');
    makeBar('ki-canvas-02c', function (r) { return r.ri === 'Low'; }, 'Low Rise %');
  }

  // ── Insight 03: Wash mix 100% stacked, rescaled to 100% w/o Unclassified ─
  function renderInsight03(detail) {
    detail.innerHTML =
      '<div class="ki-detail-title">Wash mix by retailer group — 100% stacked (Unclassified excluded)</div>' +
      '<div class="ki-detail-sub">Share of classified-wash CCs in each category. Rows rescaled to 100% after removing Unclassified entries. Sorted by Light Wash % (most to least).</div>' +
      '<div class="ki-chart-wrap" style="min-height:520px"><canvas id="ki-canvas-03"></canvas></div>' +
      '<div class="ki-detail-note">Target OB Light Wash 23% leads the cross-shop set; Dark Wash only 16% vs Walmart 20% and Kohl\'s 28%.</div>';

    var canvasId = 'ki-canvas-03';
    var allGroups = (window.GROUPS || []).slice();
    var washes = ['Light Wash','Medium Wash','Dark Wash','Black','White/Cream','Grey','Colored'];
    var washColors = (window.WASH_COLORS) || {
      'Light Wash':'#87CEEB','Medium Wash':'#4682B4','Dark Wash':'#1a3a5c',
      'Black':'#1a1a2e','White/Cream':'#f5f0e8','Grey':'#9ca3af','Colored':'#e88c4a',
    };

    // Build per-group wash percentages (excluding Unclassified). Denominator
    // is the count of rows with a classified wash for that group.
    function pctsForGroup(g) {
      var rs = byGroup(g);
      var classified = 0;
      var counts = {};
      for (var i = 0; i < washes.length; i++) counts[washes[i]] = 0;
      for (var j = 0; j < rs.length; j++) {
        var ww = rs[j].w;
        if (ww && ww !== 'Unclassified' && (ww in counts)) {
          classified++;
          counts[ww]++;
        }
      }
      var out = {};
      washes.forEach(function (w) {
        out[w] = classified ? Math.round(100 * counts[w] / classified * 10) / 10 : 0;
      });
      return out;
    }
    var groupPcts = {};
    allGroups.forEach(function (g) { groupPcts[g] = pctsForGroup(g); });

    // Sort groups by Light Wash % descending (most to least)
    var groups = allGroups.slice().sort(function (a, b) {
      return groupPcts[b]['Light Wash'] - groupPcts[a]['Light Wash'];
    });
    var labels = groups.map(function (g) { return GROUP_LABELS[g] || g; });

    var datasets = washes.map(function (w) {
      return {
        label: w,
        data: groups.map(function (g) { return groupPcts[g][w]; }),
        backgroundColor: washColors[w] || '#ccc',
        borderColor: w === 'White/Cream' ? '#bbb' : (washColors[w] || '#999'),
        borderWidth: w === 'White/Cream' ? 0.5 : 0,
      };
    });

    destroyChart(canvasId);
    var ctx = document.getElementById(canvasId);
    if (!ctx) return;
    charts[canvasId] = new Chart(ctx.getContext('2d'), {
      type: 'bar',
      data: { labels: labels, datasets: datasets },
      options: {
        indexAxis: 'y', responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom', labels: { font: { family: FONT, size: 10, weight: 600 }, boxWidth: 14 } },
          datalabels: { display: function (c) { return c.dataset.data[c.dataIndex] >= 6; },
            font: { family: FONT, size: 9, weight: 700 },
            color: function (c) {
              var w = c.dataset.label;
              return (w === 'Light Wash' || w === 'White/Cream' || w === 'Grey') ? '#1a1a1a' : '#fff';
            },
            formatter: function (v) { return v.toFixed(0) + '%'; } },
          tooltip: { callbacks: { label: function (c) { return c.dataset.label + ': ' + c.parsed.x.toFixed(1) + '%'; } } },
        },
        scales: {
          x: { stacked: true, max: 100,
            title: { display: true, text: '% of classified CCs', font: { family: FONT, size: 11, weight: 600 } },
            grid: { color: GRID_X }, ticks: { callback: function (v) { return v + '%'; }, font: { family: FONT, size: 10 } } },
          y: { stacked: true, grid: { display: false }, ticks: targetTickStyle() },
        },
      },
      plugins: [ChartDataLabels],
    });
  }

  // ── Insight 04: Target OB box plot per brand + $28-$35 highlight band ──
  function renderInsight04(detail) {
    var rs = byGroup('Target OB');
    var inBand = 0, totalWithPrice = 0;
    var brandPrices = {};
    for (var i = 0; i < rs.length; i++) {
      if (rs[i].p > 0) {
        totalWithPrice++;
        if (rs[i].p >= 28 && rs[i].p <= 35) inBand++;
        (brandPrices[rs[i].b] = brandPrices[rs[i].b] || []).push(rs[i].p);
      }
    }
    var bandPct = totalWithPrice ? Math.round(100 * inBand / totalWithPrice * 10) / 10 : 0;

    detail.innerHTML =
      '<div class="ki-callout">' + bandPct.toFixed(1) + '% of Target OB CCs fall in the $28–$35 band (' + inBand + ' of ' + totalWithPrice + ')</div>' +
      '<div class="ki-detail-title">Target OB price distribution by brand</div>' +
      '<div class="ki-detail-sub">Box plot per brand (min/Q1/median/Q3/max + mean diamond). Shaded band = $28–$35 cluster zone.</div>' +
      '<div class="ki-chart-wrap" style="min-height:340px"><canvas id="ki-canvas-04"></canvas></div>' +
      '<div class="ki-detail-note">Universal Thread $28 · Wild Fable $32 · Ava & Viv $33. The three brands sit on top of each other rather than laddering up or down.</div>';

    var brandList = Object.keys(brandPrices).sort(function (a, b) { return brandPrices[b].length - brandPrices[a].length; });
    var labels = brandList.map(function (b) { return b + ' (n=' + brandPrices[b].length + ')'; });
    var stats  = brandList.map(function (b) { return bS(brandPrices[b]); });
    var colors = brandList.map(function () { return GC['Target OB']; });

    // Replicate drawHBoxPlot but inject a custom band-highlight plugin too.
    var canvasId = 'ki-canvas-04';
    destroyChart(canvasId);
    var ctx = document.getElementById(canvasId);
    if (!ctx) return;
    var pedestal = stats.map(function (s) { return s ? [0, s.q1] : [0, 0]; });
    var box      = stats.map(function (s) { return s ? [s.q1, s.q3] : [0, 0]; });
    var xMax = 0;
    for (var i = 0; i < stats.length; i++) if (stats[i] && stats[i].wHi > xMax) xMax = stats[i].wHi;
    xMax = Math.ceil(xMax * 1.15);

    // Per-chart band plugin: shaded vertical region $28-$35
    var bandPlugin = {
      id: 'priceBandHighlight',
      beforeDatasetsDraw: function (chart) {
        var x = chart.scales.x;
        var area = chart.chartArea;
        if (!x || !area) return;
        var c = chart.ctx;
        var x1 = x.getPixelForValue(28);
        var x2 = x.getPixelForValue(35);
        c.save();
        c.fillStyle = 'rgba(204, 0, 0, 0.10)';
        c.fillRect(x1, area.top, x2 - x1, area.bottom - area.top);
        c.strokeStyle = 'rgba(204, 0, 0, 0.45)';
        c.lineWidth = 1;
        c.setLineDash([4, 3]);
        c.beginPath();
        c.moveTo(x1, area.top); c.lineTo(x1, area.bottom);
        c.moveTo(x2, area.top); c.lineTo(x2, area.bottom);
        c.stroke();
        c.setLineDash([]);
        // Label on top
        c.font = '600 10px Montserrat';
        c.fillStyle = '#CC0000';
        c.textAlign = 'center';
        c.fillText('$28–$35 cluster', (x1 + x2) / 2, area.top - 4);
        c.restore();
      }
    };

    charts[canvasId] = new Chart(ctx.getContext('2d'), {
      type: 'bar',
      data: { labels: labels, datasets: [
        { label: '_p', data: pedestal, backgroundColor: 'transparent', borderWidth: 0, barPercentage: 0.5, categoryPercentage: 0.65, borderSkipped: false },
        { label: 'IQR', data: box,
          backgroundColor: colors.map(function (c) { return c.light; }),
          borderColor: colors.map(function (c) { return c.border; }),
          borderWidth: 2, borderRadius: 4, barPercentage: 0.5, categoryPercentage: 0.65, borderSkipped: false },
      ] },
      options: {
        indexAxis: 'y', responsive: true, maintainAspectRatio: false,
        layout: { padding: { top: 18 } }, // room for the band label
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: function (c) {
            var s = stats[c.dataIndex]; if (!s) return '';
            return 'n=' + s.n + ' | Min: ' + fmt$(s.min) + ' | Q1: ' + fmt$(s.q1) +
                   ' | Med: ' + fmt$(s.med) + ' | Q3: ' + fmt$(s.q3) +
                   ' | Max: ' + fmt$(s.max) + ' | Mean: ' + fmt$(s.mean);
          } } },
          datalabels: { display: false },
        },
        scales: {
          x: { min: 0, max: xMax,
            title: { display: true, text: 'Market observed price ($)', font: { family: FONT, size: 11, weight: 600 } },
            grid: { color: GRID_X } },
          y: { grid: { display: false }, ticks: { font: { family: FONT, size: 11, weight: 600 } } },
        },
      },
      plugins: [ChartDataLabels, bandPlugin],
    });
    // Attach _bpd so the existing whisker plugin (hbpPlugin) draws on top
    charts[canvasId]._bpd = stats.map(function (s, i) {
      if (!s) return null;
      return { min: s.min, q1: s.q1, med: s.med, q3: s.q3, max: s.max, wLo: s.wLo, wHi: s.wHi, mean: s.mean, n: s.n, color: colors[i].border };
    });
  }

  // ── Insight 05: List vs Market Observed min-max range bars per group ──
  function renderInsight05(detail) {
    var canvasId = 'ki-canvas-05';
    var groups = ['Levis','Target NB','AE','Macys OB','Old Navy','Kohls OB','Walmart NB','Target OB','Walmart OB'];
    var labels = groups.map(function (g) { return GROUP_LABELS[g] || g; });
    function rangeFor(g, key) {
      var arr = []; var d = byGroup(g);
      for (var i = 0; i < d.length; i++) if (d[i][key] > 0) arr.push(d[i][key]);
      if (!arr.length) return [0, 0];
      var min = arr[0], max = arr[0];
      for (var j = 1; j < arr.length; j++) { if (arr[j] < min) min = arr[j]; if (arr[j] > max) max = arr[j]; }
      return [min, max];
    }
    var listRanges = groups.map(function (g) { return rangeFor(g, 'o'); });
    var obsRanges  = groups.map(function (g) { return rangeFor(g, 'p'); });
    var bgs     = groups.map(function (g) { return GC[g] ? GC[g].light  : 'rgba(150,150,150,.3)'; });
    var bgsObs  = groups.map(function (g) { return GC[g] ? GC[g].bg     : '#999'; });
    var borders = groups.map(function (g) { return GC[g] ? GC[g].border : '#999'; });

    // Compute Target OB market observed range to draw as the highlight band
    var tgtObs = rangeFor('Target OB', 'p');

    detail.innerHTML =
      '<div class="ki-callout">Target OB market observed range: $' + tgtObs[0].toFixed(0) + '–$' + tgtObs[1].toFixed(0) + ' (highlighted band on chart)</div>' +
      '<div class="ki-detail-title">List Price vs Market Observed Price — min-to-max range</div>' +
      '<div class="ki-detail-sub">Each row shows the full spread (min–max) of List Price (faded) vs Market Observed Price (solid). Shaded band = Target OB market observed range.</div>' +
      '<div class="ki-chart-wrap" style="min-height:420px"><canvas id="' + canvasId + '"></canvas></div>' +
      '<div class="ki-detail-note">When discounted, Macy\'s OB collapses from $50–$70 list to $32–$48 market observed; AE from $60–$70 to $42–$52. Target OB barely moves: $28–$36 list to $28–$35 market observed.</div>';

    var X_MAX = 150;
    function capped(r) { return [Math.min(r[0], X_MAX), Math.min(r[1], X_MAX)]; }

    // Per-chart band plugin: shaded vertical region for Target OB market observed range
    var bandPlugin = {
      id: 'targetObBandHighlight',
      beforeDatasetsDraw: function (chart) {
        var x = chart.scales.x;
        var area = chart.chartArea;
        if (!x || !area) return;
        var c = chart.ctx;
        var x1 = x.getPixelForValue(tgtObs[0]);
        var x2 = x.getPixelForValue(tgtObs[1]);
        c.save();
        c.fillStyle = 'rgba(204, 0, 0, 0.10)';
        c.fillRect(x1, area.top, x2 - x1, area.bottom - area.top);
        c.strokeStyle = 'rgba(204, 0, 0, 0.45)';
        c.lineWidth = 1;
        c.setLineDash([4, 3]);
        c.beginPath();
        c.moveTo(x1, area.top); c.lineTo(x1, area.bottom);
        c.moveTo(x2, area.top); c.lineTo(x2, area.bottom);
        c.stroke();
        c.setLineDash([]);
        c.font = '600 10px Montserrat';
        c.fillStyle = '#CC0000';
        c.textAlign = 'center';
        c.fillText('Target OB observed range $' + tgtObs[0].toFixed(0) + '–$' + tgtObs[1].toFixed(0),
                   (x1 + x2) / 2, area.top - 4);
        c.restore();
      }
    };

    destroyChart(canvasId);
    var ctx = document.getElementById(canvasId);
    if (!ctx) return;
    charts[canvasId] = new Chart(ctx.getContext('2d'), {
      type: 'bar',
      data: { labels: labels, datasets: [
        { label: 'List price range',            data: listRanges.map(capped),
          backgroundColor: bgs, borderColor: borders, borderWidth: 1.5, borderRadius: 4,
          barPercentage: 0.85, categoryPercentage: 0.85 },
        { label: 'Market observed price range', data: obsRanges.map(capped),
          backgroundColor: bgsObs, borderColor: borders, borderWidth: 1.5, borderRadius: 4,
          barPercentage: 0.85, categoryPercentage: 0.85 },
      ] },
      options: {
        indexAxis: 'y', responsive: true, maintainAspectRatio: false,
        layout: { padding: { top: 20 } },
        plugins: {
          legend: { position: 'top', labels: { font: { family: FONT, size: 11, weight: 600 } } },
          datalabels: { display: false },
          tooltip: { callbacks: {
            label: function (c) {
              var i = c.dataIndex;
              var r = c.dataset.label.indexOf('Market') === 0 ? obsRanges[i] : listRanges[i];
              return c.dataset.label + ': $' + r[0].toFixed(0) + ' – $' + r[1].toFixed(0) +
                     (r[1] > X_MAX ? ' (axis capped at $' + X_MAX + ')' : '');
            }
          } },
        },
        scales: {
          x: { min: 0, max: X_MAX,
            title: { display: true, text: 'Price ($)', font: { family: FONT, size: 11, weight: 600 } },
            grid: { color: GRID_X }, ticks: { callback: function (v) { return '$' + v; }, font: { family: FONT, size: 10 } } },
          y: { grid: { display: false }, ticks: targetTickStyle() },
        },
      },
      plugins: [bandPlugin],
    });
  }

  // ── Insight 06: Levi's price posture (real box plot) ──────────────────
  function renderInsight06(detail) {
    detail.innerHTML =
      '<div class="ki-detail-title">Levi\'s price posture across retailers</div>' +
      '<div class="ki-detail-sub">Box plot of market observed prices for Levi\'s-branded jeans (and licensed Levi Strauss Signature) by channel.</div>' +
      '<div class="ki-chart-wrap" style="min-height:340px"><canvas id="ki-canvas-06"></canvas></div>' +
      '<div class="ki-detail-note">Same brand, three postures: ~$70 at Target NB · ~$40 at Walmart NB · ~$75 at Levi.com. Walmart\'s licensed Levi Strauss Signature ~$27 undercuts Target OB.</div>';

    var canvasId = 'ki-canvas-06';
    function pricesFor(g, brand) {
      var arr = []; var d = byGroup(g);
      for (var i = 0; i < d.length; i++) {
        if ((!brand || d[i].b === brand) && d[i].p > 0) arr.push(d[i].p);
      }
      return arr;
    }
    var rows = [
      { lbl: 'Target Owned Brands',                 g: 'Target OB',  brand: null,                       color: GC['Target OB']  },
      { lbl: "Levi's at Target (NB)",               g: 'Target NB',  brand: "Levi's",                   color: GC['Target NB']  },
      { lbl: "Levi's Direct (levi.com)",            g: 'Levis',      brand: "Levi's",                   color: GC['Levis']      },
      { lbl: "Levi's at Walmart (NB)",              g: 'Walmart NB', brand: "Levi's",                   color: GC['Walmart NB'] },
      { lbl: "Levi Strauss Signature (Walmart NB)", g: 'Walmart NB', brand: 'Levi Strauss Signature',   color: GC['Walmart NB'] },
    ];
    var data = rows.map(function (r) { return { lbl: r.lbl, prices: pricesFor(r.g, r.brand), color: r.color }; })
                   .filter(function (r) { return r.prices.length > 0; });
    var labels = data.map(function (r) { return r.lbl + ' (n=' + r.prices.length + ')'; });
    var stats = data.map(function (r) { return bS(r.prices); });
    var colors = data.map(function (r) { return r.color || GC['Target OB']; });
    drawHBoxPlot(canvasId, labels, stats, colors, {});
    applyTargetTickStyle(canvasId);
  }

  // ── Toggle handler ────────────────────────────────────────────────────
  var renderers = { '01': renderInsight01, '02': renderInsight02, '03': renderInsight03,
                    '04': renderInsight04, '05': renderInsight05, '06': renderInsight06 };

  window.toggleInsight = function (n) {
    var detail = document.getElementById('ki-detail-' + n);
    var btn = document.getElementById('ki-toggle-' + n);
    if (!detail || !btn) return;
    var isOpen = detail.classList.contains('open');
    if (isOpen) {
      detail.classList.remove('open');
      btn.classList.remove('active');
      btn.textContent = 'View details ▾';
      return;
    }
    detail.classList.add('open');
    btn.classList.add('active');
    btn.textContent = 'Hide details ▴';
    if (!detail.dataset.built) {
      try {
        renderers[n](detail);
        detail.dataset.built = '1';
      } catch (e) {
        console.error('Insight ' + n + ' render failed:', e);
      }
    } else {
      // Re-render to refresh canvas (in case window resized while collapsed)
      setTimeout(function () {
        try { renderers[n](detail); } catch (e) { console.error(e); }
      }, 50);
    }
  };
})();
</script>'''


# ── Patcher ────────────────────────────────────────────────────────────────

def main():
    dry = "--dry-run" in sys.argv
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()
    orig_size = len(html)

    start = html.find('<div id="page-insights"')
    if start < 0:
        raise RuntimeError('Could not locate <div id="page-insights">')
    i = start
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
    end = i
    if depth != 0:
        raise RuntimeError("page-insights nesting did not balance")

    new_html = html[:start] + NEW_PAGE_HTML + html[end:]
    print(f"Replaced page-insights ({end - start:,} -> {len(NEW_PAGE_HTML):,} chars)")

    new_html = re.sub(r'<style id="key-insights-css">.*?</style>',  '', new_html, flags=re.DOTALL)
    new_html = re.sub(r'<script id="key-insights-js">.*?</script>', '', new_html, flags=re.DOTALL)

    head_close = new_html.rfind("</head>")
    if head_close >= 0:
        new_html = new_html[:head_close] + CSS_BLOCK + "\n" + new_html[head_close:]
    body_close = new_html.rfind("</body>")
    if body_close >= 0:
        new_html = new_html[:body_close] + JS_BLOCK + "\n" + new_html[body_close:]

    delta = len(new_html) - orig_size
    print(f"index.html: {orig_size:,} -> {len(new_html):,} chars ({delta:+,})")

    if dry:
        print("[dry-run] no write")
        return 0
    backup = INDEX_HTML + ".bak_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(INDEX_HTML, backup)
    print(f"Backup: {backup}")
    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(new_html)
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
