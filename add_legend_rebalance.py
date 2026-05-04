#!/usr/bin/env python3
"""
add_legend_rebalance.py — Inject JS that:
  1. Wraps renderStackedAttr so that clicking a legend item on any
     stacked-attribute chart (rise/leg/fit/wash/fabric weight/cotton)
     toggles that category off AND rescales the remaining categories
     so each retailer's bar still sums to 100%.
  2. Syncs the wash chart's visibility state to the sunburst grid —
     when a wash is hidden in the bar chart's legend, the sunbursts
     re-render with that wash filtered out and remaining washes
     rescaled to 100%.

Idempotent: bracketed by marker comments. Re-running replaces the
prior block instead of stacking.
"""

import os
import re
import shutil
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML = os.path.join(HERE, "index.html")

MARKER_BEGIN = "<!-- LEGEND_REBALANCE_INJECT_BEGIN -->"
MARKER_END = "<!-- LEGEND_REBALANCE_INJECT_END -->"

JS_BLOCK = r"""<script id="legend-rebalance-js">
(function () {
  // Tracks which wash categories the user has toggled off via any
  // wash chart legend. Default: all visible. Used by both the
  // stacked-bar wash chart and the sunburst grid.
  if (!window.WASH_VISIBLE) {
    window.WASH_VISIBLE = {};
    if (window.WASH_ORDER && window.WASH_ORDER.forEach) {
      window.WASH_ORDER.forEach(function (w) { window.WASH_VISIBLE[w] = true; });
    }
  }

  // ── Wrap renderStackedAttr to add legend rebalancing ─────────────────
  if (typeof window.renderStackedAttr === 'function' && !window.renderStackedAttr.__rebalanceWrapped) {
    var origStacked = window.renderStackedAttr;
    window.renderStackedAttr = function (canvasId, field, subtitleId, order) {
      origStacked(canvasId, field, subtitleId, order);
      var chart = window.charts && window.charts[canvasId];
      if (!chart) return;

      // Track which group keys correspond to which bar index — needed
      // because chart.data.labels are formatted strings, not group keys.
      var groupsWithData = window.GROUPS.filter(function (g) {
        return window.byGroup(g).some(function (r) { return !!r[field]; });
      });
      chart._rebalCtx = { field: field, groupKeys: groupsWithData, order: order };

      // Custom legend.onClick: toggle dataset visibility, then recompute
      // every bar so the visible categories sum to 100%.
      chart.options.plugins.legend.onClick = function (e, legendItem, legend) {
        var ch = legend.chart;
        var idx = legendItem.datasetIndex;
        var meta = ch.getDatasetMeta(idx);
        meta.hidden = meta.hidden === null ? !ch.data.datasets[idx].hidden : null;

        var ctx = ch._rebalCtx || {};
        var f = ctx.field;
        var grpKeys = ctx.groupKeys || [];
        if (!f || !grpKeys.length) { ch.update(); return; }

        // Visible category labels (dataset labels for non-hidden datasets)
        var visibleVals = ch.data.datasets
          .map(function (ds, i) {
            return ch.getDatasetMeta(i).hidden ? null : ds.label;
          })
          .filter(function (v) { return v !== null; });

        // Recompute each dataset's per-group percentage with the new
        // denominator (count of rows whose value is in visibleVals).
        ch.data.datasets.forEach(function (ds) {
          ds.data = grpKeys.map(function (g) {
            var rows = window.byGroup(g).filter(function (r) {
              return !!r[f] && visibleVals.indexOf(r[f]) >= 0;
            });
            var n = rows.length;
            if (!n) return 0;
            // Hidden datasets get 0; visible datasets get share of visible total
            if (visibleVals.indexOf(ds.label) < 0) return 0;
            var hits = 0;
            for (var k = 0; k < rows.length; k++) if (rows[k][f] === ds.label) hits++;
            return n ? (hits / n * 100) : 0;
          });
        });

        ch.update();

        // If this is the wash chart, sync the global visibility state
        // and re-render the sunburst grid with the same filter applied.
        if (f === 'w') {
          ch.data.datasets.forEach(function (ds, i) {
            window.WASH_VISIBLE[ds.label] = !ch.getDatasetMeta(i).hidden;
          });
          if (typeof window.renderSunbursts === 'function') {
            try { window.renderSunbursts(); } catch (err) { console.warn('sunburst sync failed:', err); }
          }
        }
      };

      chart.update();
    };
    window.renderStackedAttr.__rebalanceWrapped = true;
  }

  // ── Wrap renderSunbursts to respect window.WASH_VISIBLE ──────────────
  // We can't easily filter inside the existing function, so we replace it.
  // The replacement mirrors the existing logic but excludes washes that
  // are hidden via window.WASH_VISIBLE before building the hierarchy.
  if (typeof window.renderSunbursts === 'function' && !window.renderSunbursts.__rebalanceWrapped) {
    window.renderSunbursts = function () {
      var container = document.getElementById('sunburst-grid');
      if (!container) return;
      container.innerHTML = '';

      // Shared tooltip
      var tipId = 'sun-tooltip';
      var tip = document.getElementById(tipId);
      if (!tip) {
        tip = document.createElement('div');
        tip.id = tipId;
        tip.style.cssText = 'position:fixed;pointer-events:none;background:#1a1a2e;color:#fff;padding:6px 12px;border-radius:6px;font-family:Montserrat,sans-serif;font-size:11px;font-weight:600;z-index:9999;display:none;white-space:nowrap;box-shadow:0 2px 8px rgba(0,0,0,.25)';
        document.body.appendChild(tip);
      }

      var visibleWashes = (window.WASH_ORDER || []).filter(function (w) {
        return window.WASH_VISIBLE[w] !== false;
      });
      var hiddenWashes = (window.WASH_ORDER || []).filter(function (w) {
        return window.WASH_VISIBLE[w] === false;
      });

      // Filter banner above the grid (visible only when something's hidden)
      if (hiddenWashes.length) {
        var banner = document.createElement('div');
        banner.style.cssText = 'width:100%;text-align:center;font-family:Montserrat,sans-serif;font-size:.7rem;color:var(--fg2,#4b5563);margin-bottom:8px';
        banner.innerHTML = '<strong style="color:#CC0000">Filter active:</strong> ' +
          hiddenWashes.length + ' wash categor' + (hiddenWashes.length === 1 ? 'y' : 'ies') +
          ' hidden (' + hiddenWashes.join(', ') + '). Toggle via the wash chart legend above.';
        container.appendChild(banner);
      }

      window.GROUPS.forEach(function (g) {
        var d = window.byGroup(g).filter(function (r) {
          return !!r.w && visibleWashes.indexOf(r.w) >= 0;
        });
        if (!d.length) return;
        var totalCCs = d.length;
        var div = document.createElement('div');
        div.className = 'sunburst-cell';
        var sunId = 'sun-' + g.replace(/\s/g, '');
        div.innerHTML = '<div class="chart-title" style="text-align:center;font-size:.78rem">' + (window.GROUP_LABELS[g] || g) + '</div>'
          + '<div class="chart-subtitle" style="text-align:center">n=' + totalCCs + ' CCs</div>'
          + '<div id="' + sunId + '" style="display:flex;justify-content:center"></div>';
        container.appendChild(div);

        var washMap = {};
        d.forEach(function (r) {
          if (!washMap[r.w]) washMap[r.w] = {};
          var cName = r.c || 'Unknown';
          washMap[r.w][cName] = (washMap[r.w][cName] || 0) + 1;
        });

        var hier = { name: 'root', children: [] };
        visibleWashes.forEach(function (wsh) {
          if (!washMap[wsh]) return;
          var kids = [];
          Object.keys(washMap[wsh]).sort(function (a, b) { return washMap[wsh][b] - washMap[wsh][a]; }).forEach(function (cName) {
            kids.push({ name: cName, value: washMap[wsh][cName] });
          });
          hier.children.push({ name: wsh, children: kids });
        });

        var size = 260;
        var radius = size / 2;
        var svg = d3.select('#' + sunId)
          .append('svg').attr('width', size).attr('height', size)
          .append('g').attr('transform', 'translate(' + radius + ',' + radius + ')');

        var root = d3.hierarchy(hier).sum(function (nd) { return nd.value || 0; })
          .sort(function (a, b) { return b.value - a.value; });
        d3.partition().size([2 * Math.PI, radius])(root);

        var arc = d3.arc()
          .startAngle(function (nd) { return nd.x0; })
          .endAngle(function (nd) { return nd.x1; })
          .innerRadius(function (nd) { return nd.y0 * 0.7; })
          .outerRadius(function (nd) { return nd.y1 * 0.7 - 1; });

        svg.selectAll('path')
          .data(root.descendants().filter(function (nd) { return nd.depth > 0; }))
          .enter().append('path')
          .attr('d', arc)
          .attr('fill', function (nd) {
            var washName = nd.depth === 1 ? nd.data.name : (nd.parent ? nd.parent.data.name : '');
            var base = window.WASH_COLORS[washName] || '#94a3b8';
            if (nd.depth === 1) return base;
            var c2 = d3.color(base);
            return c2 ? c2.brighter(0.6).toString() : base;
          })
          .attr('stroke', '#fff')
          .attr('stroke-width', 0.5)
          .style('cursor', 'pointer')
          .on('mouseover', function (event, nd) {
            var pct = Math.round(nd.value / totalCCs * 100);
            var label = nd.depth === 1 ? nd.data.name : nd.data.name + ' (' + nd.parent.data.name + ')';
            tip.innerHTML = label + ': ' + nd.value + ' CCs (' + pct + '%)';
            tip.style.display = 'block';
          })
          .on('mousemove', function (event) {
            tip.style.left = (event.clientX + 12) + 'px';
            tip.style.top = (event.clientY - 24) + 'px';
          })
          .on('mouseout', function () { tip.style.display = 'none'; });

        svg.append('text').attr('text-anchor', 'middle').attr('dy', '0.35em')
          .style('font-family', 'Montserrat').style('font-size', '11px').style('font-weight', '700').style('fill', '#555')
          .text(totalCCs + ' CCs');
      });
    };
    window.renderSunbursts.__rebalanceWrapped = true;
  }
})();
</script>"""


def remove_existing(html):
    pat = re.compile(re.escape(MARKER_BEGIN) + r".*?" + re.escape(MARKER_END), re.DOTALL)
    return pat.sub("", html)


def patch(html):
    html = remove_existing(html)
    body_end = html.rfind("</body>")
    if body_end < 0:
        raise RuntimeError("No </body> tag found")
    block = "\n" + MARKER_BEGIN + "\n" + JS_BLOCK + "\n" + MARKER_END + "\n"
    return html[:body_end] + block + html[body_end:]


def main():
    dry = "--dry-run" in sys.argv
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()
    new_html = patch(html)
    delta = len(new_html) - len(html)
    print(f"index.html: {len(html):,} -> {len(new_html):,}  ({delta:+,} chars)")
    if dry:
        print("[dry-run] no write")
        return 0
    backup = INDEX_HTML + ".bak_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(INDEX_HTML, backup)
    print(f"Backup: {backup}")
    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(new_html)
    print("Done. Click any legend item on a stacked attribute chart to toggle + rebalance.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
