#!/usr/bin/env python3
"""
add_chart_export.py — Inject a "Download CSV" button onto every Chart.js
chart in index.html. Reads each chart's currently-rendered data
(reflecting any active filters) and downloads it as a CSV table.

Idempotent: detects the marker comment and rewrites in place if rerun.
"""

import os
import re
import shutil
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML = os.path.join(HERE, "index.html")

MARKER_BEGIN = "<!-- CHART_EXPORT_INJECT_BEGIN -->"
MARKER_END = "<!-- CHART_EXPORT_INJECT_END -->"

CSS_BLOCK = """
<style id="chart-export-css">
  .chart-export-btn {
    position: absolute;
    top: 8px;
    right: 8px;
    z-index: 50;
    background: rgba(255,255,255,0.95);
    border: 1px solid var(--bg3, #d1d5db);
    border-radius: 4px;
    padding: 3px 8px;
    font-size: 0.62rem;
    font-weight: 700;
    color: var(--fg2, #4b5563);
    cursor: pointer;
    font-family: Montserrat, system-ui, sans-serif;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    line-height: 1.2;
    transition: background .15s, color .15s, border-color .15s;
    opacity: 0.85;
    user-select: none;
  }
  .chart-export-btn:hover {
    background: var(--fg, #1a1a1a);
    color: #fff;
    border-color: var(--fg, #1a1a1a);
    opacity: 1;
  }
  .chart-export-btn:active {
    transform: translateY(1px);
  }
  .chart-export-host { position: relative; }
</style>
"""

JS_BLOCK = """
<script id="chart-export-js">
(function () {
  // ── CSV exporter for any Chart.js chart ──────────────────────────────
  function csvEscape(v) {
    if (v == null) return '';
    var s = String(v);
    return /[",\\n\\r]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  }

  function rowsFromChart(chart) {
    var data = chart.data || {};
    var labels = data.labels || [];
    var dsets = data.datasets || [];
    var rows = [];

    // Detect box-plot data (objects with q1/q3/median or min/max)
    function fmtCell(v) {
      if (v == null) return '';
      if (Array.isArray(v)) return v.join('|');
      if (typeof v === 'object') {
        // Box plot point — flatten into key=value
        var parts = [];
        for (var k in v) {
          if (Object.prototype.hasOwnProperty.call(v, k)) {
            parts.push(k + '=' + (Array.isArray(v[k]) ? v[k].length : v[k]));
          }
        }
        return parts.join(' ');
      }
      return v;
    }

    if (dsets.length === 0) return [['(no data)']];

    // For box-plots, expand q1/median/q3/min/max as separate columns per dataset
    var anyBox = dsets.some(function (ds) {
      return (ds.data || []).some(function (v) {
        return v && typeof v === 'object' && !Array.isArray(v) &&
               ('q1' in v || 'median' in v || 'q3' in v);
      });
    });

    if (anyBox) {
      // Column headers: <Label>, then per-dataset min/q1/median/q3/max
      var hdr = ['Label'];
      dsets.forEach(function (ds) {
        var lbl = ds.label || 'Series';
        ['min','q1','median','mean','q3','max','n'].forEach(function (k) {
          hdr.push(lbl + ' · ' + k);
        });
      });
      rows.push(hdr);
      var n = labels.length || (dsets[0].data ? dsets[0].data.length : 0);
      for (var i = 0; i < n; i++) {
        var row = [labels[i] != null ? labels[i] : ''];
        dsets.forEach(function (ds) {
          var v = (ds.data || [])[i] || {};
          ['min','q1','median','mean','q3','max'].forEach(function (k) {
            row.push(v[k] != null ? v[k] : '');
          });
          // n: count of items if items array present
          row.push(Array.isArray(v.items) ? v.items.length : '');
        });
        rows.push(row);
      }
      return rows;
    }

    // Standard chart: rows = labels, cols = datasets
    var hdr2 = ['Label'];
    dsets.forEach(function (ds) {
      hdr2.push(ds.label || 'Value');
    });
    rows.push(hdr2);
    var maxLen = labels.length;
    dsets.forEach(function (ds) {
      if (ds.data && ds.data.length > maxLen) maxLen = ds.data.length;
    });
    for (var i = 0; i < maxLen; i++) {
      var row = [labels[i] != null ? labels[i] : ''];
      dsets.forEach(function (ds) {
        row.push(fmtCell((ds.data || [])[i]));
      });
      rows.push(row);
    }
    return rows;
  }

  function exportChartCSV(canvasId) {
    var canvas = document.getElementById(canvasId);
    if (!canvas) {
      console.warn('Chart export: canvas not found:', canvasId);
      return;
    }
    var chart = (typeof Chart !== 'undefined' && Chart.getChart)
      ? Chart.getChart(canvas) : null;
    if (!chart) {
      console.warn('Chart export: no Chart instance on', canvasId);
      return;
    }
    var rows = rowsFromChart(chart);
    var csv = rows.map(function (r) {
      return r.map(csvEscape).join(',');
    }).join('\\n');

    // Filename = canvas id (chart titles are often missing/dynamic)
    var fname = canvasId.replace(/[^a-z0-9_-]/gi, '_') + '.csv';

    var blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
    var url = URL.createObjectURL(blob);
    var a = document.createElement('a');
    a.href = url;
    a.download = fname;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    setTimeout(function () { URL.revokeObjectURL(url); }, 500);
  }

  // ── Injector: walks every canvas, adds an export button ──────────────
  function ensureRelativeParent(el) {
    var p = el.parentElement;
    if (!p) return;
    var cs = getComputedStyle(p);
    if (cs.position === 'static') {
      p.classList.add('chart-export-host');
    }
  }

  function injectButtons() {
    if (typeof Chart === 'undefined' || !Chart.getChart) return;
    var added = 0;
    document.querySelectorAll('canvas').forEach(function (canvas) {
      var id = canvas.id;
      if (!id) return;
      var chart = Chart.getChart(canvas);
      if (!chart) return;  // canvas not a Chart.js chart (yet)
      var parent = canvas.parentElement;
      if (!parent) return;
      // Skip if a button for this canvas already exists
      var existing = parent.querySelector('button.chart-export-btn[data-for="' + id + '"]');
      if (existing) return;
      ensureRelativeParent(canvas);
      var btn = document.createElement('button');
      btn.className = 'chart-export-btn';
      btn.setAttribute('data-for', id);
      btn.setAttribute('type', 'button');
      btn.title = 'Download the data currently shown in this chart as CSV';
      btn.textContent = '⤓ CSV';
      btn.onclick = function (ev) {
        ev.stopPropagation();
        ev.preventDefault();
        exportChartCSV(id);
      };
      parent.appendChild(btn);
      added += 1;
    });
    return added;
  }

  // Multiple staged tries — charts render at different times, especially
  // on pages that aren't visible at boot.
  function tryInject() {
    try { injectButtons(); } catch (e) { console.warn('chart-export inject:', e); }
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', tryInject);
  } else {
    tryInject();
  }
  setTimeout(tryInject, 250);
  setTimeout(tryInject, 1000);
  setTimeout(tryInject, 3000);

  // Re-run on page switches (the dashboard renders charts lazily per page)
  function wrapShowPage() {
    if (typeof window.showPage === 'function' && !window.showPage.__exportWrapped) {
      var orig = window.showPage;
      window.showPage = function () {
        var r = orig.apply(this, arguments);
        setTimeout(tryInject, 100);
        setTimeout(tryInject, 800);
        return r;
      };
      window.showPage.__exportWrapped = true;
    }
  }
  wrapShowPage();
  // showPage may not be defined yet at first run; retry briefly
  var t = 0;
  var iv = setInterval(function () {
    wrapShowPage();
    t += 1;
    if (window.showPage && window.showPage.__exportWrapped) clearInterval(iv);
    if (t > 20) clearInterval(iv);
  }, 200);

  // Also re-run when filter buttons are clicked (charts often re-init)
  document.addEventListener('click', function (ev) {
    var t = ev.target;
    if (!t) return;
    // best-effort: any button or label inside the filter area
    var role = (t.getAttribute && t.getAttribute('class')) || '';
    if (/filter|toggle|pill|tab/i.test(role) ||
        (t.tagName === 'BUTTON' && !t.classList.contains('chart-export-btn'))) {
      setTimeout(tryInject, 200);
    }
  }, true);

  // Public: expose for debugging
  window.exportChartCSV = exportChartCSV;
  window.__injectChartExportButtons = tryInject;
})();
</script>
"""


def remove_existing(html):
    """Remove any prior injection block (idempotent rerun)."""
    pat = re.compile(re.escape(MARKER_BEGIN) + r".*?" + re.escape(MARKER_END),
                     re.DOTALL)
    return pat.sub("", html)


def patch(html):
    html = remove_existing(html)
    body_end = html.rfind("</body>")
    if body_end < 0:
        raise RuntimeError("No </body> tag found")
    block = (
        "\n" + MARKER_BEGIN + "\n"
        + CSS_BLOCK + "\n" + JS_BLOCK + "\n"
        + MARKER_END + "\n"
    )
    return html[:body_end] + block + html[body_end:]


def main():
    dry = "--dry-run" in sys.argv
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()
    new_html = patch(html)
    delta = len(new_html) - len(html)
    print(f"index.html: {len(html):,} -> {len(new_html):,}  ({delta:+,} chars)")
    if dry:
        print("[dry-run] no changes written")
        return 0
    backup = INDEX_HTML + ".bak_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(INDEX_HTML, backup)
    print(f"Backup: {backup}")
    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(new_html)
    print("Done. Open index.html in a browser; each chart should now have a")
    print("⬇ CSV button in its top-right corner.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
