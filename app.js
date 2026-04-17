// ============================================================
// Target Women's Jeans Dashboard — app.js (ES5 ONLY)
// ============================================================

// ── State ────────────────────────────────────────────────────
var RAW;                          // set by build.py via window.DATA
var brandStats;                   // brand-level aggregates
var activeBrands = new Set();     // currently selected brands
var charts = {};                  // Chart.js instances {id: Chart}
var labelState = {};              // per-chart label on/off
var priceType = 'current';        // 'current' | 'original'

// ── Brand Colors ─────────────────────────────────────────────
var OB_COLOR  = {bg:'#CC0000', light:'rgba(204,0,0,0.18)', border:'#CC0000'};
var NB_COLOR  = {bg:'#002855', light:'rgba(0,40,85,0.18)', border:'#002855'};
var OB_TOTAL  = {bg:'#CC0000', light:'rgba(204,0,0,0.35)', border:'#CC0000'};
var NB_TOTAL  = {bg:'#002855', light:'rgba(0,40,85,0.35)', border:'#002855'};

// ── Helpers ──────────────────────────────────────────────────
function pctl(arr, p) {
  if (!arr.length) return 0;
  var s = arr.slice().sort(function(a,b){return a-b;});
  var i = (p/100)*(s.length-1);
  var lo = Math.floor(i), hi = Math.ceil(i);
  return s[lo] + (s[hi]-s[lo])*(i-lo);
}

function bS(arr) {
  if (!arr.length) return {min:0,q1:0,med:0,q3:0,max:0,wLo:0,wHi:0,iqr:0,mean:0};
  var s = arr.slice().sort(function(a,b){return a-b;});
  var q1=pctl(s,25), q3=pctl(s,75), iqr=q3-q1;
  var sum=0; for(var i=0;i<s.length;i++) sum+=s[i];
  return {min:s[0], q1:q1, med:pctl(s,50), q3:q3, max:s[s.length-1], iqr:iqr,
    wLo: Math.max(s[0], q1-1.5*iqr),
    wHi: Math.min(s[s.length-1], q3+1.5*iqr),
    mean: sum/s.length};
}

function dC(id) { if(charts[id]){charts[id].destroy(); delete charts[id];} }

function boxYMax(statsArr) {
  var mx=0;
  for(var i=0;i<statsArr.length;i++){if(statsArr[i].wHi>mx) mx=statsArr[i].wHi;}
  return mx*1.18;
}

function getFilteredRows() {
  return RAW.filter(function(r){ return activeBrands.has(r.brand); });
}

// ── Whisker Plugin (vertical bpPlugin REMOVED — only horizontal used) ────

// ── Logical Sort Orders ──────────────────────────────────────
var SORT_ORDERS = {
  'rise': ['Low Rise', 'Mid Rise', 'High Rise', 'Ultra-High Rise', 'Other'],
  'fabric_weight': ['Extra Lightweight', 'Lightweight', 'Midweight', 'Heavyweight'],
  'garment_length': ['Short', 'Capri', 'Crop', 'Ankle', 'Full'],
  'wash_category': ['Light Wash', 'Medium Wash', 'Dark Wash', 'Black', 'White/Cream', 'Grey', 'Brown/Tan', 'Earth Tones', 'Color']
};

function getLogicalSort(attribute) {
  var order = SORT_ORDERS[attribute];
  if (!order) return null;
  return function(a, b) {
    var ia = order.indexOf(a);
    var ib = order.indexOf(b);
    if (ia === -1) ia = 999;
    if (ib === -1) ib = 999;
    return ia - ib;
  };
}

// ── Info Tooltips (? buttons) ─────────────────────────────────
var CHART_INFO = {
  'priceBox': 'Prices are extracted per color combination from Target PDPs. Current Price = selling price at time of scrape. Original Price = pre-sale price; if a product is not on sale, original price = current price. Box shows IQR (Q1–Q3), whiskers extend to 1.5×IQR, vertical line = median, diamond = mean.',
  'rise': 'Rise is standardized from Target\'s raw rise field. Low Rise, Mid Rise (includes "Classic" and "Regular"), High Rise, and Ultra-High Rise. "Other" = rise values that don\'t match standard categories (very few).',
  'legShape': 'Leg shape is parsed from Target\'s Fit field (e.g. "Straight Leg with a Regular Fit" → Straight). Categories: Straight, Skinny, Wide, Bootcut, Slim, Tapered, Flare, Jegging, Barrel, Boyfriend, Relaxed, Mom. "Other" = unrecognized leg shapes.',
  'fitStyle': 'Fit style is the second part of Target\'s Fit field (e.g. "...with a Contemporary Fit" → Contemporary). Categories: Regular, Contemporary (modern/updated fit between Regular and Slim), Casual, Slim, Straight, Curvy, Loose, Relaxed, Stretch. "Other" = unrecognized fit styles.',
  'length': 'Garment length from Target specs, grouped: Short = above knee. Capri = at/below knee (includes At Knee, Below Knee). Crop = mid-calf (includes At Calf, Low Calf, 7/8). Ankle = ankle-length. Full = full-length.',
  'weight': 'Fabric weight from Target specs. Extra Lightweight → Lightweight → Midweight (includes "Year Round Fabric Construction") → Heavyweight.',
  'wash': 'Color/wash mapped from the raw color name: Light Wash (light, bleach, faded), Medium Wash (medium, stonewash, plain "blue"/"denim"), Dark Wash (dark, indigo, rinse, navy, midnight), Black, White/Cream, Grey (grey, charcoal, gunmetal), Brown/Tan (chocolate, toffee, bourbon), Earth Tones (khaki, olive, pine, sage), Color = all other non-denim colors (pink, red, emerald, etc.).',
  'inseam': 'Inseam in inches, parsed from Target specs. Only rows with a numeric inseam value are included.',
  'cotton': 'Cotton percentage parsed from the "% Cotton" field, or extracted from the Material field (e.g. "98% Cotton, 2% Spandex" → 98). Only rows with identifiable cotton content are included.'
};

function toggleInfo(chartId) {
  var el = document.getElementById('info-' + chartId);
  if (!el) return;
  if (el.style.display === 'none' || el.style.display === '') {
    el.style.display = 'block';
  } else {
    el.style.display = 'none';
  }
}

// ── Label Toggle ─────────────────────────────────────────────
function toggleLabels(chartId, show) {
  labelState[chartId] = show;
  var ch = charts[chartId];
  if (!ch) return;
  if (ch.options.plugins && ch.options.plugins.datalabels) {
    ch.options.plugins.datalabels.display = show;
  }
  ch.update();
  var btn = document.getElementById('lb-' + chartId);
  if (btn) btn.className = show ? 'filter-btn active' : 'filter-btn';
}

// ── Brand Selector ───────────────────────────────────────────
function renderBrandSelector() {
  var container = document.getElementById('brandSelector');
  if (!container) return;
  container.innerHTML = '';

  var brands = Object.keys(brandStats).sort(function(a,b){
    return brandStats[b].color_combos - brandStats[a].color_combos;
  });
  var owned = brands.filter(function(b){return brandStats[b].is_owned;});
  var national = brands.filter(function(b){return !brandStats[b].is_owned;});

  // Select All / None row
  var ctrlRow = document.createElement('div');
  ctrlRow.className = 'gf-row';
  ctrlRow.style.marginBottom = '12px';

  var selAll = document.createElement('button');
  selAll.className = 'filter-btn';
  selAll.textContent = 'Select All';
  selAll.onclick = function(){ brands.forEach(function(b){activeBrands.add(b);}); renderBrandSelector(); renderAll(); };
  ctrlRow.appendChild(selAll);

  var selNone = document.createElement('button');
  selNone.className = 'filter-btn';
  selNone.textContent = 'Clear All';
  selNone.onclick = function(){ activeBrands.clear(); renderBrandSelector(); renderAll(); };
  ctrlRow.appendChild(selNone);

  var selOB = document.createElement('button');
  selOB.className = 'filter-btn';
  selOB.textContent = 'Owned Only';
  selOB.onclick = function(){ activeBrands.clear(); owned.forEach(function(b){activeBrands.add(b);}); renderBrandSelector(); renderAll(); };
  ctrlRow.appendChild(selOB);

  var selNB = document.createElement('button');
  selNB.className = 'filter-btn';
  selNB.textContent = 'National Only';
  selNB.onclick = function(){ activeBrands.clear(); national.forEach(function(b){activeBrands.add(b);}); renderBrandSelector(); renderAll(); };
  ctrlRow.appendChild(selNB);

  container.appendChild(ctrlRow);

  // Owned
  var obLabel = document.createElement('div');
  obLabel.className = 'gf-label';
  obLabel.textContent = 'OWNED BRANDS';
  container.appendChild(obLabel);

  var obRow = document.createElement('div');
  obRow.className = 'gf-row';
  obRow.style.marginBottom = '14px';
  owned.forEach(function(b){ obRow.appendChild(makePill(b, OB_COLOR)); });
  container.appendChild(obRow);

  // National
  var nbLabel = document.createElement('div');
  nbLabel.className = 'gf-label';
  nbLabel.textContent = 'NATIONAL BRANDS';
  container.appendChild(nbLabel);

  var nbRow = document.createElement('div');
  nbRow.className = 'gf-row';
  national.forEach(function(b){ nbRow.appendChild(makePill(b, NB_COLOR)); });
  container.appendChild(nbRow);
}

function makePill(brand, colorSet) {
  var isOn = activeBrands.has(brand);
  var btn = document.createElement('button');
  btn.className = isOn ? 'filter-btn active' : 'filter-btn';
  if (isOn) {
    btn.style.background = colorSet.bg;
    btn.style.color = '#fff';
    btn.style.borderColor = colorSet.bg;
  }
  btn.innerHTML = '<span class="brand-color-dot" style="background:' + colorSet.bg + '"></span>' + brand + ' (' + brandStats[brand].count + ')';
  btn.onclick = function() {
    if (activeBrands.has(brand)) { activeBrands.delete(brand); }
    else { activeBrands.add(brand); }
    renderBrandSelector();
    renderAll();
  };
  return btn;
}

// ── Price Box & Whisker ──────────────────────────────────────
function renderPriceChart() {
  dC('priceBox');
  var canvasEl = document.getElementById('priceBoxCanvas');
  if (!canvasEl) return;

  var priceField = priceType === 'current' ? 'prices' : 'original_prices';
  var brands = Object.keys(brandStats).filter(function(b){return activeBrands.has(b);});

  // Helper: get median price for sorting
  function medianPrice(b) {
    var p = brandStats[b][priceField];
    if (!p || !p.length) return 0;
    return pctl(p, 50);
  }

  // Sort OB and NB by median price (lowest to highest, top to bottom)
  var owned = brands.filter(function(b){return brandStats[b].is_owned;})
    .sort(function(a,b){return medianPrice(a) - medianPrice(b);});
  var national = brands.filter(function(b){return !brandStats[b].is_owned;})
    .sort(function(a,b){return medianPrice(a) - medianPrice(b);});

  // Build ordered label list: OB brands | OB TOTAL | NB TOTAL | NB brands
  var labels = [];
  var statsArr = [];
  var colors = [];
  var boldIdx = [];

  owned.forEach(function(b) {
    var prices = brandStats[b][priceField];
    labels.push(b + ' (n=' + prices.length + ')');
    statsArr.push(bS(prices));
    colors.push(OB_COLOR);
  });

  // OB TOTAL
  var obAll = [];
  owned.forEach(function(b){ obAll = obAll.concat(brandStats[b][priceField]); });
  if (obAll.length > 0) {
    labels.push('OB TOTAL (n=' + obAll.length + ')');
    statsArr.push(bS(obAll));
    colors.push(OB_TOTAL);
    boldIdx.push(labels.length - 1);
  }

  // NB TOTAL
  var nbAll = [];
  national.forEach(function(b){ nbAll = nbAll.concat(brandStats[b][priceField]); });
  if (nbAll.length > 0) {
    labels.push('NB TOTAL (n=' + nbAll.length + ')');
    statsArr.push(bS(nbAll));
    colors.push(NB_TOTAL);
    boldIdx.push(labels.length - 1);
  }

  national.forEach(function(b) {
    var prices = brandStats[b][priceField];
    labels.push(b + ' (n=' + prices.length + ')');
    statsArr.push(bS(prices));
    colors.push(NB_COLOR);
  });

  if (labels.length === 0) return;

  // Dynamic height
  var wrapEl = document.getElementById('priceBoxWrap');
  if (wrapEl) wrapEl.style.minHeight = Math.max(500, labels.length * 30) + 'px';

  // Stacked bar datasets: pedestal (transparent) + IQR box
  var pedestalData = statsArr.map(function(s){return s.q1;});
  var iqrData = statsArr.map(function(s){return s.q3 - s.q1;});
  var bgColors = colors.map(function(c){return c.light;});
  var borderColors = colors.map(function(c){return c.border;});

  var show = labelState['priceBox'] !== false;

  charts['priceBox'] = new Chart(canvasEl, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [
        {label:'_p', data:pedestalData, backgroundColor:'transparent', borderWidth:0,
         barPercentage:0.55, categoryPercentage:0.8},
        {label:'IQR', data:iqrData, backgroundColor:bgColors, borderColor:borderColors,
         borderWidth:2, borderRadius:4, barPercentage:0.55, categoryPercentage:0.8}
      ]
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {display: false},
        datalabels: {
          display: show,
          anchor: 'center',
          align: 'center',
          font: {size:9, weight:'bold', family:'Montserrat,sans-serif'},
          color: function(ctx) {
            if (ctx.datasetIndex === 0) return 'transparent';
            return '#1a1a2e';
          },
          backgroundColor: function(ctx) {
            if (ctx.datasetIndex === 0) return 'transparent';
            return 'rgba(255,255,255,0.75)';
          },
          borderRadius: 3,
          padding: {top:2,bottom:2,left:3,right:3},
          formatter: function(val, ctx) {
            if (ctx.datasetIndex === 0) return '';
            var s = statsArr[ctx.dataIndex];
            return '$' + Math.round(s.med);
          }
        },
        tooltip: {
          callbacks: {
            label: function(ctx) {
              if (ctx.datasetIndex === 0) return '';
              var s = statsArr[ctx.dataIndex];
              return 'Med $' + s.med.toFixed(0) + '  |  Mean $' + s.mean.toFixed(0) + '  |  Q1 $' + s.q1.toFixed(0) + '  |  Q3 $' + s.q3.toFixed(0) + '  |  Range $' + s.wLo.toFixed(0) + '-$' + s.wHi.toFixed(0);
            }
          }
        }
      },
      scales: {
        x: {
          stacked: true,
          suggestedMax: boxYMax(statsArr),
          grid: {color:'rgba(0,0,0,0.04)'},
          ticks: {
            font: {family:'Montserrat,sans-serif', size:10},
            callback: function(v){return '$'+v;}
          }
        },
        y: {
          stacked: true,
          grid: {display: false},
          ticks: {
            font: function(ctx) {
              var idx = ctx.index;
              var isBold = boldIdx.indexOf(idx) !== -1;
              return {family:'Montserrat,sans-serif', size: isBold ? 12 : 10, weight: isBold ? 'bold' : 'normal'};
            }
          }
        }
      }
    },
    plugins: [ChartDataLabels]
  });

  // Attach whisker data — note: horizontal chart, so whiskers draw on x-axis
  charts['priceBox']._bpd = statsArr.map(function(s, i) {
    return Object.assign({}, s, {color: colors[i].border});
  });
}

// Override whisker plugin for horizontal box plots
var hbpPlugin = {
  id: 'hbpWhiskers',
  afterDatasetsDraw: function(ch) {
    if (!ch._bpd) return;
    var ctx = ch.ctx;
    var meta = ch.getDatasetMeta(1);
    if (!meta || !meta.data) return;
    ch._bpd.forEach(function(s, i) {
      if (!s || !meta.data[i]) return;
      var bar = meta.data[i];
      var y = bar.y, hh = bar.height ? bar.height/2 : 12, ww = hh*0.6;
      var xs = ch.scales.x;
      if (!xs) return;
      ctx.save();
      ctx.strokeStyle = s.color; ctx.lineWidth = 2;
      // Left whisker (Q1 to wLo)
      ctx.beginPath(); ctx.moveTo(xs.getPixelForValue(s.q1), y);
      ctx.lineTo(xs.getPixelForValue(s.wLo), y); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(xs.getPixelForValue(s.wLo), y-ww);
      ctx.lineTo(xs.getPixelForValue(s.wLo), y+ww); ctx.stroke();
      // Right whisker (Q3 to wHi)
      ctx.beginPath(); ctx.moveTo(xs.getPixelForValue(s.q3), y);
      ctx.lineTo(xs.getPixelForValue(s.wHi), y); ctx.stroke();
      ctx.beginPath(); ctx.moveTo(xs.getPixelForValue(s.wHi), y-ww);
      ctx.lineTo(xs.getPixelForValue(s.wHi), y+ww); ctx.stroke();
      // Median line (white bg + colored)
      var xM = xs.getPixelForValue(s.med);
      ctx.lineWidth = 2.5; ctx.strokeStyle = '#fff';
      ctx.beginPath(); ctx.moveTo(xM, y-hh+2); ctx.lineTo(xM, y+hh-2); ctx.stroke();
      ctx.strokeStyle = s.color; ctx.lineWidth = 1.5;
      ctx.beginPath(); ctx.moveTo(xM, y-hh+2); ctx.lineTo(xM, y+hh-2); ctx.stroke();
      // Mean diamond
      if (s.mean !== undefined) {
        var xMn = xs.getPixelForValue(s.mean);
        var ds = Math.min(5, hh * 0.4);  // diamond size
        ctx.fillStyle = '#fff';
        ctx.beginPath(); ctx.moveTo(xMn, y-ds); ctx.lineTo(xMn+ds, y); ctx.lineTo(xMn, y+ds); ctx.lineTo(xMn-ds, y); ctx.closePath(); ctx.fill();
        ctx.strokeStyle = s.color; ctx.lineWidth = 1.5;
        ctx.beginPath(); ctx.moveTo(xMn, y-ds); ctx.lineTo(xMn+ds, y); ctx.lineTo(xMn, y+ds); ctx.lineTo(xMn-ds, y); ctx.closePath(); ctx.stroke();
      }
      ctx.restore();
    });
  }
};
Chart.register(hbpPlugin);

// ── Attribute Grouped Bar (OB vs NB) ────────────────────────
// Excludes rows where attribute is null/empty/Unknown
// Shows n= count per group
function renderGroupedBar(chartId, canvasId, attribute, chartTitle) {
  dC(chartId);
  var canvas = document.getElementById(canvasId);
  if (!canvas) return;

  var rows = getFilteredRows();

  // Filter to rows that have valid data for this attribute
  var validRows = rows.filter(function(r) {
    var v = r[attribute];
    return v !== null && v !== undefined && v !== '';
  });

  // Count by brand type
  var obCounts = {};
  var nbCounts = {};
  var obTotal = 0;
  var nbTotal = 0;

  validRows.forEach(function(r) {
    var val = String(r[attribute]);
    if (r.is_owned_brand) {
      obCounts[val] = (obCounts[val] || 0) + 1;
      obTotal++;
    } else {
      nbCounts[val] = (nbCounts[val] || 0) + 1;
      nbTotal++;
    }
  });

  // Build sorted labels — use logical order if defined, otherwise frequency desc
  var allVals = {};
  Object.keys(obCounts).forEach(function(k){allVals[k] = (allVals[k]||0) + obCounts[k];});
  Object.keys(nbCounts).forEach(function(k){allVals[k] = (allVals[k]||0) + nbCounts[k];});
  var logicalSort = getLogicalSort(attribute);
  var labels;
  if (logicalSort) {
    labels = Object.keys(allVals).sort(logicalSort);
  } else {
    labels = Object.keys(allVals).sort(function(a,b){return allVals[b] - allVals[a];});
  }

  if (labels.length === 0) {
    // No data — show message
    var wrapEl = canvas.parentElement;
    if (wrapEl) {
      var msgEl = wrapEl.querySelector('.no-data-msg');
      if (!msgEl) {
        msgEl = document.createElement('div');
        msgEl.className = 'no-data-msg';
        msgEl.style.cssText = 'text-align:center;color:var(--fg3);font-size:.8rem;padding:40px 0;';
        wrapEl.appendChild(msgEl);
      }
      msgEl.textContent = 'No data available for selected brands';
    }
    return;
  }

  // Remove any no-data message
  var wrapEl2 = canvas.parentElement;
  if (wrapEl2) {
    var old = wrapEl2.querySelector('.no-data-msg');
    if (old) old.remove();
  }

  var obData = labels.map(function(l){return obTotal>0 ? ((obCounts[l]||0)/obTotal*100) : 0;});
  var nbData = labels.map(function(l){return nbTotal>0 ? ((nbCounts[l]||0)/nbTotal*100) : 0;});

  // Update subtitle with n=
  var subtitleEl = document.getElementById('sub-' + chartId);
  if (subtitleEl) {
    subtitleEl.textContent = 'Owned Brands n=' + obTotal + '  |  National Brands n=' + nbTotal + '  (rows with data only)';
  }

  var show = labelState[chartId] !== false;

  charts[chartId] = new Chart(canvas, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [
        {label:'Owned Brands (n=' + obTotal + ')', data:obData,
         backgroundColor:OB_COLOR.light, borderColor:OB_COLOR.border, borderWidth:1.5, borderRadius:3},
        {label:'National Brands (n=' + nbTotal + ')', data:nbData,
         backgroundColor:NB_COLOR.light, borderColor:NB_COLOR.border, borderWidth:1.5, borderRadius:3}
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {position:'bottom', labels:{font:{family:'Montserrat,sans-serif',size:11,weight:'600'}}},
        datalabels: {
          display: show,
          anchor: 'end', align: 'end',
          font: {size:9, weight:'600', family:'Montserrat,sans-serif'},
          color: '#1a1a2e',
          backgroundColor: 'rgba(255,255,255,0.75)',
          borderRadius: 3,
          padding: {top:1,bottom:1,left:3,right:3},
          formatter: function(v){ return v > 0 ? v.toFixed(1)+'%' : ''; }
        },
        tooltip: {
          callbacks: {
            label: function(ctx) {
              var raw = ctx.datasetIndex === 0 ? (obCounts[labels[ctx.dataIndex]]||0) : (nbCounts[labels[ctx.dataIndex]]||0);
              return ctx.dataset.label + ': ' + ctx.raw.toFixed(1) + '% (' + raw + ' CCs)';
            }
          }
        }
      },
      scales: {
        x: {grid:{display:false}, ticks:{font:{family:'Montserrat,sans-serif',size:10,weight:'500'}}},
        y: {beginAtZero:true, suggestedMax: Math.max.apply(null, obData.concat(nbData))*1.2,
            grid:{color:'rgba(0,0,0,0.04)'},
            ticks:{font:{family:'Montserrat,sans-serif',size:10}, callback:function(v){return v+'%';}}}
      }
    },
    plugins: [ChartDataLabels]
  });
}

// ── Histogram (Inseam, Cotton %) ─────────────────────────────
function renderHistogram(chartId, canvasId, attribute, chartTitle, unit) {
  dC(chartId);
  var canvas = document.getElementById(canvasId);
  if (!canvas) return;

  var rows = getFilteredRows();

  var obVals = [];
  var nbVals = [];
  rows.forEach(function(r) {
    var v = r[attribute];
    if (v !== null && v !== undefined && v !== '') {
      if (r.is_owned_brand) { obVals.push(v); }
      else { nbVals.push(v); }
    }
  });

  var allVals = obVals.concat(nbVals);
  if (allVals.length === 0) return;

  // Update subtitle
  var subtitleEl = document.getElementById('sub-' + chartId);
  if (subtitleEl) {
    subtitleEl.textContent = 'Owned Brands n=' + obVals.length + '  |  National Brands n=' + nbVals.length + '  (rows with data only)';
  }

  var minV = Math.min.apply(null, allVals);
  var maxV = Math.max.apply(null, allVals);
  var binCount = Math.min(15, Math.max(5, Math.ceil(Math.sqrt(allVals.length))));
  var binSize = (maxV - minV + 0.01) / binCount;
  if (binSize === 0) binSize = 1;

  var bins = [];
  for (var i = 0; i < binCount; i++) {
    bins.push({min: minV + i*binSize, max: minV + (i+1)*binSize, ob:0, nb:0});
  }

  function toBin(v) { return Math.min(binCount-1, Math.floor((v-minV)/binSize)); }

  obVals.forEach(function(v){bins[toBin(v)].ob++;});
  nbVals.forEach(function(v){bins[toBin(v)].nb++;});

  // Convert to % of each group
  var labels = bins.map(function(b){
    return Math.round(b.min) + (unit||'') + '-' + Math.round(b.max) + (unit||'');
  });
  var obData = bins.map(function(b){return obVals.length>0 ? (b.ob/obVals.length*100) : 0;});
  var nbData = bins.map(function(b){return nbVals.length>0 ? (b.nb/nbVals.length*100) : 0;});

  var show = labelState[chartId] !== false;

  charts[chartId] = new Chart(canvas, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [
        {label:'Owned Brands (n=' + obVals.length + ')', data:obData,
         backgroundColor:OB_COLOR.light, borderColor:OB_COLOR.border, borderWidth:1.5, borderRadius:2},
        {label:'National Brands (n=' + nbVals.length + ')', data:nbData,
         backgroundColor:NB_COLOR.light, borderColor:NB_COLOR.border, borderWidth:1.5, borderRadius:2}
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {position:'bottom', labels:{font:{family:'Montserrat,sans-serif',size:11,weight:'600'}}},
        datalabels: {
          display: show,
          anchor: 'end', align: 'end',
          font: {size:8, weight:'600', family:'Montserrat,sans-serif'},
          color: '#1a1a2e',
          formatter: function(v){ return v>0.5 ? v.toFixed(1)+'%' : ''; }
        },
        tooltip: {
          callbacks: {
            label: function(ctx) {
              var raw = ctx.datasetIndex === 0 ? bins[ctx.dataIndex].ob : bins[ctx.dataIndex].nb;
              return ctx.dataset.label + ': ' + ctx.raw.toFixed(1) + '% (' + raw + ' CCs)';
            }
          }
        }
      },
      scales: {
        x: {grid:{display:false}, ticks:{font:{family:'Montserrat,sans-serif',size:9,weight:'500'},maxRotation:45}},
        y: {beginAtZero:true,
            grid:{color:'rgba(0,0,0,0.04)'},
            ticks:{font:{family:'Montserrat,sans-serif',size:10}, callback:function(v){return v+'%';}}}
      }
    },
    plugins: [ChartDataLabels]
  });
}

// ── KPI Cards ────────────────────────────────────────────────
function renderKPIs() {
  var rows = getFilteredRows();
  var obRows = rows.filter(function(r){return r.is_owned_brand;});
  var nbRows = rows.filter(function(r){return !r.is_owned_brand;});

  var obBrands = {};
  var nbBrands = {};
  obRows.forEach(function(r){obBrands[r.brand]=1;});
  nbRows.forEach(function(r){nbBrands[r.brand]=1;});

  function avg(arr){if(!arr.length)return 0; var s=0; arr.forEach(function(v){s+=v;}); return s/arr.length;}

  var obPrices = obRows.filter(function(r){return r.current_price;}).map(function(r){return r.current_price;});
  var nbPrices = nbRows.filter(function(r){return r.current_price;}).map(function(r){return r.current_price;});

  setText('kpi-total-ccs', rows.length.toLocaleString());
  setText('kpi-ob-brands', Object.keys(obBrands).length);
  setText('kpi-nb-brands', Object.keys(nbBrands).length);
  setText('kpi-ob-ccs', obRows.length.toLocaleString());
  setText('kpi-nb-ccs', nbRows.length.toLocaleString());
  setText('kpi-ob-avg', obPrices.length ? '$' + avg(obPrices).toFixed(2) : 'N/A');
  setText('kpi-nb-avg', nbPrices.length ? '$' + avg(nbPrices).toFixed(2) : 'N/A');
}

function setText(id, val) {
  var el = document.getElementById(id);
  if (el) el.textContent = val;
}

// ── Price Type Toggle ────────────────────────────────────────
function setPriceType(type) {
  priceType = type;
  var btnC = document.getElementById('btn-price-current');
  var btnO = document.getElementById('btn-price-original');
  if (btnC) btnC.className = type === 'current' ? 'view-toggle-btn active' : 'view-toggle-btn';
  if (btnO) btnO.className = type === 'original' ? 'view-toggle-btn active' : 'view-toggle-btn';
  renderPriceChart();
}

// ── Side Nav ─────────────────────────────────────────────────
function setupNav() {
  var sections = document.querySelectorAll('.section[id]');
  var links = document.querySelectorAll('.side-nav a');
  if (!sections.length || !links.length) return;

  var observer = new IntersectionObserver(function(entries) {
    entries.forEach(function(entry) {
      if (entry.isIntersecting) {
        links.forEach(function(l){l.classList.remove('active');});
        var target = document.querySelector('.side-nav a[href="#' + entry.target.id + '"]');
        if (target) target.classList.add('active');
      }
    });
  }, {rootMargin: '-20% 0px -70% 0px'});

  sections.forEach(function(s){observer.observe(s);});
}

// ── Render All ───────────────────────────────────────────────
function renderAll() {
  renderKPIs();
  renderPriceChart();
  renderGroupedBar('rise', 'riseCanvas', 'rise', 'Rise Distribution');
  renderGroupedBar('legShape', 'legShapeCanvas', 'leg_shape', 'Leg Shape Distribution');
  renderGroupedBar('length', 'lengthCanvas', 'garment_length', 'Garment Length Distribution');
  renderGroupedBar('weight', 'weightCanvas', 'fabric_weight', 'Fabric Weight Distribution');
  renderGroupedBar('wash', 'washCanvas', 'wash_category', 'Wash / Color Distribution');
  renderGroupedBar('fitStyle', 'fitStyleCanvas', 'fit_style', 'Fit Style Distribution');
  renderHistogram('inseam', 'inseamCanvas', 'inseam', 'Inseam Distribution', '"');
  renderHistogram('cotton', 'cottonCanvas', 'cotton_percent', 'Cotton Content Distribution', '%');
  renderHeatmap();
}

// ── Interactive Heatmap ──────────────────────────────────────
var HM_DIMS = {
  'rise':           'Rise',
  'leg_shape':      'Leg Shape',
  'fit_style':      'Fit Style',
  'garment_length': 'Garment Length',
  'fabric_weight':  'Fabric Weight',
  'wash_category':  'Wash / Color'
};

var hmDimX = 'rise';
var hmDimY = 'leg_shape';

function setupHeatmapControls() {
  var selX = document.getElementById('hm-dim-x');
  var selY = document.getElementById('hm-dim-y');
  if (!selX || !selY) return;

  var keys = Object.keys(HM_DIMS);
  keys.forEach(function(k) {
    var optX = document.createElement('option');
    optX.value = k; optX.textContent = HM_DIMS[k];
    if (k === hmDimX) optX.selected = true;
    selX.appendChild(optX);

    var optY = document.createElement('option');
    optY.value = k; optY.textContent = HM_DIMS[k];
    if (k === hmDimY) optY.selected = true;
    selY.appendChild(optY);
  });

  selX.onchange = function(){ hmDimX = this.value; renderHeatmap(); };
  selY.onchange = function(){ hmDimY = this.value; renderHeatmap(); };
}

function renderHeatmap() {
  var container = document.getElementById('heatmapContainer');
  if (!container) return;
  container.innerHTML = '';

  var rows = getFilteredRows();
  var fieldX = hmDimX;
  var fieldY = hmDimY;

  // Filter to rows with both dimensions
  var valid = rows.filter(function(r) {
    var vx = r[fieldX], vy = r[fieldY];
    return vx && vx !== '' && vy && vy !== '';
  });

  if (valid.length === 0) {
    container.innerHTML = '<div style="text-align:center;color:var(--fg3);padding:40px">No data for selected dimensions</div>';
    return;
  }

  // Separate OB and NB
  var obRows = valid.filter(function(r){return r.is_owned_brand;});
  var nbRows = valid.filter(function(r){return !r.is_owned_brand;});

  // Get unique values, sorted logically if available
  var xFreq = {};
  var yFreq = {};
  valid.forEach(function(r) {
    xFreq[r[fieldX]] = (xFreq[r[fieldX]]||0) + 1;
    yFreq[r[fieldY]] = (yFreq[r[fieldY]]||0) + 1;
  });

  var logSortX = getLogicalSort(fieldX);
  var logSortY = getLogicalSort(fieldY);
  var xLabels = Object.keys(xFreq).sort(logSortX || function(a,b){return xFreq[b]-xFreq[a];});
  var yLabels = Object.keys(yFreq).sort(logSortY || function(a,b){return yFreq[b]-yFreq[a];});

  // Build count matrices (as % of group total)
  function buildMatrix(groupRows, total) {
    var m = {};
    groupRows.forEach(function(r) {
      var key = r[fieldX] + '||' + r[fieldY];
      m[key] = (m[key]||0) + 1;
    });
    var result = {};
    Object.keys(m).forEach(function(k){ result[k] = total > 0 ? (m[k]/total*100) : 0; });
    return result;
  }

  // Also build raw count matrices
  function buildCountMatrix(groupRows) {
    var m = {};
    groupRows.forEach(function(r) {
      var key = r[fieldX] + '||' + r[fieldY];
      m[key] = (m[key]||0) + 1;
    });
    return m;
  }

  var obMatrix = buildMatrix(obRows, obRows.length);
  var nbMatrix = buildMatrix(nbRows, nbRows.length);
  var obCounts = buildCountMatrix(obRows);
  var nbCounts = buildCountMatrix(nbRows);

  // Find max absolute difference for color scale
  var maxDiff = 0;
  yLabels.forEach(function(yl) {
    xLabels.forEach(function(xl) {
      var key = xl + '||' + yl;
      var diff = Math.abs((obMatrix[key]||0) - (nbMatrix[key]||0));
      if (diff > maxDiff) maxDiff = diff;
    });
  });
  if (maxDiff === 0) maxDiff = 1;

  // Build HTML table
  var h = '';
  h += '<div style="font-size:.72rem;color:var(--fg3);margin-bottom:10px;text-align:center">';
  h += 'OB n=' + obRows.length + ' CCs  |  NB n=' + nbRows.length + ' CCs  |  ';
  h += '<span style="display:inline-block;width:12px;height:12px;background:rgba(204,0,0,0.4);border-radius:2px;vertical-align:middle"></span> OB over-indexes  ';
  h += '<span style="display:inline-block;width:12px;height:12px;background:rgba(0,40,85,0.4);border-radius:2px;vertical-align:middle"></span> NB over-indexes  ';
  h += '<span style="display:inline-block;width:12px;height:12px;background:var(--bg3);border-radius:2px;vertical-align:middle"></span> Similar';
  h += '</div>';

  h += '<div style="overflow-x:auto">';
  h += '<table style="width:100%;border-collapse:separate;border-spacing:2px;font-family:Montserrat,sans-serif">';

  // Header row
  h += '<tr><th style="padding:6px 10px;font-size:.68rem;font-weight:700;color:var(--fg2);text-align:left;vertical-align:bottom;min-width:100px">';
  h += HM_DIMS[fieldY] + ' \\ ' + HM_DIMS[fieldX];
  h += '</th>';
  xLabels.forEach(function(xl) {
    h += '<th style="padding:6px 4px;font-size:.66rem;font-weight:700;color:var(--fg2);text-align:center;min-width:70px">' + xl + '</th>';
  });
  h += '</tr>';

  // Data rows
  yLabels.forEach(function(yl) {
    h += '<tr>';
    h += '<td style="padding:6px 10px;font-size:.68rem;font-weight:600;color:var(--fg2);white-space:nowrap">' + yl + '</td>';

    xLabels.forEach(function(xl) {
      var key = xl + '||' + yl;
      var obPct = obMatrix[key] || 0;
      var nbPct = nbMatrix[key] || 0;
      var obN = obCounts[key] || 0;
      var nbN = nbCounts[key] || 0;
      var diff = obPct - nbPct;

      // Diverging color: red if OB over-indexes, blue if NB over-indexes
      var bgColor;
      var absDiff = Math.abs(diff);
      var intensity = Math.min(0.55, (absDiff / maxDiff) * 0.55);
      if (obPct === 0 && nbPct === 0) {
        bgColor = 'var(--bg2)';
      } else if (diff > 0.5) {
        bgColor = 'rgba(204,0,0,' + intensity.toFixed(2) + ')';
      } else if (diff < -0.5) {
        bgColor = 'rgba(0,40,85,' + intensity.toFixed(2) + ')';
      } else {
        bgColor = 'var(--bg3)';
      }

      var textColor = intensity > 0.35 ? '#fff' : 'var(--fg)';

      h += '<td style="padding:4px 3px;background:' + bgColor + ';border-radius:4px;text-align:center;vertical-align:middle">';
      if (obPct > 0 || nbPct > 0) {
        h += '<div style="font-size:.62rem;font-weight:700;color:' + textColor + ';line-height:1.2">';
        h += '<span style="color:' + (intensity > 0.35 ? '#ffcdd2' : '#CC0000') + '">' + obPct.toFixed(1) + '%</span>';
        h += ' <span style="opacity:.5">|</span> ';
        h += '<span style="color:' + (intensity > 0.35 ? '#bbdefb' : '#002855') + '">' + nbPct.toFixed(1) + '%</span>';
        h += '</div>';
      }
      h += '</td>';
    });
    h += '</tr>';
  });

  h += '</table></div>';

  container.innerHTML = h;
}

// ── Init ─────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', function() {
  RAW = window.DATA.rows;
  brandStats = window.DATA.brand_stats;

  // Select brands with >10 CCs by default
  Object.keys(brandStats).forEach(function(b){
    if (brandStats[b].color_combos > 10) {
      activeBrands.add(b);
    }
  });

  renderBrandSelector();
  setupHeatmapControls();
  renderAll();
  setupNav();
});
