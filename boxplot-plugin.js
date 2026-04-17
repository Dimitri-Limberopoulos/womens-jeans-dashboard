// Simple Box Plot plugin for Chart.js 4.4.4 (ES5)

Chart.register({
  id: 'boxplot',
  afterDatasetsDraw: function(chart) {
    var ctx = chart.ctx;
    var yScale = chart.scales.y;
    var xScale = chart.scales.x;

    chart.data.datasets[0].data.forEach(function(item, index) {
      if (!item || item.min === undefined) return;

      var x = xScale.getPixelForValue(item.x || index);
      var minY = yScale.getPixelForValue(item.min);
      var q1Y = yScale.getPixelForValue(item.q1);
      var medianY = yScale.getPixelForValue(item.median);
      var q3Y = yScale.getPixelForValue(item.q3);
      var maxY = yScale.getPixelForValue(item.max);

      var boxWidth = 40;

      // Draw whiskers (min-q1 and q3-max)
      ctx.strokeStyle = item.borderColor;
      ctx.lineWidth = 1;
      ctx.beginPath();
      ctx.moveTo(x, minY);
      ctx.lineTo(x, maxY);
      ctx.stroke();

      // Draw box (q1-q3)
      ctx.fillStyle = item.backgroundColor;
      ctx.strokeStyle = item.borderColor;
      ctx.lineWidth = item.borderWidth || 2;
      ctx.fillRect(x - boxWidth / 2, q3Y, boxWidth, q1Y - q3Y);
      ctx.strokeRect(x - boxWidth / 2, q3Y, boxWidth, q1Y - q3Y);

      // Draw median line
      ctx.strokeStyle = item.borderColor;
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.moveTo(x - boxWidth / 2, medianY);
      ctx.lineTo(x + boxWidth / 2, medianY);
      ctx.stroke();

      // Draw min/max whisker caps
      ctx.beginPath();
      ctx.moveTo(x - boxWidth / 4, minY);
      ctx.lineTo(x + boxWidth / 4, minY);
      ctx.stroke();

      ctx.beginPath();
      ctx.moveTo(x - boxWidth / 4, maxY);
      ctx.lineTo(x + boxWidth / 4, maxY);
      ctx.stroke();
    });
  }
});
