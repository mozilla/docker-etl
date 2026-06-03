// vim: set ts=2 sw=2 tw=99 et:

ChartDisplay.prototype.drawWebGL = function(version)
{
  var obj = this.ensureData('webgl-statistics.json', this.drawWebGL.bind(this, version));
  if (!obj)
    return;

  this.drawSampleInfo(obj);

  // Per-version data is under 'webgl1' or 'webgl2'.
  obj = obj[version];

  var name = (version == 'webgl1') ? 'WebGL 1' : 'WebGL 2';
  var sampleText = 'Number of sessions with ' + name + ' attempts: ' +
                   (obj.successes.count + obj.failures.count);

  $('#viewport').append(
    $("<p></p>").append(
      $("<strong></strong>").text(sampleText)
    )
  );

  var elt = this.prepareChartDiv(
    version + '-ratio',
    name + ' Success/Failure Ratio',
    600, 300);
  var series = this.mapToSeries({
    'Success': obj.successes.count,
    'Failures': obj.failures.count,
  });
  this.drawPieChart(elt, series);

  var elt = this.prepareChartDiv(
    version + '-success-by-os',
    name + ' Success, by Operating System',
    600, 300);
  this.drawPieChart(elt, this.buildOSSeries(obj.successes.os, 0.01));

  var elt = this.prepareChartDiv(
    version + '-fail-by-os',
    name + ' Failures, by Operating System',
    600, 300);
  this.drawPieChart(elt, this.buildOSSeries(obj.failures.os, 0.01));

  var elt = this.prepareChartDiv(
    version + '-fail-by-vendor',
    name + ' Failures, by Device Vendor',
    600, 300);
  this.drawPieChart(elt, this.buildVendorSeries(obj.failures.vendors, 0.01));

  var elt = this.prepareChartDiv(
    version + '-fail-by-chipset',
    name + ' Failures, by Device Chipset',
    600, 300);
  this.drawPieChart(elt, this.buildChipsetSeries(obj.failures.devices, 0.015));

  var elt = this.prepareChartDiv(
    version + '-fail-by-driver',
    name + ' Failures, by Device Driver',
    600, 300);
  this.drawPieChart(elt, this.buildDriverSeries(obj.failures.drivers, 0.01));

  var elt = this.prepareChartDiv(
    version + '-fail-by-gen',
    name + ' Failures, by Generation',
    600, 300);
  this.drawPieChart(elt, this.buildGenSeries(obj.failures.devices, 0.01));
}

