// vim: set ts=2 sw=2 tw=99 et:

ChartDisplay.prototype.drawGeneral = function ()
{
  var obj = this.ensureData('general-statistics.json', this.drawGeneral.bind(this));
  if (!obj)
    return;

  if ('all' in obj) {
    obj.byFx = {};
    obj.byFx['all'] = obj.all;
  }

  var options = this.createOptionList(obj.byFx, function (key) {
    if (key == 'all')
      return 'All';
    return "Firefox " + key;
  });
  var filter = this.app.addFilter(
    'fxversion',
    'Firefox Version',
    options,
    this.app.refresh.bind(this.app),
    'all');

  var subset = null;
  if (filter.val() == 'all') {
    subset = obj.all || obj.byFx.all;
    this.drawSampleInfo(obj);
  } else {
    subset = obj.byFx[filter.val()];
    var total = 0;
    for (var key in subset.os)
      total += subset.os[key];
    $('#viewport').append(
      $('<p></p>').append(
        $('<strong></strong>').text('Total sessions: '),
        $('<span></span>').text(total.toLocaleString()),
        $('<span></span>').text(' (out of ' + obj.sessions.count.toLocaleString() + ' sampled)')
      )
    );
  }

  var elt = this.prepareChartDiv('os-share', 'Operating System Usage', 600, 300);
  this.drawPieChart(elt, [
      { label: "Windows", data: parseInt(subset.os['Windows']) },
      { label: "Linux", data: parseInt(subset.os['Linux']) },
      { label: "macOS", data: parseInt(subset.os['Darwin']) },
  ]);

  if (filter.val() == 'all') {
    var elt = this.prepareChartDiv('fx-share', 'Firefox Version Usage', 600, 300);
    var fx_series = this.mapToSeries(obj.sessions.share, function (key) {
      return "Firefox " + key;
    });
    this.drawPieChart(elt, fx_series);
  }

  var vendors = this.reduce(subset.vendors, 'Unknown', 0, function(key) {
    return key in VendorMap;
  });

  var elt = this.prepareChartDiv('vendor-share', 'Device Vendor Usage', 600, 300);
  var vendor_series = this.mapToSeries(vendors, LookupVendor);
  this.drawPieChart(elt, vendor_series);

  var windows = this.reduce(subset.windows, 'Other', 0.005, function(key) {
    return WindowsVersionName(key) != 'Unknown';
  });
  var elt = this.prepareChartDiv('winver-share', 'Windows Usage', 700, 300);
  var winver_series = this.mapToSeries(windows, function (key) {
    if (key == 'Other')
      return key;
    return WindowsVersionName(key);
  });
  this.drawPieChart(elt, winver_series);

  // Everything else is specific to the "all" category.
  if (filter.val() != 'all')
    return;

  var DeviceView = function(parent, data, prop, title) {
    this.parent = parent;
    this.prop = prop;
    this.source = data;
    this.data = parent.mapToKeyedAgg(this.source,
      function (key) { return DeviceKeyToPropKey(key, prop); },
      function (key) { return DeviceKeyToPropLabel(key, prop); }
    );
    this.data = parent.reduceAgg(this.data, 0.005, 'other', 'Other');
    this.series = parent.aggToSeries(this.data);
    this.current = this.series;
    this.elt = parent.prepareChartDiv('device-' + prop, title, 1000, 500);
  };
  DeviceView.prototype.aggToSeries = function (data) {
  };
  DeviceView.prototype.render = (function() {
    this.elt.unbind('plothover');
    this.elt.unbind('plotclick');
    this.parent.drawPieChart(this.elt, this.current);
    this.elt.bind('plotclick', (function (event, pos, obj) {
      if (!obj)
        this.unzoom();
      else if (this.series == this.current)
        this.zoom(obj);
      this.render();
    }).bind(this));
  });
  DeviceView.prototype.zoom = (function (obj) {
    var zoom_key = this.series[obj.seriesIndex].key;

    var map = {};
    for (device_key in this.source) {
      var xkey = DeviceKeyToPropKey(device_key, this.prop);
      if (zoom_key == 'other') {
        if (xkey in this.data)
          continue;
      } else {
        if (xkey != zoom_key)
          continue;
      }
      map[device_key] = this.source[device_key];
    }
    map = this.parent.reduce(map, 'Other', 0.005);
    this.current = this.parent.mapToSeries(map, function (key) {
      return GetDeviceName(key);
    });
  });
  DeviceView.prototype.unzoom = (function () {
    this.current = this.series;
  });

  var dev_gen = new DeviceView(this, obj.devices, 'gen', 'Device Generations');
  var dev_chipsets = new DeviceView(this, obj.devices, 'chipset', 'Device Chipsets');
  dev_gen.render();
  dev_chipsets.render();
}

ChartDisplay.prototype.drawMonitors = function ()
{
  var obj = this.ensureData('monitor-statistics.json', this.drawMonitors.bind(this));
  if (!obj)
    return;

  this.drawSampleInfo(obj);

  var counts = this.reduce(obj.counts, 'Other', 0.001);
  var refreshRates = this.reduce(obj.refreshRates, 'Other', 0.01);
  var resolutions = this.reduce(obj.resolutions, 'Other', 0.01);

  var largest_width = 0;
  var largest_height = 0;
  var largest_total = 0, largest_total_str;
  for (var resolution in obj.resolutions) {
    var tuple = resolution.split('x');
    if (tuple.length != 2)
      continue;
    if (parseInt(tuple[0]) > largest_width)
      largest_width = parseInt(tuple[0]);
    if (parseInt(tuple[1]) > largest_height)
      largest_height = parseInt(tuple[1]);
    var pixels = parseInt(tuple[0]) * parseInt(tuple[1]);
    if (pixels > largest_total) {
      largest_total = pixels;
      largest_total_str = resolution;
    }
  }

  var res_text = largest_total_str + " " +
                 "(largest width: " + largest_width + ", " +
                 "largest height: " + largest_height + ")";

  $('#viewport').append(
    $("<p></p>").append(
      $("<strong></strong>").text("Largest resolution "),
      $("<span></span>").text(res_text)
    )
  );

  var elt = this.prepareChartDiv('monitor-counts', 'Number of Monitors', 600, 300);
  var series = this.mapToSeries(counts,
    function (key) {
      if (parseInt(key))
        return key + " monitor" + ((key > 1) ? "s" : "");
      return key;
    }
  );
  this.drawPieChart(elt, series);

  var elt = this.prepareChartDiv('refresh-rates', 'Refresh Rates', 600, 300);
  var series = this.mapToSeries(refreshRates,
    function (key) {
      if (parseInt(key))
        return key + 'hz';
      return key;
    }
  );
  this.drawPieChart(elt, series);

  var elt = this.prepareChartDiv('resolutions', 'Resolutions', 600, 300);
  var series = this.mapToSeries(resolutions,
    function (key) {
      return key;
    }
  );
  this.drawPieChart(elt, series);
};

ChartDisplay.prototype.drawWindowsFeatures = function ()
{
  var obj = this.ensureData('windows-features.json', this.drawWindowsFeatures.bind(this));
  if (!obj)
    return;

  var options = this.createOptionList(obj.byVersion, WindowsVersionName);
  options.unshift({
    value: 'all',
    text: 'All',
  });
  var filter = this.app.addFilter(
    'winver',
    'Windows Version',
    options,
    this.app.refresh.bind(this.app),
    'all');

  this.drawSampleInfo(obj);

  var source;
  if (filter.val() == 'all') {
    source = obj.all;

    // When there is no filter, draw a general Windows breakdown for this
    // data set to help users narrow down the filter further.
    var elt = this.prepareChartDiv(
      'windows-versions',
      'Windows Versions',
      600, 300);

    var winvers = {};
    for (var key in obj.byVersion)
      winvers[key] = obj.byVersion[key].count;
    winvers = this.reduce(winvers, 'Other', 0.01, function(key) {
      return WindowsVersionName(key) != 'Unknown';
    });

    var series = this.mapToSeries(winvers, WindowsVersionName);
    this.drawPieChart(elt, series);
  } else {
    source = obj.byVersion[filter.val()];

    var info_leader = WindowsVersionName(filter.val()) + " sessions:";
    var info_text = " " + source.count +
                    " (" +
                    this.toPercent(source.count / obj.sessions.count) + "% of sessions)";

    $('#viewport').append(
      $("<p></p>").append(
        $("<strong></strong>").text(info_leader),
        $("<span></span>").text(info_text)
      )
    );
  }

  var elt = this.prepareChartDiv(
    'compositors',
    'Compositor Usage',
    600, 300);
  var series = this.mapToSeries(source.compositors,
    function (key) {
      return key;
    });
  this.drawPieChart(elt, series);

  $('#viewport').append(
    $("<p></p>").append(
      $("<strong></strong>").text('Note: Advanced Layers data is Firefox 56+ only')
    )
  );

  var advanced_layers = CD.TrimMap(source.advanced_layers, 'none');
  var elt = this.prepareChartDiv(
    'advanced_layers',
    'Advanced Layers',
    600, 300);
  var series = this.mapToSeries(advanced_layers,
    function (key) {
      return key;
    });
  this.drawPieChart(elt, series);

  var elt = this.prepareChartDiv(
    'content_backends',
    'Content Backends',
    600, 300);
  var series = this.mapToSeries(source.content_backends,
    function (key) {
      return key;
    });
  this.drawPieChart(elt, series);

  // Everything else is Windows Vista+.
  if (!('d3d11' in source))
    return;

  // We don't care about the 'unused' status.
  delete source.d3d11['unused'];

  var elt = this.prepareChartDiv(
    'd3d11-breakdown',
    'Direct3D11 Support',
    600, 300);
  var series = this.mapToSeries(source.d3d11,
    function (key) {
      if (key in D3D11StatusCode)
        return D3D11StatusCode[key];
      return key.charAt(0).toUpperCase() + key.substring(1);
    });
  this.drawPieChart(elt, series);

  var elt = this.prepareChartDiv(
    'd2d-breakdown',
    'Direct2D Support',
    600, 300);
  var series = this.mapToSeries(source.d2d,
    function (key) {
      if (key in D2DStatusCode)
        return D2DStatusCode[key];
      return key.charAt(0).toUpperCase() + key.substring(1);
    });
  this.drawPieChart(elt, series);

  var elt = this.prepareChartDiv(
    'windows-media-decoder-backends',
    'Media Decoder Backends Used',
    600, 300);
  var series = this.listToSeries(source.media_decoders,
    function (index) {
      return MediaDecoderBackends[index];
    });
  this.drawPieChart(elt, series, { unitName: "instances" });

  var elt = this.prepareChartDiv(
    'windows-gpu-process',
    'GPU Process Status (Firefox 53+)',
    600, 300);
  var series = this.mapToSeries(
    CD.TrimMap(source.gpu_process, 'none'),
    function (key) {
      return key;
    });
  this.drawPieChart(elt, series);

  var elt = this.prepareChartDiv(
    'windows-plugin-drawing-models',
    'Plugin Drawing Modes Used',
    600, 300);
  var map = {
    'Windowed': source.plugin_models[0],
    'Windowless': 0,
  };
  for (var i = 1; i < source.plugin_models.length; i++)
    map.Windowless += source.plugin_models[i];
  var series = this.mapToSeries(map);
  this.drawPieChart(elt, series, { unitName: "instances" });

  var elt = this.prepareChartDiv(
    'windows-windowless-plugin-drawing-models',
    'Windowless Plugin Drawing Models Used',
    600, 300);
  var series = this.listToSeries(source.plugin_models.slice(1), function (index) {
    return PluginDrawingModels[index + 1];
  });
  this.drawPieChart(elt, series, { unitName: "instances" });

  var elt = this.prepareChartDiv(
    'texture-sharing-breakdown',
    'Direct3D11 Texture Sharing',
    600, 300);
  var series = this.mapToSeries(source.textureSharing,
    function (key) {
      return (key == "true") ? "Works" : "Doesn't work";
    });
  this.drawPieChart(elt, series);
}

ChartDisplay.prototype.drawBlacklistingStats = function ()
{
  var obj = this.ensureData('windows-features.json', this.drawBlacklistingStats.bind(this));
  if (!obj)
    return;

  this.drawSampleInfo(obj);

  // We don't care about the 'unused' status.
  delete obj.all.d3d11['unused'];

  var elt = this.prepareChartDiv(
    'd3d11-breakdown',
    'Direct3D11 Success Rates',
    600, 300);
  var series = this.mapToSeries(obj.all.d3d11,
    function (key) {
      if (key in D3D11StatusCode)
        return D3D11StatusCode[key];
      return key.charAt(0).toUpperCase() + key.substring(1);
    });
  this.drawPieChart(elt, series);

  var infoText = 'Blacklist status is reported when one of the following is true: ' +
                 '(1) Windows Vista or 7 is present (disabling WARP), or ' +
                 '(2) "nvdxgiwrap.dll" is present.';
  $('#viewport').append(
    $("<p></p>").append(
      $("<strong></strong>").text(infoText)
    )
  );

  var elt = this.prepareChartDiv(
    'blacklist-by-os',
    'D3D11 Blacklisting, by Windows Version',
    600, 300);
  var winvers = {};
  for (var key in obj.d3d11_blacklist.os)
    winvers[key] = obj.d3d11_blacklist.os[key];
  winvers = this.reduce(winvers, 'Other', 0.01, function(key) {
    return WindowsVersionName(key) != 'Unknown';
  });
  var series = this.mapToSeries(winvers, WindowsVersionName);
  this.drawPieChart(elt, series);

  var elt = this.prepareChartDiv(
    'blacklist-by-device',
    'D3D11 Blacklisting, by Device Chipset',
    600, 300);
  this.drawPieChart(elt, this.buildChipsetSeries(obj.d3d11_blacklist.devices, 0.01));

  var elt = this.prepareChartDiv(
    'blacklist-by-driver',
    'D3D11 Blacklisting, by Driver',
    600, 300);
  this.drawPieChart(elt, this.buildDriverSeries(obj.d3d11_blacklist.drivers, 0.01));

  var infoText = 'Blocked status is reported when one of the following is true: ' +
                 '(1) Safe mode is enabled, or ' +
                 '(2) Device creation failed and WARP is blocked, or ' +
                 '(3) An Intel GPU is present and DisplayLink <= 8.6.1.36484 is present.';
  $('#viewport').append(
    $("<p></p>").append(
      $("<strong></strong>").text(infoText)
    )
  );

  var elt = this.prepareChartDiv(
    'blocked-by-vendor',
    'D3D11 Blocked, by Vendor',
    600, 300);
  this.drawPieChart(elt, this.buildVendorSeries(obj.d3d11_blocked.vendors, 0.005));
}

ChartDisplay.prototype.drawStartupData = function ()
{
  var obj = this.ensureData('startup-test-statistics.json', this.drawStartupData.bind(this));
  if (!obj)
    return;

  this.drawSampleInfo(obj);

  // Since most sessions are "Ok", we zap that from the results.
  var items = obj.results.splice(1);

  var startupTestFailed = obj.startupTestPings - obj.results[0];
  var startupTestFailedText =
    startupTestFailed + " (" +
    this.toPercent(startupTestFailed / obj.startupTestPings) + "% " +
    "of sessions)";

  $("#viewport").append(
      $("<p></p>").append(
        $("<strong></strong>").text("Number of sessions with a startup guard status change: ")
      ).append(
        $("<span></span>").text(startupTestFailedText)
      )
  );

  var elt = this.prepareChartDiv(
    'startup-test-results',
    'Status-change breakdown of startup guards',
    600, 300);
  var series = this.listToSeries(items,
    function (index) {
      return StartupTestCode[index + 1];
    }
  );
  this.drawPieChart(elt, series);
}

ChartDisplay.prototype.drawSanityTests = function ()
{
  var obj = this.ensureData('sanity-test-statistics.json', this.drawSanityTests.bind(this));
  if (!obj)
    return;

  var subset = obj.windows;

  this.drawSampleInfo(obj);

  var infoText = subset.sanityTestPings + " (" +
                 this.toPercent(subset.sanityTestPings / subset.totalPings) + "% of sessions)";

  $("#viewport").append(
      $("<p></p>").append(
        $("<strong></strong>").text("Number of sanity tests attempted: ")
      ).append(
        $("<span></span>").text(infoText)
      )
  );

  var elt = this.prepareChartDiv(
    'sanity-test-results',
    'Sanity Test results',
    600, 300);
  var series = this.mapToSeries(subset.results,
    function (key) {
      return SanityTestCode[parseInt(key)];
    }
  );
  this.drawPieChart(elt, series);

  /*
  var elt = this.prepareChartDiv(
    'sanity-test-reasons',
    'Sanity Test triggers',
    600, 300);
  var series = this.listToSeries(obj.reasons,
    function (index) {
      return SanityTestReason[index];
    }
  );
  this.drawPieChart(elt, series);
  */

  for (var i = 0; i < subset.byOS.length; i++) {
    var key = subset.byOS[i][0];
    var data = subset.byOS[i][1];
    var elt = this.prepareChartDiv(
      'sanity-test-by-os-' + key,
      SanityTestCode[key] + ', by Operating System',
      600, 300);
    var series = this.mapToSeries(data,
      function (key) {
        return WindowsVersionName(key);
      });
    this.drawPieChart(elt, series);
  }

  for (var i = 0; i < subset.byVendor.length; i++) {
    var key = subset.byVendor[i][0];
    var data = subset.byVendor[i][1];
    var elt = this.prepareChartDiv(
      'sanity-test-by-vendor-' + key,
      SanityTestCode[key] + ', by Graphics Vendor',
      600, 300);
    var series = this.mapToSeries(data,
      function (key) {
        return GetVendorName(key);
      });
    this.drawPieChart(elt, series);
  }

  for (var i = 0; i < subset.byDevice.length; i++) {
    var key = subset.byDevice[i][0];
    var data = subset.byDevice[i][1];
    var elt = this.prepareChartDiv(
      'sanity-test-by-device-' + key,
      SanityTestCode[key] + ', by Graphics Device',
      800, 300);
    var series = this.mapToSeries(data,
      function (key) {
        return GetDeviceName(key);
      });
    this.drawPieChart(elt, series);
  }

  for (var i = 0; i < subset.byDriver.length; i++) {
    var key = subset.byDriver[i][0];
    var data = subset.byDriver[i][1];
    var elt = this.prepareChartDiv(
      'sanity-test-by-driver-' + key,
      SanityTestCode[key] + ', by Graphics Driver',
      600, 300);
    var series = this.mapToSeries(data,
      function (key) {
        return GetDriverName(key);
      });
    this.drawPieChart(elt, series);
  }
}

ChartDisplay.prototype.drawTDRs = function ()
{
  var obj = this.ensureData('tdr-statistics.json', this.drawTDRs.bind(this));
  if (!obj)
    return;

  this.drawSampleInfo(obj);

  var totalTDRs = 0;
  for (var i = 0; i < obj.results.length; i++)
    totalTDRs += obj.results[i];

  var avgUsers = ((obj['tdrPings'] / obj.sessions.count) * 100).toFixed(2);
  var avgTDRs = (totalTDRs / obj['tdrPings']).toFixed(1);

  $("#viewport").append(
      $("<p></p>").append(
        $("<strong></strong>").text("Percentage of sessions with TDRs: ")
      ).append(
        $("<span></span>").text(avgUsers + '%')
      ),
      $("<p></p>").append(
        $("<strong></strong>").text("Average number of TDRs per TDR-affected user: ")
      ).append(
        $("<span></span>").text(avgTDRs)
      )
  );

  var elt = this.prepareChartDiv('tdr-reasons', 'TDR Reason Breakdown', 600, 300);
  var series = this.listToSeries(obj.results,
    function (reason) {
      return DeviceResetReason[reason];
    });
  this.drawPieChart(elt, series);

  // Combine the TDR breakdown into a single map of vendor => count.
  var combinedMap = {};
  for (var i = 0; i < obj.reasonToVendor.length; i++) {
    var item = obj.reasonToVendor[i];
    var reason = item[0];
    var map = item[1];

    if (!reason || reason > DeviceResetReason.length)
      continue;

    for (var key in map) {
      if (key in combinedMap)
        combinedMap[key] += map[key];
      else
        combinedMap[key] = map[key];
    }
  }

  // Draw the pie chart for the above analysis.
  var elt = this.prepareChartDiv('tdr-vendors', 'TDRs by Vendor', 600, 300);
  var tdrs = [];
  for (var vendor in map) {
    if (!(vendor in VendorMap))
      continue;
    var vendorName = (vendor in VendorMap)
                     ? VendorMap[vendor]
                     : "Unknown vendor " + vendor;
    tdrs.push({
      label: vendorName,
      data: map[vendor],
    });
  }
  this.drawPieChart(elt, tdrs);

  // Draw the vendor -> reason charts.
  for (var i = 0; i < obj.vendorToReason.length; i++) {
    var vendor = obj.vendorToReason[i][0];
    if (!IsMajorVendor(vendor))
      continue;

    var elt = this.prepareChartDiv('tdr-reason-' + vendor, 'TDR Reasons for ' + LookupVendor(vendor), 600, 300);
    var tdrs = [];
    var map = obj.vendorToReason[i][1];
    for (var reason in map) {
      if (!map[reason])
        continue;

      tdrs.push({
        label: DeviceResetReason[reason],
        data: map[reason],
      });
    }
    this.drawPieChart(elt, tdrs);
  }

  // Draw a vendor pie chart for each TDR reason.
  for (var i = 0; i < obj.reasonToVendor.length; i++) {
    var item = obj.reasonToVendor[i];
    var reason = item[0];
    var map = item[1];

    if (!reason || reason > DeviceResetReason.length)
      continue;
    if (Object.keys(map).length == 0)
      continue;

    var elt = this.prepareChartDiv(
        'tdr-reason-' + reason,
        'TDR Reason: ' + DeviceResetReason[reason],
        600, 300);
    var tdrs = [];
    for (var vendor in map) {
      if (!(vendor in VendorMap))
        continue;
      var vendorName = (vendor in VendorMap)
                       ? VendorMap[vendor]
                       : "Unknown vendor " + vendor;
      tdrs.push({
        label: vendorName,
        data: map[vendor],
      });
    }
    this.drawPieChart(elt, tdrs);
  }
}

ChartDisplay.prototype.drawSystem = function ()
{
  var obj = this.ensureData('system-statistics.json', this.drawSystem.bind(this));
  if (!obj)
    return;

  this.drawSampleInfo(obj);

  var elt = this.prepareChartDiv('logical-cores', 'Logical Cores', 500, 300);
  var cores = this.reduce(obj.logical_cores, 'Other', 0.01);
  this.drawChart('pie', elt, this.mapToSeries(cores, function (key) {
    if (key == '1')
      return '1 core';
    if (key == 'Other')
      return 'Other';
    return key + ' cores';
  }));

  // Cull out erroneous 0.
  var memory = {};
  for (var key in obj.memory)
    memory[key] = obj.memory[key];
  if ('0' in memory) {
    memory['1'] = (memory['1'] | 0) + memory['0'];
    delete memory['1'];
  }

  var elt = this.prepareChartDiv('memory', 'Memory', 500, 300);
  this.drawChart('pie', elt, this.mapToSeries(memory, function (key) {
    switch (key) {
    case 'less_1gb':
      return '<1GB';
    case '4_to_8':
      return '4-8GB';
    case '8_to_16':
      return '8-16GB';
    case '16_to_32gb':
      return '16-32GB';
    case 'more_32':
      return '>32GB';
    }
    return key + 'GB';
  }));

  var elt = this.prepareChartDiv('windows-arch', 'Windows Architectures', 550, 300);
  this.drawChart('pie', elt, this.mapToSeries(obj.wow, function (key) {
    switch (key) {
      case '32':
        return '32-bit Windows';
      case '32_on_64':
        return '32-bit Firefox on 64-bit Windows';
      case '64':
        return '64-bit Firefox';
      default:
        return 'unknown';
    }
  }));

  var data = { series: [], labels: [] };
  for (var feature in obj.x86.features) {
    if (feature.substr(0, 3) != 'has')
      continue;
    var label = feature.substr(3);
    if (label == 'NEON' || label == 'EDSP')
      continue;
    var count = obj.x86.features[feature];
    data.series.push([data.labels.length, (count / obj.x86.total) * 100]);
    data.labels.push(label);
  }
  data.formatter = function (n, obj) {
    return n + '%';
  }

  var elt = this.prepareChartDiv('arches', 'x86/64 CPU Features', 500, 300);
  this.drawChart('bar', elt, data, { yaxis: { max: 100 }});
}

ChartDisplay.prototype.drawMacStats = function ()
{
  var obj = this.ensureData('mac-statistics.json', this.drawMacStats.bind(this));
  if (!obj)
    return;

  this.drawSampleInfo(obj);

  var elt = this.prepareChartDiv(
    'osx-versions',
    'macOS Versions',
    600, 300);

  var mac_versions = this.mapToKeyedAgg(obj.versions,
    DarwinVersionToOSX,
    function (old_key, new_key) {
      return 'macOS ' + new_key + ' (' + OSXNameMap[new_key] + ')';
    }
  );
  this.drawPieChart(elt, this.aggToSeries(mac_versions));

  var elt = this.prepareChartDiv(
    'screens',
    'Screen Scale',
    600, 300);
  this.drawPieChart(elt, this.mapToSeries(obj.retina, function (key) {
    if (key == 1)
      return 'Normal';
    if (key == 2)
      return 'Retina';
    return key;
  }));

  var elt = this.prepareChartDiv(
    'arch',
    'Firefox Architecture',
    600, 300);
  this.drawPieChart(elt, this.mapToSeries(obj.arch, function (key) {
    if (key == 64)
      return '64-bit';
    if (key == 32)
      return '32-bit';
    return 'Unknown';
  }));

  for (var i = 9; i <= 15; i++) {
    var osx_version = '10.' + i;
    var elt = this.prepareChartDiv(
      'osx-' + osx_version,
      'macOS ' + osx_version + ' (' + OSXNameMap[osx_version] + ') - Breakdown',
      600, 300);

    var new_map = {};
    for (var key in obj.versions) {
      if (DarwinVersionToOSX(key) == osx_version)
        new_map[key] = obj.versions[key];
    }
    var reduced = this.mapToKeyedAgg(new_map,
      DarwinVersionToOSXFull,
      function (old_key, new_key) { return new_key; }
    );
    this.drawPieChart(elt, this.aggToSeries(reduced));
  }
}

ChartDisplay.prototype.drawLinuxStats = function ()
{
  var obj = this.ensureData('linux-statistics.json', this.drawLinuxStats.bind(this));
  if (!obj)
    return;

  this.drawSampleInfo(obj);

  var elt = this.prepareChartDiv(
    'compositors',
    'Compositor Usage',
    600, 300);
  var series = this.mapToSeries(obj.compositors,
    function (key) {
      return key;
    });
  this.drawPieChart(elt, series);

  var elt = this.prepareChartDiv(
    'driver-vendors',
    'Driver Vendors',
    600, 300);
  var series = this.mapToSeries(obj.driverVendors,
    function (key) {
      if (key == '')
        return 'unknown';
      return key;
    }
  );
  this.drawPieChart(elt, series);
}

ChartDisplay.prototype.displayHardwareSearch = function() {
  var detail = this.ensureData('device-statistics.json', this.displayHardwareSearch.bind(this));
  if (!detail)
    return;

  var general = this.ensureData('general-statistics.json', this.displayHardwareSearch.bind(this));
  if (!general)
    return;

  this.drawSampleInfo(general);

  var vendorChooser = (function () {
    var vendorSelector = $('<select></select>', { id: 'vendor-chooser' });
    for (var i = 0; i < MajorVendors.length; i++) {
      var key = MajorVendors[i];
      vendorSelector.append($('<option></option>', {
        value: key,
      }).text(VendorMap[key]));
    }
    return vendorSelector;
  })();

  function getSearchTerm(str) {
    if (str.indexOf('*') == -1) {
      return str;
    }
    var escaped = str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    var converted = escaped.replace('\\*', '.*');
    return new RegExp('^' + converted + '$');
  }

  function startSearch() {
    var vendor = vendorChooser.val();

    var devicestr = $('#device-search').val().trim();
    var driverstr = $('#driver-search').val().trim();

    var result;
    if (devicestr.length && !driverstr.length) {
      var devices = [];

      var devicearr = devicestr.split(',');
      for (var i = 0; i < devicearr.length; i++) {
        var device = devicearr[i].trim();
        devices.push(vendor + '/' + device.toLowerCase());
      }

      result = Search.ByDevices(general.devices, devices);
    } else if (driverstr.length && !devicestr.length) {
      var drivers = [];

      var driverarr = driverstr.split(',');
      for (var i = 0; i < driverarr.length; i++) {
        var driver = vendor + '/' + driverarr[i].trim();
        drivers.push(getSearchTerm(driver));
      }

      result = Search.ByTerm(general.drivers, drivers);
    } else {
      var devices = devicestr.split(',');
      var drivers = driverstr.split(',');
      var terms = [];

      for (var device_index = 0; device_index < devices.length; device_index++) {
        var prefix = vendor + '/' + devices[device_index].toLowerCase() + '/';
        for (var driver_index = 0; driver_index < drivers.length; driver_index++) {
          terms.push(getSearchTerm(prefix + drivers[driver_index]));
        }
      }

      result = Search.ByTerm(detail.deviceAndDriver, terms);
    }

    var result_box = $('#result-box');
    result_box.text(result[0].toLocaleString() + ' out of ' +
                    result[1].toLocaleString() + ' sessions matched (' +
                    this.toPercent(result[0] / result[1]) + '%)'); 

    this.app.updateViewHash('vendor', devicestr);
    if (devicestr.length)
      this.app.updateViewHash('devices', devicestr);
    if (driverstr.length)
      this.app.updateViewHash('drivers', driverstr);
  }

  function makeChooser(kind) {
    var searchBox = $('<input></input>', {
      id: kind + '-search',
      type: 'text',
    }).prop({
      size: 30,
    });
    if (kind == 'driver')
      searchBox.attr('placeholder', '8.15.10.*');
    else if (kind == 'device')
      searchBox.attr('placeholder', '0x0102, 0x0116');

    var div = $('<div></div>');
    div.append(searchBox);
    return div;
  }

  var control_div = $('<div></div>');
  control_div.append(
    $('<p></p>').text('Fill in filter options below, then click "Search".'),
    $('<p></p>').text('Using both filters is an AND. Using multiple patterns (joined by commas) is an OR.'),
    $('<span></span>').text('Vendor: '),
    vendorChooser,
    $('<p></p>'),
    $('<span></span>').text('Devices: '),
    makeChooser('device'),
    $('<p></p>'),
    $('<span></span>').text('Drivers: '),
    makeChooser('driver'),
    $('<p></p>')
  );

  var button = $('<input type="button" value="Search"></input>');
  button.click(startSearch.bind(this));
  control_div.append(button);

  control_div.append($('<p></p>', {
    id: 'result-box'
  }));

  $('#viewport').append(control_div);

  if (this.app.getParam('vendor', undefined) !== undefined)
    vendorChooser.val(this.app.getParam('vendor'));
  if (this.app.getParam('drivers', undefined) !== undefined)
    $('#driver-search').val(this.app.getParam('drivers'));
  if (this.app.getParam('devices', undefined) !== undefined)
    $('#device-search').val(this.app.getParam('devices'));
}
