// vim: set ts=2 sw=2 tw=99 et:
"use strict";

ChartDisplay.prototype.plotPercentageTrend = function (elt, points, options)
{
  options = options || {};

  var labelFn = options.gfxLabelFn || function (key) { return key; };
  var preprocess = options.gfxPreprocess || function (point, data) { return data; };

  // We track the total average to make the legend appear roughly sorted
  // in the same order as each line.
  var total = 0;
  var totalsMap = {};

  var trends = {};
  for (var i = 0; i < points.length; i++) {
    var point = points[i];
    if (point.total == 0)
      continue;

    var data = preprocess(point, point.data);
    for (var key in data) {
      var trend = trends[key];
      if (!trend) {
        totalsMap[key] = 0;
        trend = (trends[key] = {line: [], raw: []});
      }

      totalsMap[key] += data[key];
      trend.line.push([point.start * 1000, (data[key] / point.total) * 100]);
      trend.raw.push({ point: point, count: data[key] });
    }
    total += point.total;
  }

  var series = [];
  for (var key in trends) {
    series.push({
      // Note: we shove the index into the label, since the legend sorting
      // function doesn't have access to the series object.
      label: labelFn(key),
      data: trends[key].line,
      info: trends[key].raw,

      // Custom - used in the sorted callback.
      gfxTotal: totalsMap[key],
    });
  }

  options.series = options.series || {};
  options.series.lines = options.series.lines || {};
  options.series.lines.show = true;
  options.series.points = options.series.points || {};
  options.series.points.show = true;
  options.xaxis = options.xaxis || {};
  options.xaxis.mode = 'time';
  options.xaxis.timeformat = '%b %d %Y';
  options.yaxis = options.yaxis || {};
  options.yaxis.min = options.yaxis.min || 0;
  options.yaxis.tickFormatter = options.yaxis.tickFormatter || function (num, str) {
    return num + '%';
  };
  options.legend = options.legend || {};
  options.legend.show = true;
  options.legend.container = $('#' + elt.attr('id') + '-legend');
  options.legend.sorted = function (x, y) {
    return y.series.gfxTotal - x.series.gfxTotal;
  };
  options.grid = options.grid || {};
  options.grid.hoverable = true;
  options.hooks = options.hooks || {};

  this.bindHoverDraw(elt, (function (event, pos, item) {
    var label = series[item.seriesIndex].label;
    var value = item.datapoint[1].toFixed(2);
    var date = new Date(item.datapoint[0]);
    var info = series[item.seriesIndex].info[item.dataIndex];
    try {
      var dateString = date.toLocaleDateString(undefined, {
        formatMatcher: 'best fit',
        month: 'short',
        day: 'numeric',
        year: 'numeric',
      });
    } catch (e) {
      var dateString = date.toString();
    }
    var text = label + ': ' + 'Week of ' + dateString + '<br/>' +
               value + '% (' +
                  info.count.toLocaleString() + ' out of ' +
                  info.point.total.toLocaleString() + ' sessions sampled)';
    return text;
  }).bind(this));;

  $.plot(elt, series, options);
}

ChartDisplay.prototype.drawTrends = function ()
{
  this.prefetch([
    'trend-firefox-v2.json',
    'trend-windows-versions-v2.json',
    'trend-windows-compositors-v2.json',
    'trend-windows-arch-v2.json',
    'trend-windows-d3d11-v2.json',
    'trend-windows-d2d-v2.json',
    'trend-windows-vendors-v2.json',
    'trend-windows-device-gen-amd-v2.json',
    'trend-windows-device-gen-intel-v2.json',
    'trend-windows-device-gen-nvidia-v2.json',
  ]);

  var fxversion_elt = this.prepareChartDiv(
    'firefox-versions-trend',
    'Firefox Versions',
    800, 300, 150);
  this.onFetch('trend-firefox-v2.json', (function (obj) {
    this.plotPercentageTrend(fxversion_elt, obj.trend, {
      gfxLabelFn: function (key) {
        return 'Firefox ' + key;
      }
    });
  }).bind(this));

  var winver_elt = this.prepareChartDiv(
    'windows-versions-trend',
    'Windows Versions',
    800, 300, 150);
  this.onFetch('trend-windows-versions-v2.json', (function (obj) {
    this.plotPercentageTrend(winver_elt, obj.trend, {
      gfxLabelFn: WindowsVersionName,
      gfxPreprocess: function (point, data) {
        return CD.CollapseMap(data, point.total, 0.01, ReduceWindowsVersion);
      }.bind(this),
    });
  }).bind(this));

  var wincc_elt = this.prepareChartDiv(
    'windows-compositors-trend',
    'Windows Compositors',
    800, 300, 150);
  this.onFetch('trend-windows-compositors-v2.json', (function (obj) {
    this.plotPercentageTrend(wincc_elt, obj.trend, {
      gfxLabelFn: function (key) {
        switch (key) {
          case 'd3d11': return 'Direct3D 11';
          case 'basic': return 'Software';
          case 'none': return 'None';
          case 'd3d9': return 'Direct3D 9';
          case 'opengl': return 'OpenGL';
          case 'webrender': return 'WebRender';
        }
        return 'Unknown';
      }
    });
  }).bind(this));

  var winarch_elt = this.prepareChartDiv(
    'windows-arch-trend',
    'Firefox CPU Architecture',
    800, 300, 150);
  this.onFetch('trend-windows-arch-v2.json', (function (obj) {
    this.plotPercentageTrend(winarch_elt, obj.trend, {
      gfxLabelFn: function (key) {
        switch (key) {
          case '32': return '32-bit Fx, Win';
          case '64': return '64-bit Fx, Win';
          case '32_on_64': return '32-bit Fx, 64-bit Win';
        }
        return key;
      }
    });
  }).bind(this));

  var d3d11_elt = this.prepareChartDiv(
    'd3d11-trend',
    'Direct3D 11 Trends',
    750, 300, 200);
  this.onFetch('trend-windows-d3d11-v2.json', (function (obj) {
    this.plotPercentageTrend(d3d11_elt, obj.trend, {
      gfxLabelFn: function (key) {
        if (key in D3D11StatusCode)
          return D3D11StatusCode[key];
        switch (key) {
          case 'blacklisted': return 'Blacklisted';
          case 'blocked': return 'Blocked (DirectLink)';
          case 'disabled': return 'Disabled';
          case 'failed': return 'Failed';
          case 'unavailable': return 'Unavailable';
          case 'crashed': return 'Crashed';
        }
        return key;
      },
      gfxPreprocess: function (point, data) {
        delete data['unknown'];
        delete data['other'];
        return CD.CollapseMap(data, point.total, 0, function (key) {
          if (key == 'unused')
            return 'other';
          return key;
        });
      }.bind(this),
    });
  }).bind(this));

  var d2d_elt = this.prepareChartDiv(
    'd2d-trend',
    'Direct2D Trends',
    800, 300, 150);
  this.onFetch('trend-windows-d2d-v2.json', (function (obj) {
    this.plotPercentageTrend(d2d_elt, obj.trend, {
      gfxLabelFn: function (key) {
        switch (key) {
          case '1.1':
          case '1.0':
            return 'Direct2D ' + key;
          case 'blacklisted': return 'Blacklisted';
          case 'failed': return 'Failed';
          case 'unavailable': return 'Unavailable';
          case 'blocked': return 'Blocked (WARP)';
          case 'disabled': return 'Disabled';
        }
        return key;
      },
      gfxPreprocess: function (point, data) {
        delete data['unknown'];
        return data;
      },
    });
  }).bind(this));

  var winvendor_elt = this.prepareChartDiv(
    'windows-vendor-trend',
    'Graphics Vendors, Windows',
    800, 300, 150)
  this.onFetch('trend-windows-vendors-v2.json', (function (obj) {
    this.plotPercentageTrend(winvendor_elt, obj.trend, {
      gfxLabelFn: function (key) {
        if (key == 'other')
          return 'Other';
        return GetVendorName(key);
      },
      gfxPreprocess: function (point, data) {
        return CD.CollapseMap(data, point.total, 0.005, function (key) {
          return (key in VendorMap) ? key : 'other';
        });
      },
    });
  }).bind(this));

  var vendors = [
    { name: 'Intel', nick: 'intel' },
    { name: 'NVIDIA', nick: 'nvidia' },
    { name: 'ATI', nick: 'amd' },
  ];
  for (var i = 0; i < vendors.length; i++) {
    // So we can close over loop vars.
    ((function (vendor) {
      var elt = this.prepareChartDiv(
        'windows-vendor-gen-trend-' + vendor.nick,
        vendor.name + ' Device Generations, Windows',
        800, 300, 150)
      this.onFetch('trend-windows-device-gen-' + vendor.nick + '-v2.json', (function (obj) {
        this.plotPercentageTrend(elt, obj.trend, {});
      }).bind(this));
    }).bind(this))(vendors[i]);
  }
}
