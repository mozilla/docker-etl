// vim: set ts=2 sw=2 tw=99 et:
function Controller()
{
  this.lastHash = null;
  this.queryParams = {};
  this.view = null;
  this.charts = new ChartDisplay(this);
  this.ignoreHashChange = false;
  this.queryHooks = {};
  this.filters = [];
}

Controller.prototype.startup = function ()
{
  $('#viewChooser').change((function () {
    this.changeView($('#viewChooser').val());
    this.updateHash('view', $('#viewChooser').val());
  }).bind(this));

  $(window).hashchange(this.onHashChange.bind(this));
  this.onHashChange();
}

// Invoke the callback when the given key is changed in the URL hash.
Controller.prototype.registerParam = function (key, callback)
{
  this.queryHooks[key] = callback;
}

// Add a filter dropdown box.
Controller.prototype.addFilter = function (id, title, options, callback, defaultValue)
{
  var elt = $('<span></span>');
  elt.append($('<strong></strong>').text(title + ':'));

  var chooser = $('<select></select>', { id: id });
  elt.append(chooser);

  $("#filters").append(elt);

  for (var i = 0; i < options.length; i++) {
    chooser.append($('<option></option>', {
      value: options[i].value
    }).text(options[i].text));
  }

  this.registerParam(id, callback);

  chooser.val(this.getParam(id, defaultValue));

  chooser.change((function () {
    this.updateHash(id, chooser.val());
    callback();
  }).bind(this));

  this.filters.push({
    elt: elt,
    id: id,
  });

  return chooser;
}

// Invoked when the URL changes.
Controller.prototype.onHashChange = function ()
{
  if (this.lastHash == window.location.hash)
    return;
  if (this.ignoreHashChange)
    return;

  var query = window.location.hash.substring(1);
  var items = query.split('&');
  this.queryParams = {};
  for (var i = 0; i < items.length; i++) {
    var item = items[i].split('=');
    if (item.length <= 1)
      continue;
    this.queryParams[item[0]] = item[1];
  }

  var view = this.getParam('view', 'general');
  if (this.view == view)
    return;

  this.changeView(view);
}

// Update the hash URL based on local parameters.
Controller.prototype.updateViewHash = function (key, val)
{
  // This will make sure it gets wiped on view change.
  if (!(key in this.queryHooks))
    this.queryHooks[key] = null;
  this.updateHash(key, val);
}

// Update the hash URL based on local parameters.
Controller.prototype.updateHash = function (key, val)
{
  this.ignoreHashChange = true;
  try {
    if (key)
      this.queryParams[key] = val;
    var items = [];
    for (var key in this.queryParams)
      items.push(key + '=' + encodeURIComponent(this.queryParams[key]));
    window.location.hash = items.join('&');
  } catch (e) {
  } finally {
    this.ignoreHashChange = false;
  }
}

Controller.prototype.refresh = function ()
{
  this.changeView($('#viewchooser').val());
}

// Change the top-level view of the page.
Controller.prototype.changeView = function (view)
{
  if (this.view != $('#viewChooser').val()) {
    // When changing views, zap anything we don't want to stick in the hash.
    for (var key in this.queryHooks)
      delete this.queryParams[key];

    $("#viewChooser").val(view);
  }

  $("#viewport").empty();
  this.charts.clear();
  this.queryHooks = {};

  // Clear filters.
  for (var i = 0; i < this.filters.length; i++)
    this.filters[i].elt.remove();
  this.filters = [];

  this.view = view;

  switch (this.view) {
    case 'general':
      this.charts.drawGeneral();
      break;
    case 'tdrs':
      this.charts.drawTDRs();
      break;
    case 'sanity':
      this.charts.drawSanityTests();
      break;
    case 'startup':
      this.charts.drawStartupData();
      break;
    case 'windows-features':
      this.charts.drawWindowsFeatures();
      break;
    case 'monitors':
      this.charts.drawMonitors();
      break;
    case 'hwsearch':
      this.charts.displayHardwareSearch();
      break;
    case 'system':
      this.charts.drawSystem();
      break;
    case 'mac':
      this.charts.drawMacStats();
      break;
    case 'linux':
      this.charts.drawLinuxStats();
      break;
    case 'blacklisting':
      this.charts.drawBlacklistingStats();
      break;
    case 'trends':
      this.charts.drawTrends();
      break;
    case 'webgl1':
    case 'webgl2':
      this.charts.drawWebGL(this.view);
      break;
    case 'failure-ids':
      this.charts.drawFailureIds();
      break;
    case 'about':
      this.displayAbout();
      break;
  }
}

// Return a parameter, or if not set, return a default value.
Controller.prototype.getParam = function (key, defaultValue)
{
  if (key in this.queryParams)
    return this.queryParams[key];
  return defaultValue;
}

Controller.prototype.displayAbout = function ()
{
  $('#viewport').append(
    $('<p></p>').text(
      'The graphics telemetry dashboard currently updates every night at 1PM UTC (6AM Eastern Time). The update process takes about 2 hours.'
    ),
    $('<p></p>').text(
      'Note that some graphs rely on Telemetry instumentation that only exists on newer channels.'
    ),
    $('<p></p>').text(
      'Make sure to check the sample source on each page, since certain channels (like nightly) often have different biases than the general beta and aurora populations.'
    ),
    $('<p></p>').append(
      $('<span></span>').text("Source code: "),
      $('<a href="https://github.com/FirefoxGraphics/moz-gfx-telemetry">').text("https://github.com/FirefoxGraphics/moz-gfx-telemetry")
    )
  );
}

function Startup()
{
  var controller = new Controller();
  controller.startup();
}
