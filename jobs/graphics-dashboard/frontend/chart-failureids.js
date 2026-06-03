// vim: set ts=2 sw=2 tw=99 et:
//
ChartDisplay.prototype.drawFailureIds = function ()
{
  var webglObj = this.ensureData('webgl-statistics.json', this.drawFailureIds.bind(this));
  if (!webglObj)
    return;

  var layersObj = this.ensureData('layers-failureid-statistics.json', this.drawFailureIds.bind(this));
  if (!layersObj)
    return;

  this.drawSampleInfo(webglObj);

  var infoText = 'These failure IDs report why the user is not getting any WebGL.' +
                 'If we try to fallback to a software fallback it will report the ' +
                 'latest most relevant failure.';

  var webgl = webglObj.general.webgl;
  var successCount = webgl.status.SUCCESS | 0;

  var instanceCount = 0;
  for (var key in webgl.status)
    instanceCount += webgl.status[key];
  var failureCount = instanceCount - successCount;
  var instanceRate = CD.ToPercent(failureCount / instanceCount);

  var statusText = instanceCount + " instances total; " +
                   instanceRate + "% (" + failureCount + ") failed.";

  $('#viewport').append(
    $("<p></p>").append(
      $("<span></span>").text(infoText)
    ),
    $("<p></p>").append(
      $("<strong></strong>").text(statusText)
    )
  );

  // WebGL failures

  var failureMap = CD.TrimMap(webgl.status, 'SUCCESS');

  var elt = this.prepareChartDiv(
    'gl-fail-webgl',
    'WebGL Failure Codes',
    600, 300);
  var map = CD.CollapseMap(failureMap, undefined, 0.0003);
  this.drawPieChart(elt, this.mapToSeries(map), { unitName: "instances" });

  var acclInfoText = 'The acceleration failure ID reports the failure that lead us ' +
                     'to give up creating an HW accelerated WebGL context. It may ' +
                     'or may not fallback properly to a software context.';

  $('#viewport').append(
    $("<p></p>").append(
      $("<span></span>").text(acclInfoText)
    )
  );

  // WebGL Accelerated failures

  var acclFailureMap = CD.TrimMap(webgl.acceleration_status, 'SUCCESS');

  var elt = this.prepareChartDiv(
    'gl-fail-webgl',
    'WebGL Acceleration Failure Codes',
    600, 300);
  var acclMap = CD.CollapseMap(acclFailureMap, undefined, 0.0003);
  this.drawPieChart(elt, this.mapToSeries(acclMap), { unitName: "instances" });

  // The legeng uses absolute position so it doesn't expand out for long legend.
  // For now hardcode spaces.
  $('#viewport').append(
    $("<br>"),
    $("<br>"),
    $("<br>")
  );
  // Compositor failures

  this.featureFailureIds(layersObj.general.layers.d3d11, 'd3d11-fail', 'D3D11 Compositor Failure Codes');
  this.featureFailureIds(layersObj.general.layers.opengl, 'ogl-fail', 'OGL Compositor Failure Codes');
}

ChartDisplay.prototype.featureFailureIds = function (failureObj, id, description)
{
  $('#viewport').append(
    $("<br>"),
    $("<br>")
  );

  var successCount = failureObj.SUCCESS | 0;

  var instanceCount = 0;
  for (var key in failureObj)
    instanceCount += failureObj[key];
  var failureCount = instanceCount - successCount;
  var instanceRate = CD.ToPercent(failureCount / instanceCount);

  var statusText = instanceCount + " instances total; " +
                   instanceRate + "% (" + failureCount + ") failed.";

  $('#viewport').append(
    $("<p></p>").append(
      $("<strong></strong>").text(statusText)
    )
  );

  var failureMap = CD.TrimMap(failureObj, 'SUCCESS');

  var elt = this.prepareChartDiv(
    id,
    description,
    600, 300);
  var map = CD.CollapseMap(failureMap, undefined, 0.0003);
  this.drawPieChart(elt, this.mapToSeries(map), { unitName: "instances" });

}
