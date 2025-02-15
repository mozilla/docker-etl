import json
import os
import sys
import numpy as np
from scipy import interpolate
from django.template import Template, Context
from django.template.loader import get_template
from airium import Airium
from bs4 import BeautifulSoup as bs

# These values are mostly hand-wavy that seem to 
# fit the telemetry result impacts.
def get_cohen_effect_meaning(d):
  d_abs = abs(d)
  if d_abs <= 0.05:
    return "Small"
  if d_abs <= 0.1:
    return "Medium"
  else:
    return "Large"

def get_rank_biserial_corr_meaning(r):
  r_abs = abs(r)
  if r_abs <= 0.05:
    return "Small"
  if r_abs <= 0.1:
    return "Medium"
  else:
    return "Large"

# CubicSpline requires a monotonically increasing x.
# Remove duplicates.
def cubic_spline_prep(x, y):
  new_x = []
  new_y = []
  for i in range(1, len(x)):
    if x[i]-x[i-1] > 0:
      new_x.append(x[i])
      new_y.append(y[i])
  return [new_x, new_y]

def cubic_spline_smooth(x, y, x_new):
  [x_prep, y_prep] = cubic_spline_prep(x, y)
  tck = interpolate.splrep(x_prep, y_prep, k=3)
  y_new = interpolate.splev(x_new, tck, der=0)
  return list(y_new)

def find_value_at_quantile(values, cdf, q=0.95):
  for i, e in reversed(list(enumerate(cdf))):
    if cdf[i] <= q:
      if i==len(cdf)-1:
        return values[i]
      else:
        return values[i+1]

def getIconForSegment(segment):
  iconMap = {
      "All": "fa-solid fa-globe",
      "Windows": "fa-brands fa-windows",
      "Linux": "fa-brands fa-linux",
      "Mac": "fa-brands fa-apple",
      "Android": "fa-brands fa-android"
  }
  if segment in iconMap:
    return iconMap[segment]
  else:
    return "fa-solid fa-chart-simple"

def flip_row_background(color):
  if color == "white":
    return "#ececec"
  else:
    return "white"

class ReportGenerator:
  def __init__(self, data):
    self.data = data
    self.doc = Airium()

  def createHeader(self):
    t = get_template("header.html")
    context = {
          "title": f"{self.data['slug']} experimental results"
    }
    self.doc(t.render(context))

  def endDocument(self):
    self.doc("</body>")
    return

  def createSidebar(self):
    t = get_template("sidebar.html")

    segments = []
    for segment in self.data['segments']:
      entry = { "name": segment,
                "icon": getIconForSegment(segment),
                "pageload_metrics" : [],
                "histograms" : []
              }
      for metric in self.data['pageload_event_metrics']:
        entry["pageload_metrics"].append(metric)

      for histogram in self.data['histograms']:
        hist_name = histogram.split('.')[-1]
        entry["histograms"].append(hist_name)

      segments.append(entry)

    ctx = {
        "segments": segments
    }
    self.doc(t.render(ctx))

  def createSummarySection(self):
    t = get_template("summary.html")
    control=self.data["branches"][0]

    row_background="white";

    segments = []
    for segment in self.data["segments"]:
      numerical_metrics = []
      categorical_metrics = []
      for metric_type in ["histograms", "pageload_event_metrics"]:
        for metric in self.data[metric_type]:
          # Alternate between white and #ececec for row backgound.
          row_background = flip_row_background(row_background)

          if metric_type == "pageload_event_metrics":
            kind = "numerical"
            metric_name = f"pageload event: {metric}"
          else:
            kind = self.data[metric_type][metric]["kind"]
            metric = metric.split(".")[-1]
            metric_name = metric

          # Generate summary for categorical histograms here.
          if kind == "categorical":
            branches = []
            for branch in self.data["branches"]:
              if "uplift" in self.data[branch][segment][metric_type][metric]:
                rows = []
                n_labels = len(self.data[branch][segment][metric_type][metric]["labels"])
                for i in range(n_labels):
                  label = self.data[branch][segment][metric_type][metric]["labels"][i]
                  uplift = self.data[branch][segment][metric_type][metric]["uplift"][i]

                  # Enumerated histograms have a lot of labels, so try and limit the ones
                  # we show.
                  if n_labels > 5 and abs(uplift)<0.05:
                    continue

                  weight="font-weight:normal;"
                  if abs(uplift) >= 10:
                    effect = "Large"
                    weight = "font-weight:bold;"
                  elif abs(uplift) >= 5:
                    effect = "Medium"
                    weight = "font-weight:bold;"
                  elif abs(uplift) >= 2:
                    effect = "Small"
                  else:
                    effect = "None"

                  if uplift > 0:
                    uplift = "+{0:.2f}".format(self.data[branch][segment][metric_type][metric]["uplift"][i])
                  else:
                    uplift = "{0:.2f}".format(self.data[branch][segment][metric_type][metric]["uplift"][i])

                  uplift_desc=f"{label:<15}: {uplift}%"

                  rows.append({
                    "uplift": uplift_desc,
                    "effect": effect,
                    "weight": weight,
                    "style": f"background:{row_background};",
                  })
                rows[-1]["style"] = rows[-1]["style"] + "border-bottom-style: solid;"

                branches.append({
                  "branch": branch,
                  "style": f"background:{row_background};",
                  "branch_rowspan": len(rows),
                  "rows": rows
                })
                branches[-1]["style"] = branches[-1]["style"] + "border-bottom-style: solid;"

            total_rowspan = 0
            for i in range(len(branches)):
              total_rowspan = total_rowspan + branches[i]["branch_rowspan"]

            categorical_metrics.append({
              "name": metric_name,
              "desc": self.data[branch][segment][metric_type][metric]["desc"],
              "style": f"background:{row_background}; border-bottom-style: solid; border-right-style: solid;",
              "name_rowspan": total_rowspan,
              "branches": branches
            })
            continue

          # Generate summary for numerical histograms here.
          datasets = []
          for branch in self.data["branches"]:
            if branch == control:
              continue

            mean = "{0:.1f}".format(self.data[branch][segment][metric_type][metric]["mean"])
            std  = "{0:.1f}".format(self.data[branch][segment][metric_type][metric]["std"])

            branch_mean = self.data[branch][segment][metric_type][metric]["mean"]
            control_mean = self.data[control][segment][metric_type][metric]["mean"]
            uplift = (branch_mean-control_mean)/control_mean*100.0
            if uplift > 0:
              uplift_str = "+{0:.1f}".format(uplift)
            else:
              uplift_str = "{0:.1f}".format(uplift)

            pval = self.data[branch][segment][metric_type][metric]["tests"]["mwu"]["p-value"]
            effect_size = self.data[branch][segment][metric_type][metric]["tests"]["mwu"]["effect"]
            effect_meaning = get_rank_biserial_corr_meaning(effect_size)
            effect_size = "{0:.2f}".format(effect_size)
            effect = f"{effect_meaning} (r={effect_size})"
        
            if pval >= 0.001:
              pval = "{0:.2f}".format(pval)
              effect = f"None (p={pval})"
              effect_meaning = "None"

            if effect_meaning == "None" or effect_meaning == "Small":
              color="font-weight: normal"
            else:
              if uplift >= 1.5:
                color="font-weight: bold; color: red"
              elif uplift <= -1.5:
                color="font-weight: bold; color: green"
              else:
                color="font-weight: normal"


            dataset = {
                "branch": branch,
                "mean": mean,
                "uplift": uplift_str,
                "std": std,
                "effect": effect,
                "color": color,
                "style": f"background:{row_background};"
            }
            datasets.append(dataset);
          datasets[-1]["style"] = datasets[-1]["style"] + "border-bottom-style:solid;"

          numerical_metrics.append({ "desc": metric_name, 
                           "name": metric,
                           "desc": self.data[branch][segment][metric_type][metric]["desc"],
                           "style": f"background:{row_background}; border-bottom-style:solid; border-right-style:solid;",
                           "datasets": datasets, 
                           "rowspan": len(datasets)})

      segments.append({
        "name": segment, 
        "numerical_metrics": numerical_metrics,
        "categorical_metrics": categorical_metrics
      }) 

    slug = self.data['slug']
    is_experiment = self.data['is_experiment']

    if is_experiment:
      startDate = self.data['startDate']
      endDate = self.data['endDate']
      channel = self.data['channel']
    else:
      startDate = None,
      endDate = None
      channel = None

    branches=[]
    for i in range(len(self.data['input']['branches'])):
      if is_experiment:
        branchInfo = {
            "name": self.data['input']['branches'][i]['name']
        }
      else:
        branchInfo = {
            "name": self.data['input']['branches'][i]['name'],
            "startDate": self.data['input']['branches'][i]['startDate'],
            "endDate": self.data['input']['branches'][i]['endDate'],
            "channel": self.data['input']['branches'][i]['channel']
            
        }
      branches.append(branchInfo)

    context = { 
      "slug": slug,
      "is_experiment": is_experiment,
      "startDate": startDate,
      "endDate": endDate,
      "channel": channel,
      "branches": branches,
      "segments": segments,
      "branchlen": len(branches)
    }
    self.doc(t.render(context))

  def createConfigSection(self):
    t = get_template("config.html")
    context = { 
                "config": json.dumps(self.data["input"], indent=4),
                "queries": self.data['queries']
              }
    self.doc(t.render(context))

  def createCDFComparison(self, segment, metric, metric_type):
    t = get_template("cdf.html")

    control = self.data["branches"][0]
    values_control = self.data[control][segment][metric_type][metric]["pdf"]["values"]
    cdf_control = self.data[control][segment][metric_type][metric]["pdf"]["cdf"]

    maxValue = find_value_at_quantile(values_control, cdf_control)
    values_int = list(np.around(np.linspace(0, maxValue, 100), 2))

    datasets = []
    for branch in self.data["branches"]:
      values = self.data[branch][segment][metric_type][metric]["pdf"]["values"]
      density = self.data[branch][segment][metric_type][metric]["pdf"]["density"]
      cdf = self.data[branch][segment][metric_type][metric]["pdf"]["cdf"]

      # Smooth out pdf and cdf, and use common X values for each branch.
      density_int = cubic_spline_smooth(values, density, values_int)
      cdf_int = cubic_spline_smooth(values, cdf, values_int)

      dataset = {
          "branch": branch,
          "cdf": cdf_int,
          "density": density_int,
      }

      datasets.append(dataset)

    context = {
        "segment": segment,
        "metric": metric,
        "values": values_int,
        "datasets": datasets
    }
    self.doc(t.render(context))
    return

  def calculate_uplift_interp(self, quantiles, branch, segment, metric_type, metric):
    control = self.data["branches"][0]

    quantiles_control = self.data[control][segment][metric_type][metric]["quantiles"]
    values_control = self.data[control][segment][metric_type][metric]["quantile_vals"]
    [quantiles_control_n, values_control_n] = cubic_spline_prep(quantiles_control, values_control)
    tck = interpolate.splrep(quantiles_control_n, values_control_n, k=1)
    values_control_n = interpolate.splev(quantiles, tck, der=0)

    quantiles_branch = self.data[branch][segment][metric_type][metric]["quantiles"]
    values_branch = self.data[branch][segment][metric_type][metric]["quantile_vals"]
    [quantiles_branch_n, values_branch_n] = cubic_spline_prep(quantiles_branch, values_branch)
    tck = interpolate.splrep(quantiles_branch_n, values_branch_n, k=1)
    values_branch_n = interpolate.splev(quantiles, tck, der=0)

    uplifts = []
    diffs = []
    for i in range(len(quantiles)):
      diff = values_branch_n[i] - values_control_n[i]
      uplift = diff/values_control_n[i]*100
      diffs.append(diff)
      uplifts.append(uplift)

    return [diffs, uplifts]

  def createUpliftComparison(self, segment, metric, metric_type):
    t = get_template("uplift.html")

    control = self.data["branches"][0]
    quantiles = list(np.around(np.linspace(0.1, 0.99, 99), 2))

    datasets = []
    for branch in self.data["branches"]:
      if branch == control:
        continue

      [diff, uplift] = self.calculate_uplift_interp(quantiles, branch, segment, metric_type, metric)
      dataset = {
          "branch": branch,
          "diff": diff,
          "uplift": uplift,
      }
      datasets.append(dataset)

    maxVal = 0
    for x in diff:
      if abs(x) > maxVal:
        maxVal = abs(x)

    maxPerc = 0
    for x in uplift:
      if abs(x) > maxPerc:
        maxPerc = abs(x)

    context = {
        "segment": segment,
        "metric": metric,
        "quantiles": quantiles,
        "datasets": datasets,
        "upliftMax": maxPerc,
        "upliftMin": -maxPerc,
        "diffMax": maxVal,
        "diffMin": -maxVal
    }
    self.doc(t.render(context))
  
  def createMeanComparison(self, segment, metric, metric_type):
    t = get_template("mean.html")

    datasets = []
    control=self.data["branches"][0]
      
    for branch in self.data["branches"]:
      n = int(self.data[branch][segment][metric_type][metric]["n"])
      n = f'{n:,}'
      mean = "{0:.1f}".format(self.data[branch][segment][metric_type][metric]["mean"])

      if branch != control:
        branch_mean = self.data[branch][segment][metric_type][metric]["mean"]
        control_mean = self.data[control][segment][metric_type][metric]["mean"]
        uplift = (branch_mean-control_mean)/control_mean*100.0
        uplift = "{0:.1f}".format(uplift)
      else:
        uplift = ""

      se   = "{0:.1f}".format(self.data[branch][segment][metric_type][metric]["se"])
      std  = "{0:.1f}".format(self.data[branch][segment][metric_type][metric]["std"])

      dataset = {
          "branch": branch,
          "mean": mean,
          "uplift": uplift,
          "n": n,
          "se": se,
          "std": std,
          "control": branch==control
      }
      
      if branch != control:
        for test in self.data[branch][segment][metric_type][metric]["tests"]:
          effect = "{0:.2f}".format(self.data[branch][segment][metric_type][metric]["tests"][test]["effect"])
          pval = "{0:.2g}".format(self.data[branch][segment][metric_type][metric]["tests"][test]["p-value"])
          dataset[test] = {
              "effect": effect,
              "pval": pval
          }

      datasets.append(dataset)

    context = {
        "segment": segment,
        "metric": metric,
        "branches": self.data["branches"],
        "datasets": datasets
    }
    self.doc(t.render(context))

  def createCategoricalComparison(self, segment, metric, metric_type):
    t = get_template("categorical.html")

    # If the histogram has too many buckets, then only display a 
    # set of interesting comparisons instead of all of them.
    indices = set()

    control = self.data["branches"][0]
    n_elem = len(self.data[control][segment][metric_type][metric]["ratios"])
    if n_elem <= 10:
      indices = set(range(0, n_elem-1))

    for branch in self.data["branches"]:
      if branch == control:
        continue
      uplift = self.data[branch][segment][metric_type][metric]["uplift"]
      ratios = self.data[branch][segment][metric_type][metric]["ratios"]
      ratios_control = self.data[control][segment][metric_type][metric]["ratios"]

      for i in range(len(uplift)):
        if abs(uplift[i]) > 0.01 and (ratios[i] >= 0.05 or ratios_control[i] >= 0.1):
          indices.add(i)

    datasets=[]
    for branch in self.data["branches"]:
      ratios_branch = [self.data[branch][segment][metric_type][metric]["ratios"][i] for i in indices]
      datasets.append({
        "branch": branch,
        "ratios": ratios_branch,
      })

      if branch != control:
        ratios_control = [self.data[control][segment][metric_type][metric]["ratios"][i] for i in indices]
        uplift = [self.data[branch][segment][metric_type][metric]["uplift"][i] for i in indices]
        datasets[-1]["uplift"] = uplift

    labels=[self.data[control][segment][metric_type][metric]["labels"][i] for i in indices]
    context = {
      "labels": labels,
      "datasets": datasets,
      "metric": metric,
      "segment": segment
        
    }
    self.doc(t.render(context))

  def createMetrics(self, segment, metric, metric_type, kind):
    # Perform a separate comparison when data is categorical.
    if kind=="categorical":
      self.createCategoricalComparison(segment, metric, metric_type)
      return

    # Add mean comparison
    self.createMeanComparison(segment, metric, metric_type)
    # Add PDF and CDF comparison
    self.createCDFComparison(segment, metric, metric_type)
    # Add uplift comparison
    self.createUpliftComparison(segment, metric, metric_type)

  def createPageloadEventMetrics(self, segment):
    for metric in self.data['pageload_event_metrics']:
      with self.doc.div(id=f"{segment}-{metric}", klass="cell"):
        # Add title for metric
        with self.doc.div(klass="title"):
          self.doc(f"({segment}) - {metric}")
        self.createMetrics(segment, metric, "pageload_event_metrics", "numerical")

  def createHistogramMetrics(self, segment):
    for hist in self.data['histograms']:
      kind = self.data["histograms"][hist]["kind"]
      metric = hist.split('.')[-1]
      with self.doc.div(id=f"{segment}-{metric}", klass="cell"):
        # Add title for metric
        with self.doc.div(klass="title"):
          self.doc(f"({segment}) - {metric}")
        self.createMetrics(segment, metric, "histograms", kind)
    return

  def createHTMLReport(self):
    self.createHeader()
    self.createSidebar()

    # Create a summary of results
    self.createSummarySection()

    # Generate charts and tables for each segment and metric
    for segment in self.data['segments']:
      self.createHistogramMetrics(segment)
      self.createPageloadEventMetrics(segment)

    # Dump the config and queries used for the report
    self.createConfigSection()

    self.endDocument()
    
    # Prettify the output
    soup = bs(str(self.doc), 'html.parser')
    return soup.prettify()
