import os
import sys
import numpy as np
import pandas as pd
from google.cloud import bigquery
from django.template import Template, Context
from django.template.loader import get_template

# Remove any histograms that have empty datasets in
# either a branch, or branch segment.
def invalidDataSet(df, histogram, branches, segments):
  if df.empty:
    print(f"Empty dataset found, removing: {histogram}.")
    return True

  for branch in branches:
    branch_name = branch['name']
    branch_df = df[df["branch"]==branch_name]
    if branch_df.empty:
      print(f"Empty dataset found for branch={branch_name}, removing: {histogram}.")
      return True
    for segment in segments:
      if segment=="All":
        continue
      branch_segment_df = branch_df[branch_df["segment"]==segment]
      if branch_segment_df.empty:
        print(f"Empty dataset found for segment={segment}, removing: {histogram}.")
        return True

  return False

def segments_are_all_OS(segments):
  os_segments = set(["Windows", "All", "Linux", "Mac", "Android"])
  for segment in segments:
    if segment not in os_segments:
      return False
  return True

class TelemetryClient:
  def __init__(self, dataDir, config, skipCache):
    self.client = bigquery.Client()
    self.config = config
    self.dataDir = dataDir
    self.skipCache = skipCache
    self.queries = []

  def collectResultsFromQuery_OS_segments(self, results, branch, segment, event_metrics, histograms):
    for histogram in self.config['histograms']:
      df = histograms[histogram]
      if segment == "All":
        subset = df[df["branch"] == branch][['bucket', 'counts']].groupby(['bucket']).sum()
        buckets = list(subset.index)
        counts = list(subset['counts'])
      else:
        subset = df[(df["segment"] == segment) & (df["branch"] == branch)]
        print(subset)
        buckets = list(subset['bucket'])
        counts = list(subset['counts'])

      # Some clients report bucket sizes that are not real, and these buckets
      # end up having 1-5 samples in them.  Filter these out entirely.
      if self.config['histograms'][histogram]['kind'] == 'numerical':
        remove=[]
        for i in range(1,len(counts)-1):
          if (counts[i-1] > 1000 and counts[i] < counts[i-1]/100) or \
             (counts[i+1] > 1000 and counts[i] < counts[i+1]/100):
            remove.append(i)
        for i in sorted(remove, reverse=True):
          del buckets[i]
          del counts[i]

      # Add labels to the buckets for categorical histograms.
      if self.config['histograms'][histogram]['kind'] == 'categorical':
        labels = self.config['histograms'][histogram]['labels']

        # Remove overflow bucket if it exists
        if len(labels)==(len(buckets)-1) and counts[-1]==0:
          del buckets[-1]
          del counts[-1]

        # Add missing buckets so they line up in each branch.
        if len(labels) > len(buckets):
          for i in range(len(buckets)):
            print(buckets[i], counts[i])
          new_counts = []
          for i,b in enumerate(labels):
            j = buckets.index(b) if b in buckets else None
            if j:
              new_counts.append(counts[j])
            else:
              new_counts.append(0)
          counts  = new_counts
         
        # Remap bucket values to the appropriate label names.
        buckets = labels

      # If there is a max, then overflow larger buckets into the max.
      if 'max' in self.config['histograms'][histogram]:
        maxBucket = self.config['histograms'][histogram]['max']
        remove=[]
        maxBucketCount=0
        for i,x in enumerate(buckets):
          if x >= maxBucket:
            remove.append(i)
            maxBucketCount = maxBucketCount + counts[i]
        for i in sorted(remove, reverse=True):
          del buckets[i]
          del counts[i]
        buckets.append(maxBucket)
        counts.append(maxBucketCount)

      assert len(buckets) == len(counts)
      results[branch][segment]['histograms'][histogram] = {}
      results[branch][segment]['histograms'][histogram]['bins'] = buckets
      results[branch][segment]['histograms'][histogram]['counts'] = counts
      print(f"    segment={segment} len(histogram: {histogram}) = ", len(buckets))

    for metric in self.config['pageload_event_metrics']:
      df = event_metrics[metric]
      if segment == "All":
        subset = df[df["branch"] == branch][['bucket', 'counts']].groupby(['bucket']).sum()
        buckets = list(subset.index)
        counts = list(subset['counts'])
      else:
        subset = df[(df["segment"] == segment) & (df["branch"] == branch)]
        buckets = list(subset['bucket'])
        counts = list(subset['counts'])

      assert len(buckets) == len(counts)
      results[branch][segment]['pageload_event_metrics'][metric] = {}
      results[branch][segment]['pageload_event_metrics'][metric]['bins'] = buckets
      results[branch][segment]['pageload_event_metrics'][metric]['counts'] = counts
      print(f"    segment={segment} len(pageload event: {metric}) = ", len(buckets))

  def getResults(self):
    if self.config['is_experiment'] is True:
      return self.getResultsForExperiment()
    else:
      return self.getResultsForNonExperiment()

  def getResultsForNonExperiment(self):
    # Get data for each pageload event metric.
    event_metrics = {}
    for metric in self.config['pageload_event_metrics']:
      event_metrics[metric] = self.getPageloadEventDataNonExperiment(metric)
      print(event_metrics[metric])

    #Get data for each histogram in this segment.
    histograms = {}
    remove = []
    for histogram in self.config['histograms']:
      df = self.getHistogramDataNonExperiment(self.config, histogram)
      print(df)

      # Remove histograms that are empty.
      if invalidDataSet(df, histogram, self.config['branches'], self.config['segments']):
        remove.append(histogram)
        continue
      histograms[histogram] = df

    for hist in remove:
      if hist in self.config['histograms']:
        del self.config['histograms'][hist]

    # Combine histogram and pageload event results.
    results = {}
    for i in range(len(self.config['branches'])):
      branch_name = self.config['branches'][i]['name']
      results[branch_name] = {}
      for segment in self.config['segments']:
        print (f"Aggregating results for segment={segment} and branch={branch_name}")
        results[branch_name][segment] = {"histograms": {}, "pageload_event_metrics": {}}

        # Special case when segments is OS only.
        self.collectResultsFromQuery_OS_segments(results, branch_name, segment, event_metrics, histograms)

    results['queries'] = self.queries
    return results

  def getResultsForExperiment(self):
    # Get data for each pageload event metric.
    event_metrics = {}
    for metric in self.config['pageload_event_metrics']:
      event_metrics[metric] = self.getPageloadEventData(metric)
      print(event_metrics[metric])

    #Get data for each histogram in this segment.
    histograms = {}
    remove = []
    for histogram in self.config['histograms']:
      df = self.getHistogramData(self.config, histogram)

      # Remove invalid histogram data.
      if invalidDataSet(df, histogram, self.config['branches'], self.config['segments']):
        remove.append(histogram)
        continue
      histograms[histogram] = df

    for hist in remove:
      if hist in self.config['histograms']:
        print(f"Empty dataset found, removing: {histogram}.")
        del self.config['histograms'][hist]

    # Combine histogram and pageload event results.
    results = {}
    for branch in self.config['branches']:
      branch_name = branch['name']
      results[branch_name] = {}
      for segment in self.config['segments']:
        print (f"Aggregating results for segment={segment} and branch={branch_name}")
        results[branch_name][segment] = {"histograms": {}, "pageload_event_metrics": {}}

        # Special case when segments is OS only.
        self.collectResultsFromQuery_OS_segments(results, branch_name, segment, event_metrics, histograms)

    results['queries'] = self.queries
    return results

  def generatePageloadEventQuery_OS_segments_non_experiment(self, metric):
    t = get_template("other/glean/pageload_events_os_segments.sql")

    minVal = self.config['pageload_event_metrics'][metric]['min']
    maxVal = self.config['pageload_event_metrics'][metric]['max']

    branches = self.config["branches"]
    for i in range(len(branches)):
      branches[i]["last"] = False
      if "version" in self.config["branches"][i]:
        version = self.config["branches"][i]["version"]
        branches[i]["ver_condition"] = f"AND SPLIT(client_info.app_display_version, '.')[offset(0)] = \"{version}\""
      if "architecture" in self.config["branches"][i]:
        arch = self.config["branches"][i]["architecture"]
        branches[i]["arch_condition"] = f"AND client_info.architecture = \"{arch}\""
      if "glean_conditions" in self.config["branches"][i]:
        branches[i]["glean_conditions"] = self.config["branches"][i]["glean_conditions"]
    branches[-1]["last"] = True

    print(branches)

    context = {
        "minVal": minVal,
        "maxVal": maxVal,
        "metric": metric,
        "branches": branches
    }

    query = t.render(context)
    # Remove empty lines before returning
    query = "".join([s for s in query.strip().splitlines(True) if s.strip()])
    self.queries.append({
      "name": f"Pageload event: {metric}",
      "query": query
    })
    return query

  def generatePageloadEventQuery_OS_segments(self, metric):
    t = get_template("experiment/glean/pageload_events_os_segments.sql")

    print(self.config['pageload_event_metrics'][metric])

    metricMin = self.config['pageload_event_metrics'][metric]['min']
    metricMax = self.config['pageload_event_metrics'][metric]['max']

    isp_blacklist = []
    if 'isp_blacklist' in self.config:
      with open(self.config['isp_blacklist'], 'r') as file:
        isp_blacklist = [line.strip() for line in file]

    context = {
        "include_non_enrolled_branch": self.config['include_non_enrolled_branch'],
        "minVal": metricMin,
        "maxVal": metricMax,
        "slug": self.config['slug'],
        "channel": self.config['channel'],
        "startDate": self.config['startDate'],
        "endDate": self.config['endDate'],
        "metric": metric,
        "blacklist": isp_blacklist
    }
    query = t.render(context)
    # Remove empty lines before returning
    query = "".join([s for s in query.strip().splitlines(True) if s.strip()])
    self.queries.append({
      "name": f"Pageload event: {metric}",
      "query": query
    })
    return query

  # Not currently used, and not well supported.
  def generatePageloadEventQuery_Generic(self):
    t = get_template("archived/events_generic.sql")

    segmentInfo = []
    for segment in self.config['segments']:
      segmentInfo.append({
            "name": segment, 
            "conditions": self.config['segments'][segment]
            })

    maxBucket = 0
    minBucket = 30000
    for metric in self.config['pageload_event_metrics']:
      metricMin = self.config['pageload_event_metrics'][metric]['min']
      metricMax = self.config['pageload_event_metrics'][metric]['max']
      if metricMax > maxBucket:
        maxBucket = metricMax
      if metricMin < minBucket:
        minBucket = metricMin

    context = {
        "minBucket": minBucket,
        "maxBucket": maxBucket,
        "is_experiment": self.config['is_experiment'],
        "slug": self.config['slug'],
        "channel": self.config['channel'],
        "startDate": self.config['startDate'],
        "endDate": self.config['endDate'],
        "metrics": self.config['pageload_event_metrics'],
        "segments": segmentInfo
    }
    query = t.render(context)
    # Remove empty lines before returning
    query = "".join([s for s in query.strip().splitlines(True) if s.strip()])
    self.queries.append({
      "name": f"Pageload event: {metric}",
      "query": query
    })
    return query

  # Use *_os_segments queries if the segments is OS only which is much faster than generic query.
  def generateHistogramQuery_OS_segments_legacy(self, histogram):
    t = get_template("experiment/legacy/histogram_os_segments.sql")

    isp_blacklist = []
    if 'isp_blacklist' in self.config:
      with open(self.config['isp_blacklist'], 'r') as file:
        isp_blacklist = [line.strip() for line in file]

    context = {
        "include_non_enrolled_branch": self.config['include_non_enrolled_branch'],
        "slug": self.config['slug'],
        "channel": self.config['channel'],
        "startDate": self.config['startDate'],
        "endDate": self.config['endDate'],
        "histogram": histogram,
        "available_on_desktop": self.config['histograms'][histogram]['available_on_desktop'],
        "available_on_android": self.config['histograms'][histogram]['available_on_android'],
        "blacklist": isp_blacklist
    }
    query = t.render(context)
    # Remove empty lines before returning
    query = "".join([s for s in query.strip().splitlines(True) if s.strip()])
    self.queries.append({
      "name": f"Histogram: {histogram}",
      "query": query
    })
    return query

  def generateHistogramQuery_OS_segments_glean(self, histogram):
    t = get_template("experiment/glean/histogram_os_segments.sql")

    context = {
        "include_non_enrolled_branch": self.config['include_non_enrolled_branch'],
        "slug": self.config['slug'],
        "channel": self.config['channel'],
        "startDate": self.config['startDate'],
        "endDate": self.config['endDate'],
        "histogram": histogram,
        "available_on_desktop": self.config['histograms'][histogram]['available_on_desktop'],
        "available_on_android": self.config['histograms'][histogram]['available_on_android'],
    }
    query = t.render(context)
    # Remove empty lines before returning
    query = "".join([s for s in query.strip().splitlines(True) if s.strip()])
    self.queries.append({
      "name": f"Histogram: {histogram}",
      "query": query
    })
    return query

  def generateHistogramQuery_OS_segments_non_experiment_legacy(self, histogram):
    t = get_template("other/legacy/histogram_os_segments.sql")

    branches = self.config["branches"]
    for i in range(len(branches)):
      branches[i]["last"] = False
      if "version" in self.config["branches"][i]:
        version = self.config["branches"][i]["version"]
        branches[i]["ver_condition"] = f"AND SPLIT(application.display_version, '.')[offset(0)] = \"{version}\""
      if "architecture" in self.config["branches"][i]:
        arch = self.config["branches"][i]["architecture"]
        branches[i]["arch_condition"] = f"AND application.architecture = \"{arch}\""
      if "legacy_conditions" in self.config["branches"][i]:
        branches[i]["legacy_conditions"] = self.config["branches"][i]["legacy_conditions"]

    branches[-1]["last"] = True

    context = {
        "histogram": histogram,
        "available_on_desktop": self.config['histograms'][histogram]['available_on_desktop'],
        "available_on_android": self.config['histograms'][histogram]['available_on_android'],
        "branches": branches,
        "channel": self.config["branches"][0]["channel"],
    }
    query = t.render(context)
    # Remove empty lines before returning
    query = "".join([s for s in query.strip().splitlines(True) if s.strip()])
    self.queries.append({
      "name": f"Histogram: {histogram}",
      "query": query
    })
    return query

  def generateHistogramQuery_OS_segments_non_experiment_glean(self, histogram):
    t = get_template("other/glean/histogram_os_segments.sql")

    branches = self.config["branches"]
    for i in range(len(branches)):
      branches[i]["last"] = False
      if "version" in self.config["branches"][i]:
        version = self.config["branches"][i]["version"]
        branches[i]["ver_condition"] = f"AND SPLIT(client_info.app_display_version, '.')[offset(0)] = \"{version}\""
      if "architecture" in self.config["branches"][i]:
        arch = self.config["branches"][i]["architecture"]
        branches[i]["arch_condition"] = f"AND client_info.architecture = \"{arch}\""
      if "glean_conditions" in self.config["branches"][i]:
        branches[i]["glean_conditions"] = self.config["branches"][i]["glean_conditions"]

    branches[-1]["last"] = True

    context = {
        "histogram": histogram,
        "available_on_desktop": self.config['histograms'][histogram]['available_on_desktop'],
        "available_on_android": self.config['histograms'][histogram]['available_on_android'],
        "branches": branches
    }

    query = t.render(context)
    # Remove empty lines before returning
    query = "".join([s for s in query.strip().splitlines(True) if s.strip()])
    self.queries.append({
      "name": f"Histogram: {histogram}",
      "query": query
    })
    return query

  # Not currently used, and not well supported.
  def generateHistogramQuery_Generic(self, histogram):
    t = get_template("archived/histogram_generic.sql")

    segmentInfo = []
    for segment in self.config['segments']:
      segmentInfo.append({
            "name": segment, 
            "conditions": self.config['segments'][segment]
            })

    context = {
        "is_experiment": self.config['is_experiment'],
        "slug": self.config['slug'],
        "channel": self.config['channel'],
        "startDate": self.config['startDate'],
        "endDate": self.config['endDate'],
        "histogram": histogram,
        "available_available_on_desktop": self.config['histograms'][histogram]['available_on_desktop'],
        "available_on_android": self.config['histograms'][histogram]['available_on_android'],
        "segments": segmentInfo
    }
    query = t.render(context)
    # Remove empty lines before returning
    query = "".join([s for s in query.strip().splitlines(True) if s.strip()])
    self.queries.append({
      "name": f"Histogram: {histogram}",
      "query": query
    })
    return query

  def checkForExistingData(self, filename):
    if self.skipCache:
      df = None
    else:
      try:
        df = pd.read_pickle(filename)
        print(f"Found local data in {filename}")
      except:
        df = None
    return df

  def getHistogramDataNonExperiment(self, config, histogram):
    slug = config['slug']
    hist_name = histogram.split('.')[-1]
    filename=os.path.join(self.dataDir, f"{slug}-{hist_name}.pkl")

    df = self.checkForExistingData(filename)
    if df is not None:
      return df

    if segments_are_all_OS(self.config['segments']):
      if config["histograms"][histogram]["glean"]:
        query = self.generateHistogramQuery_OS_segments_non_experiment_glean(histogram)
      else:
        query = self.generateHistogramQuery_OS_segments_non_experiment_legacy(histogram)
    else:
      print("No current support for generic non-experiment queries.")
      sys.exit(1)

    print("Running query:\n" + query)
    job = self.client.query(query)
    df = job.to_dataframe()
    print(f"Writing '{slug}' histogram results for {histogram} to disk.")
    df.to_pickle(filename)
    return df

  def getHistogramData(self, config, histogram):
    slug = config['slug']
    hist_name = histogram.split('.')[-1]
    filename=os.path.join(self.dataDir, f"{slug}-{hist_name}.pkl")

    df = self.checkForExistingData(filename)
    if df is not None:
      return df

    if segments_are_all_OS(self.config['segments']):
      if config["histograms"][histogram]["glean"]:
        query = self.generateHistogramQuery_OS_segments_glean(histogram)
      else:
        query = self.generateHistogramQuery_OS_segments_legacy(histogram)
    else:
      # Generic segments are not well supported right now.
      print("No current support for generic non-experiment queries.")
      sys.exit(1)

    print("Running query:\n" + query)
    job = self.client.query(query)
    df = job.to_dataframe()
    print(f"Writing '{slug}' histogram results for {histogram} to disk.")
    df.to_pickle(filename)
    return df

  def getPageloadEventDataNonExperiment(self, metric):
    slug = self.config['slug']
    filename=os.path.join(self.dataDir, f"{slug}-pageload-events-{metric}.pkl")

    df = self.checkForExistingData(filename)
    if df is not None:
      return df

    if segments_are_all_OS(self.config['segments']):
      query = self.generatePageloadEventQuery_OS_segments_non_experiment(metric)
    else:
      print("Generic non-experiment query currently not supported.")
      sys.exit(1)

    print("Running query:\n" + query)
    job = self.client.query(query)
    df = job.to_dataframe()
    print(f"Writing '{slug}' pageload event results to disk.")
    df.to_pickle(filename)
    return df

  def getPageloadEventData(self, metric):
    slug = self.config['slug']
    filename=os.path.join(self.dataDir, f"{slug}-pageload-events-{metric}.pkl")

    df = self.checkForExistingData(filename)
    if df is not None:
      return df

    if segments_are_all_OS(self.config['segments']):
      query = self.generatePageloadEventQuery_OS_segments(metric)
    else:
      #query = self.generatePageloadEventQuery_Generic()
      print("No current support for generic pageload event queries.")
      sys.exit(1)

    print("Running query:\n" + query)
    job = self.client.query(query)
    df = job.to_dataframe()
    print(f"Writing '{slug}' pageload event results to disk.")
    df.to_pickle(filename)
    return df
