from scipy import stats
import numpy as np
import json
import sys

# Expand the histogram into an array of values
def flatten_histogram(bins, counts):
  array = []
  for i in range(len(bins)):
    for j in range(1, int(counts[i]/2.0)):
      array.append(bins[i])
  return array

# effect size calculation for t-test
def calc_cohen_d(x1, x2, s1, s2, n1, n2):
  effect_size = (x1-x2)/np.sqrt(((n1-1)*s1**2 + (n2-1)*s2**2) / (n1+n2-2))
  return effect_size

# effect size calculation for mwu
def rank_biserial_correlation(n1, n2, U):
  return (1-2*U/(n1*n2))

# Calculate two-way t-test with unequal sample size and unequal variances.
# Return the t-value, p-value, and effect size via cohen's d.
def calc_t_test(x1, x2, s1, s2, n1, n2): 
    s_prime = np.sqrt((s1**2/n1) + (s2**2)/n2)
    t_value = (x1-x2)/s_prime

    df = (s1**2/n1 + s2**2/n2)**2/( (s1**2/n1)**2/(n1-1) + (s2**2/n2)**2/(n2-1) )
    p_value = 2 * (1-(stats.t.cdf(abs(t_value), df)))
    effect_size = calc_cohen_d(x1, x2, s1, s2, n1, n2)
    return [t_value, p_value, effect_size]

def create_subsample(bins, counts, sample_size=100000):
  total_counts = sum(counts)
  if total_counts <= sample_size:
    return flatten_histogram(bins, counts)

  ratio = total_counts/sample_size
  subsample = []
  for i in range(len(bins)):
    subsample.extend(np.repeat(bins[i], counts[i]/ratio))
  return subsample

def calc_cdf_from_density(density, vals):
  cdf = []
  sum = 0
  for i in range(0,len(density)-2):
    width = vals[i+1]-vals[i]
    cdf_val = sum+density[i]*width
    sum = cdf_val
    cdf.append(cdf_val)

  width = vals[-1]-vals[-2]
  cdf_val = sum+density[-1]*width
  sum = cdf_val
  cdf.append(cdf_val)
  return cdf

# TODO: Interpolate the quantiles.
def calc_histogram_quantiles(bins, density):
  vals = []
  quantiles = []
  q = 0
  j = 0
  for i in range(len(bins)):
    q = q + density[i]
    vals.append(bins[i])
    quantiles.append(q)

  return [quantiles, vals]

def calc_histogram_density(counts, n):
  density = []
  cdf = []
  cum = 0
  for i in range(len(counts)):
    density.append(float(counts[i]/n))
    cum = cum+counts[i]
    cdf.append(float(cum))
  cdf = [x / cum for x in cdf]
  return [density, cdf]

def calc_histogram_mean_var(bins, counts):
  mean = 0
  n = 0
  for i in range(len(bins)):
    bucket = float(bins[i])
    count  = float(counts[i])
    n = n + count
    mean = mean + bucket*count
  mean = float(mean)/float(n)

  var = 0
  for i in range(len(bins)):
    bucket = float(bins[i])
    count =  float(counts[i])
    var = var + count*(bucket-mean)**2
  var = float(var)/float(n)
  std = np.sqrt(var)

  return [mean, var, std, n]

def calculate_histogram_stats(bins, counts, data):
  # Calculate mean, std, and var
  [mean, var, std, n] = calc_histogram_mean_var(bins, counts)
  data['mean'] = mean
  data['std'] = std
  data['var'] = var
  data['n'] = n

  # Calculate densities
  [density, cdf] = calc_histogram_density(counts, n)
  data["pdf"]["cdf"] = cdf
  data["pdf"]["density"] = density
  data["pdf"]["values"] = bins

  # Calculate quantiles
  [quantiles, vals] = calc_histogram_quantiles(bins, density)
  data["quantiles"] = quantiles
  data["quantile_vals"] = vals

def calculate_histogram_tests_subsampling(control_data, branch_data, result):
  bins_control = control_data["bins"]
  counts_control = control_data["counts"]
  control_sample = create_subsample(bins_control, counts_control)

  bins_branch = branch_data["bins"]
  counts_branch = branch_data["counts"]
  branch_sample = create_subsample(bins_branch, counts_branch)

  # Calculate t-test and effect
  x1 = np.mean(control_sample)
  s1 = np.std(control_sample)
  n1 = len(control_sample)
  x2 = np.mean(branch_sample)
  s2 = np.std(branch_sample)
  n2 = len(branch_sample)
  effect = calc_cohen_d(x1, x2, s1, s2, n1, n2)
  [t, p] = stats.ttest_ind(control_sample, branch_sample)
  result["tests"]["ttest"] = {}
  result["tests"]["ttest"]["score"] = t
  result["tests"]["ttest"]["p-value"] = p
  result["tests"]["ttest"]["effect"] = effect

  # Calculate mwu-test
  [U, p] = stats.mannwhitneyu(control_sample, branch_sample)
  r = rank_biserial_correlation(n1, n2, U)
  result["tests"]["mwu"] = {}
  result["tests"]["mwu"]["score"] = U
  result["tests"]["mwu"]["p-value"] = p
  result["tests"]["mwu"]["effect"] = r

  # Calculate ks-test
  [D, p] = stats.ks_2samp(control_sample, branch_sample)
  result["tests"]["ks"] = {}
  result["tests"]["ks"]["score"] = D
  result["tests"]["ks"]["p-value"] = p
  result["tests"]["ks"]["effect"] = D

def calculate_histogram_ttest(bins, counts, data, control):
  mean_control = control['mean']
  std_control = control['std']
  n_control = control['n']

  mean = data['mean']
  std = data['std']
  n = data['n']
  
  # Calculate t-test
  [t_value, p_value, effect] = calc_t_test(mean, mean_control, std, std_control, n, n_control)
  data["tests"]["ttest"] = {}
  data["tests"]["ttest"]["score"] = t_value
  data["tests"]["ttest"]["p-value"] = p_value
  data["tests"]["ttest"]["effect"] = effect

def calc_confidence_interval(data, confidence=0.95):
    a = 1.0 * np.array(data)
    n = len(a)
    m, se = np.mean(a), stats.sem(a)
    h = se * stats.t.ppf((1 + confidence) / 2., n-1)
    return [m, se, m-h, m+h]

def createNumericalTemplate():
  template = {
      "desc": "",
      "mean": 0,
      "confidence": {
        "min": 0,
        "max": 0
        },
      "se": 0,
      "var": 0,
      "std": 0,
      "n": 0,
      "pdf":
      {
        "values" : [],
        "density" : [],
        "cdf": []
        },
      "quantiles": [],
      "quantile_vals": [],
      "tests": {}
  }
  return template

def createCategoricalTemplate():
  template = {
      "desc": "",
      "labels": [],
      "counts": [],
      "ratios": [],
      "sum": 0
  }
  return template

def createResultsTemplate(config):
  template = {}
  for branch in config['branches']:
    template[branch] = {}
    for segment in config['segments']:
      template[branch][segment] = {
                      "histograms": {},
                      "pageload_event_metrics": {}
                    }

      
      for histogram in config['histograms']:
        hist_name = histogram.split(".")[-1]
        if config['histograms'][histogram]['kind'] == 'categorical':
          template[branch][segment]["histograms"][hist_name] = createCategoricalTemplate()
        else:
          template[branch][segment]["histograms"][hist_name] = createNumericalTemplate()

      for metric in config["pageload_event_metrics"]:
        template[branch][segment]["pageload_event_metrics"][metric] = createNumericalTemplate()

  return template

class DataAnalyzer:
  def __init__(self, config):
    self.config = config
    self.event_controldf = None
    self.control = self.config["branches"][0]
    self.results = createResultsTemplate(config)

    self.binVals = {}
    for field in self.config["pageload_event_metrics"]:
      self.binVals[field] = 'auto'

  def processTelemetryData(self, telemetryData):
    for branch in self.config['branches']:
      self.processTelemetryDataForBranch(telemetryData, branch)
    return self.results

  def processTelemetryDataForBranch(self, data, branch):
    self.processHistogramData(data, branch)
    self.processPageLoadEventData(data, branch)

  def processNumericalHistogramData(self, hist, data, branch, segment):
    hist_name = hist.split('.')[-1]
    print(f"      processing numerical histogram: {hist}")

    # Calculate stats
    bins = data[branch][segment]["histograms"][hist]["bins"]
    counts = data[branch][segment]["histograms"][hist]["counts"]

    desc = self.config["histograms"][hist]["desc"]
    self.results[branch][segment]["histograms"][hist_name]["desc"] = desc

    calculate_histogram_stats(bins, counts, self.results[branch][segment]["histograms"][hist_name])

    # Calculate statistical tests
    if branch != self.control:
      control_data = data[self.control][segment]["histograms"][hist]
      branch_data = data[branch][segment]["histograms"][hist]
      result = self.results[branch][segment]["histograms"][hist_name]
      calculate_histogram_tests_subsampling(control_data, branch_data, result)

  def processCategoricalHistogramData(self, hist, data, branch, segment):
    hist_name = hist.split('.')[-1]
    print(f"      processing categorical histogram: {hist}")
    desc = self.config["histograms"][hist]["desc"]
    labels = data[branch][segment]["histograms"][hist]["bins"]
    counts = data[branch][segment]["histograms"][hist]["counts"]

    self.results[branch][segment]["histograms"][hist_name]["desc"] = desc
    self.results[branch][segment]["histograms"][hist_name]["labels"] = labels
    self.results[branch][segment]["histograms"][hist_name]["counts"] = counts
    total = sum(counts)

    self.results[branch][segment]["histograms"][hist_name]["sum"] = total
    ratios = [x/total for x in counts]
    self.results[branch][segment]["histograms"][hist_name]["ratios"] = ratios

    if branch != self.control:
      ratios_control = self.results[self.control][segment]["histograms"][hist_name]["ratios"]
      uplift = []
      for i in range(len(ratios)):
        uplift.append((ratios[i]-ratios_control[i])*100)
        self.results[branch][segment]["histograms"][hist_name]["uplift"] = uplift

  def processHistogramData(self, data, branch):
    print(f"Calculating histogram statistics for branch: {branch}")
    for segment in self.config['segments']:
      print(f"  processing segment: {segment}")
    
      for hist in self.config["histograms"]:
        kind = self.config["histograms"][hist]["kind"]
        if kind=="categorical":
          self.processCategoricalHistogramData(hist, data, branch, segment)
        else:
          self.processNumericalHistogramData(hist, data, branch, segment)


  def processPageLoadEventData(self, data, branch):
    print(f"Calculating pageload event statistics for branch: {branch}")
    for segment in self.config['segments']:
      print(f"  processing segment: {segment}")
    
      for metric in self.config["pageload_event_metrics"]:
        print(f"      processing metric: {metric}")

        # Calculate stats
        bins = data[branch][segment]["pageload_event_metrics"][metric]["bins"]
        counts = data[branch][segment]["pageload_event_metrics"][metric]["counts"]
        desc = self.config["pageload_event_metrics"][metric]["desc"]
        self.results[branch][segment]["pageload_event_metrics"][metric]["desc"] = desc
        calculate_histogram_stats(bins, counts, self.results[branch][segment]["pageload_event_metrics"][metric])

        # Calculate statistical tests
        if branch != self.control:
          control_data = data[self.control][segment]["pageload_event_metrics"][metric]
          branch_data = data[branch][segment]["pageload_event_metrics"][metric]
          result = self.results[branch][segment]["pageload_event_metrics"][metric]
          calculate_histogram_tests_subsampling(control_data, branch_data, result)

        # Calculate statistical tests
        #if branch != self.control:
        #  control_data = self.results[self.control][segment]["pageload_event_metrics"][metric]
        #  branch_data = self.results[branch][segment]["pageload_event_metrics"][metric]
        #  calculate_histogram_ttest(bins, counts, branch_data, control_data)
