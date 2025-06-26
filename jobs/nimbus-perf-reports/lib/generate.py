#!/usr/bin/env python3
import json
import os
import sys
import time
import numpy as np
import django
from django.apps import apps
import lib.parser as parser
from django.conf import settings
from lib.telemetry import TelemetryClient
from lib.analysis import DataAnalyzer
from lib.report import ReportGenerator

class NpEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, np.integer):
      return int(obj)
    if isinstance(obj, np.floating):
      return float(obj)
    if isinstance(obj, np.ndarray):
      return obj.tolist()
    return super(NpEncoder, self).default(obj)

def setupDjango():
  if apps.ready:
    return

  TEMPLATES = [
      {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        'DIRS': [os.path.join(os.path.dirname(__file__),'templates','sql'),
                 os.path.join(os.path.dirname(__file__),'templates','html')]
        }
      ]
  settings.configure(TEMPLATES=TEMPLATES)
  django.setup()

def setupDirs(slug, dataDir, reportDir, generate_report):
  if not os.path.isdir(dataDir):
    os.mkdir(dataDir)
  if not os.path.isdir(os.path.join(dataDir,slug)):
    os.mkdir(os.path.join(dataDir,slug))
  if generate_report:
    if not os.path.isdir(reportDir):
      os.mkdir(reportDir)

def getResultsForExperiment(slug, dataDir, config, skipCache):
  sqlClient = TelemetryClient(dataDir, config, skipCache)
  telemetryData = sqlClient.getResults()

  # Change the branches to a list for easier use during analysis.
  branch_names = []
  for i in range(len(config['branches'])):
    branch_names.append(config['branches'][i]['name'])
  config['branches'] = branch_names

  analyzer = DataAnalyzer(config)
  results = analyzer.processTelemetryData(telemetryData)

  # Save the queries into the results and cache them.
  queriesFile=os.path.join(dataDir, f"{slug}-queries.json")
  if 'queries' in telemetryData and telemetryData['queries']:
    with open(queriesFile, 'w') as f:
      json.dump(telemetryData['queries'], f, indent=2, cls=NpEncoder)
  else:
    queries = checkForLocalResults(queriesFile)
    if queries is not None:
      telemetryData['queries'] = queries

  results['queries'] = telemetryData['queries']
  return results

def checkForLocalResults(resultsFile):
  if os.path.isfile(resultsFile):
    with open(resultsFile, 'r') as f:
      results = json.load(f)
      return results
  return None

def generate_report(args):
  startTime = time.time()

  setupDjango()

  # Parse config file.
  print("Loading config file: ", args.config)
  config = parser.parseConfigFile(args.config)
  slug = config['slug']

  # Setup local dirs
  print("Setting up local directories.")
  setupDirs(slug, args.dataDir, args.reportDir, args.html_report)
  dataDir=os.path.join(args.dataDir, slug)
  reportDir=args.reportDir
  skipCache=args.skip_cache

  # Check for local results first.
  resultsFile= os.path.join(dataDir, f"{slug}-results.json")
  if skipCache:
    results = None
  else:
    results = checkForLocalResults(resultsFile)

  # If results not found, generate them.
  if results is None:
    # Annotate metrics
    parser.annotateMetrics(config)

    if config["is_experiment"] == True:
      # Parse Nimbus API.
      api = parser.parseNimbusAPI(dataDir, slug, skipCache)
      config = config | api

      # If the experiment is a rollout, then use the non-enrolled branch
      # as the control.
      if config['isRollout'] == True:
        config['include_non_enrolled_branch'] = True

      # If non-enrolled branch was included, add an extra branch.
      if 'include_non_enrolled_branch' in config:
        include_non_enrolled_branch = config['include_non_enrolled_branch']
        if include_non_enrolled_branch == True or include_non_enrolled_branch.lower() == "true":
          config['include_non_enrolled_branch'] = True
          if config['isRollout'] == True:
            config["branches"].insert(0, {'name': 'default'})
          else:
            config["branches"].append({'name': 'default'})
      else:
        config['include_non_enrolled_branch'] = False

      # Make control the first element if not already.
      if "control" in config:
        control = config["control"]
        del config["control"]
        if config["branches"][0]["name"] != control:
          for i,b in enumerate(config["branches"]):
            if b["name"] == control:
              tmpFirst   = config["branches"][0]
              tmpControl = config["branches"][i]
              config["branches"][i] = tmpFirst
              config["branches"][0] = tmpControl
              break

    print("Using Config:")
    configStr = json.dumps(config, indent=2)
    print(configStr)

    # Get statistical results
    origConfig = config.copy()
    results = getResultsForExperiment(slug, dataDir, config, skipCache)
    results = results | config
    results['input'] = origConfig

    # Save results to disk.
    print("---------------------------------")
    print(f"Writing results to {resultsFile}")
    with open(resultsFile, 'w') as f:
      json.dump(results, f, indent=2, cls=NpEncoder)
  else:
    print("---------------------------------")
    print(f"Found local results in {resultsFile}")

  if args.html_report:
    reportFile = os.path.join(reportDir, f"{slug}.html")
    print(f"Generating html report in {reportFile}")

    gen = ReportGenerator(results)
    report = gen.createHTMLReport()
    with open(reportFile, "w") as f:
      f.write(report)

  executionTime = time.time()-startTime
  print(f"Execution time: {executionTime:.1f} seconds")
