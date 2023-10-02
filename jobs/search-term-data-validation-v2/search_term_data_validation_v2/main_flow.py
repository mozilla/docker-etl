print("Hello from main_flow.py!")

import argparse
import pandas as pd

from datetime import date, timedelta
from collections import namedtuple

from metaflow import FlowSpec, Parameter, step

from data_validation import retrieve_data_validation_metrics, record_validation_results

print("Dependencies successfully imported!")

class SearchTermDataValidationFlow(FlowSpec):
    data_validation_origin = Parameter('data_validation_origin',
                                       help='The table from which to draw the data for validation',
                                       required=True,
                                       type=str)

    data_validation_reporting_destination = Parameter('data_validation_reporting_destination',
                                                      help='The table into which to put the validation results',
                                                      required=True,
                                                      type=str)

    @step
    def start(self):
        print(f"Data Validation Origin: {self.data_validation_origin}")
        print(f"Data Validation Reporting Destination: {self.data_validation_reporting_destination}")

        print("success up to here...")
        self.next(self.retrieve_metrics)

    @step
    def retrieve_metrics(self):
        print("Retrieving Data Validation Metrics Now...")

        self.validation_df = retrieve_data_validation_metrics(self.data_validation_origin)
        self.next(self.record_results)

    @step
    def record_results(self):
        print(f"Input Dataframe Shape: {validation_df.shape}")
        print("Recording validation results...")
        record_validation_results(self.validation_df, self.data_validation_reporting_destination)
        self.next(self.end)

    @step
    def end(self):
        print(f'That was easy!')

if __name__ == '__main__':
    SearchTermDataValidationFlow()
