print("Hello from main_flow.py!")

import argparse
import pandas as pd

from datetime import date, timedelta
from collections import namedtuple

import wandb
from wandb.integration.metaflow import wandb_log
from metaflow import FlowSpec, Parameter, step

from data_validation import retrieve_data_validation_metrics, record_validation_results


@wandb_log(datasets=True, settings=wandb.Settings(project='search-terms-data-validation'))
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
        '''
        Metaflow flows must begin with a function called 'start.'
        So here's the start function.
        It prints out the input parameters to the job
        and initializes an experiment tracking run.
        '''
        print(f"Data Validation Origin: {self.data_validation_origin}")
        print(f"Data Validation Reporting Destination: {self.data_validation_reporting_destination}")

        self.next(self.retrieve_metrics)

    @step
    def retrieve_metrics(self):
        '''
        Retrieves search term sanitization aggregation data from BigQuery,
        then checks that they have not varied outside appreciable tolerances
        in the past X days ('X' is a window set for each metric)
        '''
        print("Retrieving Data Validation Metrics Now...")

        self.validation_df = retrieve_data_validation_metrics(self.data_validation_origin)
        self.next(self.record_results)

    @step
    def record_results(self):
        '''
        Shoves the validation metrics calculated in the prior step into a BigQuery table.
        That table has a dashboard in looker, complete with alerts
        to notify data scientists if there are any changes that require manual inspection.
        '''
        print(f"Input Dataframe Shape: {self.validation_df.shape}")
        print("Recording validation results...")
        record_validation_results(self.validation_df, self.data_validation_reporting_destination)
        self.next(self.end)

    @step
    def end(self):
        '''
         Metaflow flows end with a function called 'end.'
         So here's the end function. It prints an encouraging message.
         We could all use one every now and then.
         '''
        print(f'That was easy!')

if __name__ == '__main__':
    SearchTermDataValidationFlow()
