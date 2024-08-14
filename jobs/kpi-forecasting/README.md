# KPI and other Metric Forecasting

This job forecasts [Metric Hub](https://mozilla.acryl.io/glossaryNode/urn:li:glossaryNode:Metric%20Hub/Contents?is_lineage_mode=false) metrics based on YAML configs defined in `.kpi-forecasting/configs`.

# Usage

## Docker Container

This job is intended to be run in a Docker container. If you're not familiar with Docker, it can be helpful to first install
[Docker Desktop](https://docs.docker.com/desktop/) which provides a GUI.

First, ensure that you have `CLOUDSDK_CONFIG` set as a environment variable for your shell so that Docker can find your gcloud credentials.
The default is `~/.config/gcloud`:

```sh
export CLOUDSDK_CONFIG="~/.config/gcloud"
```

To [build or re-build the Docker image](https://docs.docker.com/engine/reference/commandline/compose_build/), run the following command from the top-level `kpi-forecasting`. To force Docker to rebuild from scratch, pass the `--no-cache` flag.

```sh
docker compose build
```

Start a container from the Docker image with the following command:

```sh
docker compose up
```

To run the tests in the container the way they are run in the CI use the following command:
```sh
docker run kpi-forecasting-app  pytest --ruff --ruff-format
```

Note that if the code changes, `docker compose build` needs to be re-run for `docker run` to reflect the changes.

## Local Python
### Setup

You can also run the code outside of a Docker container. The code below creates a new Conda environment called `kpi-forecasting-dev`.
It assumes you have Conda installed. If you'd like to run the code in a Jupyter notebook, it is handy to install Jupyter in your `base` environment.
The `ipykernel` commands below will ensure that the `kpi-forecasting-dev` environment is made available to Jupyter.

```sh
conda create --name kpi-forecasting-dev python=3.10 pip ipykernel
conda activate kpi-forecasting-dev
ipython kernel install --name kpi-forecasting-dev --user
pip install -r requirements.txt
conda deactivate
```

If you're running on an M1 Mac, there are [currently some additional steps](https://github.com/facebook/prophet/issues/2250#issuecomment-1317709209) that you'll need to take to get Prophet running. From within your python environment, run the following (making sure to update the path appropriately):

```python
import cmdstanpy
cmdstanpy.install_cmdstan(overwrite=True, compiler=True, dir='/PATH/TO/CONDA/envs/kpi-forecasting-dev/lib/')
```

and then from the command line:

```sh
cd ~/PATH/TO/CONDA/envs/kpi-forecasting-dev/lib/python3.10/site-packages/prophet/stan_model
install_name_tool -add_rpath /PATH/TO/CONDA/envs/kpi-forecasting-dev/lib/cmdstan-2.32.2/stan/lib/stan_math/lib/tbb prophet_model.bin
```

### Running locally
A metric can be forecasted by using a command line argument that passes the relevant YAML file to the `kpi_forecasting.py` script.
[Here are approaches for accessing a Docker container's terminal](https://docs.docker.com/desktop/use-desktop/container/#integrated-terminal).

For example, the following command forecasts Desktop DAU numbers:

```sh
python ./kpi_forecasting.py -c ./kpi_forecasting/configs/dau_desktop.yaml
```

Similarly, the model performance for the KPI forecast can be done by executing:
```sh
python ./performance_analysis.py -c ./kpi_forecasting/configs/kpi_model_performance.yaml
```

Note that, without write permissions to `moz-fx-data-shared-prod` this will generate a permissions error.

The tests can be run locally with `python -m pytest` in the root directory of this subpackage.

# YAML Configs

Configuration for each forecast is found in the `configs` folder.  Below is an example config file with sample values and a description of what the field means as a comment when it is not self-evident

```
metric_hub:  # this configures the observed data fed to the model which is obtained via metrichub
  app_name: "multi_product"  # metric-hub app name
  slug: "search_forecasting_ad_clicks"  # metric-hub slug
  alias: "search_forecasting_ad_clicks"  # metric-hub alias
  start_date: "2018-01-01"  # date at which the observed data should start
  end_date: "last complete month"
    # date at which the observed data will end, can be a date or "last complete month" 
    # which uses `utils.parse_end_date` to determine the last complete month  
  segments:  
        # this section is optional and currently only used in funnel forecast, 
        # specifies which segments are used to partition the data, 
        # enabling separate models to be fit for each partition.  
        # Values underneath are a map of column names to be output by the 
        # metric-hub call and the SQL queries to populate those columns 
    device: "device"
    channel: "'all'"
    country: "CASE WHEN country = 'US' THEN 'US' ELSE 'ROW' END"
    partner: "partner"
  where: "partner = 'Google'"  # filter to apply to the metric hub pull

forecast_model:  # this section configures the model 
  model_type: "funnel"  
    # type of model object to use, current options are "funnel" for FunnelForecast and "prophet" for ProphetForecast
  start_date: NULL  
    # starting date for the predicted data (unless predict_historical_dates is set), 
    # if unset, value depends on predict_historical_dates.
  end_date: NULL
    # final date for the predicted data
  use_all_us_holidays: False
    For prophet-based models, when true, call `model.add_country_holidays(country_name="US")` on model
  predict_historical_dates: True
    # if predict_historical_dates is True, set to first date of the observed data
    # if predict_historical_dates is False, defaults to the day after the last day in the observed data
  number_of_simulations: 1000
    # for prophet-based models,number of simulations to run
  parameters:
    # this section can be a map or a list.  
    # If it's a map, these parameters are used for all models
    # (recall multiple models are train if there is a metric_hub.segments)
    # If it's a list, it will set different parameters
    # for different subsets of the parition specified in `metric_hub.segments`. 
    - segment: 
        # specifies which subset of the partitions this applies to
        # key is a column specified in metric_hub.segments
        # value is a value that column can take to which the configuration is applied
        device: desktop
      start_date: "2018-01-01"  # only applies to FunnelForecast, allows one to set start date for each sub-model
      end_date: NULL # only applies to FunnelForecast, allows one to set end date for each sub-model
      holidays: ["easter", "covid_sip11"]  # holidays specified in `configs.model_inputs.holidays` to use.
      regressors: ["post_esr_migration", "in_covid", "ad_click_bug"] # regressors specified in `configs.model_inputs.regressors`
      grid_parameters:
        # sets grid for hyperparameter tuning
        changepoint_prior_scale: [0.001, 0.01, 0.1, 0.2, 0.5] # parameter of prior distribution controlling how much the trend fluctuates at changepoints
        changepoint_range: [0.8, 0.9] # the proportion of the time series over which the changepoints are distributed
        n_changepoints: [25, 50] # number of trend changepoints, equally spaced over the time series
        weekly_seasonality: True # if weekly seasonality is included in the model
        yearly_seasonality: True # if yearly seasonality is included in the model
      cv_settings:
        # sets parameters for prophet cross-validation used in FunnelForecast
        initial: "1296 days" # the initial training period, used to train the first iteration of the model for CV
        period: "30 days" # spacing between cutoff dates, the sliding window over which each round of cross validation is performed
        horizon: "30 days" # forecast horizon used to make predictions and calculate model fit metrics for optimization
        parallel: "processes" # how parallelization is performed by Prophet, or None if no paralellization is used
    ...

summarize:
    # parameters used to summarize and aggregate the predictions
  periods: ["day", "month"]  # periods to aggregate up to
  numpy_aggregations: ["mean"] # numpy aggregation functions to use when aggregating predictions
  percentiles: [10, 50, 90] # precentiles to calculate on aggregation

write_results:
    # set the project, dataset and table for output data
  project: "moz-fx-data-shared-prod"
  dataset: "search_derived"
  table: "search_funnel_forecasts_v1"
  components_table: "search_forecast_model_components_v1"
```

# Development

- `./kpi_forecasting.py` is the main control script for KPI and Search Forecasting.
- `./kpi_forecasting/results_processing.py` is the control script for running validation on the tables crated by `kpi_forecasting.py`
- `./kpi_forecasting/configs` contains configuration YAML files.
- `./kpi_forecasting/models` contains the forecasting models.

This repo was designed to make it simple to add new forecasting models in the future. In general, a model needs to inherit
the `models.base_forecast.BaseForecast` class and to implement the `_fit` and `_predict` methods. Output from the `_fit` method will automatically be validated by `BaseForecast._validate_forecast_df`.

One caveat is that, in order for aggregations over time periods to work (e.g. monthly forecasts), the `_predict` method must generate a number
of simulated timeseries. This enables the measurement of variation across a range of possible outcomes. This number is set by `BaseForecast.number_of_simulations`.

When testing locally, be sure to modify any config files to use non-production `project` and `dataset` values that you have write access to; otherwise the `write_output` step will fail.
