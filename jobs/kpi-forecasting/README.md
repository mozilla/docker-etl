# KPI and other Metric Forecasting

This job forecasts [Metric Hub](https://mozilla.github.io/metric-hub/) metrics based on YAML configs defined in `.kpi-forecasting/configs`.

# Usage

### Docker Container

This job is intended to be run in a Docker container. If you're not familiar with Docker, it can be helpful to first install
[Docker Desktop](https://docs.docker.com/desktop/) which provides a GUI.

From the top-level `kpi-forecasting` directory, build the Docker image with the following command (note the trailing `.`):

```sh
docker build -t kpi-forecasting .
```

A metric can be forecasted by using a command line argument to pass the relevant YAML file to the `kpi_forecasting.py` script.
[Here are approaches for accessing a Docker container's terminal](https://docs.docker.com/desktop/use-desktop/container/#integrated-terminal).

The following command forecasts Desktop DAU numbers:

```sh
python ~/kpi-forecasting/kpi_forecasting.py -c ~/kpi-forecasting/configs/dau_desktop.yaml
```

### Local Python

You can also run the code outside of a Docker container. The code below creates a new Conda environment called `kpi-forecasting-dev`.
It assumes you have Conda installed. If you'd like to run the code in a Jupyter notebook, it is handy to install Jupyter in a `base` environment.
The `ipykernel` commands below will ensure that the `kpi-forecasting-dev` environment is made available to Jupyter.

```sh
conda create --name kpi-forecasting-dev python=3.10 pip ipykernel
conda activate kpi-forecasting-dev
ipython kernel install --name kpi-forecasting-dev --user
pip install -r requirements.txt
conda deactivate
```

If you're running on an M1 Mac, there are [currently some additional steps](https://github.com/facebook/prophet/issues/2250#issuecomment-1317709209) that you'll need to take to get Prophet running. From within
your python environment, run:

```python
import cmdstanpy
cmdstanpy.install_cmdstan(overwrite=True, compiler=True, dir='/path/to/conda/envs/kpi-forecasting-dev/lib/')
```

and then from the command line:

```sh
cd ~/path/to/conda/envs/kpi-forecasting-dev/lib/python3.10/site-packages/prophet/stan_model
install_name_tool -add_rpath /path/to/conda/envs/kpi-forecasting-dev/lib/cmdstan-2.32.2/stan/lib/stan_math/lib/tbb prophet_model.bin
```

### YAML Configs

For consistency, keys are lowercased

- target: platform you wish to run, current accepted values are 'desktop' and 'mobile'
- query_name: the name of the .sql file in the sql_queries folder to use to pull data from
- columns: will cut down query to only the columns included in this list. Rather than try to be so supremely flexible that it ends up making more work down the road to comply to an API spec, this repo is set up to handle the desktop and mobile scripts as they currently exist. If you wish to add a new forecast, model it after Mobile
- forecast_parameters: model fit parameters, must conform to prophet API spec
- dataset_project: the project to use for pulling data from, e.g. mozdata
- write_project: project that results will be written too, e.g. moz-fx-data-bq-data-science
- output_table: table to write results to, if testing consider something like {your-name}.automation_experiment
- confidences_table: table to write confidences too, if confidences is not None. if it is, will be ignored
- forecast_variable: the variable you are actually forecasting, e.g. QDOU or DAU
- holidays: boolean - include holidays (if set to False holidays will always show zero, but the columns will still exist)
- stop_date: date to stop the forecast at
- confidences: aggregation unit for confidence intervals, can be ds_month, ds_year or None

## Development

./kpi_forecasting.py is the main control script
/Utils contains the bulk of the python code
/yaml contains configuration yaml files
/sql_queries contains the queries to pull data from bigquery
