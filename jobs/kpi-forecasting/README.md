# KPI and other Metric Forecasting

This job forecasts [Metric Hub](https://mozilla.github.io/metric-hub/) metrics based on YAML configs defined in `.kpi-forecasting/configs`.

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
python ~/kpi_forecasting.py -c ~/kpi_forecasting/configs/dau_desktop.yaml
```

Similarly, the model performance for the KPI forecast can be done by executing:
```sh
python ~/model_performance_analysis.py.py -c ~/kpi_forecasting/configs/kpi_model_validation.yaml   
```

Note that, without write permissions to `moz-fx-data-shared-prod` this will generate a permissions error.

The tests can be run locally with `python -m pytest` in the root directory of this subpackage.

# YAML Configs

Each of the sections in the YAML files contains a list of arguments that are passed to their relevant objects or methods.
Definitions should be documented in the code.

# Development

- `./kpi_forecasting/kpi_forecasting.py` is the main control script for KPI and Search Forecasting.
- `./result_processing.py` is the control script for running validation on the tables crated by `kpi_forecasting.py`
- `./kpi_forecasting/configs` contains configuration YAML files.
- `./kpi_forecasting/models` contains the forecasting models.

This repo was designed to make it simple to add new forecasting models in the future. In general, a model needs to inherit
the `models.base_forecast.BaseForecast` class and to implement the `_fit` and `_predict` methods. Output from the `_fit` method will automatically be validated by `BaseForecast._validate_forecast_df`.

One caveat is that, in order for aggregations over time periods to work (e.g. monthly forecasts), the `_predict` method must generate a number
of simulated timeseries. This enables the measurement of variation across a range of possible outcomes. This number is set by `BaseForecast.number_of_simulations`.

When testing locally, be sure to modify any config files to use non-production `project` and `dataset` values that you have write access to; otherwise the `write_output` step will fail.
