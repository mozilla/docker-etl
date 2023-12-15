# Telemetry Dev Cycle Dashboard external Data

This is a Docker file that downloads metrics from APIs for the telemetry dev cycle dashboard 

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t telemetry_dev_cycle .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the script with 

```sh   
python3 -m python_template_job.main
```

## Development

Run tests with:

```sh
pytest
```

`flake8` and `black` are included for code linting and formatting:

```sh
pytest --black --flake8
```

or

```sh
flake8 telemetry_dev_cycle/ tests/
black --diff telemetry_dev_cycle/ tests/
```
