# Ad Campaign Metrics Importer Job

This job is responsible for importing Kevel campaign impression data (the number of impressions and clicks) for active Kevel campaigns and storing it in Shepherd DB.

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t ad-campaign-metrics-importer .
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
flake8 python_template_job/ tests/
black --diff python_template_job/ tests/
```
