# Macroeconomic Indices Job

This job pulls stock exchange and foreign exchange data from [financialmodelingprep.com]
and loads to BigQuery for use in analysis. We have an FMP account that allows us to pull
and use this data for internal use. Credentials can be found in the `Revenue data` vault
in 1Password

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t macroeconomic-indices .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the script with 

```sh   
python -m macroeconomic_indices.main --project-id PROJECT --submission-date YYYY-MM-DD
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
flake8 macroeconomic_indices/ tests/
black --diff macroeconomic_indices/ tests/
```
