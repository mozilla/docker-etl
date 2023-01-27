# Python Template Job

This script pulls macroeconomic data into BigQuery daily

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t macroeconomic-indicies .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the script with 

```sh   
python -m macroeconomic_indicies/main.py --dry-run --submission-date "2023-01-01"
```
