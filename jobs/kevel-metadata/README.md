# Kevel Metadata Extraction Job

This job:

- Extracts data from the Kevel API into memory. 
- Transforms the data into a single dataset saved to a local file.
- Loads that data into a file in Google Cloud Storage.
- Finally, we merge the data to create daily paritions.

This job is a migration of the logic that live here: https://github.com/Pocket/lambda-adzerk

Kevel used to be called Adzerk.

The API documentation is here: https://dev.kevel.com/reference/getting-started-with-kevel

The data extracts only 'active' flights.  This means we need to include existing data when doing the partition replacement.

TO DO: **Decide on proper merge logic to maintain proper history of inactive and active flights**.

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t kevel-metadata .
```

To run locally:

```sh
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

To run unit tests locally (from within venv):

```sh
cd kevel_metadata
python -m pytest -svv  --cov=src --cov-report term-missing --cov-fail-under=100 --cache-clear tests
```

Run the script with locally (needs gcloud auth):

```sh
python src/handler.py --project test-project --bucket test-bucket --env dev --api-key kevel API key
```

Run the script from docker locally (needs gcloud auth):

```sh
docker run -t kevel python kevel_metadata/src/handler.py --project test-project --bucket test-bucket --env dev --api-key kevel API key
```

Python code will need to be formatted with `black` by running:

```sh
cd kevel_metadata
black --exclude .venv . 
```

Production execution of this ETL is expected to managed by [WTMO](https://workflow.telemetry.mozilla.org/home) with the following command:


```sh
python src/handler.py --env production --api-key kevel API key
```

In production, the project and bucket values are static.