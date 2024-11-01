# wclouser-fxa-db-counts

This is a simple job to enable us to identify trends within accounts data.  E.g. "How many inactive accounts are there?"

## Usage

This script is intended to be run in a docker container.
Build the docker image with:

```sh
docker build -t wclouser_fxa_db_counts .
```

To run locally, install dependencies with:

```sh
pip install -r requirements.txt
```

Run the script with 

```sh   
python3 -m wclouser_fxa_db_counts.main
```