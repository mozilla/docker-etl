import logging
import os
import urllib
from datetime import datetime

import click
import requests
from google.cloud import bigquery
from typing import Dict, List, Optional

BASE_URL = "https://financialmodelingprep.com/api/v3"
DATASET = "revenue_derived"  # TODO: Figure out where this will go
TABLE = "macroeconomic_indices_v1"
INDEX_TICKERS = [
    "^DJI",  # Dow Jones Industrial Average
    "^GSPC",  # SNP - SNP Real Time Price. Currency in USD
    "^IXIC",  # Nasdaq GIDS - Nasdaq GIDS Real Time Price. Currency in USD
]
FOREX_TICKERS = [
    "EURUSD=X",  # Euro to USD exchange rate
    "GBPUSD=X",  # GB pound to USD exchange rate
]


def get_index_ticker(
    api_key: str, ticker: str, start_date: datetime, end_date: datetime
) -> str:
    encoded_ticker = urllib.parse.quote(ticker)
    url = f"{BASE_URL}/historical-price-full/index/{encoded_ticker}"

    response = requests.get(
        url,
        params={
            "apikey": api_key,
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
        },
    )
    return response.json()


def get_forex_ticker(
    api_key: str, ticker: str, start_date: datetime, end_date: datetime
) -> str:
    encoded_ticker = urllib.parse.quote(ticker)
    url = f"{BASE_URL}/historical-price-full/forex/{encoded_ticker}"

    response = requests.get(
        url,
        params={
            "apikey": api_key,
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
        },
    )
    return response.json()


def get_macro_data(api_key: str, start_date: datetime, end_date: datetime) -> dict:
    """Pull macroeconomic data from start_date to end_date (inclusive)"""
    macro_data = []

    for ticker in INDEX_TICKERS:
        ticker_data = get_index_ticker(
            api_key=api_key,
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
        )
        # Exchanges are closed on weekends and some holidays, so this could
        # return nothing
        if ticker_data:
            macro_data.append(ticker_data)

    for ticker in FOREX_TICKERS:
        ticker_data = get_forex_ticker(
            api_key=api_key,
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
        )
        macro_data.append(ticker_data)

    return [
        {
            "symbol": row["symbol"],
            "market_date": day["date"],
            "open": day["open"],
            "close": day["close"],
            "adj_close": day["adjClose"],
            "high": day["high"],
            "low": day["low"],
            "volume": day["volume"],
        }
        for row in macro_data
        for day in row["historical"]
    ]


def load_data_to_bq(
    project_id: str, macro_data: List[Dict], partition: Optional[str] = None
):
    client = bigquery.Client(project=project_id)
    dataset = client.dataset(DATASET)
    if partition:
        partition_str = partition.strftime("%Y%m%d")
        table = dataset.table(f"{TABLE}${partition_str}")
    else:
        table = dataset.table(TABLE)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("symbol", "STRING"),
            bigquery.SchemaField("market_date", "DATE"),
            bigquery.SchemaField("open", "NUMERIC"),
            bigquery.SchemaField("close", "NUMERIC"),
            bigquery.SchemaField("adj_close", "NUMERIC"),
            bigquery.SchemaField("high", "NUMERIC"),
            bigquery.SchemaField("low", "NUMERIC"),
            bigquery.SchemaField("volume", "INTEGER"),
        ],
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    job = client.load_table_from_json(macro_data, table, job_config=job_config)
    job.result()


@click.command()
@click.option("--project-id", required=True)
@click.option("--submission-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--backfill", is_flag=True, default=False)
@click.option("--start-date", type=click.DateTime(formats=["%Y-%m-%d"]))
@click.option("--end-date", type=click.DateTime(formats=["%Y-%m-%d"]))
def main(project_id, submission_date, dry_run, backfill, start_date, end_date):
    api_key = os.getenv("FMP_API_KEY")
    assert api_key, "Environment variable FMP_API_KEY must be set"

    if backfill:
        assert (
            start_date is not None and end_date is not None
        ), "You must provide a start and end date to backfill"
        assert submission_date is None, "submission-date does not apply to backfills"
        if dry_run:
            logging.info(
                f"Dry-run mode: Skipping download of {INDEX_TICKERS + FOREX_TICKERS} "
                f"from {start_date} to {end_date}"
            )
        else:
            macro_data = get_macro_data(api_key, start_date, end_date)
            load_data_to_bq(
                project_id=project_id,
                macro_data=macro_data,
            )
            logging.info(f"Backfilled {TABLE} from {start_date} to {end_date}")

    else:
        assert (
            start_date is None and end_date is None
        ), "Start and end date only apply to backfills"
        assert (
            submission_date is not None
        ), "You must provide a submission date or --backfill + start and end date"
        if dry_run:
            logging.info(
                f"Dry-run mode: Skipping download of {INDEX_TICKERS + FOREX_TICKERS} "
                f"on {submission_date}"
            )
        else:
            macro_data = get_macro_data(api_key, submission_date, submission_date)
            load_data_to_bq(
                project_id=project_id,
                macro_data=macro_data,
                partition=submission_date,
            )
            logging.info(f"Loaded data for {submission_date} to {TABLE}")


if __name__ == "__main__":
    main()
