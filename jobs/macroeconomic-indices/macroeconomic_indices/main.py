import click
import pandas as pd
import yfinance as yf
from google.cloud import bigquery


DATASET = "revenue_derived"
TABLE = "macroeconomic_indices"
TICKER_LIST = [
    "^DJI",  # Dow Jones Industrial Average
    "^GSPC",  # SNP - SNP Real Time Price. Currency in USD
    "^IXIC",  # Nasdaq GIDS - Nasdaq GIDS Real Time Price. Currency in USD
    "EURUSD=X",  # Euro to USD exchange rate
    "GBPUSD=X",  # GB pound to USD exchange rate
]


def get_macro_data(start_date: str, end_date: str) -> dict:
    """Pull macroeconomic data from start_date to end_date (inclusive)"""
    macro_data = []

    for ticker in TICKER_LIST:
        data = yf.download(ticker, start=start_date, end=end_date)
        data["ticker"] = ticker
        macro_data.append(data)

    macro_df = pd.concat(macro_data)
    macro_df = macro_df.reset_index()
    macro_df.rename(
        columns={
            "Date": "market_date",
            "Open": "open",
            "Close": "close",
            "Adj Close": "adj_close",
            "High": "high",
            "Low": "low",
            "Volume": "volume",
        },
        inplace=True,
    )
    macro_df.market_date = macro_df.market_date.apply(pd.Timestamp.date)
    macro_df.ticker = macro_df.ticker.astype(str)
    return macro_df


@click.command()
@click.option("--project-id", required=True)
@click.option("--submission-date")
@click.option("--dry-run", is_flag=True, default=False)
@click.option("--backfill", is_flag=True, default=False)
@click.option("--start-date")
@click.option("--end-date")
def main(project_id, submission_date, dry_run, backfill, start_date, end_date):
    assert (
        submission_date is not None or backfill
    ), "Either a submission date or the --backfill flag is required"

    if backfill:
        assert (
            start_date is not None and end_date is not None
        ), "You must provide a start and end date to backfill"
        if dry_run:
            print(
                f"Dry-run mode: Skipping downloading {TICKER_LIST} from {start_date} "
                f"to {end_date}"
            )
        else:
            macro_df = get_macro_data(start_date, end_date)

    else:
        if dry_run:
            print(
                f"Dry-run mode: Skipping download of {TICKER_LIST} from "
                f"{submission_date}"
            )
        else:
            macro_df = get_macro_data(submission_date, submission_date)

    if dry_run:
        print(
            f"Dry-run mode: Skipping loading data to `{project_id}.{DATASET}.{TABLE}`"
        )
        return

    client = bigquery.Client(project=project_id)
    dataset = client.dataset(DATASET)
    table = dataset.table(TABLE)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_dataframe(macro_df, table, job_config=job_config)
    print(job.result())


if __name__ == "__main__":
    main()
