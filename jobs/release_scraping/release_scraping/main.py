# Load libraries
import requests
from google.cloud import storage
from datetime import datetime
from bs4 import BeautifulSoup
from argparse import ArgumentParser

# Website with chrome release data
CHROME_RELEASES_URL = "https://developer.chrome.com/release-notes"

# Website with AI in Chrome information
AI_IN_CHROME_URL = "https://developer.chrome.com/docs/ai"

# Define where to write the data in GCS
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
BUCKET_NO_GS = "moz-fx-data-prod-external-data"
RESULTS_FPATH_1 = "MARKET_RESEARCH/SCRAPED_INFO/ChromeReleaseNotes/WebScraping_"
RESULTS_FPATH_2 = "MARKET_RESEARCH/SCRAPED_INFO/ChromeAI/WebScraping_"
TIMEOUT_IN_SECONDS = 20


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    # Get DAG logical date
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    logical_dag_date_string = logical_dag_date.strftime("%Y%m%d")

    final_output_fpath1 = RESULTS_FPATH_1 + logical_dag_date_string + ".txt"
    final_output_fpath2 = RESULTS_FPATH_2 + logical_dag_date_string + ".txt"

    # Get latest Chrome release info
    chrome_release_url_response = requests.get(CHROME_RELEASES_URL)
    soup = BeautifulSoup(chrome_release_url_response.text, "html.parser")
    final_output_1 = soup.get_text(separator="\n", strip=True)

    # Get info about AI on Chrome
    chrome_ai_url_response = requests.get(AI_IN_CHROME_URL)
    soup = BeautifulSoup(chrome_ai_url_response.text, "html.parser")
    final_output_2 = soup.get_text(separator="\n", strip=True)

    # Open up a client to GCS
    client = storage.Client()
    bucket = client.get_bucket(BUCKET_NO_GS)

    blob = bucket.blob(final_output_fpath1)
    blob.upload_from_string(final_output_1)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath1}")

    blob2 = bucket.blob(final_output_fpath2)
    blob2.upload_from_string(final_output_2)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath1}")


if __name__ == "__main__":
    main()
