# Load libraries
import requests
from google.cloud import storage
from datetime import datetime
import time
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from argparse import ArgumentParser
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.webdriver import WebDriver as ChromiumDriver
from selenium import webdriver

# Website with chrome release data
CHROME_RELEASES_URL = "https://developer.chrome.com/release-notes"
CHROME_RELEASES_BASE_URL = "https://developer.chrome.com"

# Website with AI in Chrome information
AI_IN_CHROME_URL = "https://developer.chrome.com/docs/ai"

# Define where to write the data in GCS
GCS_BUCKET = "gs://moz-fx-data-prod-external-data/"
BUCKET_NO_GS = "moz-fx-data-prod-external-data"
RESULTS_FPATH_1 = "MARKET_RESEARCH/SCRAPED_INFO/ChromeReleaseNotes/WebScraping_"
RESULTS_FPATH_2 = "MARKET_RESEARCH/SCRAPED_INFO/ChromeAI/WebScraping_"
TIMEOUT_IN_SECONDS = 20
DRIVER_TYP = "Chrome"
BINARY_LOC = "/usr/bin/google-chrome-stable"
DRIVER_PATH = "/usr/local/bin/chromedriver"


def initialize_driver(driver_type, binary_location, driver_path):
    """Inputs: Driver type (Chrome or Chromium), binary location, driver path
    Outputs: A webdriver
    """
    if driver_type == "Chromium":
        options = Options()
        options.binary_location = binary_location
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        driver = ChromiumDriver(service=Service(driver_path), options=options)

    elif driver_type == "Chrome":
        options = Options()
        options.binary_location = binary_location
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        driver = webdriver.Chrome(service=Service(driver_path), options=options)

    else:
        raise ValueError("DRIVER_TYPE needs to be either Chrome or Chromium")

    return driver


def get_latest_chrome_release_url(driver, chrome_releases_main_page, base_url):
    """Get the URL for the latest chrome release"""
    # Load the main Chrome releases web page
    driver.get(chrome_releases_main_page)

    # Give it 3 seconds for JS to load
    time.sleep(3)

    # Get the web page source
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Close the driver
    driver.quit()

    # Get all the links found
    links = [urljoin(base_url, a["href"]) for a in soup.find_all("a", href=True)]

    # Initialize a list of all unique links found on the page
    unique_links = []

    # For each link, if link not in the unique list, add the link
    for link in links:
        if link not in unique_links:
            unique_links.append(link)

    # Get only the links that are for "release note" pages
    release_detail_links = []
    print("all release detail links found: ")
    for link in unique_links:
        if link.startswith(
            "https://developer.chrome.com/release-notes/"
        ) and link.endswith("?hl=en"):
            release_detail_links.append(link)
            print(link)

    # Find the link for the release note page with the highest number
    highest_release_number = None
    highest_release_detail_link = None

    for release_dtl_lnk in release_detail_links:
        match = re.search(r"(?<!\d)(\d+)(?!\d)", release_dtl_lnk)
        if match:
            found_number = match.group(1)
            if highest_release_number is None:
                highest_release_number = found_number
                highest_release_detail_link = release_dtl_lnk
            else:
                if highest_release_number < found_number:
                    highest_release_number = found_number
                    highest_release_detail_link = release_dtl_lnk

    return highest_release_detail_link


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    # Get DAG logical date
    logical_dag_date = datetime.strptime(args.date, "%Y-%m-%d").date()
    logical_dag_date_string = logical_dag_date.strftime("%Y%m%d")

    # Make final output filepaths using DAG date
    final_output_fpath1 = RESULTS_FPATH_1 + logical_dag_date_string + ".txt"
    final_output_fpath2 = RESULTS_FPATH_2 + logical_dag_date_string + ".txt"

    # Initialize the driver
    driver = initialize_driver(DRIVER_TYP, BINARY_LOC, DRIVER_PATH)

    # Get latest chrome release URL
    latest_chrome_release_url = get_latest_chrome_release_url(
        driver, CHROME_RELEASES_URL, CHROME_RELEASES_BASE_URL
    )
    print("Latest Chrome Release URL Found: ", latest_chrome_release_url)

    # Get latest Chrome release info
    chrome_release_url_response = requests.get(
        latest_chrome_release_url, timeout=TIMEOUT_IN_SECONDS
    )
    soup = BeautifulSoup(chrome_release_url_response.text, "html.parser")
    final_output_1 = soup.get_text(separator="\n", strip=True)

    # Get info about AI on Chrome
    chrome_ai_url_response = requests.get(AI_IN_CHROME_URL, timeout=TIMEOUT_IN_SECONDS)
    soup = BeautifulSoup(chrome_ai_url_response.text, "html.parser")
    final_output_2 = soup.get_text(separator="\n", strip=True)

    # Open up a client to GCS
    client = storage.Client(project="moz-fx-data-shared-prod")
    bucket = client.bucket(BUCKET_NO_GS)

    blob = bucket.blob(final_output_fpath1)
    blob.upload_from_string(final_output_1)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath1}")

    blob2 = bucket.blob(final_output_fpath2)
    blob2.upload_from_string(final_output_2)
    print(f"Summary uploaded to gs://{BUCKET_NO_GS}/{final_output_fpath1}")


if __name__ == "__main__":
    main()
