import json
import re
import time

import feedparser
import requests
from argparse import ArgumentParser
from bs4 import BeautifulSoup
from datetime import datetime
from google.cloud import storage
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.webdriver import WebDriver as ChromiumDriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

BROWSERS_FYI_FEED = "https://www.browsers.fyi/feed/"
FEED_TITLE_RE = re.compile(r"^(.+) release (.+) is out!$")

GCS_BUCKET_NAME = "moz-fx-data-prod-external-data"
GCS_STRUCTURED_PREFIX = "MARKET_RESEARCH/STRUCTURED"

MIN_RELEASE_DATE = "2020-01-01"

TIMEOUT_IN_SECONDS = 20
REQUEST_DELAY_SECONDS = 2
DRIVER_TYP = "Chromium"
BINARY_LOC = "/usr/bin/chromium"
DRIVER_PATH = "/usr/bin/chromedriver"

# Browsers whose release notes pages require JavaScript rendering
JS_RENDERED_BROWSERS = {
    "Safari",
    "Safari on iOS",
}


def initialize_driver(driver_type, binary_location, driver_path):
    """Initialize a Selenium WebDriver instance."""
    options = Options()
    options.binary_location = binary_location
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    if driver_type == "Chromium":
        driver = ChromiumDriver(service=Service(driver_path), options=options)
    elif driver_type == "Chrome":
        driver = webdriver.Chrome(service=Service(driver_path), options=options)
    else:
        raise ValueError("DRIVER_TYPE needs to be either Chrome or Chromium")

    return driver


def parse_feed():
    """Parse the browsers.fyi Atom feed and return all browser release entries.

    Returns a list of dicts with keys: name, version, release_date, release_notes.
    Entries are ordered newest-first as provided by the feed.
    """
    feed = feedparser.parse(BROWSERS_FYI_FEED)
    releases = []
    for entry in feed.entries:
        title = getattr(entry, "title", "") or ""
        link = getattr(entry, "link", "") or ""
        updated = getattr(entry, "updated", "") or ""
        match = FEED_TITLE_RE.match(title)
        if not match or not link:
            continue
        releases.append(
            {
                "name": match.group(1),
                "version": match.group(2),
                "release_date": updated[:10],  # "2026-03-24T00:00:00Z" -> "2026-03-24"
                "release_notes": link,
            }
        )
    return releases


def gcs_path_for(browser_name, version, release_date):
    """Construct the GCS object path for a browser release.

    Uses release_date (not scraped_date) in the filename so the path is stable
    and can be used for deduplication across runs.
    """
    browser_path = browser_name.replace(" ", "_")
    version_clean = version.replace(".", "_")
    date_clean = release_date.replace("-", "")
    return (
        f"{GCS_STRUCTURED_PREFIX}/{browser_path}"
        f"/release_{version_clean}_{date_clean}.json"
    )


def scrape_page_text(url, driver=None, use_js=False):
    """Scrape plain text from a URL, using Selenium for JS-rendered pages."""
    if use_js and driver is not None:
        driver.get(url)
        WebDriverWait(driver, TIMEOUT_IN_SECONDS).until(
            EC.presence_of_element_located(("tag name", "body"))
        )
        # Allow additional time for JS to populate the body content
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")
    else:
        response = requests.get(url, timeout=TIMEOUT_IN_SECONDS)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    # Prefer the most specific semantic content element to avoid nav/sidebar bloat.
    # 1. <article> — MDN, Chrome for Developers
    # 2. Largest <div class="content"> — Microsoft Learn (Edge)
    # 3. <main> — MDN fallback, other sites
    # 4. Full page — last resort
    content = soup.find("article")
    if not content:
        content_divs = soup.find_all("div", class_="content")
        if content_divs:
            content = max(content_divs, key=lambda el: len(el.get_text()))
    if not content:
        content = soup.find("main")
    if not content:
        content = soup
    return content.get_text(separator="\n", strip=True)


def main():
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    scraped_date = datetime.strptime(args.date, "%Y-%m-%d").strftime("%Y%m%d")

    releases = [r for r in parse_feed() if r["release_date"] >= MIN_RELEASE_DATE]
    print(f"Found {len(releases)} releases in feed since {MIN_RELEASE_DATE}")

    client = storage.Client(project="moz-fx-data-shared-prod")
    bucket = client.bucket(GCS_BUCKET_NAME)

    # Fetch all existing GCS paths once to avoid N individual exists() calls
    existing_paths = {
        blob.name
        for blob in client.list_blobs(GCS_BUCKET_NAME, prefix=GCS_STRUCTURED_PREFIX)
    }
    print(f"Found {len(existing_paths)} existing objects in GCS")

    driver = None
    try:
        for release in releases:
            name = release["name"]
            version = release["version"]
            release_date = release["release_date"]
            release_notes_url = release["release_notes"]

            gcs_path = gcs_path_for(name, version, release_date)

            if gcs_path in existing_paths:
                print(f"Skipping {name} {version} — already in GCS")
                continue

            print(f"Scraping {name} {version} ({release_date}): {release_notes_url}")
            use_js = name in JS_RENDERED_BROWSERS

            if use_js and driver is None:
                driver = initialize_driver(DRIVER_TYP, BINARY_LOC, DRIVER_PATH)

            try:
                raw_text = scrape_page_text(
                    release_notes_url, driver=driver, use_js=use_js
                )
            except Exception as e:
                print(f"Failed to scrape {name} {version}: {e}")
                continue

            record = {
                "browser": name,
                "version": version,
                "release_date": release_date,
                "scraped_date": scraped_date,
                "source_url": release_notes_url,
                "features": [],
                "raw_text": raw_text,
            }

            blob = bucket.blob(gcs_path)
            blob.upload_from_string(
                json.dumps(record, indent=2), content_type="application/json"
            )
            print(f"Uploaded to gs://{GCS_BUCKET_NAME}/{gcs_path}")
            time.sleep(REQUEST_DELAY_SECONDS)

    finally:
        if driver is not None:
            driver.quit()


if __name__ == "__main__":
    main()
