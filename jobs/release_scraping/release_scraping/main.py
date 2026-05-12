import html
import json
import re
import time
import xml.etree.ElementTree as ET

import feedparser
import requests
from argparse import ArgumentParser
from bs4 import BeautifulSoup
from datetime import datetime
from google.cloud import storage
from urllib.parse import urlparse
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

# Firefox user-facing release notes
FIREFOX_PRODUCT_DETAILS_URL = "https://product-details.mozilla.org/1.0/firefox.json"
FIREFOX_USER_NOTES_URL = "https://www.firefox.com/en-US/firefox/{version}/releasenotes/"

# User-facing blog RSS feeds
GCS_BLOGS_PREFIX = "MARKET_RESEARCH/BLOGS"

BLOG_FEEDS = {
    "Chrome": "https://blog.google/products-and-platforms/products/chrome/rss/",
    "Edge": "https://blogs.windows.com/msedgedev/feed/",
    "Brave": "https://brave.com/blog/index.xml",
    "Opera": "https://blogs.opera.com/desktop/feed/",
    "Vivaldi": "https://vivaldi.com/feed/",
}

# Job postings
GCS_JOBS_PREFIX = "MARKET_RESEARCH/JOBS"

GREENHOUSE_BOARDS = {
    "Mozilla": "mozilla",
    "Brave": "brave",
}
GREENHOUSE_API_URL = (
    "https://boards-api.greenhouse.io/v1/boards/{board}/jobs?content=true"
)

OPERA_SITEMAP_URL = "https://jobs.opera.com/sitemap.xml"

TIMEOUT_IN_SECONDS = 20
REQUEST_DELAY_SECONDS = 2
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko)"
}
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


def gcs_user_release_path_for(browser_name, version, release_date):
    """Construct the GCS object path for a user-facing browser release.

    Uses a user_release_ prefix to distinguish from developer release notes
    stored by gcs_path_for.
    """
    browser_path = browser_name.replace(" ", "_")
    version_clean = version.replace(".", "_")
    date_clean = release_date.replace("-", "")
    return (
        f"{GCS_STRUCTURED_PREFIX}/{browser_path}"
        f"/user_release_{version_clean}_{date_clean}.json"
    )


def gcs_blog_path_for(browser_name, publish_date, url):
    """Construct the GCS object path for a browser blog post.

    Uses the last URL path segment as a stable slug for deduplication.
    """
    browser_path = browser_name.replace(" ", "_")
    date_clean = publish_date.replace("-", "")
    slug = urlparse(url).path.rstrip("/").split("/")[-1]
    slug = re.sub(r"[^a-zA-Z0-9-]", "-", slug)[:40]
    return f"{GCS_BLOGS_PREFIX}/{browser_path}/post_{date_clean}_{slug}.json"


def fetch_firefox_user_releases():
    """Fetch all major Firefox releases from the Mozilla product-details API.

    Returns a list of dicts with keys: version, release_date.
    Only major releases are included. Ordered newest-first.
    """
    response = requests.get(FIREFOX_PRODUCT_DETAILS_URL, headers=REQUEST_HEADERS, timeout=TIMEOUT_IN_SECONDS)
    response.raise_for_status()
    data = response.json()

    releases = []
    for release in data["releases"].values():
        if release["category"] != "major":
            continue
        releases.append(
            {
                "version": release["version"],
                "release_date": release["date"],
            }
        )

    releases.sort(key=lambda r: r["release_date"], reverse=True)
    return releases


def parse_blog_feed(feed_url):
    """Parse an RSS/Atom blog feed and return post entries.

    Returns a list of dicts with keys: title, release_date, url.
    Ordered as provided by the feed (typically newest-first).
    Entries missing a link or date are skipped.
    """
    feed = feedparser.parse(feed_url)
    posts = []
    for entry in feed.entries:
        title = getattr(entry, "title", "") or ""
        link = getattr(entry, "link", "") or ""
        date_parsed = getattr(entry, "published_parsed", None) or getattr(
            entry, "updated_parsed", None
        )
        if not link or not date_parsed:
            continue
        publish_date = datetime(*date_parsed[:3]).strftime("%Y-%m-%d")
        posts.append({"title": title, "release_date": publish_date, "url": link})
    return posts


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
        response = requests.get(url, headers=REQUEST_HEADERS, timeout=TIMEOUT_IN_SECONDS)
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

    # Fetch all existing GCS paths once to avoid N individual exists() calls.
    # Use the common "MARKET_RESEARCH/" ancestor to cover both STRUCTURED/ and BLOGS/.
    existing_paths = {
        blob.name
        for blob in client.list_blobs(GCS_BUCKET_NAME, prefix="MARKET_RESEARCH/")
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

    scrape_and_upload_user_releases(scraped_date, bucket, existing_paths)
    scrape_and_upload_blog_posts(scraped_date, bucket, existing_paths)
    scrape_and_upload_jobs(scraped_date, bucket)


def scrape_and_upload_user_releases(scraped_date, bucket, existing_paths):
    """Scrape Firefox user-facing release notes and upload new ones to GCS."""
    print("--- Scraping Firefox user-facing release notes ---")
    try:
        ff_releases = fetch_firefox_user_releases()
    except Exception as e:
        print(f"Failed to fetch Firefox product details: {e}")
        return

    ff_releases = [r for r in ff_releases if r["release_date"] >= MIN_RELEASE_DATE]
    print(f"Found {len(ff_releases)} Firefox user releases since {MIN_RELEASE_DATE}")

    for release in ff_releases:
        version = release["version"]
        release_date = release["release_date"]
        url = FIREFOX_USER_NOTES_URL.format(version=version)
        gcs_path = gcs_user_release_path_for("Firefox", version, release_date)

        if gcs_path in existing_paths:
            print(f"Skipping Firefox {version} user release — already in GCS")
            continue

        print(f"Scraping Firefox {version} user release ({release_date}): {url}")
        try:
            raw_text = scrape_page_text(url)
        except Exception as e:
            print(f"Failed to scrape Firefox {version} user release: {e}")
            continue

        record = {
            "browser": "Firefox",
            "version": version,
            "release_date": release_date,
            "scraped_date": scraped_date,
            "source_url": url,
            "source_type": "user_release_notes",
            "features": [],
            "raw_text": raw_text,
        }

        blob = bucket.blob(gcs_path)
        blob.upload_from_string(
            json.dumps(record, indent=2), content_type="application/json"
        )
        print(f"Uploaded to gs://{GCS_BUCKET_NAME}/{gcs_path}")
        time.sleep(REQUEST_DELAY_SECONDS)


def scrape_and_upload_blog_posts(scraped_date, bucket, existing_paths):
    """Scrape browser blog RSS feeds and upload new posts to GCS."""
    print("--- Scraping browser blog posts ---")
    for browser_name, feed_url in BLOG_FEEDS.items():
        print(f"Fetching {browser_name} blog feed")
        try:
            posts = parse_blog_feed(feed_url)
        except Exception as e:
            print(f"Failed to fetch {browser_name} blog feed: {e}")
            continue

        for post in posts:
            publish_date = post["release_date"]
            url = post["url"]
            title = post["title"]

            if publish_date < MIN_RELEASE_DATE:
                continue

            gcs_path = gcs_blog_path_for(browser_name, publish_date, url)

            if gcs_path in existing_paths:
                print(f"Skipping {browser_name} post ({publish_date}) — already in GCS")
                continue

            print(f"Scraping {browser_name} blog post: {title}")
            try:
                raw_text = scrape_page_text(url)
            except Exception as e:
                print(f"Failed to scrape {browser_name} post {url}: {e}")
                continue

            record = {
                "browser": browser_name,
                "version": None,
                "release_date": publish_date,
                "scraped_date": scraped_date,
                "source_url": url,
                "source_type": "blog_post",
                "title": title,
                "features": [],
                "raw_text": raw_text,
            }

            blob = bucket.blob(gcs_path)
            blob.upload_from_string(
                json.dumps(record, indent=2), content_type="application/json"
            )
            print(f"Uploaded to gs://{GCS_BUCKET_NAME}/{gcs_path}")
            time.sleep(REQUEST_DELAY_SECONDS)


def gcs_job_path_for(company, scraped_date, job_id):
    """Construct GCS path for a job posting snapshot.

    Path includes the scrape date so each run produces a full snapshot
    and the same job appears in every snapshot where it's still open.
    """
    company_path = company.replace(" ", "_")
    return f"{GCS_JOBS_PREFIX}/{company_path}/{scraped_date}/job_{job_id}.json"


def fetch_greenhouse_jobs(board_slug):
    """Fetch all jobs from a Greenhouse board with full descriptions.

    Uses ?content=true to get everything in a single API call.
    Returns the raw list of job dicts from the API response.
    """
    url = GREENHOUSE_API_URL.format(board=board_slug)
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=TIMEOUT_IN_SECONDS)
    response.raise_for_status()
    return response.json()["jobs"]


def greenhouse_job_to_record(company, job, scraped_date):
    """Transform a Greenhouse API job dict into our unified record schema."""
    # Greenhouse returns content as HTML-entity-encoded text; unescape first
    content_raw = job.get("content", "")
    content_html = html.unescape(content_raw)
    description_text = BeautifulSoup(content_html, "html.parser").get_text(
        separator="\n", strip=True
    )

    departments = [d["name"] for d in job.get("departments", []) if d.get("name")]
    offices = [o["name"] for o in job.get("offices", []) if o.get("name")]

    return {
        "company": company,
        "source": "greenhouse",
        "scraped_date": scraped_date,
        "job_id": str(job["id"]),
        "title": job.get("title", ""),
        "department": departments[0] if departments else None,
        "location": job.get("location", {}).get("name", ""),
        "offices": offices if offices else None,
        "url": job.get("absolute_url", ""),
        "first_published": job.get("first_published", ""),
        "updated_at": job.get("updated_at", ""),
        "description_html": content_html,
        "description_text": description_text,
    }


def fetch_opera_job_urls():
    """Parse Opera's sitemap.xml to extract individual job posting URLs."""
    response = requests.get(
        OPERA_SITEMAP_URL, headers=REQUEST_HEADERS, timeout=TIMEOUT_IN_SECONDS
    )
    response.raise_for_status()

    root = ET.fromstring(response.content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    urls = []
    for loc in root.findall(".//sm:loc", ns):
        url = loc.text.strip()
        if re.search(r"/jobs/\d+-", url):
            urls.append(url)
    return urls


def opera_job_id_from_url(url):
    """Extract the numeric job ID from an Opera job URL."""
    match = re.search(r"/jobs/(\d+)", url)
    return match.group(1) if match else url.rstrip("/").split("/")[-1]


def scrape_opera_job(url):
    """Scrape a single Opera job page and return a record dict.

    Extracts title, description, department, and location from the
    server-rendered Teamtailor HTML. Handles cookie consent dialogs.
    """
    response = requests.get(url, headers=REQUEST_HEADERS, timeout=TIMEOUT_IN_SECONDS)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    # Remove cookie consent dialogs before extracting content
    for el in soup.find_all(
        ["div", "dialog", "section"],
        attrs={"class": re.compile(r"cookie|consent|gdpr", re.I)},
    ):
        el.decompose()
    for el in soup.find_all(
        ["div", "dialog", "section"],
        attrs={"id": re.compile(r"cookie|consent|gdpr", re.I)},
    ):
        el.decompose()

    title = ""
    for h1 in soup.find_all("h1"):
        text = h1.get_text(strip=True)
        if text and "cookie" not in text.lower() and "consent" not in text.lower():
            title = text
            break

    content_el = soup.find("article")
    if not content_el:
        content_el = soup.find("main")
    if not content_el:
        content_el = soup.find("body")

    description_text = (
        content_el.get_text(separator="\n", strip=True) if content_el else ""
    )
    description_html = str(content_el) if content_el else ""

    # Extract metadata from JSON-LD structured data (schema.org/JobPosting)
    department = None
    location = None
    first_published = None
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            if ld.get("@type") == "JobPosting":
                if ld.get("datePosted"):
                    first_published = ld["datePosted"][:10]
                job_locations = ld.get("jobLocation", [])
                if isinstance(job_locations, dict):
                    job_locations = [job_locations]
                loc_parts = []
                for loc in job_locations:
                    addr = loc.get("address", {})
                    city = addr.get("addressLocality", "")
                    country = addr.get("addressRegion", "") or addr.get(
                        "addressCountry", ""
                    )
                    if city and country:
                        loc_parts.append(f"{city}, {country}")
                    elif city or country:
                        loc_parts.append(city or country)
                if loc_parts:
                    location = "; ".join(loc_parts)
                break
        except (json.JSONDecodeError, TypeError):
            continue

    # Fall back to <dl> definition list for department and location
    for dt in soup.find_all("dt"):
        label = dt.get_text(strip=True).lower()
        dd = dt.find_next_sibling("dd")
        if not dd:
            continue
        value = dd.get_text(strip=True)
        if "department" in label:
            department = value
        elif "location" in label and not location:
            location = value

    return {
        "title": title,
        "department": department,
        "location": location,
        "first_published": first_published,
        "description_html": description_html,
        "description_text": description_text,
    }


def scrape_and_upload_jobs(scraped_date, bucket):
    """Scrape job postings from all configured sources and upload to GCS.

    Each run writes a complete snapshot under a date directory. A job that
    stays open across runs appears in every snapshot (no cross-date dedup).
    Same-day reruns overwrite (idempotent).
    """
    print("--- Scraping job postings ---")

    for company, board in GREENHOUSE_BOARDS.items():
        print(f"{company} (Greenhouse: {board})")
        try:
            jobs = fetch_greenhouse_jobs(board)
        except Exception as e:
            print(f"Failed to fetch {company} jobs: {e}")
            continue

        print(f"  Found {len(jobs)} jobs")
        for job in jobs:
            try:
                record = greenhouse_job_to_record(company, job, scraped_date)
            except Exception as e:
                print(f"  Failed to parse job {job.get('id', '?')}: {e}")
                continue

            gcs_path = gcs_job_path_for(company, scraped_date, record["job_id"])
            try:
                blob = bucket.blob(gcs_path)
                blob.upload_from_string(
                    json.dumps(record, indent=2, ensure_ascii=False),
                    content_type="application/json",
                )
                print(f"  {record['title']} -> gs://{GCS_BUCKET_NAME}/{gcs_path}")
            except Exception as e:
                print(f"  Failed to upload {record['title']}: {e}")

        time.sleep(REQUEST_DELAY_SECONDS)

    print("Opera (Teamtailor)")
    try:
        job_urls = fetch_opera_job_urls()
    except Exception as e:
        print(f"Failed to fetch Opera sitemap: {e}")
        job_urls = []

    print(f"  Found {len(job_urls)} jobs")
    for url in job_urls:
        job_id = opera_job_id_from_url(url)
        try:
            job_data = scrape_opera_job(url)
        except Exception as e:
            print(f"  Failed to scrape {url}: {e}")
            continue

        record = {
            "company": "Opera",
            "source": "teamtailor",
            "scraped_date": scraped_date,
            "job_id": job_id,
            "url": url,
            "offices": None,
            "updated_at": None,
            **job_data,
        }

        gcs_path = gcs_job_path_for("Opera", scraped_date, job_id)
        try:
            blob = bucket.blob(gcs_path)
            blob.upload_from_string(
                json.dumps(record, indent=2, ensure_ascii=False),
                content_type="application/json",
            )
            print(f"  {job_data['title']} -> gs://{GCS_BUCKET_NAME}/{gcs_path}")
        except Exception as e:
            print(f"  Failed to upload {job_data.get('title', url)}: {e}")

        time.sleep(REQUEST_DELAY_SECONDS)


if __name__ == "__main__":
    main()
