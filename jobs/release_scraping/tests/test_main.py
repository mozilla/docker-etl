import importlib
import json
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

from release_scraping.main import (
    BLOG_FEEDS,
    gcs_blog_path_for,
    gcs_path_for,
    gcs_user_release_path_for,
    fetch_firefox_user_releases,
    parse_blog_feed,
    parse_feed,
    scrape_page_text,
)


def test_import_main():
    """Confirms main.py exists and is valid."""
    mod = importlib.import_module("release_scraping.main")
    assert hasattr(mod, "main")


def test_gcs_path_for():
    assert gcs_path_for("Chrome", "146", "2026-03-10") == (
        "MARKET_RESEARCH/STRUCTURED/Chrome/release_146_20260310.json"
    )
    assert gcs_path_for("Safari on iOS", "26.4", "2026-03-24") == (
        "MARKET_RESEARCH/STRUCTURED/Safari_on_iOS/release_26_4_20260324.json"
    )
    assert gcs_path_for("Chrome Android", "146", "2026-03-10") == (
        "MARKET_RESEARCH/STRUCTURED/Chrome_Android/release_146_20260310.json"
    )


def test_parse_feed_title_parsing():
    """Feed entries are parsed correctly from title into name + version."""
    fake_feed = MagicMock()
    fake_feed.entries = [
        MagicMock(
            title="Chrome release 146 is out!",
            link="https://example.com/chrome",
            updated="2026-03-10T00:00:00Z",
        ),
        MagicMock(
            title="Safari on iOS release 26.4 is out!",
            link="https://example.com/safari",
            updated="2026-03-24T00:00:00Z",
        ),
        MagicMock(
            title="Firefox for Android release 149 is out!",
            link="https://example.com/firefox",
            updated="2026-03-24T00:00:00Z",
        ),
        MagicMock(
            title="Unrecognised entry format",
            link="https://example.com/other",
            updated="2026-01-01T00:00:00Z",
        ),
    ]

    with patch("release_scraping.main.feedparser.parse", return_value=fake_feed):
        results = parse_feed()

    assert len(results) == 3  # unrecognised entry is skipped
    assert results[0] == {
        "name": "Chrome",
        "version": "146",
        "release_date": "2026-03-10",
        "release_notes": "https://example.com/chrome",
    }
    assert results[1] == {
        "name": "Safari on iOS",
        "version": "26.4",
        "release_date": "2026-03-24",
        "release_notes": "https://example.com/safari",
    }
    assert results[2] == {
        "name": "Firefox for Android",
        "version": "149",
        "release_date": "2026-03-24",
        "release_notes": "https://example.com/firefox",
    }


def test_parse_feed_skips_missing_link():
    """Entries with no link URL are skipped."""
    fake_feed = MagicMock()
    fake_feed.entries = [
        MagicMock(
            title="Chrome release 146 is out!", link="", updated="2026-03-10T00:00:00Z"
        ),
    ]

    with patch("release_scraping.main.feedparser.parse", return_value=fake_feed):
        results = parse_feed()

    assert results == []


def test_scrape_page_text_requests():
    """Uses requests when use_js=False."""
    mock_resp = MagicMock()
    mock_resp.text = "<html><body><p>Hello world</p></body></html>"

    with patch("release_scraping.main.requests.get", return_value=mock_resp):
        text = scrape_page_text("https://example.com", use_js=False)

    assert "Hello world" in text


def test_scrape_page_text_selenium():
    """Uses Selenium driver when use_js=True."""
    mock_driver = MagicMock()
    mock_driver.page_source = "<html><body><p>JS content</p></body></html>"

    text = scrape_page_text("https://example.com", driver=mock_driver, use_js=True)

    mock_driver.get.assert_called_once_with("https://example.com")
    assert "JS content" in text


def test_main_skips_existing_and_continues_on_failure():
    """main() skips GCS-existing entries and continues after a scrape failure."""
    from release_scraping.main import main

    fake_releases = [
        {
            "name": "Chrome",
            "version": "146",
            "release_date": "2026-03-10",
            "release_notes": "https://example.com/chrome",
        },
        {
            "name": "Firefox",
            "version": "149",
            "release_date": "2026-03-24",
            "release_notes": "https://example.com/firefox",
        },
        {
            "name": "Edge",
            "version": "146",
            "release_date": "2026-03-13",
            "release_notes": "https://example.com/edge",
        },
    ]

    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    # Chrome is already in GCS; Firefox scrape fails; Edge should still succeed
    chrome_path = "MARKET_RESEARCH/STRUCTURED/Chrome/release_146_20260310.json"
    existing_blob = MagicMock()
    existing_blob.name = chrome_path
    mock_client.list_blobs.return_value = [existing_blob]

    def fake_scrape(url, driver=None, use_js=False):
        if "firefox" in url:
            raise Exception("connection timeout")
        return "release notes text"

    with patch("release_scraping.main.parse_feed", return_value=fake_releases), patch(
        "release_scraping.main.storage.Client", return_value=mock_client
    ), patch("release_scraping.main.scrape_page_text", side_effect=fake_scrape), patch(
        "release_scraping.main.fetch_firefox_user_releases", return_value=[]
    ), patch(
        "release_scraping.main.parse_blog_feed", return_value=[]
    ), patch(
        "release_scraping.main.scrape_and_upload_jobs"
    ):
        import sys

        sys.argv = ["main.py", "--date", "2026-03-13"]
        main()

    # Only Edge should have been uploaded (Chrome skipped, Firefox failed)
    uploaded_paths = [call.args[0] for call in mock_bucket.blob.call_args_list]
    assert uploaded_paths == [
        "MARKET_RESEARCH/STRUCTURED/Edge/release_146_20260313.json"
    ]


# ---------------------------------------------------------------------------
# New unit tests for additive sources
# ---------------------------------------------------------------------------


def test_gcs_user_release_path_for():
    assert gcs_user_release_path_for("Firefox", "149.0", "2026-03-24") == (
        "MARKET_RESEARCH/STRUCTURED/Firefox/user_release_149_0_20260324.json"
    )
    # Must not collide with the dev-notes path for the same browser/date
    assert gcs_user_release_path_for("Firefox", "149.0", "2026-03-24") != gcs_path_for(
        "Firefox", "149.0", "2026-03-24"
    )


def test_gcs_blog_path_for():
    assert gcs_blog_path_for("Chrome", "2026-03-10", "https://blog.google/chrome/new-tab-redesign/") == (
        "MARKET_RESEARCH/BLOGS/Chrome/post_20260310_new-tab-redesign.json"
    )
    # Spaces in browser name become underscores
    assert gcs_blog_path_for("Opera Desktop", "2026-04-01", "https://example.com/some-post/") == (
        "MARKET_RESEARCH/BLOGS/Opera_Desktop/post_20260401_some-post.json"
    )
    # Slug is capped at 40 characters
    long_url = "https://example.com/" + "a" * 60 + "/"
    result = gcs_blog_path_for("Brave", "2026-01-01", long_url)
    slug = result.split("post_20260101_")[1].replace(".json", "")
    assert len(slug) <= 40


def test_fetch_firefox_user_releases():
    fake_response = MagicMock()
    fake_response.json.return_value = {
        "releases": {
            "firefox-149.0": {
                "category": "major",
                "version": "149.0",
                "date": "2026-03-24",
            },
            "firefox-149.0b1": {
                "category": "dev",
                "version": "149.0b1",
                "date": "2026-03-10",
            },
            "firefox-148.0": {
                "category": "major",
                "version": "148.0",
                "date": "2026-02-10",
            },
        }
    }

    with patch("release_scraping.main.requests.get", return_value=fake_response):
        results = fetch_firefox_user_releases()

    assert len(results) == 2  # dev release filtered out
    assert results[0] == {"version": "149.0", "release_date": "2026-03-24"}
    assert results[1] == {"version": "148.0", "release_date": "2026-02-10"}


def test_parse_blog_feed():
    fake_feed = MagicMock()
    fake_feed.entries = [
        MagicMock(
            title="What's new in Chrome 146",
            link="https://blog.google/chrome-146",
            published_parsed=(2026, 3, 10, 0, 0, 0, 0, 0, 0),
            updated_parsed=None,
        ),
        MagicMock(
            title="No link post",
            link="",
            published_parsed=(2026, 3, 1, 0, 0, 0, 0, 0, 0),
            updated_parsed=None,
        ),
        MagicMock(
            title="No date post",
            link="https://blog.google/no-date",
            published_parsed=None,
            updated_parsed=None,
        ),
    ]

    with patch("release_scraping.main.feedparser.parse", return_value=fake_feed):
        results = parse_blog_feed("https://example.com/feed")

    assert len(results) == 1  # no-link and no-date entries skipped
    assert results[0] == {
        "title": "What's new in Chrome 146",
        "release_date": "2026-03-10",
        "url": "https://blog.google/chrome-146",
    }


def test_main_firefox_user_release_skips_existing():
    """main() skips Firefox user releases that are already in GCS."""
    from release_scraping.main import main

    fake_ff_releases = [{"version": "149.0", "release_date": "2026-03-24"}]

    mock_bucket = MagicMock()
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    existing_path = "MARKET_RESEARCH/STRUCTURED/Firefox/user_release_149_0_20260324.json"
    existing_blob = MagicMock()
    existing_blob.name = existing_path
    mock_client.list_blobs.return_value = [existing_blob]

    with patch("release_scraping.main.parse_feed", return_value=[]), patch(
        "release_scraping.main.fetch_firefox_user_releases",
        return_value=fake_ff_releases,
    ), patch(
        "release_scraping.main.parse_blog_feed", return_value=[]
    ), patch(
        "release_scraping.main.storage.Client", return_value=mock_client
    ), patch(
        "release_scraping.main.scrape_and_upload_jobs"
    ):
        import sys

        sys.argv = ["main.py", "--date", "2026-04-17"]
        main()

    mock_bucket.blob.assert_not_called()


def test_main_blog_posts_skips_existing_and_continues_on_failure():
    """main() skips existing blog posts and continues after a scrape failure."""
    from release_scraping.main import main, BLOG_FEEDS

    chrome_post = {
        "title": "Existing Chrome post",
        "release_date": "2026-03-10",
        "url": "https://blog.google/chrome/existing-post/",
    }
    edge_post = {
        "title": "Failing Edge post",
        "release_date": "2026-03-17",
        "url": "https://blogs.windows.com/msedgedev/2026/03/17/failing-post/",
    }
    brave_post = {
        "title": "Successful Brave post",
        "release_date": "2026-03-20",
        "url": "https://brave.com/blog/brave-post/",
    }

    mock_bucket = MagicMock()
    mock_blob = MagicMock()
    mock_bucket.blob.return_value = mock_blob
    mock_client = MagicMock()
    mock_client.bucket.return_value = mock_bucket

    chrome_path = gcs_blog_path_for("Chrome", chrome_post["release_date"], chrome_post["url"])
    existing_blob = MagicMock()
    existing_blob.name = chrome_path
    mock_client.list_blobs.return_value = [existing_blob]

    def fake_blog_feed(feed_url):
        if "chrome" in feed_url:
            return [chrome_post]
        if "msedgedev" in feed_url:
            return [edge_post]
        if "brave" in feed_url:
            return [brave_post]
        return []

    def fake_scrape(url, driver=None, use_js=False):
        if "failing" in url:
            raise Exception("timeout")
        return "blog post text"

    with patch("release_scraping.main.parse_feed", return_value=[]), patch(
        "release_scraping.main.fetch_firefox_user_releases", return_value=[]
    ), patch(
        "release_scraping.main.parse_blog_feed", side_effect=fake_blog_feed
    ), patch(
        "release_scraping.main.scrape_page_text", side_effect=fake_scrape
    ), patch(
        "release_scraping.main.storage.Client", return_value=mock_client
    ), patch(
        "release_scraping.main.scrape_and_upload_jobs"
    ):
        import sys

        sys.argv = ["main.py", "--date", "2026-04-17"]
        main()

    # Only Brave should have been uploaded (Chrome skipped, Edge failed)
    uploaded_paths = [call.args[0] for call in mock_bucket.blob.call_args_list]
    assert gcs_blog_path_for("Brave", brave_post["release_date"], brave_post["url"]) in uploaded_paths
    assert chrome_path not in uploaded_paths
    assert gcs_blog_path_for("Edge", edge_post["release_date"], edge_post["url"]) not in uploaded_paths


# ---------------------------------------------------------------------------
# Integration tests — run with: pytest --integration
# ---------------------------------------------------------------------------

EXPECTED_BROWSERS = {
    "Chrome",
    "Chrome Android",
    "Edge",
    "Firefox",
    "Firefox for Android",
    "Safari",
    "Safari on iOS",
}


@pytest.mark.integration
def test_feed_returns_all_browsers():
    """Feed contains at least one entry for every expected browser."""
    releases = parse_feed()
    names = {r["name"] for r in releases}
    missing = EXPECTED_BROWSERS - names
    assert not missing, f"Missing browsers from feed: {missing}"


@pytest.mark.integration
def test_feed_entry_fields():
    """Every feed entry has all required fields with non-empty values."""
    releases = parse_feed()
    for r in releases:
        for field in ("name", "version", "release_date", "release_notes"):
            assert r.get(field), f"Entry missing or empty '{field}': {r}"


@pytest.mark.integration
def test_feed_goes_back_to_2023():
    """Feed contains entries from 2023, confirming historical depth."""
    releases = parse_feed()
    years = {r["release_date"][:4] for r in releases}
    assert "2023" in years, f"No 2023 entries found. Years present: {years}"


@pytest.mark.integration
def test_firefox_release_notes_scrapeable():
    """Firefox MDN release notes page returns non-empty text via requests."""
    releases = parse_feed()
    firefox = next(r for r in releases if r["name"] == "Firefox")
    text = scrape_page_text(firefox["release_notes"], use_js=False)
    assert len(text) > 500, "Firefox release notes page returned too little text"


@pytest.mark.integration
def test_scrape_all_current_browsers_to_file(local_driver):
    """Scrape the most recent release of each browser and write to a local JSON file.

    Uses the RSS feed as the data source. Only processes the latest entry per
    browser (not the full history) to keep the test fast.

    Output: tests/integration_output/scrape_{date}.json
    """
    from release_scraping.main import JS_RENDERED_BROWSERS

    releases = parse_feed()
    scraped_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    # Keep only the most recent release per browser (feed is newest-first)
    seen = set()
    latest_releases = []
    for r in releases:
        if r["name"] not in seen:
            seen.add(r["name"])
            latest_releases.append(r)

    results = []
    for release in latest_releases:
        name = release["name"]
        version = release["version"]
        release_notes_url = release["release_notes"]

        record = {
            "browser": name,
            "version": version,
            "release_date": release["release_date"],
            "scraped_date": scraped_date,
            "source_url": release_notes_url,
            "features": [],
            "raw_text": None,
            "error": None,
        }

        use_js = name in JS_RENDERED_BROWSERS
        try:
            record["raw_text"] = scrape_page_text(
                release_notes_url, driver=local_driver, use_js=use_js
            )
        except Exception as e:
            record["error"] = str(e)

        char_count = len(record["raw_text"]) if record["raw_text"] else 0
        status = (
            f"{char_count:,} chars"
            if record["raw_text"]
            else f"FAILED ({record['error']})"
        )
        print(f"  {name} {version}: {status}")
        results.append(record)

    output_dir = os.path.join(os.path.dirname(__file__), "integration_output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"scrape_{scraped_date}.json")

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nOutput written to: {output_path}")
    assert os.path.exists(output_path)
    assert len(results) > 0


@pytest.mark.integration
def test_scrape_last_year_to_file(local_driver):
    """Scrape all releases from the past year and write results to a local JSON file.

    Deduplicates by URL — browsers sharing the same release notes page (e.g.
    Safari and Safari on iOS) only trigger one network request.

    Output: tests/integration_output/scrape_last_year_{date}.json
    """
    from release_scraping.main import JS_RENDERED_BROWSERS

    since = datetime.now(timezone.utc) - timedelta(days=365)
    scraped_date = datetime.now(timezone.utc).strftime("%Y%m%d")

    releases = [
        r
        for r in parse_feed()
        if datetime.fromisoformat(r["release_date"]) >= since.replace(tzinfo=None)
    ]
    print(f"\nFound {len(releases)} releases since {since.date()}")

    # Cache scraped text by URL to avoid hitting the same page twice
    url_cache = {}

    results = []
    for release in releases:
        name = release["name"]
        version = release["version"]
        url = release["release_notes"]

        if url not in url_cache:
            use_js = name in JS_RENDERED_BROWSERS
            try:
                url_cache[url] = scrape_page_text(
                    url, driver=local_driver, use_js=use_js
                )
            except Exception as e:
                url_cache[url] = None
                print(f"  FAILED {name} {version}: {e}")

        raw_text = url_cache[url]
        char_count = len(raw_text) if raw_text else 0
        status = f"{char_count:,} chars" if raw_text else "FAILED"
        print(f"  {name} {version} ({release['release_date']}): {status}")

        results.append(
            {
                "browser": name,
                "version": version,
                "release_date": release["release_date"],
                "scraped_date": scraped_date,
                "source_url": url,
                "features": [],
                "raw_text": raw_text,
            }
        )

    output_dir = os.path.join(os.path.dirname(__file__), "integration_output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"scrape_last_year_{scraped_date}.json")

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nOutput written to: {output_path}")
    assert os.path.exists(output_path)
    assert len(results) > 0


# ---------------------------------------------------------------------------
# Integration tests for additive sources — run with: pytest --integration
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_firefox_product_details_api():
    """Product-details API returns major Firefox releases with required fields."""
    import re as _re

    releases = fetch_firefox_user_releases()
    assert len(releases) > 0
    for r in releases:
        assert r.get("version"), f"Missing version: {r}"
        assert r.get("release_date"), f"Missing release_date: {r}"
        assert _re.match(r"\d{4}-\d{2}-\d{2}", r["release_date"]), (
            f"Bad date format: {r['release_date']}"
        )


@pytest.mark.integration
def test_blog_feeds_parseable():
    """All blog feeds return at least one post with required fields."""
    for browser_name, feed_url in BLOG_FEEDS.items():
        posts = parse_blog_feed(feed_url)
        assert len(posts) > 0, f"No posts from {browser_name} feed ({feed_url})"
        for p in posts:
            for field in ("title", "release_date", "url"):
                assert p.get(field), f"{browser_name} post missing '{field}': {p}"


@pytest.mark.integration
def test_scrape_user_content_to_file():
    """Scrape the latest entry from each user-facing source and write to a local JSON file.

    Output: tests/integration_output/scrape_user_content_{date}.json
    """
    scraped_date = datetime.now(timezone.utc).strftime("%Y%m%d")
    results = []

    # Latest Firefox user-facing release
    ff_releases = fetch_firefox_user_releases()
    if ff_releases:
        latest = ff_releases[0]
        url = "https://www.firefox.com/en-US/firefox/{version}/releasenotes/".format(
            version=latest["version"]
        )
        record = {
            "browser": "Firefox",
            "version": latest["version"],
            "release_date": latest["release_date"],
            "scraped_date": scraped_date,
            "source_url": url,
            "source_type": "user_release_notes",
            "raw_text": None,
            "error": None,
        }
        try:
            record["raw_text"] = scrape_page_text(url)
        except Exception as e:
            record["error"] = str(e)
        char_count = len(record["raw_text"]) if record["raw_text"] else 0
        status = f"{char_count:,} chars" if record["raw_text"] else f"FAILED ({record['error']})"
        print(f"  Firefox {latest['version']} (user): {status}")
        results.append(record)

    # Latest post from each blog feed
    for browser_name, feed_url in BLOG_FEEDS.items():
        posts = parse_blog_feed(feed_url)
        if not posts:
            print(f"  {browser_name}: no posts in feed")
            continue
        post = posts[0]
        record = {
            "browser": browser_name,
            "version": None,
            "release_date": post["release_date"],
            "scraped_date": scraped_date,
            "source_url": post["url"],
            "source_type": "blog_post",
            "title": post["title"],
            "raw_text": None,
            "error": None,
        }
        try:
            record["raw_text"] = scrape_page_text(post["url"])
        except Exception as e:
            record["error"] = str(e)
        char_count = len(record["raw_text"]) if record["raw_text"] else 0
        status = f"{char_count:,} chars" if record["raw_text"] else f"FAILED ({record['error']})"
        print(f"  {browser_name} blog ({post['release_date']}): {status}")
        results.append(record)

    output_dir = os.path.join(os.path.dirname(__file__), "integration_output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"scrape_user_content_{scraped_date}.json")

    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)

    print(f"\nOutput written to: {output_path}")
    assert os.path.exists(output_path)
    assert len(results) > 0
