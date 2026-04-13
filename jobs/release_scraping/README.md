# Release Scraping

A dockerized Python job that scrapes browser release notes for all major browsers and saves structured JSON to GCS. Used as part of the Market Intel Bot pipeline.

## What it does

1. Fetches all browser releases from the [browsers.fyi](https://www.browsers.fyi/) RSS feed
2. Scrapes the release notes page for each release
3. Saves a structured JSON record to GCS

Browsers tracked: Chrome, Chrome Android, Edge, Firefox, Firefox for Android, Safari, Safari on iOS

## Output

Records are written to GCS at:
```
gs://moz-fx-data-prod-external-data/MARKET_RESEARCH/STRUCTURED/{browser}/release_{version}_{YYYYMMDD}.json
```

Each record contains:
```json
{
  "browser": "Firefox",
  "version": "149",
  "release_date": "2026-03-24",
  "scraped_date": "20260402",
  "source_url": "https://developer.mozilla.org/docs/Mozilla/Firefox/Releases/149",
  "features": [],
  "raw_text": "..."
}
```

Filenames use `release_date` (not `scraped_date`), so paths are stable across runs and used for deduplication — already-uploaded entries are skipped.

## Scraping approach

- **Safari / Safari on iOS**: Selenium + Chromium (pages are JS-rendered)
- **All other browsers**: plain `requests` (pages are static)

## First run

On the first production run the job will process all historical entries in the feed (back to ~Feb 2023, ~280 entries). Subsequent runs only scrape new releases not yet in GCS.

## Usage

```bash
python release_scraping/main.py --date YYYY-MM-DD
```

## Running tests

```bash
# Unit tests
pytest tests/

# Integration tests (hit live APIs and scrape real pages)
pytest tests/ --integration
```

## Airflow

Triggered by the `web_scraping` DAG in `telemetry-airflow` on the 2nd of each month. Downstream: `bqetl_market_intel_bot`.
