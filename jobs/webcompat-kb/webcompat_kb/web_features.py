import argparse
import logging
from datetime import datetime
from typing import Any, Mapping

import webfeatures
from google.cloud import bigquery
from webfeatures import FeaturesFile

from .base import EtlJob, dataset_arg
from .bqhelpers import BigQuery, Json


def get_imported_releases(client: BigQuery) -> dict[str, datetime]:
    query = "SELECT name, published_at FROM releases"
    try:
        return {item.name: item.published_at for item in client.query(query)}
    except Exception:
        # If the table doesn't exist
        return {}


def update_releases(
    client: BigQuery,
    release: webfeatures.github.Release,
    recreate: bool,
) -> None:
    releases_schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("published_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField(
            "version",
            "RECORD",
            mode="REQUIRED",
            fields=[
                bigquery.SchemaField("major", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("minor", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("patch", "INTEGER", mode="REQUIRED"),
            ],
        ),
    ]
    releases_table = client.ensure_table("releases", releases_schema)
    version = release.parsed_version
    rows: list[Mapping[str, Json]] = [
        {
            "id": release.id,
            "name": release.name,
            "created_at": release.created_at.isoformat(),
            "published_at": release.published_at.isoformat(),
            "version": {
                "major": version.major,
                "minor": version.minor,
                "patch": version.patch,
            },
        }
    ]
    client.write_table(
        releases_table,
        releases_schema,
        rows,
        recreate,
    )


def update_browsers(
    client: BigQuery,
    browser_data: Mapping[str, webfeatures.features.BrowserData],
    recreate: bool,
) -> None:
    browsers_schema = [
        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    ]
    browser_versions_schema = [
        bigquery.SchemaField("browser_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("version", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    ]
    browsers_table = client.ensure_table("browsers", browsers_schema)
    browser_versions_table = client.ensure_table(
        "browser_versions",
        browser_versions_schema,
    )

    if not recreate:
        known_browsers = {
            item.id
            for item in client.query(f"SELECT id FROM `{browsers_table.table_id}`")
        }
        known_versions = {
            (item.browser_id, item.version): item.date
            for item in client.query(
                f"SELECT browser_id, version, date FROM `{browser_versions_table.table_id}`"
            )
        }
    else:
        known_browsers = set()
        known_versions = {}
    insert_browsers = []
    insert_versions = []
    for browser_id, data in browser_data.items():
        if browser_id not in known_browsers:
            insert_browsers.append({"id": browser_id, "name": data.name})
        for release in data.releases:
            key = (browser_id, release.version)
            if key not in known_versions:
                insert_versions.append(
                    {
                        "browser_id": browser_id,
                        "version": release.version,
                        "date": release.date.isoformat(),
                    }
                )
            else:
                if known_versions[key] != release.date:
                    logging.warning(
                        f"Recorded date of {known_versions[key]} for {browser_id} {release.version} but data has {release.date}"
                    )

    for table, schema, rows, desc in [
        (browsers_table, browsers_schema, insert_browsers, "browsers"),
        (
            browser_versions_table,
            browser_versions_schema,
            insert_versions,
            "browser versions",
        ),
    ]:
        client.write_table(table, schema, rows, recreate)


def update_features(
    client: BigQuery,
    release_name: str,
    features_data: Mapping[str, webfeatures.features.Feature],
    recreate: bool,
) -> None:
    schema = [
        bigquery.SchemaField("release", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("feature", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("description_html", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("status_baseline", "STRING", mode="REQUIRED"),
        bigquery.SchemaField(
            "status_baseline_high_date",
            "RECORD",
            fields=[
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("is_upper_bound", "BOOL", mode="REQUIRED"),
            ],
        ),
        bigquery.SchemaField(
            "status_baseline_low_date",
            "RECORD",
            fields=[
                bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
                bigquery.SchemaField("is_upper_bound", "BOOL", mode="REQUIRED"),
            ],
        ),
        bigquery.SchemaField(
            "support",
            "RECORD",
            mode="REPEATED",
            fields=[
                bigquery.SchemaField("browser", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("browser_version", "STRING", mode="REQUIRED"),
            ],
        ),
        # Optional Data
        bigquery.SchemaField("caniuse", "STRING", mode="REPEATED"),
        bigquery.SchemaField("compat_features", "STRING", mode="REPEATED"),
        bigquery.SchemaField("group", "STRING", mode="REPEATED"),
        bigquery.SchemaField("spec", "STRING", mode="REPEATED"),
        bigquery.SchemaField("snapshot", "STRING", mode="REPEATED"),
    ]

    table = client.ensure_table(
        "features",
        schema,
    )
    rows: list[dict[str, Any]] = []
    for feature_id, data in features_data.items():
        rows.append(
            {
                "release": release_name,
                "feature": feature_id,
                "name": data.name,
                "description": data.description,
                "description_html": data.description_html,
                "status_baseline": data.status.baseline,
                "status_baseline_high_date": {
                    "date": data.status.baseline_high_date.date.isoformat(),
                    "is_upper_bound": data.status.baseline_high_date.is_upper_bound,
                }
                if data.status.baseline_high_date
                else None,
                "status_baseline_low_date": {
                    "date": data.status.baseline_low_date.date.isoformat(),
                    "is_upper_bound": data.status.baseline_low_date.is_upper_bound,
                }
                if data.status.baseline_low_date
                else None,
                "caniuse": data.caniuse,
                "compat_features": data.compat_features,
                "group": data.group,
                "spec": data.spec,
                "snapshot": data.snapshot,
                "support": [
                    {
                        "browser": browser,
                        "browser_version": browser_version,
                    }
                    for browser, browser_version in data.status.support.items()
                ],
            }
        )

    client.write_table(table, schema, rows, recreate)


def import_release(
    client: BigQuery,
    recreate: bool,
    release: webfeatures.github.Release,
) -> None:
    data = webfeatures.github.get_data(release)
    try:
        features = FeaturesFile.model_validate(data)
    except Exception:
        logging.warning(f"Failed to get web features data for {release.name}")
        return
    update_browsers(client, features.browsers, recreate)
    update_features(
        client,
        release.name,
        features.features,
        recreate,
    )
    update_releases(client, release, recreate)


def update_web_features(client: BigQuery, recreate: bool) -> None:
    github_releases = webfeatures.github.get_releases()
    imported_releases = {}
    last_published_import = None
    if not recreate:
        imported_releases = get_imported_releases(client)
        if imported_releases:
            last_published_import = max(imported_releases.values())

    for release in github_releases:
        if (
            release.name != "next"
            and release.name not in imported_releases
            and (
                last_published_import is None
                or release.published_at > last_published_import
            )
        ):
            try:
                logging.info(f"Reading web features data for {release.name}")
                import_release(client, recreate, release)
            except KeyError as e:
                logging.warning(
                    f"Failed to read web-featured data for {release.name}: {e}"
                )
        # After the first iteration we never want to overwrite tables
        recreate = False


class WebFeaturesJob(EtlJob):
    name = "web_features"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Web Features", description="Web Features arguments"
        )
        group.add_argument(
            "--bq-web-features-dataset",
            type=dataset_arg,
            help="BigQuery Web Features dataset id",
        )
        group.add_argument(
            "--recreate-web-features",
            action="store_true",
            help="Delete and recreate web features tables from scratch",
        )

    def required_args(self) -> set[str | tuple[str, str]]:
        return {"bq_web_features_dataset"}

    def default_dataset(self, args: argparse.Namespace) -> str:
        return args.bq_web_features_dataset

    def main(self, client: BigQuery, args: argparse.Namespace) -> None:
        update_web_features(client, args.recreate_web_features)
