import argparse
from dataclasses import dataclass
import logging
from datetime import datetime
from typing import Mapping, Union

import webfeatures
from webfeatures import FeaturesFile
from webfeatures.features import Feature, FeatureMoved, FeatureSplit

from .base import Context, EtlJob, dataset_arg
from .bqhelpers import BigQuery, Json, TableSchema
from .projectdata import Project


def get_imported_releases(client: BigQuery, table: TableSchema) -> dict[str, datetime]:
    query = f"SELECT name, published_at FROM {table}"
    try:
        return {item.name: item.published_at for item in client.query(query)}
    except Exception:
        # If the table doesn't exist
        return {}


def update_releases(
    client: BigQuery,
    releases_table: TableSchema,
    release: webfeatures.github.Release,
    recreate: bool,
) -> None:
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
        releases_table.schema,
        rows,
        recreate,
    )


def update_browsers(
    project: Project,
    client: BigQuery,
    browser_data: Mapping[str, webfeatures.features.BrowserData],
    recreate: bool,
) -> None:
    browsers_table = project["web_features"]["browsers"].table()
    browser_versions_table = project["web_features"]["browser_versions"].table()

    if not recreate:
        known_browsers = {
            item.id for item in client.query(f"SELECT id FROM `{browsers_table}`")
        }
        known_versions = {
            (item.browser_id, item.version): item.date
            for item in client.query(
                f"SELECT browser_id, version, date FROM `{browser_versions_table}`"
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

    for table, rows, desc in [
        (browsers_table, insert_browsers, "browsers"),
        (
            browser_versions_table,
            insert_versions,
            "browser versions",
        ),
    ]:
        client.write_table(table, table.schema, rows, recreate)


@dataclass
class FeaturesTable:
    table: TableSchema
    rows: list[dict[str, Json]]


def update_features(
    project: Project,
    client: BigQuery,
    release_name: str,
    features_data: Mapping[str, Union[Feature, FeatureMoved, FeatureSplit]],
    recreate: bool,
) -> None:
    features = FeaturesTable(
        table=project["web_features"]["features"].table(),
        rows=[],
    )

    features_moved = FeaturesTable(
        table=project["web_features"]["features_moved"].table(),
        rows=[],
    )

    features_split = FeaturesTable(
        table=project["web_features"]["features_split"].table(),
        rows=[],
    )

    for feature_id, data in features_data.items():
        if isinstance(data, Feature):
            features.rows.append(
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
        elif isinstance(data, FeatureMoved):
            features_moved.rows.append(
                {
                    "release": release_name,
                    "feature": feature_id,
                    "redirect_target": data.redirect_target,
                }
            )
        elif isinstance(data, FeatureSplit):
            features_split.rows.append(
                {
                    "release": release_name,
                    "feature": feature_id,
                    "redirect_target": data.redirect_targets,
                }
            )

    for table_dfn in [features, features_moved, features_split]:
        assert table_dfn.table is not None
        client.write_table(
            table_dfn.table, table_dfn.table.schema, table_dfn.rows, recreate
        )


def import_release(
    project: Project,
    client: BigQuery,
    releases_table: TableSchema,
    release: webfeatures.github.Release,
    recreate: bool,
) -> None:
    data = webfeatures.github.get_data(release)
    try:
        features = FeaturesFile.model_validate(data)
    except Exception:
        logging.warning(f"Failed to get web features data for {release.name}")
        return
    update_browsers(project, client, features.browsers, recreate)
    update_features(
        project,
        client,
        release.name,
        features.features,
        recreate,
    )
    update_releases(client, releases_table, release, recreate)


def update_web_features(project: Project, client: BigQuery, recreate: bool) -> None:
    github_releases = webfeatures.github.get_releases()
    imported_releases = {}
    last_published_import = None

    releases_table = project["web_features"]["releases"].table()

    if not recreate:
        imported_releases = get_imported_releases(client, releases_table)
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
                import_release(project, client, releases_table, release, recreate)
            except KeyError as e:
                logging.warning(
                    f"Failed to read web-featured data for {release.name}: {e}"
                )
        # After the first iteration we never want to overwrite tables
        recreate = False


class WebFeaturesJob(EtlJob):
    name = "web-features"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Web Features", description="Web Features arguments"
        )
        # Legacy: BigQuery Web Features dataset id
        group.add_argument(
            "--bq-web-features-dataset", type=dataset_arg, help=argparse.SUPPRESS
        )
        group.add_argument(
            "--recreate-web-features",
            action="store_true",
            help="Delete and recreate web features tables from scratch",
        )

    def default_dataset(self, context: Context) -> str:
        return "web_features"

    def main(self, context: Context) -> None:
        update_web_features(
            context.project, context.bq_client, context.args.recreate_web_features
        )
