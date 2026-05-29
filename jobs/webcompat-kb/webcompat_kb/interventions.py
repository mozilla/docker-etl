import logging
from dataclasses import dataclass, asdict
import argparse
from typing import Optional, Self, Mapping

from google.api_core.exceptions import NotFound

from pydantic import BaseModel, Field, model_validator
from google.cloud import bigquery

from .base import Context, EtlJob
from .bqhelpers import BigQuery, TableSchema
from .github import GitHub
from .httphelpers import get_json
from .interop import repo_arg
from .projectdata import Project


class AlterHeader(BaseModel):
    headers: list[str]
    replacement: str
    fallback: Optional[str] = None
    types: Optional[list[str]] = None
    replace: Optional[str] = None
    urls: Optional[list[str]] = None


class MatchesOrBlocksEntry(BaseModel):
    types: list[str]
    url: str


MatchesOrBlocks = list[str] | list[MatchesOrBlocksEntry]


class InterventionBug(BaseModel):
    issue: str
    matches: Optional[MatchesOrBlocks] = None
    exclude_matches: Optional[MatchesOrBlocks] = None
    blocks: Optional[MatchesOrBlocks] = None
    exclude_blocks: Optional[MatchesOrBlocks] = None

    @model_validator(mode="after")
    def check_required(self) -> "InterventionBug":
        if not (
            self.matches or self.exclude_matches or self.blocks or self.exclude_blocks
        ):
            raise ValueError(
                "at least one of matches, exclude_matches, blocks, "
                "exclude_blocks is required"
            )
        return self


class ContentScripts(BaseModel):
    all_frames: Optional[bool] = None
    isolated: Optional[bool] = None
    match_origin_as_fallback: Optional[bool] = None
    css: Optional[list[str]] = None
    js: Optional[list[str]] = None

    @model_validator(mode="after")
    def check_required(self) -> "ContentScripts":
        if not (self.css or self.js):
            raise ValueError("at least one of css, js is required")
        return self


class CssIntervention(BaseModel):
    which: list[str]
    all_frames: Optional[bool] = None
    match_origin_as_fallback: Optional[bool] = None


class HideAlertsConfig(BaseModel):
    alerts: str | list[str]
    all_frames: Optional[bool] = None
    match_origin_as_fallback: Optional[bool] = None


HideAlerts = str | list[str] | HideAlertsConfig


class HiddenMessage(BaseModel):
    message: str
    container: str
    click_adjacent: Optional[str] = None


class HideMessagesConfig(BaseModel):
    messages: list[HiddenMessage]
    all_frames: Optional[bool] = None
    match_origin_as_fallback: Optional[bool] = None


HideMessages = HiddenMessage | list[HiddenMessage] | HideMessagesConfig


class ModifyMetaViewportChangeSpecObject(BaseModel):
    value: Optional[str]
    only_if_equals: Optional[str | list[str]] = None
    only_if_not_equals: Optional[str | list[str]] = None


ModifyMetaViewportChangeSpec = str | ModifyMetaViewportChangeSpecObject | None


class ModifyMetaViewportChanges(BaseModel):
    height: Optional[ModifyMetaViewportChangeSpec] = None
    initial_scale: Optional[ModifyMetaViewportChangeSpec] = Field(
        default=None, alias="initial-scale"
    )
    interactive_widget: Optional[ModifyMetaViewportChangeSpec] = Field(
        default=None, alias="interactive-widget"
    )
    maximum_scale: Optional[ModifyMetaViewportChangeSpec] = Field(
        default=None, alias="maximum-scale"
    )
    minimum_scale: Optional[ModifyMetaViewportChangeSpec] = Field(
        default=None, alias="minimum-scale"
    )
    user_scalable: Optional[ModifyMetaViewportChangeSpec] = Field(
        default=None, alias="user-scalable"
    )
    viewport_fit: Optional[ModifyMetaViewportChangeSpec] = Field(
        default=None, alias="viewport-fit"
    )
    width: Optional[ModifyMetaViewportChangeSpec] = None


class ModifyMetaViewportConfig(BaseModel):
    modify: ModifyMetaViewportChanges
    all_frames: Optional[bool] = None
    match_origin_as_fallback: Optional[bool] = None


ModifyMetaViewport = ModifyMetaViewportChanges | ModifyMetaViewportConfig


class ReplaceStringInRequestEntry(BaseModel):
    find: str
    replace: str
    urls: list[str]
    types: Optional[list[str]] = None


class RunScriptBeforeRequest(BaseModel):
    message: str
    script: str
    urls: list[str]


class UAString(BaseModel):
    change: str
    version: Optional[str]


class InterventionData(BaseModel):
    alter_request_headers: Optional[list[AlterHeader]] = None
    alter_response_headers: Optional[list[AlterHeader]] = None
    content_scripts: Optional[ContentScripts] = None
    css: Optional[list[str] | CssIntervention] = None
    hide_alerts: Optional[HideAlerts] = None
    hide_messages: Optional[HideMessages] = None
    modify_meta_viewport: Optional[ModifyMetaViewport] = None
    max_version: Optional[float] = None
    min_version: Optional[float] = None
    not_channels: Optional[list[str]] = None
    not_platforms: Optional[list[str]] = None
    only_channels: Optional[list[str]] = None
    platforms: Optional[list[str]] = None
    pref_check: Optional[dict[str, bool]] = None
    replace_string_in_request: Optional[list[ReplaceStringInRequestEntry]] = None
    run_script_before_request: Optional[RunScriptBeforeRequest] = None
    skip_if: Optional[list[str]] = None
    ua_string: Optional[list[str | UAString]] = None

    @model_validator(mode="after")
    def check_platforms(self) -> "InterventionData":
        if self.platforms is None and self.not_platforms is None:
            raise ValueError("at least one of platforms, not_platforms is required")
        return self


class Intervention(BaseModel):
    bugs: dict[str, InterventionBug]
    interventions: list[InterventionData]
    label: str
    css: Optional[dict[str, str]] = None


@dataclass
class CSSStruct:
    id: str
    src: str


@dataclass
class UAStringStruct:
    change: str
    version: Optional[str]

    @classmethod
    def from_model(cls, src: Optional[list[str | UAString]]) -> Optional[list[Self]]:
        if src is None:
            return None
        rv = []
        for item in src:
            if isinstance(item, str):
                rv.append(cls(item, None))
            else:
                rv.append(cls(item.change, item.version))
        return rv


@dataclass
class MatchTypeStruct:
    types: Optional[list[str]]
    url: str

    @classmethod
    def from_model(cls, src: Optional[MatchesOrBlocks]) -> Optional[list[Self]]:
        if src is None:
            return None
        rv = []
        for item in src:
            if isinstance(item, str):
                rv.append(cls(types=None, url=item))
            else:
                rv.append(cls(types=item.types, url=item.url))

        return rv


@dataclass
class AlterHeaderStruct:
    headers: list[str]
    replacement: str
    fallback: Optional[str]
    types: Optional[list[str]]
    replace: Optional[str]
    urls: Optional[list[str]]

    @classmethod
    def from_model(cls, src: Optional[list[AlterHeader]]) -> Optional[list[Self]]:
        if src is None:
            return None
        return [
            cls(
                headers=item.headers,
                replacement=item.replacement,
                fallback=item.fallback,
                types=item.types,
                replace=item.replace,
                urls=item.urls,
            )
            for item in src
        ]


@dataclass
class ContentScriptsStruct:
    all_frames: Optional[bool]
    isolated: Optional[bool]
    match_origin_as_fallback: Optional[bool]
    css: Optional[list[str]]
    js: Optional[list[str]]

    @classmethod
    def from_model(cls, src: Optional[ContentScripts]) -> Optional[Self]:
        if src is None:
            return None
        return cls(
            all_frames=src.all_frames,
            isolated=src.isolated,
            match_origin_as_fallback=src.match_origin_as_fallback,
            css=src.css,
            js=src.js,
        )


@dataclass
class HideAlertsStruct:
    alerts: list[str]
    all_frames: Optional[bool]
    match_origin_as_fallback: Optional[bool]

    @classmethod
    def from_model(cls, src: Optional[HideAlerts]) -> Optional[Self]:
        if src is None:
            return None
        if isinstance(src, str):
            return cls(alerts=[src], all_frames=None, match_origin_as_fallback=None)
        elif isinstance(src, list):
            return cls(alerts=src, all_frames=None, match_origin_as_fallback=None)
        else:
            return cls(
                alerts=src.alerts if isinstance(src.alerts, list) else [src.alerts],
                all_frames=src.all_frames,
                match_origin_as_fallback=src.match_origin_as_fallback,
            )


@dataclass
class CssInterventionStruct:
    which: list[str]
    all_frames: Optional[bool] = None
    match_origin_as_fallback: Optional[bool] = None

    @classmethod
    def from_model(cls, src: Optional[CssIntervention | list[str]]) -> Optional[Self]:
        if src is None:
            return None
        if isinstance(src, list):
            return cls(which=src, all_frames=None, match_origin_as_fallback=None)
        else:
            return cls(
                which=src.which,
                all_frames=src.all_frames,
                match_origin_as_fallback=src.match_origin_as_fallback,
            )


@dataclass
class HiddenMessageStruct:
    message: str
    container: str
    click_adjacent: Optional[str] = None

    @classmethod
    def from_model(cls, src: HiddenMessage) -> Self:
        return cls(
            message=src.message,
            container=src.container,
            click_adjacent=src.click_adjacent,
        )


@dataclass
class HideMessagesStruct:
    messages: list[HiddenMessageStruct]
    all_frames: Optional[bool]
    match_origin_as_fallback: Optional[bool]

    @classmethod
    def from_model(cls, src: Optional[HideMessages]) -> Optional[Self]:
        if src is None:
            return None
        if isinstance(src, HiddenMessage):
            return cls(
                messages=[HiddenMessageStruct.from_model(src)],
                all_frames=None,
                match_origin_as_fallback=None,
            )
        elif isinstance(src, list):
            return cls(
                messages=[HiddenMessageStruct.from_model(item) for item in src],
                all_frames=None,
                match_origin_as_fallback=None,
            )
        else:
            return cls(
                messages=[
                    HiddenMessageStruct.from_model(item) for item in src.messages
                ],
                all_frames=src.all_frames,
                match_origin_as_fallback=src.match_origin_as_fallback,
            )


@dataclass
class RunScriptBeforeRequestStruct:
    message: str
    script: str
    urls: list[str]

    @classmethod
    def from_model(cls, src: Optional[RunScriptBeforeRequest]) -> Optional[Self]:
        if src is None:
            return None
        return cls(message=src.message, script=src.script, urls=src.urls)


@dataclass
class ReplaceStringInRequestStruct:
    find: str
    replace: str
    urls: list[str]
    types: Optional[list[str]]

    @classmethod
    def from_model(
        cls, src: Optional[list[ReplaceStringInRequestEntry]]
    ) -> Optional[list[Self]]:
        if src is None:
            return None
        return [
            cls(find=item.find, replace=item.replace, urls=item.urls, types=item.types)
            for item in src
        ]


@dataclass
class ModifyMetaViewportValueStruct:
    value: Optional[str]
    only_if_equals: Optional[list[str]]
    only_if_not_equals: Optional[list[str]]

    @classmethod
    def from_model(cls, src: Optional[ModifyMetaViewportChangeSpec]) -> Optional[Self]:
        if src is None:
            return None
        if isinstance(src, str):
            return cls(value=src, only_if_equals=None, only_if_not_equals=None)
        return cls(
            value=src.value,
            only_if_equals=[src.only_if_equals]
            if isinstance(src.only_if_equals, str)
            else src.only_if_equals,
            only_if_not_equals=[src.only_if_not_equals]
            if isinstance(src.only_if_not_equals, str)
            else src.only_if_not_equals,
        )


@dataclass
class ModifyMetaViewportStruct:
    height: Optional[ModifyMetaViewportValueStruct]
    initial_scale: Optional[ModifyMetaViewportValueStruct]
    interactive_widget: Optional[ModifyMetaViewportValueStruct]
    maximum_scale: Optional[ModifyMetaViewportValueStruct]
    minimum_scale: Optional[ModifyMetaViewportValueStruct]
    user_scalable: Optional[ModifyMetaViewportValueStruct]
    viewport_fit: Optional[ModifyMetaViewportValueStruct]
    width: Optional[ModifyMetaViewportValueStruct]

    @classmethod
    def from_model(cls, src: ModifyMetaViewportChanges) -> Self:
        return cls(
            height=ModifyMetaViewportValueStruct.from_model(src.height),
            initial_scale=ModifyMetaViewportValueStruct.from_model(src.initial_scale),
            interactive_widget=ModifyMetaViewportValueStruct.from_model(
                src.interactive_widget
            ),
            maximum_scale=ModifyMetaViewportValueStruct.from_model(src.maximum_scale),
            minimum_scale=ModifyMetaViewportValueStruct.from_model(src.minimum_scale),
            user_scalable=ModifyMetaViewportValueStruct.from_model(src.user_scalable),
            viewport_fit=ModifyMetaViewportValueStruct.from_model(src.viewport_fit),
            width=ModifyMetaViewportValueStruct.from_model(src.width),
        )


@dataclass
class ModifyMetaViewportConfigStruct:
    modify: ModifyMetaViewportStruct
    all_frames: Optional[bool]
    match_origin_as_fallback: Optional[bool]

    @classmethod
    def from_model(
        cls, src: Optional[ModifyMetaViewportConfig | ModifyMetaViewportChanges]
    ) -> Optional[Self]:
        if src is None:
            return None
        if isinstance(src, ModifyMetaViewportChanges):
            return cls(
                modify=ModifyMetaViewportStruct.from_model(src),
                all_frames=None,
                match_origin_as_fallback=None,
            )
        return cls(
            modify=ModifyMetaViewportStruct.from_model(src.modify),
            all_frames=src.all_frames,
            match_origin_as_fallback=src.match_origin_as_fallback,
        )


@dataclass
class InterventionDataStruct:
    alter_request_headers: Optional[list[AlterHeaderStruct]]
    alter_response_headers: Optional[list[AlterHeaderStruct]]
    content_scripts: Optional[ContentScriptsStruct]
    css: Optional[CssInterventionStruct]
    hide_alerts: Optional[HideAlertsStruct]
    hide_messages: Optional[HideMessagesStruct]
    modify_meta_viewport: Optional[ModifyMetaViewportConfigStruct]
    max_version: Optional[float]
    min_version: Optional[float]
    not_channels: Optional[list[str]]
    not_platforms: Optional[list[str]]
    only_channels: Optional[list[str]]
    platforms: Optional[list[str]]
    replace_string_in_request: Optional[list[ReplaceStringInRequestStruct]]
    run_script_before_request: Optional[RunScriptBeforeRequestStruct]
    pref_check: Optional[dict[str, bool]]
    skip_if: Optional[list[str]]
    ua_string: Optional[list[UAStringStruct]]

    @classmethod
    def from_model(cls, src: InterventionData) -> Self:
        return cls(
            alter_request_headers=AlterHeaderStruct.from_model(
                src.alter_request_headers
            ),
            alter_response_headers=AlterHeaderStruct.from_model(
                src.alter_response_headers
            ),
            content_scripts=ContentScriptsStruct.from_model(src.content_scripts),
            css=CssInterventionStruct.from_model(src.css),
            hide_alerts=HideAlertsStruct.from_model(src.hide_alerts),
            hide_messages=HideMessagesStruct.from_model(src.hide_messages),
            modify_meta_viewport=ModifyMetaViewportConfigStruct.from_model(
                src.modify_meta_viewport
            ),
            max_version=src.max_version,
            min_version=src.min_version,
            not_channels=src.not_channels,
            not_platforms=src.not_platforms,
            only_channels=src.only_channels,
            platforms=src.platforms,
            replace_string_in_request=ReplaceStringInRequestStruct.from_model(
                src.replace_string_in_request
            ),
            run_script_before_request=RunScriptBeforeRequestStruct.from_model(
                src.run_script_before_request
            ),
            pref_check=src.pref_check,
            skip_if=src.skip_if,
            ua_string=UAStringStruct.from_model(src.ua_string),
        )


@dataclass
class InterventionRow:
    intervention_id: str
    label: str
    interventions: list[InterventionDataStruct]
    css: Optional[list[CSSStruct]] = None

    @classmethod
    def from_model(cls, name: str, src: Intervention) -> Self:
        return cls(
            intervention_id=name,
            label=src.label,
            interventions=[
                InterventionDataStruct.from_model(item) for item in src.interventions
            ],
            css=[CSSStruct(id, src) for id, src in src.css.items()]
            if src.css is not None
            else None,
        )


@dataclass
class InterventionBugRow:
    intervention_id: str
    number: int
    issue: str
    matches: Optional[list[MatchTypeStruct]]
    exclude_matches: Optional[list[MatchTypeStruct]]
    blocks: Optional[list[MatchTypeStruct]]
    exclude_blocks: Optional[list[MatchTypeStruct]]

    @classmethod
    def from_model(cls, name: str, bug_id: int, src: InterventionBug) -> Self:
        return cls(
            intervention_id=name,
            number=bug_id,
            issue=src.issue,
            matches=MatchTypeStruct.from_model(src.matches),
            exclude_matches=MatchTypeStruct.from_model(src.exclude_matches),
            blocks=MatchTypeStruct.from_model(src.blocks),
            exclude_blocks=MatchTypeStruct.from_model(src.exclude_blocks),
        )


def get_all_interventions(gh_client: GitHub, repo: str) -> Mapping[str, Intervention]:
    rv = {}
    for item in gh_client.repository_contents(
        repo, "/browser/extensions/webcompat/data/interventions"
    ):
        if item.type == "file" and item.name.endswith(".json"):
            name = item.name.rsplit(".", 1)[0]
            assert item.download_url is not None
            rv[name] = Intervention.model_validate(get_json(item.download_url))
    return rv


def last_import(client: BigQuery, table: TableSchema) -> Optional[str]:
    query = f"""SELECT sha1
FROM {table}
ORDER BY run_at DESC
LIMIT 1"""
    try:
        rows = list(client.query(query))
    except NotFound:
        return None
    if rows:
        return rows[0].sha1
    return None


def update_last_import(client: BigQuery, table: TableSchema, sha1: str) -> None:
    client.ensure_table(table, table.schema)
    client.insert_query(
        table,
        columns=[item.name for item in table.fields],
        query="SELECT CURRENT_DATETIME(), @sha1",
        parameters=[
            bigquery.ScalarQueryParameter("sha1", "STRING", sha1),
        ],
    )


def update_interventions(
    project: Project, client: BigQuery, gh_client: GitHub, repo: str
) -> None:
    import_table = project["interventions"]["import_runs"].table()
    last_sha1 = last_import(client, import_table)

    current_sha1 = None
    for item in gh_client.repository_contents(
        repo, "/browser/extensions/webcompat/data"
    ):
        if item.name == "interventions":
            current_sha1 = item.sha
            break
    if current_sha1 is None:
        raise ValueError("Failed to get current sha1 for interventions data")

    if last_sha1 == current_sha1:
        logging.info("Interventions not updated")
        return

    logging.info(f"Updating interventions to tree {current_sha1}")
    bugs_rows = []
    interventions_rows = []
    for name, intervention in get_all_interventions(gh_client, repo).items():
        interventions_rows.append(InterventionRow.from_model(name, intervention))
        for bug_id, bug_data in intervention.bugs.items():
            bugs_rows.append(InterventionBugRow.from_model(name, int(bug_id), bug_data))

    interventions_table = project["interventions"]["interventions"].table()
    client.write_table(
        interventions_table,
        interventions_table.schema,
        [asdict(row) for row in interventions_rows],
        overwrite=True,
    )
    bugs_table = project["interventions"]["bugs"].table()
    client.write_table(
        bugs_table,
        bugs_table.schema,
        [asdict(row) for row in bugs_rows],
        overwrite=True,
    )

    update_last_import(client, import_table, current_sha1)


class InterventionsJob(EtlJob):
    name = "interventions"

    @classmethod
    def add_arguments(cls, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group(
            title="Intervensions", description="Interventions arguments"
        )
        group.add_argument(
            "--firefox-repo",
            type=repo_arg,
            default="mozilla-firefox/firefox",
            help="Firefox repository in the format org/repo",
        )

    def default_dataset(self, context: Context) -> str:
        return "interventions"

    def main(self, context: Context) -> None:
        gh_client = GitHub(context.args.github_token)
        update_interventions(
            context.project,
            context.bq_client,
            gh_client,
            context.args.firefox_repo,
        )
