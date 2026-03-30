from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime
import logging
import os
import pathlib
import tomllib
from typing import Annotated, Callable, Literal, Mapping, Optional, Self, Sequence

import jinja2
import tomli_w
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationError,
    model_validator,
)

from .metrics import metrics
from .redash import client


class DashboardMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    description: str = ""


class RedashParameterBase(BaseModel):
    model_config = ConfigDict(frozen=True)

    type: str
    title: Optional[str] = None

    def _get_title(
        self, name: str, reference: Optional[client.RedashParameterBase]
    ) -> str:
        if self.title is not None:
            return self.title
        if reference is not None:
            return reference.title
        return name


class RedashEnumParameter(RedashParameterBase):
    type: Literal["enum"] = "enum"
    enum_options: list[str]
    value: Optional[str | list[str]] = None
    multi_values_options: Optional[client.RedashEnumMultiValuesOptions] = None

    @model_validator(mode="after")
    def check_value(self) -> Self:
        if self.multi_values_options is not None:
            if self.value is not None and not isinstance(self.value, list):
                raise ValueError(
                    "Enum with multi_values_options set must have a list of values"
                )
        elif isinstance(self.value, list):
            raise ValueError(
                "Enum without multi_values_options set must have a single value"
            )
        return self

    def to_client(
        self, name: str, reference: Optional[client.RedashParameterBase]
    ) -> client.RedashEnumParameter:
        if not isinstance(reference, client.RedashEnumParameter):
            reference = None
        enum_options = "\n".join(self.enum_options)
        if self.value is None:
            if reference is not None:
                value = reference.value
            else:
                value = self.enum_options[0]
                if self.multi_values_options:
                    value = [value]
        else:
            value = self.value

        return client.RedashEnumParameter(
            name=name,
            title=self._get_title(name, reference),
            enumOptions=enum_options,
            value=value,
            multiValuesOptions=self.multi_values_options,
        )


class RedashQueryParameter(RedashParameterBase):
    type: Literal["query"] = "query"
    query_id: int
    value: Optional[str | list[str]] = None
    multi_values_options: Optional[client.RedashEnumMultiValuesOptions] = None

    @model_validator(mode="after")
    def check_value(self) -> Self:
        if self.multi_values_options is not None:
            if self.value is not None and not isinstance(self.value, list):
                raise ValueError(
                    "Enum with multi_values_options set must have a list of values"
                )
        elif isinstance(self.value, list):
            raise ValueError(
                "Enum without multi_values_options set must have a single value"
            )
        return self

    def to_client(
        self, name: str, reference: Optional[client.RedashParameterBase]
    ) -> client.RedashQueryParameter:
        if not isinstance(reference, client.RedashQueryParameter):
            reference = None

        if self.value is None:
            if reference is not None:
                value = reference.value
            elif self.multi_values_options:
                value = []
            else:
                value = ""
        else:
            value = self.value

        return client.RedashQueryParameter(
            name=name,
            title=self._get_title(name, reference),
            queryId=self.query_id,
            value=value,
            multiValuesOptions=self.multi_values_options,
        )


class RedashTextParameter(RedashParameterBase):
    type: Literal["text"] = "text"
    value: Optional[str] = None

    def to_client(
        self, name: str, reference: Optional[client.RedashParameterBase]
    ) -> client.RedashTextParameter:
        if not isinstance(reference, client.RedashTextParameter):
            reference = None
        if self.value is not None:
            value = self.value
        elif reference is not None:
            value = reference.value
        else:
            value = ""
        return client.RedashTextParameter(
            name=name, title=self._get_title(name, reference), value=value
        )


class RedashNumberParameter(RedashParameterBase):
    type: Literal["number"] = "number"
    value: Optional[int | float] = None

    def to_client(
        self, name: str, reference: Optional[client.RedashParameterBase]
    ) -> client.RedashNumberParameter:
        if not isinstance(reference, client.RedashNumberParameter):
            reference = None
        if self.value is not None:
            value = self.value
        elif reference is not None:
            value = reference.value
        else:
            value = 0
        return client.RedashNumberParameter(
            name=name, title=self._get_title(name, reference), value=value
        )


class RedashDateParameter(RedashParameterBase):
    type: Literal["date"] = "date"
    value: Optional[date] = None

    def to_client(
        self, name: str, reference: Optional[client.RedashParameterBase]
    ) -> client.RedashDateParameter:
        if not isinstance(reference, client.RedashDateParameter):
            reference = None
        if self.value is not None:
            value = self.value
        elif reference is not None:
            value = reference.value
        else:
            value = date.today()
        return client.RedashDateParameter(
            name=name, title=self._get_title(name, reference), value=value
        )


class RedashDateTimeParameter(RedashParameterBase):
    type: Literal["datetime-local"] = "datetime-local"
    value: Optional[datetime] = None

    def to_client(
        self, name: str, reference: Optional[client.RedashParameterBase]
    ) -> client.RedashDateTimeParameter:
        if not isinstance(reference, client.RedashDateTimeParameter):
            reference = None
        if self.value is not None:
            value = self.value
        elif reference is not None:
            value = reference.value
        else:
            value = datetime.now()
        return client.RedashDateTimeParameter(
            name=name, title=self._get_title(name, reference), value=value
        )


class RedashDateTimeWithSecondsParameter(RedashParameterBase):
    type: Literal["datetime-with-seconds"] = "datetime-with-seconds"
    value: Optional[datetime] = None

    def to_client(
        self, name: str, reference: Optional[client.RedashParameterBase]
    ) -> client.RedashDateTimeWithSecondsParameter:
        if not isinstance(reference, client.RedashDateTimeWithSecondsParameter):
            reference = None
        if self.value is not None:
            value = self.value
        elif reference is not None:
            value = reference.value
        else:
            value = datetime.now()
        return client.RedashDateTimeWithSecondsParameter(
            name=name, title=self._get_title(name, reference), value=value
        )


class RedashDateRangeValue(BaseModel):
    model_config = ConfigDict(frozen=True)

    start: date
    end: date


class RedashDateRangeParameter(RedashParameterBase):
    type: Literal["date-range"] = "date-range"
    value: Optional[RedashDateRangeValue] = None

    def to_client(
        self, name: str, reference: Optional[client.RedashParameterBase]
    ) -> client.RedashDateRangeParameter:
        if not isinstance(reference, client.RedashDateRangeParameter):
            reference = None
        if self.value is not None:
            value = client.RedashDateRangeValue(
                start=self.value.start, end=self.value.end
            )
        elif reference is not None:
            value = reference.value
        else:
            today = date.today()
            value = client.RedashDateRangeValue(start=today, end=today)
        return client.RedashDateRangeParameter(
            name=name, title=self._get_title(name, reference), value=value
        )


class RedashDateTimeRangeValue(BaseModel):
    model_config = ConfigDict(frozen=True)

    start: datetime
    end: datetime


class RedashDateTimeRangeParameter(RedashParameterBase):
    type: Literal["datetime-range"] = "datetime-range"
    value: Optional[RedashDateTimeRangeValue] = None

    def to_client(
        self, name: str, reference: Optional[client.RedashParameterBase]
    ) -> client.RedashDateTimeRangeParameter:
        if not isinstance(reference, client.RedashDateTimeRangeParameter):
            reference = None
        if self.value is not None:
            value = client.RedashDateTimeRangeValue(
                start=self.value.start, end=self.value.end
            )
        elif reference is not None:
            value = reference.value
        else:
            now = datetime.now()
            value = client.RedashDateTimeRangeValue(start=now, end=now)
        return client.RedashDateTimeRangeParameter(
            name=name, title=self._get_title(name, reference), value=value
        )


class RedashDateTimeWithSecondsRangeParameter(RedashParameterBase):
    type: Literal["datetime-range-with-seconds"] = "datetime-range-with-seconds"
    value: Optional[RedashDateTimeRangeValue] = None

    def to_client(
        self, name: str, reference: Optional[client.RedashParameterBase]
    ) -> client.RedashDateTimeWithSecondsRangeParameter:
        if not isinstance(reference, client.RedashDateTimeWithSecondsRangeParameter):
            reference = None
        if self.value is not None:
            value = client.RedashDateTimeWithSecondsRangeValue(
                start=self.value.start, end=self.value.end
            )
        elif reference is not None:
            value = reference.value
        else:
            now = datetime.now()
            value = client.RedashDateTimeWithSecondsRangeValue(start=now, end=now)
        return client.RedashDateTimeWithSecondsRangeParameter(
            name=name, title=self._get_title(name, reference), value=value
        )


RedashParameter = Annotated[
    RedashTextParameter
    | RedashNumberParameter
    | RedashEnumParameter
    | RedashQueryParameter
    | RedashDateParameter
    | RedashDateTimeParameter
    | RedashDateTimeWithSecondsParameter
    | RedashDateRangeParameter
    | RedashDateTimeRangeParameter
    | RedashDateTimeWithSecondsRangeParameter,
    Field(discriminator="type"),
]


class QueryMetadata(BaseModel):
    id: Optional[int] = None
    name: str
    description: Optional[str] = None


class QueryParameters(RootModel):
    root: Mapping[str, RedashParameter]


@dataclass
class RedashQueryTemplate:
    path: pathlib.Path
    metadata: QueryMetadata
    parameters_template: Optional[str]
    query_template: str

    @property
    def meta_path(self) -> pathlib.Path:
        return self.path / "meta.toml"

    @property
    def parameters_path(self) -> pathlib.Path:
        return self.path / "parameters.toml"

    @property
    def query_path(self) -> pathlib.Path:
        return self.path / "query.sql"

    @classmethod
    def load_from_dir(cls, path: pathlib.Path) -> Optional[Self]:
        """Load a query template from a directory

        Expected structure:
        - meta.toml: Query metadata
        - parameters.toml: Query metadata (may contain Jinja2 templates)
        - view.sql: SQL query (may contain Jinja2 templates)
        """
        path_obj = path.absolute()
        if not path_obj.is_dir():
            raise ValueError(f"Expected a directory, got {path_obj}")

        meta_path = path_obj / "meta.toml"
        try:
            with open(meta_path, "rb") as f:
                metadata_dict = tomllib.load(f)
        except OSError:
            logging.warning(f"Failed to find {meta_path}")
            return None
        except tomllib.TOMLDecodeError as e:
            raise ValueError(f"Failed to load Redash query metadata {meta_path}") from e

        try:
            metadata = QueryMetadata.model_validate(metadata_dict)
        except ValidationError as e:
            raise ValueError(f"Failed to load Redash query metadata {meta_path}") from e

        parameters_path = path_obj / "parameters.toml"
        try:
            with open(parameters_path, "r") as f:
                parameters_template = f.read()
        except OSError:
            parameters_template = None

        query_path = path_obj / "query.sql"
        try:
            with open(query_path, "r") as f:
                query_template = f.read()
        except OSError:
            logging.warning(f"Failed to find {query_path}")
            return None

        return cls(
            path=path_obj,
            metadata=metadata,
            parameters_template=parameters_template,
            query_template=query_template,
        )

    def update(self, write: bool) -> None:
        metadata = self.metadata.model_dump(exclude_unset=True)
        if write:
            with open(self.meta_path, "wb") as f:
                tomli_w.dump(metadata, f, indent=2)
            with open(self.query_path, "w") as f:
                f.write(self.query_template)
        else:
            logging.info(
                f"Would write metadata file {self.meta_path}:\n{tomli_w.dumps(metadata, indent=2)}"
            )
            logging.info(
                f"Would write template {self.query_path}:\n{self.query_template}"
            )


@dataclass
class RedashDashboard:
    metadata: DashboardMetadata
    parameters_template: Optional[str]
    queries: Sequence[RedashQueryTemplate]

    @property
    def name(self) -> str:
        return self.metadata.name

    @classmethod
    def _load_from_dir(cls, path: pathlib.Path) -> Optional[Self]:
        if not path.is_dir():
            raise ValueError(
                f"Tried to load a dashboard from {path} which is not a directory"
            )

        meta_path = path / "meta.toml"

        # Load dashboard metadata
        try:
            with open(meta_path, "rb") as f:
                dashboard_data = tomllib.load(f)
        except OSError:
            logging.warning(f"Failed to find {meta_path}, skipping dashboard")
            return None

        try:
            metadata = DashboardMetadata.model_validate(dashboard_data)
        except ValidationError as e:
            logging.error(f"Failed to parse metadata for {meta_path}: {e}")
            return None

        parameters_path = path / "parameters.toml"
        try:
            with open(parameters_path, "r") as f:
                parameters_template = f.read()
        except OSError:
            parameters_template = None

        # Load queries for this dashboard
        queries = []
        for query_dir in path.iterdir():
            if not query_dir.is_dir():
                continue

            query = RedashQueryTemplate.load_from_dir(query_dir)
            if query is not None:
                queries.append(query)

        if not queries:
            logging.warning(f"No queries found for dashboard {path.name}")

        return cls(metadata, parameters_template, queries)


@dataclass
class RedashData:
    path: pathlib.Path
    dashboards: Mapping[str, RedashDashboard]

    def get_dashboard(self, name: str) -> RedashDashboard:
        return self.dashboards[name]

    def iter_named(self, name_filter: Optional[set[str]]) -> Iterator[RedashDashboard]:
        if name_filter:
            missing = name_filter - set(self.dashboards.keys())
            if missing:
                raise ValueError(
                    f"Tried to update unknown dashboards: {','.join(missing)}"
                )
            for name, value in self.dashboards.items():
                if name in name_filter:
                    yield value
        else:
            for item in self.dashboards.values():
                yield item


class RedashTemplateRenderer:
    """Renders Redash templates with Jinja2"""

    def __init__(
        self,
        metric_dfns: Sequence[metrics.Metric],
        metric_types: Sequence[metrics.MetricType],
        ref: Callable[[str], str],
    ):
        """Initialize renderer with metrics context

        :param metric_dfns: List of metric definitions
        :param metric_types: List of metric types
        :param ref: Callable that takes a reference string and returns a fully qualified reference
        """
        self.metrics_by_name = {item.name: item for item in metric_dfns}
        self.metric_types = metric_types
        self.ref = ref
        self.jinja_env = jinja2.Environment()

    def dashboard_metrics(self, dashboard: RedashDashboard) -> list[metrics.Metric]:
        """Get metrics filtered for a specific dashboard

        :param dashboard_name: Dashboard name to filter by
        :return: List of metrics that have this dashboard in their dashboards list
        """
        return [
            metric
            for metric in self.metrics_by_name.values()
            if dashboard.name in metric.dashboards
        ]

    def render_parameters(
        self, dashboard: RedashDashboard, query: RedashQueryTemplate
    ) -> Optional[Mapping[str, RedashParameter]]:
        """Load query parameters"""
        if dashboard.parameters_template is None and query.parameters_template is None:
            return None
        dashboard_metrics = self.dashboard_metrics(dashboard)

        context = {
            "dashboard_metrics": dashboard_metrics,
            "metrics": self.metrics_by_name,
            "metric_types": self.metric_types,
        }

        all_parameters: dict[str, RedashParameter] = {}
        for template_str in [dashboard.parameters_template, query.parameters_template]:
            if template_str is None:
                continue
            try:
                template = self.jinja_env.from_string(template_str)
                rendered_toml = template.render(**context)
            except Exception as e:
                raise ValueError(
                    f"Failed to render parameters template for {query.path}"
                ) from e

            try:
                metadata_dict = tomllib.loads(rendered_toml)
                parameters = QueryParameters.model_validate(metadata_dict)
            except (tomllib.TOMLDecodeError, ValidationError) as e:
                raise ValueError(
                    f"Failed to parse parameters for {query.path}:\n{rendered_toml}"
                ) from e
            all_parameters.update(parameters.root)

        return all_parameters

    def render_query(
        self,
        dashboard: RedashDashboard,
        query: RedashQueryTemplate,
        param: Callable[[str], str],
    ) -> str:
        """Render query SQL template

        :return: Rendered SQL string
        """
        dashboard_metrics = self.dashboard_metrics(dashboard)

        context = {
            "dashboard_metrics": dashboard_metrics,
            "metrics": self.metrics_by_name,
            "metric_types": self.metric_types,
            "ref": self.ref,
            "param": param,
        }

        try:
            template = self.jinja_env.from_string(query.query_template)
            return template.render(**context)
        except Exception as e:
            raise ValueError(f"Failed to render SQL template for {query.path}") from e


def load(root_path: os.PathLike) -> RedashData:
    """Load all Redash templates from the data directory

    Expected structure:
    data/redash/
        <dashboard_name>/
            meta.toml
            <query_name>/
                meta.toml
                view.sql
                parameters.toml

    :param root_path: Path to the data directory
    :return: RedashTemplateData containing all loaded templates
    """
    path = pathlib.Path(root_path).resolve()
    redash_path = path / "redash"

    if not redash_path.exists():
        logging.warning(f"Redash templates directory not found: {redash_path}")
        return RedashData(redash_path, {})

    logging.info(f"Loading Redash templates from {redash_path}")

    dashboards = {}

    for dashboard_dir in redash_path.iterdir():
        if not dashboard_dir.is_dir():
            continue
        dashboard = RedashDashboard._load_from_dir(dashboard_dir)
        if dashboard is not None:
            dashboards[dashboard_dir.name] = dashboard

    return RedashData(redash_path, dashboards)
