import os
import tomllib
from abc import ABC, abstractmethod
from typing import Literal, Mapping, Optional, Sequence

from pydantic import BaseModel, RootModel


class Metric(ABC):
    conditional = True

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def condition(self, table: str) -> str: ...

    def host_min_ranks_condition(self) -> Optional[str]:
        return None

    def site_reports_condition(self, table: str) -> Optional[str]:
        return None


class UnconditionalMetric(Metric):
    conditional = False

    def condition(self, table: str) -> str:
        return "TRUE"


class SiteReportsFieldMetric(Metric):
    def __init__(
        self,
        name: str,
        host_min_ranks_condition: Optional[str],
        site_reports_conditions: Optional[list[str]],
    ):
        super().__init__(name)
        self._host_min_ranks_condition = host_min_ranks_condition
        self._site_reports_conditions = site_reports_conditions

    def condition(self, table: str) -> str:
        return f"{table}.is_{self.name}"

    def host_min_ranks_condition(self) -> Optional[str]:
        return self._host_min_ranks_condition

    def site_reports_condition(self, table: str) -> Optional[str]:
        if self._site_reports_conditions:
            return " AND ".join(
                item.format(table=table) for item in self._site_reports_conditions
            )
        return f"IFNULL({table}.is_{self.name}, FALSE)"


class UnconditionalMetricData(BaseModel):
    type: Literal["unconditional"]

    def to_metric(self, name: str) -> UnconditionalMetric:
        return UnconditionalMetric(name)


class SiteReportsFieldMetricData(BaseModel):
    type: Literal["site_reports_field"]
    host_min_ranks_condition: Optional[str] = None
    conditions: Optional[list[str]] = None

    def to_metric(self, name: str) -> SiteReportsFieldMetric:
        return SiteReportsFieldMetric(
            name, self.host_min_ranks_condition, self.conditions
        )


class MetricData(RootModel):
    root: Mapping[str, UnconditionalMetricData | SiteReportsFieldMetricData]


default_contexts = {"history", "daily"}


class MetricType(ABC):
    field_type: str

    def __init__(
        self,
        name: str,
        metric_type_field: Optional[str] = None,
        contexts: Optional[set[str]] = None,
    ):
        self.name = name
        self.metric_type_field = metric_type_field
        self.contexts = default_contexts if contexts is None else contexts
        if not self.contexts.issubset(default_contexts):
            raise ValueError(f"Invalid contexts: {','.join(self.contexts)}")

    @abstractmethod
    def agg_function(
        self, table: str, metric: Metric, include_metric_condition: bool = True
    ) -> str: ...

    def condition(
        self, table: str, metric: Metric, include_metric_condition: bool = True
    ) -> str:
        """Condition applied to the scored_site_reports table to decide if the entry contributes to the metric score.

        :param str table: Alias for scored_site_reports.
        :param Metric metric: Metric for which the condition applies
        :param bool include_metric_condition: Include the condition for the metric itself as well as the type
        :returns str: SQL condition that is TRUE when the scored_site_reports row is included in the metric.conditional
        """
        conds = []
        if self.metric_type_field is not None:
            conds.append(f"{table}.{self.metric_type_field}")
        if metric.conditional and include_metric_condition:
            conds.append(metric.condition(table))
        if not conds:
            return "TRUE"
        return " AND ".join(conds)


class CountMetricType(MetricType):
    field_type = "INTEGER"

    def agg_function(
        self, table: str, metric: Metric, include_metric_condition: bool = True
    ) -> str:
        if not metric.conditional:
            return f"COUNT({table}.number)"
        return f"COUNTIF({self.condition(table, metric, include_metric_condition)})"


class SumMetricType(MetricType):
    field_type = "NUMERIC"

    def agg_function(
        self, table: str, metric: Metric, include_metric_condition: bool = True
    ) -> str:
        return f"SUM(IF({self.condition(table, metric, include_metric_condition)}, {table}.score, 0))"


_metrics: dict[str, Sequence[Metric]] = {}

_metric_types = [
    CountMetricType("bug_count", None),
    SumMetricType("needs_diagnosis_score", "metric_type_needs_diagnosis"),
    SumMetricType("not_supported_score", "metric_type_firefox_not_supported"),
    SumMetricType("platform_score", "metric_type_platform_bug", contexts={"history"}),
    SumMetricType("total_score", None),
]


def load(path: Optional[str] = None) -> tuple[Sequence[Metric], Sequence[MetricType]]:
    if path is None:
        path = os.path.join(
            os.path.dirname(__file__),
            os.pardir,
            os.pardir,
            "data",
            "metrics",
            "metrics.toml",
        )
    path = os.path.abspath(path)
    if path not in _metrics:
        metrics = []
        with open(path, "rb") as f:
            data = tomllib.load(f)

        for name, metric_data in MetricData.model_validate(data).root.items():
            metrics.append(metric_data.to_metric(name))
        _metrics[path] = metrics

    return _metrics[path], _metric_types
