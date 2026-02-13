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


_metric_types = [
    CountMetricType("bug_count", None),
    SumMetricType("needs_diagnosis_score", "metric_type_needs_diagnosis"),
    SumMetricType("not_supported_score", "metric_type_firefox_not_supported"),
    SumMetricType("platform_score", "metric_type_platform_bug", contexts={"history"}),
    SumMetricType("total_score", None),
]


class MetricTable(ABC):
    type: Literal["table"] | Literal["view"]

    @abstractmethod
    def name(self, metric: Metric) -> str: ...

    @abstractmethod
    def template(self, metric: Metric) -> str: ...


class CurrentMetricTable(MetricTable):
    type = "view"

    def name(self, metric: Metric) -> str:
        return f"webcompat_topline_metric_{metric.name}"

    def template(self, metric: Metric) -> str:
        return f"""{{% set metric_name = "{metric.name}" %}}
SELECT
  date,
  {{% for metric_type in metric_types -%}}
    {{{{ metric_type.agg_function('bugs', metrics[metric_name], False) }}}} as {{{{ metric_type.name }}}}{{{{ ',' if not loop.last }}}}
  {{% endfor %}}
FROM
  UNNEST(GENERATE_DATE_ARRAY(DATE_TRUNC(DATE("2024-01-01"), week), DATE_TRUNC(CURRENT_DATE(), week), INTERVAL 1 week)) AS date
LEFT JOIN
  `{{{{ ref('scored_site_reports') }}}}` AS bugs
ON
  DATE(bugs.creation_time) <= date
  AND
IF
  (bugs.resolved_time IS NOT NULL, DATE(bugs.resolved_time) >= date, TRUE)
WHERE {{{{ metrics[metric_name].condition('bugs') }}}}
GROUP BY
  date
order by date
"""


class HistoryMetricTable(MetricTable):
    type = "table"

    def name(self, metric: Metric) -> str:
        return f"webcompat_topline_metric_{metric.name}_history"

    def template(self, metric: Metric) -> str:
        return """[recorded_date]
type = "DATE"
mode = "REQUIRED"

[date]
type = "DATE"
mode = "REQUIRED"

{% for metric_type in metric_types %}
[{{ metric_type.name }}]
type = "{{ metric_type.field_type }}"
mode = "REQUIRED"
{% endfor %}
"""


metric_tables = [CurrentMetricTable(), HistoryMetricTable()]


def load(root_path: os.PathLike) -> tuple[Sequence[Metric], Sequence[MetricType]]:
    metrics_root = os.path.join(root_path, "metrics")
    path = os.path.abspath(os.path.join(metrics_root, "metrics.toml"))
    metrics = []
    with open(path, "rb") as f:
        data = tomllib.load(f)

    for name, metric_data in MetricData.model_validate(data).root.items():
        metrics.append(metric_data.to_metric(name))

    return metrics, _metric_types
