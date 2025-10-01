from abc import ABC, abstractmethod
from typing import Optional


class Metric(ABC):
    conditional = True

    def __init__(self, name: str):
        self.name = name

    @abstractmethod
    def condition(self, table: str) -> str: ...


class UnconditionalMetric(Metric):
    conditional = False

    def condition(self, table: str) -> str:
        return "TRUE"


class SiteReportsFieldMetric(Metric):
    def condition(self, table: str) -> str:
        return f"{table}.is_{self.name}"


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


metrics = [
    UnconditionalMetric("all"),
    SiteReportsFieldMetric("sightline"),
    SiteReportsFieldMetric("japan_1000"),
    SiteReportsFieldMetric("japan_1000_mobile"),
    SiteReportsFieldMetric("global_1000"),
]


metric_types = [
    CountMetricType("bug_count", None),
    SumMetricType("needs_diagnosis_score", "metric_type_needs_diagnosis"),
    SumMetricType("not_supported_score", "metric_type_firefox_not_supported"),
    SumMetricType("platform_score", "metric_type_platform_bug", contexts={"history"}),
    SumMetricType("total_score", None),
]
