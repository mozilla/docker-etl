import pathlib

import pytest

from webcompat_kb.metrics.metrics import (
    CountMetricType,
    SiteReportsFieldMetric,
    SumMetricType,
    UnconditionalMetric,
)
from webcompat_kb.redashdata import (
    QueryMetadata,
    RedashQueryTemplate,
    RedashTemplateRenderer,
)
from webcompat_kb.commands.update_redash import ReferenceResolver, ParameterResolver


@pytest.fixture
def make_query():
    def _make(query_template, parameters_template=None, name="test_query"):
        return RedashQueryTemplate(
            path=pathlib.Path("/fake/path/test_query"),
            metadata=QueryMetadata(name=name),
            parameters_template=parameters_template,
            query_template=query_template,
        )

    return _make


def test_render_query_static_template(dashboard, make_query):
    """A query with no templating is returned as-is."""
    renderer = RedashTemplateRenderer([], [], ReferenceResolver("project"))

    query = make_query("SELECT 1")
    assert renderer.render_query(dashboard, query, lambda x: x) == "SELECT 1"


def test_render_query_ref_resolves_table_reference(dashboard, make_query):
    """ref() in template resolves to the value returned by the ref callable."""
    renderer = RedashTemplateRenderer([], [], ReferenceResolver("project"))
    query = make_query("SELECT * FROM `{{ ref('dataset.my_table') }}`")
    result = renderer.render_query(dashboard, query, lambda x: x)
    assert result == "SELECT * FROM `project.dataset.my_table`"


def test_render_query_param_resolves_parameter_reference(dashboard, make_query):
    """param() in template resolves to the value returned by the param callable."""
    renderer = RedashTemplateRenderer([], [], ReferenceResolver("project"))

    query = make_query("WHERE metric = {{ param('metric_filter') }}")
    result = renderer.render_query(
        dashboard, query, ParameterResolver({"metric_filter"})
    )
    assert result == "WHERE metric = {{ metric_filter }}"


def test_render_query_dashboard_metrics_empty_when_no_matching_metrics(
    dashboard, make_query
):
    """dashboard_metrics is empty when no metrics list the dashboard."""
    metric = UnconditionalMetric("all_bugs", dashboards=["other_dashboard"])
    renderer = RedashTemplateRenderer([metric], [], ReferenceResolver("project"))

    query = make_query("SELECT {{ dashboard_metrics | length }}")
    result = renderer.render_query(dashboard, query, lambda x: x)
    assert result == "SELECT 0"


def test_render_query_dashboard_metrics_filtered_by_dashboard_name(
    dashboard, make_query
):
    """dashboard_metrics only includes metrics that list the current dashboard."""
    m1 = UnconditionalMetric("all_bugs", dashboards=["test"])
    m2 = UnconditionalMetric("mobile_bugs", dashboards=["other_dashboard"])
    m3 = SiteReportsFieldMetric(
        "etp_blocked",
        host_min_ranks_condition=None,
        site_reports_conditions=None,
        dashboards=["test"],
    )
    renderer = RedashTemplateRenderer([m1, m2, m3], [], ReferenceResolver("project"))

    query = make_query("{% for m in dashboard_metrics %}{{ m.name }} {% endfor %}")
    result = renderer.render_query(dashboard, query, lambda x: x)
    assert "all_bugs" in result
    assert "etp_blocked" in result
    assert "mobile_bugs" not in result


def test_render_query_metrics_context_provides_dict_access_by_name(
    dashboard, make_query
):
    """metrics context allows accessing a specific metric by name."""
    metric = UnconditionalMetric(
        "all_bugs", pretty_name="All Bugs", dashboards=["test"]
    )
    renderer = RedashTemplateRenderer([metric], [], ReferenceResolver("project"))

    query = make_query("SELECT '{{ metrics['all_bugs'].pretty_name }}'")
    result = renderer.render_query(dashboard, query, lambda x: x)
    assert result == "SELECT 'All Bugs'"


def test_render_query_metric_types_context_iterates_all_types(dashboard, make_query):
    """metric_types context exposes all metric types for iteration."""
    metric_types = [
        CountMetricType("bug_count", None),
        SumMetricType("total_score", None),
    ]
    renderer = RedashTemplateRenderer([], metric_types, ReferenceResolver("project"))

    query = make_query(
        "{% for t in metric_types %}{{ t.name }}{% if not loop.last %},{% endif %}{% endfor %}"
    )
    result = renderer.render_query(dashboard, query, lambda x: x)
    assert result == "bug_count,total_score"


def test_render_query_metric_condition_method_callable_from_template(
    dashboard, make_query
):
    """metric.condition() can be called from a template."""
    metric = SiteReportsFieldMetric(
        "etp_blocked",
        host_min_ranks_condition=None,
        site_reports_conditions=None,
        dashboards=["test"],
    )
    renderer = RedashTemplateRenderer([metric], [], ReferenceResolver("project"))

    query = make_query("WHERE {{ metrics['etp_blocked'].condition('bugs') }}")
    result = renderer.render_query(dashboard, query, lambda x: x)
    assert result == "WHERE bugs.is_etp_blocked"


def test_render_query_count_metric_type_agg_function(dashboard, make_query):
    """CountMetricType.agg_function() can be called from a template."""
    metric = UnconditionalMetric("all_bugs", dashboards=["test"])
    renderer = RedashTemplateRenderer(
        [metric], [CountMetricType("bug_count", None)], ReferenceResolver("project")
    )

    query = make_query(
        "{{ metric_types[0].agg_function('bugs', metrics['all_bugs']) }}"
    )
    result = renderer.render_query(dashboard, query, lambda x: x)
    assert result == "COUNT(bugs.number)"


def test_render_query_sum_metric_type_agg_function_with_condition(
    dashboard, make_query
):
    """SumMetricType.agg_function() includes condition for conditional metrics."""
    metric = SiteReportsFieldMetric(
        "mobile",
        host_min_ranks_condition=None,
        site_reports_conditions=None,
        dashboards=["test"],
    )
    renderer = RedashTemplateRenderer(
        metric_dfns=[metric],
        metric_types=[SumMetricType("total_score", None)],
        ref=ReferenceResolver("project"),
    )

    query = make_query("{{ metric_types[0].agg_function('bugs', metrics['mobile']) }}")
    result = renderer.render_query(dashboard, query, lambda x: x)
    assert result == "SUM(IF(bugs.is_mobile, bugs.score, 0))"


def test_render_query_all_context_variables_combined(dashboard, make_query):
    """Template can reference dashboard_metrics, metrics, metric_types, ref, and param together."""
    metric = UnconditionalMetric("all_bugs", dashboards=["test"])
    renderer = RedashTemplateRenderer(
        metric_dfns=[metric],
        metric_types=[CountMetricType("bug_count", None)],
        ref=ReferenceResolver("project"),
    )

    query = make_query("""SELECT {{ metric_types[0].agg_function('b', metrics['all_bugs']) }}
FROM `{{ ref('dataset.bugs') }}`
WHERE {{ param('metric') }}
-- {{ dashboard_metrics | length }} metric(s)""")
    result = renderer.render_query(dashboard, query, ParameterResolver({"metric"}))
    assert "COUNT(b.number)" in result
    assert "`project.dataset.bugs`" in result
    assert "{{ metric }}" in result
    assert "1 metric(s)" in result


def test_render_parameters_both_templates_none_returns_none(dashboard, make_query):
    """Returns None when neither dashboard nor query has a parameters template."""
    renderer = RedashTemplateRenderer([], [], ReferenceResolver("project"))

    query = make_query("SELECT 1", parameters_template=None)
    assert renderer.render_parameters(dashboard, query) is None


def test_render_parameters_dashboard_template_only(dashboard, make_query):
    """Parameters from the dashboard-level template are returned."""
    renderer = RedashTemplateRenderer([], [], ReferenceResolver("project"))
    dashboard.parameters_template = '[my_param]\ntype = "text"\nvalue = "hello"\n'

    query = make_query("SELECT 1", parameters_template=None)
    result = renderer.render_parameters(dashboard, query)
    assert result is not None
    assert "my_param" in result
    assert result["my_param"].type == "text"
    assert result["my_param"].value == "hello"


def test_render_parameters_query_template_only(dashboard, make_query):
    """Parameters from the query-level template are returned."""
    renderer = RedashTemplateRenderer([], [], ReferenceResolver("project"))

    query = make_query(
        "SELECT 1",
        parameters_template='[my_param]\ntype = "text"\nvalue = "world"\n',
    )
    result = renderer.render_parameters(dashboard, query)
    assert result is not None
    assert "my_param" in result
    assert result["my_param"].value == "world"


def test_render_parameters_query_overrides_dashboard(dashboard, make_query):
    """When both templates define the same parameter, the query value wins."""
    renderer = RedashTemplateRenderer([], [], ReferenceResolver("project"))
    dashboard.parameters_template = (
        '[shared]\ntype = "text"\nvalue = "dashboard_value"\n'
    )
    query = make_query(
        "SELECT 1",
        parameters_template='[shared]\ntype = "text"\nvalue = "query_value"\n',
    )
    result = renderer.render_parameters(dashboard, query)
    assert result is not None
    assert result["shared"].value == "query_value"


def test_render_parameters_dashboard_and_query_merged(dashboard, make_query):
    """Dashboard and query templates each contribute their own parameters."""
    renderer = RedashTemplateRenderer([], [], ReferenceResolver("project"))
    dashboard.parameters_template = '[dashboard_param]\ntype = "text"\nvalue = "d"\n'
    query = make_query(
        "SELECT 1",
        parameters_template='[query_param]\ntype = "text"\nvalue = "q"\n',
    )
    result = renderer.render_parameters(dashboard, query)
    assert result is not None
    assert "dashboard_param" in result
    assert "query_param" in result
    assert result["dashboard_param"].value == "d"
    assert result["query_param"].value == "q"


def test_render_parameters_enum_parameter_parsed_correctly(dashboard, make_query):
    """An enum parameter is parsed with its options and value."""
    renderer = RedashTemplateRenderer([], [], ReferenceResolver("project"))

    query = make_query(
        "SELECT 1",
        parameters_template=(
            '[metric_filter]\ntype = "enum"\n'
            'enum_options = ["opt1", "opt2"]\nvalue = "opt1"\n'
        ),
    )
    result = renderer.render_parameters(dashboard, query)
    assert result is not None
    assert result["metric_filter"].type == "enum"
    assert result["metric_filter"].enum_options == ["opt1", "opt2"]
    assert result["metric_filter"].value == "opt1"


def test_render_parameters_dashboard_metrics_context_filters_by_dashboard(
    dashboard, make_query
):
    """dashboard_metrics in template contains only metrics for the current dashboard."""
    m1 = UnconditionalMetric("all_bugs", dashboards=["test"])
    m2 = UnconditionalMetric("mobile_bugs", dashboards=["other_dashboard"])
    renderer = RedashTemplateRenderer([m1, m2], [], ReferenceResolver("project"))

    query = make_query(
        "SELECT 1",
        """
[metric_filter]
type = "enum"
enum_options = [{% for m in dashboard_metrics %}"{{ m.name }}"{% if not loop.last %}, {% endif %}{% endfor %}]
value = "{{ dashboard_metrics[0].name }}" """,
    )
    result = renderer.render_parameters(dashboard, query)
    assert result is not None
    assert result["metric_filter"].enum_options == ["all_bugs"]
    assert result["metric_filter"].value == "all_bugs"


def test_render_parameters_metrics_context_provides_named_access(dashboard, make_query):
    """metrics context provides access to a specific metric by name."""
    metric = UnconditionalMetric(
        "all_bugs", pretty_name="All Bugs", dashboards=["test"]
    )
    renderer = RedashTemplateRenderer([metric], [], ReferenceResolver("project"))

    query = make_query(
        "SELECT 1",
        parameters_template='[my_param]\ntype = "text"\nvalue = "{{ metrics[\'all_bugs\'].pretty_name }}"\n',
    )
    result = renderer.render_parameters(dashboard, query)
    assert result is not None
    assert result["my_param"].value == "All Bugs"


def test_render_parameters_metric_types_context_provides_type_names(
    dashboard, make_query
):
    """metric_types context exposes all metric types for iteration."""
    types = [CountMetricType("bug_count", None), SumMetricType("total_score", None)]
    renderer = RedashTemplateRenderer([], types, ReferenceResolver("project"))

    query = make_query(
        "SELECT 1",
        """[type_filter]
type = "enum"
enum_options = [{% for t in metric_types %}"{{ t.name }}"{% if not loop.last %}, {% endif %}{% endfor %}]
value = "{{ metric_types[0].name }}" """,
    )
    result = renderer.render_parameters(dashboard, query)
    assert result is not None
    assert result["type_filter"].enum_options == ["bug_count", "total_score"]
    assert result["type_filter"].value == "bug_count"


def test_render_parameters_invalid_toml_raises_value_error(dashboard, make_query):
    """A template that renders to invalid TOML raises ValueError."""
    renderer = RedashTemplateRenderer([], [], ReferenceResolver("project"))

    query = make_query("SELECT 1", parameters_template="not valid toml ::::")
    with pytest.raises(ValueError, match="Failed to parse parameters"):
        renderer.render_parameters(dashboard, query)
