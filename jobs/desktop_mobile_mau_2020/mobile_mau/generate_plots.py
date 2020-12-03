import datetime
import os

import click
import jinja2
import pandas as pd
import plotly.graph_objects as go
from google.cloud import storage
from plotly.offline import plot

from .plot_config import *

GCS_PREFIX = "mobile-mau-2020"

query_nofire = """
WITH forecast_base AS (
  SELECT
    replace(datasource, " Global MAU", "") as datasource, 
    date, type, value as mau, low90, high90, 
    p10 as low80, 
    p90 as high80, 
    p20, p30, p40, p50, p60, p70, p80
  FROM
    `moz-fx-data-derived-datasets.telemetry.simpleprophet_forecasts`
  WHERE
    asofdate = (select max(asofdate) from `moz-fx-data-derived-datasets.telemetry.simpleprophet_forecasts`)
    and datasource in (
    "Fenix Global MAU", 
    "Fennec Android Global MAU", 
    "Fennec iOS Global MAU", 
    "Firefox Lite Global MAU", 
    "FirefoxConnect Global MAU", 
    "Focus Android Global MAU", 
    "Focus iOS Global MAU", 
    "Lockwise Android Global MAU")
),
mobile_base AS (
  SELECT
    *
  FROM
    `moz-fx-data-derived-datasets.telemetry.firefox_nondesktop_exact_mau28_by_dimensions_v1`
  WHERE
    product in ("Fenix", "Fennec Android", "Fennec iOS", "Firefox Lite", "FirefoxConnect", "Focus Android", "Focus iOS", "Lockwise Android")
  ),
  per_bucket AS (
  SELECT
    product AS datasource,
    'actual' AS type,
    submission_date,
    id_bucket,
    SUM(mau) AS mau
  FROM
    mobile_base
  GROUP BY
    product, 
    id_bucket,
    submission_date
), 
with_ci AS (
  SELECT
    datasource,
    type,
    submission_date,
    `moz-fx-data-derived-datasets.udf_js.jackknife_sum_ci`(20, array_agg(mau)) as mau
  FROM
    per_bucket
  GROUP BY
    datasource,
    type,
    submission_date
), 
with_forecast AS (
  SELECT
    datasource,
    type,
    submission_date AS `date`,
    mau.total AS mau,
    mau.low AS mau_low,
    mau.high AS mau_high
  FROM with_ci
  UNION ALL
  SELECT
    datasource,
    type,
    `date`,
    mau, 
    low90 AS mau_low,
    high90 AS mau_high
  FROM
    forecast_base
  WHERE
    type != 'original' 
  and date > (select max(submission_date) from with_ci)
)    

SELECT
  case when datasource = "Fennec iOS" then "Fx-iOS"
       else datasource end as datasource, 
  type, 
  `date`, 
  mau, mau_low, mau_high
FROM
  with_forecast
ORDER BY
  datasource,
  type,
  `date`
"""


def extract_mobile_product_mau(project):
    """
    Read query results from BigQuery and return as a pandas dataframe.
    """
    query = query_nofire
    df = pd.read_gbq(query, project_id=project, dialect="standard")
    df["date"] = pd.to_datetime(df["date"])
    df.columns = ["datasource", "type", "date", "value", "low", "high"]
    return df


def commafy(x):
    """Return comma-formatted number string."""
    return f"{int(x):,}"


def create_table(template, platform, actual, forecast):
    # end of year 12/15 YoY (last year vs current year)
    eoy_yoy = 100 * (
        forecast.query("date=='{}'".format(goal_date_2020[platform])).value.iloc[0]
        / actual.query("date=='{}'".format(goal_date_2019[platform])).value.iloc[0]
        - 1
    )

    data_end_date = actual["date"].max().date()

    # add current YoY growth
    current_yoy = (
        100
        * (
            actual.query("date=='{}'".format(data_end_date)).value.iloc[0]
            - actual.query(
                "date=='{}'".format(data_end_date - datetime.timedelta(days=365))
            ).value.iloc[0]
        )
        / float(
            actual.query(
                "date=='{}'".format(data_end_date - datetime.timedelta(days=365))
            ).value.iloc[0]
        )
    )

    return template.render(
        platform=platform,
        metric=metric_names[platform],
        current=commafy(actual.query("date==@data_end_date").value.iloc[0]),
        current_pm=commafy(
            actual.query("date==@data_end_date").value.iloc[0]
            - actual.query("date==@data_end_date").low.iloc[0]
        ),
        # current_yoy=current_yoy,
        actual_2019=commafy(
            actual.query("date=='{}'".format(goal_date_2019[platform])).value.iloc[0]
        ),
        forecast=commafy(
            forecast.query("date=='{}'".format(goal_date_2020[platform])).value.iloc[0]
        ),
        forecast_pm=commafy(
            forecast.query("date=='{}'".format(goal_date_2020[platform])).value.iloc[0]
            - forecast.query("date=='{}'".format(goal_date_2020[platform])).low.iloc[0]
        ),
        forecast_color=("green-text" if (eoy_yoy >= 0) else "red-text"),
        eoy_yoy=f"{eoy_yoy:.2f}",
    )


def create_plot(platform, y_min, y_max, actuals, forecast, slice_name):
    """
    Display a plot given a platform (each mobile product), data for actuals and forecast, and slice (Global or Tier 1).
    """
    main_metric_color = "#CA3524"
    main_metric_color_ci = "#DDDDDD"
    main_palette = [main_metric_color, main_metric_color_ci]

    ci_fillcolor = "rgba(68, 68, 68, 0.2)"
    ci_markercolor = "#444"

    data_end_date = actuals["date"].max().date()

    plotly_data = [
        go.Scatter(
            name="Actuals Lower Bound",
            x=actuals.date,
            y=actuals["low"],
            showlegend=False,
            line={"color": "rgba(0,0,255, 0.8)", "width": 0},
            mode="lines",
            marker=dict(color=ci_markercolor),
            hoverlabel={"namelength": -1},
        ),
        go.Scatter(
            name="Actuals Upper Bound",
            x=actuals.date,
            y=actuals["high"],
            showlegend=False,
            line={"color": "rgba(0,0,255, 0.8)", "width": 0},
            mode="lines",
            marker=dict(color=ci_markercolor),
            hoverlabel={"namelength": -1},
            fillcolor=ci_fillcolor,
            fill="tonexty",
        ),
        go.Scatter(
            name="Actuals",
            x=actuals.date,
            y=actuals["value"],
            showlegend=True,
            line={"color": "rgba(0,0,255, 0.8)"},
            hoverlabel={"namelength": -1},
        ),
        go.Scatter(
            name="Credible Interval",
            x=[0],
            y=[0],
            showlegend=True,
            line={"color": ci_fillcolor, "width": 10},
            mode="lines",
            hoverlabel={"namelength": -1},
        ),
        go.Scatter(
            name="Previous Year Actuals",
            x=actuals.date + pd.Timedelta("365 day"),
            y=actuals["value"],
            showlegend=True,
            line={"color": "rgba(68,120,68,0.5)", "dash": "dashdot"},
            hoverlabel={"namelength": -1},
        ),
    ]

    if forecast is not None:
        plotly_data.extend(
            [
                go.Scatter(
                    name="Forecast Lower Bound",
                    x=forecast.date,
                    y=forecast["low"],
                    showlegend=False,
                    line={"color": "rgba(0,0,255, 0.8)", "width": 0},
                    mode="lines",
                    marker=dict(color=ci_markercolor),
                    hoverlabel={"namelength": -1},
                ),
                go.Scatter(
                    name="Forecast Upper Bound",
                    x=forecast.date,
                    y=forecast["high"],
                    showlegend=False,
                    line={"color": "rgba(0,0,255, 0.8)", "width": 0},
                    mode="lines",
                    marker=dict(color=ci_markercolor),
                    hoverlabel={"namelength": -1},
                    fillcolor=ci_fillcolor,
                    fill="tonexty",
                ),
                go.Scatter(
                    name="Forecast",
                    x=forecast.date,
                    y=forecast["value"],
                    showlegend=True,
                    line={"color": main_palette[0], "dash": "dot"},
                    hoverlabel={"namelength": -1},
                ),
            ]
        )

    layout = go.Layout(
        # autosize=force_width is None,
        # width=force_width,
        # height=force_height,
        title='<b>{} {} {}</b> <span style="font-size: medium;">at end of day {}</span>'.format(
            slice_name,
            platform,
            metric_names[platform],
            data_end_date,
        ),
        titlefont={
            "size": 24,
        },
        xaxis=dict(
            title="<b>Date</b>",
            titlefont=dict(family="Courier New, monospace", size=18, color="#7f7f7f"),
            range=[plot_start_date[platform], plot_end_date[platform]],
            tickmode="linear",
            tick0=[tick_start[platform]],
            dtick="M1",
            tickfont=dict(color="grey"),
        ),
        yaxis=dict(
            title="<b>{}</b>".format(metric_names[platform]),
            titlefont=dict(family="Courier New, monospace", size=18, color="#7f7f7f"),
            hoverformat=",.0f",
            range=([y_min, y_max] if y_min is not None else None),
            tickfont=dict(color="grey"),
        ),
        legend=dict(
            x=0.5,
            y=1.0,
            traceorder="normal",
            font=dict(family="sans-serif", size=12, color="#000"),
            bgcolor="#FEFEFE",
            bordercolor="#A0A0A0",
            borderwidth=2,
            orientation="h",
        ),
    )
    return plot(
        {"data": plotly_data, "layout": layout},
        output_type="div",
        include_plotlyjs=False,
    )


def create_table_and_plot(
    product,
    mobile_product_mau_data,
    y_min,
    y_max,
    table_template,
    plot_forecast=True,
):
    actual = mobile_product_mau_data[
        (mobile_product_mau_data.datasource == product)
        & (mobile_product_mau_data.type == "actual")
    ]
    forecast = mobile_product_mau_data[
        (mobile_product_mau_data.datasource == product)
        & (mobile_product_mau_data.type == "forecast")
    ]

    table = create_table(
        table_template,
        product,
        actual,
        forecast,
    )
    plot = create_plot(
        product,
        y_min,
        y_max,
        actual,
        forecast if plot_forecast else None,
        slice_name="Global",
    )

    return table, plot


@click.command()
@click.option("--project", help="GCP project id", required=True)
@click.option("--bucket-name", help="GCP bucket name", required=True)
def main(project, bucket_name):
    work_dir = os.path.dirname(__file__)
    static_dir = os.path.join(work_dir, "static")

    template_loader = jinja2.FileSystemLoader(os.path.join(work_dir, "templates"))
    template_env = jinja2.Environment(loader=template_loader)

    main_template = template_env.get_template("main.template.html")
    table_template = template_env.get_template("table.template.html")

    mobile_product_mau_data = extract_mobile_product_mau(project)

    fenix_table, fenix_plot = create_table_and_plot(
        "Fenix",
        mobile_product_mau_data,
        y_min=0,
        y_max=40000000,
        plot_forecast=False,
        table_template=table_template,
    )
    fennec_table, fennec_plot = create_table_and_plot(
        "Fennec Android",
        mobile_product_mau_data,
        y_min=18000000,
        y_max=38000000,
        plot_forecast=False,
        table_template=table_template,
    )
    fx_ios_table, fx_ios_plot = create_table_and_plot(
        "Fx-iOS",
        mobile_product_mau_data,
        y_min=4000000,
        y_max=9000000,
        table_template=table_template,
    )
    firefox_lite_table, firefox_lite_plot = create_table_and_plot(
        "Firefox Lite",
        mobile_product_mau_data,
        y_min=0,
        y_max=2000000,
        table_template=table_template,
    )
    firefox_connect_table, firefox_connect_plot = create_table_and_plot(
        "FirefoxConnect",
        mobile_product_mau_data,
        y_min=0,
        y_max=900000,
        table_template=table_template,
    )
    focus_android_table, focus_android_plot = create_table_and_plot(
        "Focus Android",
        mobile_product_mau_data,
        y_min=1100000,
        y_max=3000000,
        table_template=table_template,
    )
    focus_ios_table, focus_ios_plot = create_table_and_plot(
        "Focus iOS",
        mobile_product_mau_data,
        y_min=250000,
        y_max=900000,
        table_template=table_template,
    )
    lockwise_android_table, lockwise_android_plot = create_table_and_plot(
        "Lockwise Android",
        mobile_product_mau_data,
        y_min=0,
        y_max=300000,
        table_template=table_template,
    )

    output_html = main_template.render(
        fenix_table=fenix_table,
        fenix_plot=fenix_plot,
        fennec_table=fennec_table,
        fennec_plot=fennec_plot,
        fx_ios_table=fx_ios_table,
        fx_ios_plot=fx_ios_plot,
        firefox_lite_table=firefox_lite_table,
        firefox_lite_plot=firefox_lite_plot,
        firefox_connect_table=firefox_connect_table,
        firefox_connect_plot=firefox_connect_plot,
        focus_android_table=focus_android_table,
        focus_android_plot=focus_android_plot,
        focus_ios_table=focus_ios_table,
        focus_ios_plot=focus_ios_plot,
        lockwise_android_table=lockwise_android_table,
        lockwise_android_plot=lockwise_android_plot,
    )

    with open(os.path.join(static_dir, "index.html"), "w") as f:
        f.write(output_html)

    storage_client = storage.Client(project=project)

    bucket = storage_client.bucket(bucket_name=bucket_name)
    for filename in os.listdir(static_dir):
        if os.path.isfile(os.path.join(static_dir, filename)):
            blob = bucket.blob(os.path.join(GCS_PREFIX, filename))
            blob.upload_from_filename(os.path.join(static_dir, filename))


if __name__ == "__main__":
    main()
