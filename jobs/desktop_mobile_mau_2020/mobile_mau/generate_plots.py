from pathlib import Path

import click
import jinja2
import pandas as pd
import plotly.graph_objects as go
from google.cloud import storage
from plotly.offline import plot

from .plot_config import *

GCS_PREFIX = "mobile-mau-2020"

forecast_and_actuals_query = (
    Path(__file__).parent / "forecast_and_actual.sql"
).read_text()


def extract_mobile_product_mau(project):
    """
    Read query results from BigQuery and return as a pandas dataframe.
    """
    df = pd.read_gbq(forecast_and_actuals_query, project_id=project, dialect="standard")
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

    return template.render(
        platform=platform,
        metric="MAU",
        current=commafy(actual.query("date==@data_end_date").value.iloc[0]),
        current_pm=commafy(
            actual.query("date==@data_end_date").value.iloc[0]
            - actual.query("date==@data_end_date").low.iloc[0]
        ),
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
        title='<b>{} {} MAU</b> <span style="font-size: medium;">at end of day {}</span>'.format(
            slice_name,
            platform,
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
            title="<b>MAU</b>",
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
    print(f"Generating table and plot for {product}")
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
@click.option("--bucket-name", help="GCP bucket name")
def main(project, bucket_name):
    work_dir = Path(__file__).parent
    static_dir = work_dir / "static"

    template_loader = jinja2.FileSystemLoader(work_dir / "templates")
    template_env = jinja2.Environment(loader=template_loader)

    main_template = template_env.get_template("main.template.html")
    table_template = template_env.get_template("table.template.html")

    mobile_product_mau_data = extract_mobile_product_mau(project)

    # tuples of (product_name, y_min, y_max, plot_forecast)
    products = [
        ("Fenix", 0, 40000000, False),
        ("Fennec", 18000000, 38000000, False),
        ("Firefox iOS", 4000000, 9000000, True),
        ("Firefox Lite", 0, 2000000, True),
        ("Firefox Echo", 0, 900000, True),
        ("Focus Android", 1100000, 3000000, True),
        ("Focus iOS", 250000, 900000, True),
        ("Lockwise Android", 0, 300000, True),
    ]

    tables_and_plots = {}

    for product_name, y_min, y_max, plot_forecast in products:
        product_table, product_plot = create_table_and_plot(
            product_name,
            mobile_product_mau_data,
            y_min,
            y_max,
            table_template=table_template,
            plot_forecast=plot_forecast,
        )
        formatted_product_name = product_name.replace(" ", "_").lower()
        tables_and_plots[f"{formatted_product_name}_table"] = product_table
        tables_and_plots[f"{formatted_product_name}_plot"] = product_plot

    output_html = main_template.render(**tables_and_plots)

    (static_dir / "index.html").write_text(output_html)

    if bucket_name is not None:
        storage_client = storage.Client(project=project)

        bucket = storage_client.bucket(bucket_name=bucket_name)
        for filename in static_dir.glob("*"):
            if (static_dir / filename).is_file():
                blob = bucket.blob(str(Path(GCS_PREFIX) / filename))
                blob.upload_from_filename(str(static_dir / filename))


if __name__ == "__main__":
    main()
