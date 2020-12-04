import glob
import os
from datetime import timedelta

import click
import matplotlib
import numpy as np
import pandas as pd
from google.cloud import bigquery, bigquery_storage_v1beta1, storage
from matplotlib import rcParams
from matplotlib import dates as mdates
from matplotlib import pyplot as plt

pd.options.mode.chained_assignment = None
rcParams.update(
    {
        "figure.autolayout": True,
        "figure.dpi": 100,
    }
)

GCS_PREFIX = "desktop-mau-dau-tier1-2020"

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
IMG_DIR = os.path.join(STATIC_DIR, "img")

DESKTOP_QUERY = """
WITH mau_dau AS (
  SELECT
    submission_date AS date,
    country,
    SUM(mau) AS MAU,
    SUM(dau) AS DAU,
  FROM
    `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_by_dimensions`
  WHERE
    country IN ("US", "CA", "DE", "FR", "GB", "CN", "IN", "BR", "ID", "RU", "PL")
    AND submission_date >= "2016-12-01"
  GROUP BY
    date,
    country
  UNION ALL
  SELECT
    submission_date AS date,
    "Global" AS country,
    SUM(mau) AS MAU,
    SUM(dau) AS DAU
  FROM
    `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_by_dimensions`
  WHERE
    submission_date >= "2016-12-01"
  GROUP BY
    date,
    country
  UNION ALL
  SELECT
    submission_date AS date,
    CASE
      WHEN country IN ("US", "CA", "DE", "FR", "GB") THEN "Tier1"
    ELSE
    "RoW"
  END
    AS country,
    SUM(mau) AS MAU,
    SUM(dau) AS DAU
  FROM
    `moz-fx-data-shared-prod.telemetry.firefox_desktop_exact_mau28_by_dimensions`
  WHERE
    submission_date >= "2016-12-01"
  GROUP BY
    date,
    country
)

SELECT
  *
FROM
  mau_dau
ORDER BY
  date,
  country
"""

DESKTOP_USER_STATE_QUERY = """
SELECT 
    *
FROM 
    `moz-fx-data-derived-datasets.analysis.xluo_kpi_plot_country_new_or_rescurrected_dau` 
"""


def plot_year_over_year(full_dat, country):
    """Save a plot for MAU and DAU (7d MA) at country level."""
    dat = full_dat.loc[full_dat.country == country]

    plt.style.use("seaborn-white")
    fig, axs = plt.subplots(nrows=1, ncols=4, figsize=(20, 4), sharex="col")

    metric_list = ["MAU", "mau_pcnt_Jan01", "DAU_MA7d", "dau_pcnt_Jan01"]
    for num, metric in enumerate(metric_list):
        axs[num].set(ylim=(dat[metric].min() * 0.95, dat[metric].max() * 1.05))
        for year in range(2017, 2021):
            axs[num].plot(
                dat.loc[dat["year"] == year, "fakedate"],
                dat.loc[dat["year"] == year, metric],
                label=str(year),
                linestyle="solid",
            )
            axs[num].xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
            if num % 2 == 0:
                axs[num].yaxis.set_major_formatter(
                    matplotlib.ticker.FuncFormatter(
                        lambda x, pos: "{:,.0f}".format(x / 1000000) + "MM"
                    )
                )
            else:
                axs[num].yaxis.set_major_formatter(
                    matplotlib.ticker.FuncFormatter(
                        lambda x, pos: "{:,.0f}".format(x * 100) + "%"
                    )
                )
            axs[num].legend(
                ("2017", "2018", "2019", "2020"),
                title=country + " " + metric,
                bbox_to_anchor=(0.4, 0.8, 0.6, 0.2),
                loc="upper left",
                ncol=4,
                mode="expand",
                fontsize=9,
            )

    fig.tight_layout()
    axs[0].set_title(
        "Year over Year MAU & DAU(7d MA) in {}".format(country), loc="left", fontsize=18
    )
    filename = f"desktop_{country}_mau_dau.jpeg"
    plt.savefig(os.path.join(IMG_DIR, filename))
    plt.close(fig)
    return filename


def plot_dau_mau_contribution_individual_country(full_dat, country):
    """

    Save a plot with subplots showing the contribution of DAU/MAU w.r.t. global

    """
    new_dat = (
        pd.merge(
            full_dat[full_dat["country"] == country],
            full_dat[full_dat["country"] == "Global"],
            on=["date"],
        )
        .rename(
            columns={
                "MAU_x": "MAU",
                "DAU_x": "DAU",
                "MAU_y": "MAU_global",
                "DAU_y": "DAU_global",
            }
        )
        .sort_values(by="date")
    )

    new_dat["DAU_global_7dMA"] = new_dat["DAU_global"].rolling(window=7).mean()
    new_dat["DAU_7dMA"] = new_dat["DAU"].rolling(window=7).mean()
    new_dat["pcnt_MAU"] = new_dat["MAU"] / new_dat["MAU_global"]
    new_dat["pcnt_DAU"] = new_dat["DAU_7dMA"] / new_dat["DAU_global_7dMA"]

    plt.style.use("seaborn-white")
    fig = plt.figure(figsize=(12, 6))
    ax = plt.axes(label=f"desktop_{country}_mau_dau_ratio")
    ax.set(xlim=(pd.to_datetime("20190101"), new_dat["date"].max()))
    ax.plot(
        new_dat["date"],
        new_dat["pcnt_MAU"],
        color="#0000ff",
        linestyle="solid",
        label="MAU",
    )
    ax.plot(
        new_dat["date"],
        new_dat["pcnt_DAU"],
        color="gray",
        linestyle="dashdot",
        label="DAU_7dMA",
    )
    plt.title(
        label="Contribution of MAU/DAU from {} as of {}".format(
            country, new_dat["date"].max().strftime("%Y-%m-%d")
        ),
        loc="left",
        fontdict={"fontsize": 20, "color": "black"},
    )
    plt.xlabel("Date", fontsize=16)
    ax.xaxis.set_major_locator(matplotlib.dates.YearLocator())
    ax.xaxis.set_minor_locator(
        matplotlib.dates.MonthLocator((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))
    )
    ax.xaxis.set_major_formatter(matplotlib.dates.DateFormatter("\n%Y"))
    ax.xaxis.set_minor_formatter(matplotlib.dates.DateFormatter("%b %d"))
    ax.yaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, pos: "{:,.0f}".format(x * 100) + "%")
    )
    plt.setp(ax.get_xticklabels(), rotation=0, ha="center", fontsize=14)
    plt.setp(ax.get_yticklabels(), rotation=0, ha="right", fontsize=12)
    ax.legend(
        bbox_to_anchor=(0.9, 0.9),
        loc=3,
        ncol=2,
        mode="expand",
        borderaxespad=0.0,
        fontsize=10,
    )
    ax.grid(which="major", linestyle="-", linewidth="0.5", color="lightgray")

    filename = f"desktop_{country}_mau_dau_ratio.jpeg"
    plt.savefig(os.path.join(IMG_DIR, filename))
    plt.close(fig)
    return filename


def plot_group_contribution(full_data, country_list, cat, metric):
    full_data = full_data[["date", "country", metric]]
    if metric == "MAU":
        dat = pd.merge(
            full_data[full_data["country"].isin(country_list)],
            full_data[full_data["country"] == "Global"],
            on=["date"],
        ).sort_values(by=["country_x", "date"])
        dat["pcnt"] = dat["MAU_x"] / dat["MAU_y"]

    if metric == "DAU":
        dat = pd.merge(
            full_data[full_data["country"].isin(country_list)],
            full_data[full_data["country"] == "Global"],
            on=["date"],
        ).sort_values(by=["country_x", "date"])
        dat["DAU_global_7dMA"] = dat["DAU_y"].rolling(window=7).mean()
        dat["DAU_7dMA"] = dat["DAU_x"].rolling(window=7).mean()
        dat["pcnt"] = dat["DAU_7dMA"] / dat["DAU_global_7dMA"]

    plt.style.use("seaborn-white")
    fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(20, 5), sharex="col")
    dat = dat[dat["date"] >= pd.to_datetime("20190101")]
    # individual
    for country in country_list:
        axs[0].plot(
            dat[dat["country_x"] == country]["date"],
            dat[dat["country_x"] == country]["pcnt"],
            label=str(country),
            linestyle="solid",
        )
    axs[0].set(ylim=(0, 0.2))
    axs[0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%b"))
    axs[0].yaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, pos: "{:,.0f}".format(x * 100) + "%")
    )
    axs[0].legend(
        country_list,
        bbox_to_anchor=(0.9, 0.8, 0.2, 0.2),
        loc="upper left",
        ncol=1,
        mode="expand",
        fontsize=10,
    )

    # stacked
    dat2 = dat[dat["country_x"].isin(country_list)][
        ["date", "country_x", "pcnt"]
    ].pivot(index="date", columns="country_x", values="pcnt")
    dat2 = pd.DataFrame(data=dat2)
    y = np.vstack([dat2[x] for x in country_list])
    axs[1].stackplot(dat2.index, y, labels=country_list, alpha=0.5)
    # axs[1].set(ylim=(0, 0.4))
    axs[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%b"))
    axs[1].yaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, pos: "{:,.0f}".format(x * 100) + "%")
    )
    axs[1].legend(
        country_list,
        bbox_to_anchor=(0.9, 0.8, 0.2, 0.2),
        loc="upper left",
        ncol=1,
        mode="expand",
        fontsize=10,
    )
    axs[1].grid(which="major", linestyle="-", linewidth="0.5", color="lightgray")

    axs[0].set_title(
        "Contribution of {} from Tier1 as of {} (non-stacked/stacked)".format(
            metric, dat["date"].max().strftime("%Y-%m-%d")
        ),
        loc="left",
        fontdict={"fontsize": 18, "color": "black"},
    )

    filename = f"desktop_{metric}_{cat}_contribution.jpeg"
    plt.savefig(os.path.join(IMG_DIR, filename))
    plt.close(fig)
    return filename


def plot_dau_user_state_individual_country(full_dat, country):
    """

    Save a plot with subplots showing the contribution of each user state to DAU
    Data is for this specific country only

    """
    full_dat["dau"] = (
        full_dat["dau_regular"]
        + full_dat["dau_new_or_resurrected"]
        + full_dat["dau_other"]
    )
    dat = (
        full_dat[["date", "dau", "dau_regular", "dau_new_or_resurrected", "dau_other"]]
        .melt(id_vars=["date", "dau"])
        .rename(columns={"variable": "user_state"})
        .sort_values("date")
    )

    dat["user_state"] = dat["user_state"].str.replace("dau_", "")
    dat["pcnt"] = dat["value"] / dat["dau"]

    plt.style.use("seaborn-white")
    fig, axs = plt.subplots(nrows=1, ncols=4, figsize=(18, 4), sharex="col")
    dat = dat[dat["date"] >= dat["date"].max() + timedelta(-182)]
    cat_list = ["new_or_resurrected", "regular", "other"]

    # individual
    for cat in cat_list:
        axs[0].plot(
            dat[dat["user_state"] == cat]["date"],
            dat[dat["user_state"] == cat]["pcnt"],
            label=str(cat),
            linestyle="solid",
        )
    axs[0].set(ylim=(0, 1))
    axs[0].xaxis.set_major_formatter(mdates.DateFormatter("%y-%b"))
    axs[0].yaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, pos: "{:,.0f}".format(x * 100) + "%")
    )
    axs[0].legend(
        cat_list,
        bbox_to_anchor=(0.5, 0.8, 0.2, 0.2),
        loc="upper left",
        ncol=1,
        mode="expand",
        fontsize=10,
    )

    # stacked
    dat2 = dat[dat["user_state"].isin(cat_list)][["date", "user_state", "pcnt"]].pivot(
        index="date", columns="user_state", values="pcnt"
    )
    dat2 = pd.DataFrame(data=dat2)
    y = np.vstack([dat2[x] for x in cat_list])
    axs[1].stackplot(dat2.index, y, labels=cat_list, alpha=0.5)
    # axs[1].set(ylim=(0, 0.4))
    axs[1].xaxis.set_major_formatter(mdates.DateFormatter("%y-%b"))
    axs[1].yaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, pos: "{:,.0f}".format(x * 100) + "%")
    )

    # New only, as it"s small
    cat = "new_or_resurrected"
    axs[2].plot(
        dat[dat["user_state"] == cat]["date"],
        dat[dat["user_state"] == cat]["pcnt"],
        label=str(cat),
        linestyle="solid",
    )
    axs[2].xaxis.set_major_formatter(mdates.DateFormatter("%y-%b"))
    axs[2].yaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, pos: "{:,.0f}".format(x * 100) + "%")
    )
    axs[2].legend(
        bbox_to_anchor=(0.5, 0.8, 0.2, 0.2),
        loc="upper left",
        ncol=1,
        mode="expand",
        fontsize=10,
    )

    # Regular only
    cat = "regular"
    axs[3].plot(
        dat[dat["user_state"] == cat]["date"],
        dat[dat["user_state"] == cat]["pcnt"],
        label=str(cat),
        linestyle="solid",
        color="orange",
    )
    axs[3].xaxis.set_major_formatter(mdates.DateFormatter("%y-%b"))
    axs[3].yaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, pos: "{:,.0f}".format(x * 100) + "%")
    )
    axs[3].legend(
        bbox_to_anchor=(0.6, 0.1, 0.2, 0.2),
        loc="upper left",
        ncol=1,
        mode="expand",
        fontsize=10,
    )

    axs[0].set_title(
        "DAU per User State (individual/stacked) from {}".format(country),
        loc="left",
        fontdict={"fontsize": 18, "color": "black"},
    )

    filename = f"desktop_{country}_user_state_dau_contribution.jpeg"
    plt.savefig(os.path.join(IMG_DIR, filename))
    plt.close(fig)
    return filename


def plot_dau_mau_ratio(desktop_data):
    ind_dat = pd.merge(
        desktop_data, desktop_data[desktop_data["country"] == "Global"], on=["date"]
    ).sort_values(by=["country_x", "date"])
    ind_dat["DAU_global_7dMA"] = ind_dat["DAU_y"].rolling(window=7).mean()
    ind_dat["DAU_7dMA"] = ind_dat["DAU_x"].rolling(window=7).mean()
    ind_dat["pcnt_MAU"] = ind_dat["MAU_x"] / ind_dat["MAU_y"]
    ind_dat["pcnt_DAU"] = ind_dat["DAU_7dMA"] / ind_dat["DAU_global_7dMA"]
    ind_dat = ind_dat[ind_dat["date"] >= pd.to_datetime("20190101")]

    plt.style.use("seaborn-white")
    fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(20, 5), sharex="col")

    country_list = ["US", "CA", "DE", "FR", "GB"]
    metric_list = ["MAU", "DAU"]
    for i, metric in enumerate(metric_list):
        for country in country_list:
            axs[i].plot(
                ind_dat[ind_dat["country_x"] == country]["date"],
                ind_dat[ind_dat["country_x"] == country]["pcnt_" + metric],
                label=str(metric),
                linestyle="solid",
            )
        axs[i].set(ylim=(0, 0.20))
        axs[i].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%b"))
        axs[i].yaxis.set_major_formatter(
            matplotlib.ticker.FuncFormatter(
                lambda x, pos: "{:,.0f}".format(x * 100) + "%"
            )
        )
        axs[i].legend(
            country_list,
            bbox_to_anchor=(0.9, 0.8, 0.2, 0.2),
            loc="upper left",
            ncol=1,
            mode="expand",
            fontsize=10,
        )
        axs[i].grid(which="major", linestyle="-", linewidth="0.5", color="lightgray")

    axs[0].set_title(
        "Contribution of MAU/DAU per Tier1 country as of "
        + ind_dat["date"].max().strftime("%Y-%m-%d"),
        loc="left",
        fontdict={"fontsize": 18, "color": "black"},
    )
    filename_1 = "desktop_mau_dau_ratio.jpeg"
    plt.savefig(os.path.join(IMG_DIR, filename_1))
    plt.close(fig)

    ind_dat = pd.merge(
        desktop_data[desktop_data["country"].isin(["US", "CA", "DE", "FR", "GB"])],
        desktop_data[desktop_data["country"] == "Global"],
        on=["date"],
    )
    ind_dat["pcnt_MAU"] = ind_dat["MAU_x"] / ind_dat["MAU_y"]
    ind_dat = ind_dat[ind_dat["date"] >= pd.to_datetime("20190101")]

    plt.style.use("seaborn-white")
    fig, axs = plt.subplots(nrows=1, ncols=2, figsize=(20, 5), sharex="col")

    metric = "MAU"
    country_list = ["US", "CA", "DE", "FR", "GB"]
    for country in country_list:
        axs[0].plot(
            ind_dat[ind_dat["country_x"] == country]["date"],
            ind_dat[ind_dat["country_x"] == country]["pcnt_" + metric],
            label=str(metric),
            linestyle="solid",
        )
    axs[0].set(ylim=(0, 0.20))
    axs[0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%b"))
    axs[0].yaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, pos: "{:,.0f}".format(x * 100) + "%")
    )
    axs[0].legend(
        country_list,
        bbox_to_anchor=(0.9, 0.8, 0.2, 0.2),
        loc="upper left",
        ncol=1,
        mode="expand",
        fontsize=10,
    )
    axs[0].grid(which="major", linestyle="-", linewidth="0.5", color="lightgray")

    axs[0].set_title(
        "Contribution of MAU/DAU per Tier1 country as of "
        + ind_dat["date"].max().strftime("%Y-%m-%d"),
        loc="left",
        fontdict={"fontsize": 18, "color": "black"},
    )

    # axs[1].plot.area([ind_dat["country_x"].isin(country_list)]["date", "country_x", "pcnt"], subplots=True)
    dat2 = ind_dat[ind_dat["country_x"].isin(country_list)][
        ["date", "country_x", "pcnt_MAU"]
    ].pivot(index="date", columns="country_x", values="pcnt_MAU")
    dat2 = pd.DataFrame(data=dat2)
    y = np.vstack([dat2[x] for x in country_list])
    axs[1].stackplot(dat2.index, y, labels=country_list, alpha=0.5)
    # axs[1].set(ylim=(0, 0.4))
    axs[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%b"))
    axs[1].yaxis.set_major_formatter(
        matplotlib.ticker.FuncFormatter(lambda x, pos: "{:,.0f}".format(x * 100) + "%")
    )
    axs[1].legend(
        country_list,
        bbox_to_anchor=(0.9, 0.8, 0.2, 0.2),
        loc="upper left",
        ncol=1,
        mode="expand",
        fontsize=10,
    )
    axs[1].grid(which="major", linestyle="-", linewidth="0.5", color="lightgray")
    filename_2 = "desktop_mau_dau_ratio_2.jpeg"
    plt.savefig(os.path.join(IMG_DIR, filename_2))
    plt.close(fig)

    return filename_1, filename_2


@click.command()
@click.option("--project", help="GCP project id", required=True)
@click.option("--bucket-name", help="GCP bucket name")
def main(project, bucket_name):
    bq_client = bigquery.Client(project=project)
    bq_storage_client = bigquery_storage_v1beta1.BigQueryStorageClient()

    try:
        os.mkdir(IMG_DIR)
    except FileExistsError:
        pass

    desktop_data = (
        bq_client.query(DESKTOP_QUERY)
        .result()
        .to_dataframe(bqstorage_client=bq_storage_client)
    )
    desktop_user_state_data = (
        bq_client.query(DESKTOP_USER_STATE_QUERY)
        .result()
        .to_dataframe(bqstorage_client=bq_storage_client)
    )

    desktop_data = pd.merge(
        desktop_data, desktop_user_state_data, on=["date", "country"]
    )
    desktop_data["year"] = pd.DatetimeIndex(desktop_data["date"]).year
    desktop_data["doy"] = pd.DatetimeIndex(desktop_data["date"]).dayofyear
    desktop_data["fakedate"] = [
        pd.to_datetime("20170101") + timedelta(days=x) for x in desktop_data["doy"]
    ]
    desktop_data["DAU_MA7d"] = desktop_data.groupby("country")["DAU"].transform(
        lambda x: x.rolling(window=7).mean()
    )
    desktop_data["MAU_base"] = desktop_data.groupby(["country", "year"]).MAU.transform(
        "first"
    )
    desktop_data["DAU_MA7d_base"] = desktop_data.groupby(
        ["country", "year"]
    ).DAU_MA7d.transform("first")
    desktop_data["dau_pcnt_Jan01"] = (
        desktop_data["DAU_MA7d"] / desktop_data["DAU_MA7d_base"]
    )
    desktop_data["mau_pcnt_Jan01"] = desktop_data["MAU"] / desktop_data["MAU_base"]

    # desktop_US_mau_dau.jpeg
    plot_year_over_year(desktop_data, "US")
    # desktop_CA_mau_dau.jpeg
    plot_year_over_year(desktop_data, "CA")
    # desktop_DE_mau_dau.jpeg
    plot_year_over_year(desktop_data, "DE")
    # desktop_FR_mau_dau.jpeg
    plot_year_over_year(desktop_data, "FR")
    # desktop_GB_mau_dau.jpeg
    plot_year_over_year(desktop_data, "GB")
    # desktop_Global_mau_dau.jpeg
    plot_year_over_year(desktop_data, "Global")
    # desktop_Tier1_mau_dau.jpeg
    plot_year_over_year(desktop_data, "Tier1")
    # desktop_RoW_mau_dau.jpeg
    plot_year_over_year(desktop_data, "RoW")
    # desktop_CN_mau_dau.jpeg
    plot_year_over_year(desktop_data, "CN")
    # desktop_IN_mau_dau.jpeg
    plot_year_over_year(desktop_data, "IN")
    # desktop_BR_mau_dau.jpeg
    plot_year_over_year(desktop_data, "BR")
    # desktop_ID_mau_dau.jpeg
    plot_year_over_year(desktop_data, "ID")
    # desktop_RU_mau_dau.jpeg
    plot_year_over_year(desktop_data, "RU")
    # desktop_PL_mau_dau.jpeg
    plot_year_over_year(desktop_data, "PL")

    # desktop_Tier1_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "Tier1")
    # desktop_US_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "US")
    # desktop_CA_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "CA")
    # desktop_DE_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "DE")
    # desktop_FR_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "FR")
    # desktop_GB_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "GB")
    # desktop_RoW_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "RoW")
    # desktop_CN_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "CN")
    # desktop_IN_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "IN")
    # desktop_BR_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "BR")
    # desktop_ID_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "ID")
    # desktop_RU_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "RU")
    # desktop_CN_mau_dau_ratio.jpeg
    plot_dau_mau_contribution_individual_country(desktop_data, "PL")

    tier1 = ["US", "CA", "DE", "FR", "GB"]
    # desktop_MAU_tier1_contribution.jpeg
    plot_group_contribution(desktop_data, tier1, "Tier1", "MAU")
    # desktop_DAU_tier1_contribution.jpeg
    plot_group_contribution(desktop_data, tier1, "Tier1", "DAU")

    top11 = ["US", "CA", "DE", "FR", "GB", "CN", "IN", "ID", "BR", "RU", "PL"]
    # desktop_MAU_top11_contribution.jpeg
    plot_group_contribution(desktop_data, top11, "Top11", "MAU")
    # desktop_DAU_top11_contribution.jpeg
    plot_group_contribution(desktop_data, top11, "Top11", "DAU")

    # desktop_US_user_state_dau_contribution.jpeg
    plot_dau_user_state_individual_country(
        desktop_data[desktop_data["country"] == "US"], "US"
    )
    # desktop_CA_user_state_dau_contribution.jpeg
    plot_dau_user_state_individual_country(
        desktop_data[desktop_data["country"] == "CA"], "CA"
    )
    # desktop_DE_user_state_dau_contribution.jpeg
    plot_dau_user_state_individual_country(
        desktop_data[desktop_data["country"] == "DE"], "DE"
    )
    # desktop_FR_user_state_dau_contribution.jpeg
    plot_dau_user_state_individual_country(
        desktop_data[desktop_data["country"] == "FR"], "FR"
    )
    # desktop_GB_user_state_dau_contribution.jpeg
    plot_dau_user_state_individual_country(
        desktop_data[desktop_data["country"] == "GB"], "GB"
    )
    # desktop_CN_user_state_dau_contribution.jpeg
    plot_dau_user_state_individual_country(
        desktop_data[desktop_data["country"] == "CN"], "CN"
    )
    # desktop_IN_user_state_dau_contribution.jpeg
    plot_dau_user_state_individual_country(
        desktop_data[desktop_data["country"] == "IN"], "IN"
    )
    # desktop_ID_user_state_dau_contribution.jpeg
    plot_dau_user_state_individual_country(
        desktop_data[desktop_data["country"] == "ID"], "ID"
    )
    # desktop_PL_user_state_dau_contribution.jpeg
    plot_dau_user_state_individual_country(
        desktop_data[desktop_data["country"] == "PL"], "PL"
    )
    # desktop_BR_user_state_dau_contribution.jpeg
    plot_dau_user_state_individual_country(
        desktop_data[desktop_data["country"] == "BR"], "BR"
    )
    # desktop_RU_user_state_dau_contribution.jpeg
    plot_dau_user_state_individual_country(
        desktop_data[desktop_data["country"] == "RU"], "RU"
    )

    # desktop_mau_dau_ratio.jpeg
    # desktop_mau_dau_ratio_2.jpeg
    plot_dau_mau_ratio(desktop_data)

    if bucket_name is not None:
        storage_client = storage.Client(project=project)

        bucket = storage_client.bucket(bucket_name=bucket_name)
        for pathname in glob.glob(os.path.join(STATIC_DIR, "**"), recursive=True):
            if os.path.isfile(pathname):
                blob = bucket.blob(
                    os.path.join(GCS_PREFIX, pathname.replace(STATIC_DIR + "/", ""))
                )
                blob.upload_from_filename(pathname)


if __name__ == "__main__":
    main()
