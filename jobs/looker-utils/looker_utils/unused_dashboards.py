import json
import os

import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import click
import csv

@click.option("--client_id", "--client-id", envvar="LOOKER_CLIENT_ID", required=True)
@click.option(
    "--client_secret",
    "--client-secret",
    envvar="LOOKER_CLIENT_SECRET",
    required=True,
)
@click.option("--date", required=True )

CSV_FIELDS = [
    "submission_date",
    "url",
    "dashboard_id",
    "last_accessed_at",
    "last_viewed_at",
    "updated_at",
    "user_name",
    "last_updater_id",
    "last_updater_name",
    "view_count",
]

def get_response(url, headers, params):
    """GET response function."""
    response = requests.get(url, headers=headers, params=params)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        if response.status_code == 404:
            raise err
        return("Not Found. Possible Permissions Error")
        if response.status_code != 500:
            raise err
        return ({"items": []}, "skipped")
    return (response.json(), "completed")

def write_dict_to_csv(json_data, filename):
    """Write a dictionary to a csv."""
    with open(filename, "w") as out_file:
        dict_writer = csv.DictWriter(out_file, CSV_FIELDS)
        dict_writer.writeheader()
        dict_writer.writerows(json_data)

def looker_login_post(client_id, client_secret):
    url = "https://mozilla.cloud.looker.com/api/4.0/login"
    query_params = {"client_id": client_id, "client_secret": client_secret}
    response = requests.post(url, query_params)
    return response

def looker_dashboards_download(access_token):
    url = "https://mozilla.cloud.looker.com/api/4.0/dashboards"
    headers = {'Authorization': f'token {access_token}'}
    params = {}
    dashboard_data = get_response(url, headers, params)
    return dashboard_data


def looker_one_dashboard_download(submission_date, access_token, dashboard_id):
    url = f"https://mozilla.cloud.looker.com/api/4.0/dashboards/{dashboard_id}"
    headers = {'Authorization': f'token {access_token}'}
    params = {}
    dashboard_data = {}
    default_dashboard_dict = {
        "submission_date": submission_date,
        "url": f"https://mozilla.cloud.looker.com/{dashboard_id}",
        "dashboard_id": dashboard_id,
        "last_accessed_at": None,
        "last_viewed_at": None,
        "updated_at": None,
        "user_name": None,
        "last_updater_id": None,
        "last_updater_name": None,
        "view_count": None,
    }
    # this returns a tuple
    dashboard_data_response = get_response(url, headers, params)
    if dashboard_data_response == "Not Found. Possible Permissions Error":
        return default_dashboard_dict
    dashboard_data = dashboard_data_response[0]
    last_viewed_str = dashboard_data.get("last_viewed_at")
    if last_viewed_str:
        last_viewed_date = datetime.strptime(last_viewed_str, "%Y-%m-%dT%H:%M:%S.%fZ").date()
        six_months_ago = datetime.today().date() - relativedelta(months=6)
        if last_viewed_date < six_months_ago:
            return {
                **default_dashboard_dict,
                "last_accessed_at": dashboard_data["last_accessed_at"],
                "last_viewed_at": dashboard_data["last_viewed_at"],
                "updated_at": dashboard_data["updated_at"],
                "user_name": dashboard_data["user_name"],
                "last_updater_id": dashboard_data["last_updater_id"],
                "last_updater_name": dashboard_data["last_updater_name"],
                "view_count": dashboard_data["view_count"],
            }        

def main():

    submission_date = date

    auth_looker_response = looker_login_post(client_id, client_secret)
    looker_access_token = auth_looker_response.json()['access_token']

    # The next line returns a tuple. The first entry is the actual data, the second is the status of the query
    all_dashboards_export =looker_dashboards_download(looker_access_token)

    # Set variable to the first part of the tuple. Returns a list of dictionaries.
    all_dashboards_data = all_dashboards_export[0]

    # create list of just the individual dashboard ids
    dashboard_ids_list = []
    for datum in all_dashboards_data:
        dashboard_ids_list.append(datum["id"])

    dashboard_ids_data_list = []

    for id in dashboard_ids_list:
        dashboard_datum = looker_one_dashboard_download(submission_date, looker_access_token, id)
        dashboard_ids_data_list.append(dashboard_datum)

    csv_name = f"unused_looker_dashboards_{submission_date}.csv"
    write_dict_to_csv(dashboard_ids_data_list, csv_name)

if __name__ == "__main__":
    main()
