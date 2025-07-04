import json
import os

import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import click
import csv

CSV_FIELDS = [
    "submission_date",
    "explores",
    "has_content",
    "label",
    "name",
    "project_name",
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

def looker_lookml_download(submission_date,access_token):
    url = f"https://mozilla.cloud.looker.com/api/4.0/lookml_models"
    headers = {'Authorization': f'token {access_token}'}
    params = {}
    looks_data_list = []
        # print(f"this is look id {look_id}" )
    default_look_dict = {
        "submission_date": submission_date,
        "explores": None,
        "has_content": None,
        "label": None,
        "name": None,
        "project_name": None,

    }
    # this returns a tuple
    look_data_response = get_response(url, headers, params)
    if look_data_response == "Not Found. Possible Permissions Error":
        looks_data_list.append(default_look_dict)
        return looks_data_list
    look_data = look_data_response[0]
    for datum in look_data:
        looks_data = {
            **default_look_dict,
            "explores": datum["explores"],
            "has_content": datum["has_content"],
            "label": datum["label"],
            "name": datum["name"],
            "project_name": datum["project_name"],
        } 
        looks_data_list.append(looks_data)
    return looks_data_list

@click.option("--client_id", "--client-id", envvar="LOOKER_CLIENT_ID", required=True)
@click.option(
    "--client_secret",
    "--client-secret",
    envvar="LOOKER_CLIENT_SECRET",
    required=True,
)
@click.option("--date", required=True)

def main(date, client_id, client_secret):

    submission_date = date

    auth_looker_response = looker_login_post(client_id, client_secret)
    looker_access_token = auth_looker_response.json()['access_token']

    lookml_export = looker_lookml_download(submission_date, looker_access_token)

csv_name = f"unused_lookmls_{submission_date}"
write_dict_to_csv(lookml_export, csv_name)

if __name__ == "__main__":
    main()
