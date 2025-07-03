import json
import os
from argparse import ArgumentParser

import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta

CLIENT_ID = os.environ.get("LOOKER_CLIENT_ID")
CLIENT_SECRET = os.environ.get("LOOKER_CLIENT_SECRET")


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

def looker_login_post(client_id, client_secret):
    url = "https://mozilla.cloud.looker.com/api/4.0/login"
    query_params = {"client_id": client_id, "client_secret": client_secret}
    response = requests.post(url, query_params)
    return response

def looker_looks_download(submission_date,access_token):
    url = f"https://mozilla.cloud.looker.com/api/4.0/looks"
    headers = {'Authorization': f'token {access_token}'}
    params = {}
    looks_data_list = []
        # print(f"this is look id {look_id}" )
    default_look_dict = {
        "submission_date": submission_date,
        "url": None,
        "look_id": None,
        "last_accessed_at": None,
        "last_viewed_at": None,
        "user_name": None,
        "last_updater_id": None,
        "view_count": None,
    }
    # this returns a tuple
    look_data_response = get_response(url, headers, params)
    if look_data_response == "Not Found. Possible Permissions Error":
        looks_data_list.append(default_look_dict)
        return looks_data_list
    look_data = look_data_response[0]
    for datum in look_data:
        last_viewed_str = datum.get("last_viewed_at")
        if last_viewed_str:
            last_viewed_date = datetime.strptime(last_viewed_str, "%Y-%m-%dT%H:%M:%S.%fZ").date()
            six_months_ago = datetime.today().date() - relativedelta(months=6)
            if last_viewed_date < six_months_ago:
                looks_data = {
                        **default_look_dict,
                        "url": "https.mozilla.cloud.looker.com/" + datum["short_url"],
                        "last_accessed_at": datum["last_accessed_at"],
                        "last_viewed_at": datum["last_viewed_at"],
                        "user_name": datum["user_name"],
                        "last_updater_id": datum["last_updater_id"],
                        "view_count": datum["view_count"],
                    } 
                looks_data_list.append(looks_data)
    return looks_data_list

def main():
    parser.add_argument("--date", required=True)

    client_id = CLIENT_ID
    client_secret = CLIENT_SECRET
    args = parser.parse_args()

    submission_date = args.date

    auth_looker_response = looker_login_post(client_id, client_secret)
    looker_access_token = auth_looker_response.json()['access_token']

    looks_export = looker_looks_download(submission_date, looker_access_token)


if __name__ == "__main__":
    main()
