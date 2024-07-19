import json
import logging
import os
import uuid

import click
import pendulum
import requests
from google.cloud import bigquery, storage

logger = logging.getLogger(__name__)

"""
DEFINED CONSTANTS
"""
ADZERK_ALL_FLIGHTS = "https://api.adzerk.net/v1/flight?isActive=true"
# https://dev.kevel.com/reference/list-flights
ADZERK_FLIGHT = "https://api.adzerk.net/v1/flight/{}"
# https://dev.kevel.com/reference/get-flight
ADZERK_ZONES = "https://api.adzerk.net/v1/zone"
# https://dev.kevel.com/reference/list-zones

ADZERK_ALL_CAMPAIGNS = "https://api.adzerk.net/v1/campaign"
# https://dev.kevel.com/reference/list-campaigns
ADZERK_ADVERTISER = "https://api.adzerk.net/v1/advertiser/{0}"
# https://dev.kevel.com/reference/get-advertisers
ADZERK_FLIGHT_IN_CAMPAIGN = "https://api.adzerk.net/v1/campaign/{0}/flight"
# https://dev.kevel.com/reference/list-flights-for-campaign-id
ADZERK_BULK_INSTANT_REPORT = "https://api.adzerk.net/v1/instantcounts/bulk"
# https://dev.kevel.com/reference/get-bulk-counts
ADZERK_SITES = "https://api.adzerk.net/v1/site"
# https://dev.kevel.com/reference/list-sites
MAX_PUT_BATCH_RECORDS = 500
ADZERK_API_HEADER = {"X-Adzerk-ApiKey": None}
date_format = "%Y-%m-%dT%H:%M:%S"
# https://dev.kevel.com/docs/flights#flight-fields
# map to convert kevel Ids for each rate type to the human readable value
RATE_TYPE = {
    1: "Flat",
    2: "CPM",
    3: "CPC",
    4: "CPA View",
    5: "CPA Click",
    6: "CPA View & Click",
}

"""
END CONSTANTS
"""


def _get_site_mapping():
    site_mapping = dict()
    resp = requests.get(url=ADZERK_SITES, headers=ADZERK_API_HEADER)
    if resp.status_code == 200:
        sites = resp.json()["items"]
        if sites:
            for s in sites:
                if not s["IsDeleted"]:
                    site_mapping[s["Id"]] = s["Title"]
        return site_mapping
    else: # pragma: no cover
        logger.error(
            f"Failed request, status {resp.status_code} while obtaining site information"
        )


def _get_zone_mapping():
    zone_mapping = dict()
    resp = requests.get(url=ADZERK_ZONES, headers=ADZERK_API_HEADER)
    if resp.status_code == 200:
        zones = resp.json()["items"]
        if zones:
            for z in zones:
                if not z["IsDeleted"]:
                    zone_mapping[z["Id"]] = z["Name"]
        return zone_mapping
    else: # pragma: no cover
        logger.error(
            f"Failed request, status {resp.status_code} while obtaining zone information"
        )


def get_all_active_creative_maps():
    """
    All active creative maps are returned.
    Without pagination, all data is returned.
    Pagination can happen with pageSize and page params.
    Since this is an offline reporting call, we don't use pagination right now.
    :return: List of active flights' creative maps
    """
    resp = requests.get(url=ADZERK_ALL_FLIGHTS, headers=ADZERK_API_HEADER)
    all_flight_ads = []
    if resp.status_code == 200:
        if "items" in resp.json():
            flights = resp.json()["items"]
            for flight in flights:
                all_flight_ads.extend(get_single_flight(flight))
    else: # pragma: no cover
        raise Exception(
            "Error calling Kevel for all active flights: {0}".format(
                json.dumps(resp.json())
            )
        )

    return all_flight_ads


def get_single_flight(flight):
    flight_id = flight["Id"]
    resp = requests.get(url=ADZERK_FLIGHT.format(flight_id), headers=ADZERK_API_HEADER)
    if resp.status_code == 200:
        try:
            flight = resp.json()
            # if flight has no creatives, default to an empty list
            flight_creatives = flight.get("CreativeMaps", [])
            if not flight_creatives:
                flight_creatives = []

            flight_ads = _arrange_ad_details(
                flight_creatives, flight.get("SiteZoneTargeting")
            )
            flight_ads = decorate_with_info(
                flight_ads, flight, "flight_name", "Name", ad_value=False
            )
            flight_ads = decorate_with_info(
                flight_ads, flight, "price", "Price", ad_value=False
            )
            flight_ads = decorate_with_info(
                flight_ads, flight, "rate_type", "RateType", ad_value=False
            )
            flight_ads = decorate_with_info(
                flight_ads, RATE_TYPE, "rate_type", "rate_type"
            )
            return flight_ads
        except Exception as err: # pragma: no cover
            logger.error(
                "Error calling for flight {0}, data: {1}, error: ${2}".format(
                    flight_id, resp.json(), err
                )
            )

    else: # pragma: no cover
        logger.error(
            "Error calling for flight {0}, error: {1}".format(flight_id, resp.json())
        )
    return [] # pragma: no cover


def get_campaign_name_map():
    resp = requests.get(url=ADZERK_ALL_CAMPAIGNS, headers=ADZERK_API_HEADER)
    campaign_map = {}
    if resp.status_code == 200:
        campaigns = resp.json()["items"]
        for item in campaigns:
            campaign_map[item["Id"]] = item["Name"]

        return campaign_map
    else: # pragma: no cover
        logger.error("Error calling for campaigns: {0}".format(resp.json()))
        return {}


def _arrange_ad_details(creative_map, site_zone_targeting):
    ad_list = []
    # https://dev.kevel.com/docs/flights#sitezone-targeting
    # flights do not have to specify site or zone targeting
    site_zones = (
        []
        if not site_zone_targeting
        else [
            {"site_id": x["SiteId"], "zone_id": x["ZoneId"]}
            for x in site_zone_targeting
        ]
    )
    for creative in creative_map:
        ad_list.extend(_get_ad_details(creative, site_zones))

    return ad_list


def _get_ad_details(ad_object, site_zone_mapping):
    template_values = __parse_creative_template(ad_object["Creative"])
    ad_details = {
        "flight_id": ad_object["FlightId"],
        "campaign_id": ad_object["CampaignId"],
        "ad_id": ad_object["Id"],
        "creative_id": ad_object["Creative"]["Id"],
        "advertiser_id": ad_object["Creative"]["AdvertiserId"],
        # parse out template variables, but use .get in case creatives are created with a different
        # template that has different fields.
        "sponsor": None if not template_values else template_values.get("ctSponsor"),
        "creative_title": (
            None if not template_values else template_values.get("ctTitle")
        ),
        "creative_url": None if not template_values else template_values.get("ctUrl"),
        "content_url": __parse_friendly_name(ad_object["Creative"]["Title"]),
        "image_url": ad_object["Creative"]["ImageLink"],
    }

    ad_zone_list = []
    if not site_zone_mapping:
        ad_details_ = ad_details.copy()
        ad_zone_list.append(ad_details_)
    else:
        for sitezone in site_zone_mapping:
            ad_details_ = ad_details.copy()
            ad_details_["site_id"] = sitezone["site_id"]
            ad_details_["zone_id"] = sitezone["zone_id"]
            ad_zone_list.append(ad_details_)

    return ad_zone_list


def decorate_with_info(
    ad_map_details, info_map, key, value, ad_value=True, suppress_warning=False
):
    """
    Returns a changed ad map decorated with information from another map.
    :param ad_map_details:
    :param info_map: The info that has to be merged into the ad_map
    :param key: They key at which to insert info from the info_map
    :param value: Where to find the value in info_map
    :param ad_value: Should we find information in the info map from the individual ad,
    or use the raw value?
    :param suppress_warning: If True, this will suppress logging when a value is not found. This
    should be used when it is a valid case for the value to be absent. e.g. site_id is optional
    in flights, so copying the name of the site_id may not find a site_name.
    e.g. we find the campaign name given the campaign id from the ad: info_map[ad['campaign_id']]
    e.g. we find price from the flight using raw 'Price': info_map['Price']
    :return:
    """
    decorated_ad_list = []
    for ad in ad_map_details:
        ad_ = ad.copy()
        getter_ = ad[value] if ad_value else value
        try:
            ad_[key] = info_map[getter_]
        except KeyError: # pragma: no cover
            if not suppress_warning:
                logger.warning(
                    "Value for {} does not exist in info map for key {}".format(
                        getter_, key
                    )
                )
            ad_[key] = None

        decorated_ad_list.append(ad_)

    return decorated_ad_list


def upload_to_storage(obj_list, project, bucket, env):
    client = storage.Client(project=project)
    bucket = client.bucket(bucket_name=bucket)
    to_insert = len(obj_list)
    records_inserted_total = 0

    path_name = "/tmp/batch.json"
    blob_path = f"kevel_metadata/{env}/batch.json"

    if os.path.exists(path_name):
        os.remove(path_name)
    with open(path_name, "w") as f:
        for index in range(0, len(obj_list), MAX_PUT_BATCH_RECORDS):
            mini_batch = obj_list[index : index + MAX_PUT_BATCH_RECORDS]
            f.writelines(mini_batch)
            records_inserted_total += len(mini_batch)

    blob = bucket.blob(blob_path)
    blob.upload_from_filename(path_name)

    # else return last response
    if records_inserted_total != to_insert:
        raise Exception( # pragma: no cover
            f"inserted {records_inserted_total} but there were {to_insert} in total"
        )
    else:
        logger.info(f"{to_insert} inserted into {bucket}")

    return blob_path


def upload_to_bq(project, bucket, blob_path):

    # create Big Query client
    bqclient = bigquery.Client(project=project)

    # create history table

    script_path = os.path.dirname(os.path.realpath(__file__))

    history_schema_path = f"{script_path}/schemas/history_table.json"

    history_table_name = f"{project}.ads_derived.kevel_metadata_history_v1"

    history_schema = bqclient.schema_from_json(history_schema_path)
    table = bigquery.Table(history_table_name, schema=history_schema)

    table.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="submission_date",
    )
    logger.info(f"Making sure the table {history_table_name} exists.")
    table = bqclient.create_table(table, exists_ok=True)

    # First, we load the data from GCS to a temp table.
    uri = f"gs://{bucket}/{blob_path}"
    tmp_suffix = uuid.uuid4().hex[0:6]
    tmp_table = f"tmp.kevel_metadata_load_{tmp_suffix}"
    bqclient.load_table_from_uri(
        uri,
        tmp_table,
        job_config=bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ),
    ).result()

    # Next, we run a query that populates the destination table partition.
    now_date = pendulum.now().start_of("day")
    bqclient.query(
        f"""
with prep as (
      select
        ad_id,
        creative_id,
        flight_id,
        flight_name,
        campaign_id,
        campaign_name,
        advertiser_id,
        sponsor,
        rate_type,
        price,
        creative_title,
        creative_url,
        content_url,
        image_url,

        --it is possible for the same ad_id to target mutliple sites and/or zones
        --thus, we need to keep track of all sites and zones for each ad_id
        STRING_AGG(distinct cast(site_id as string)) as all_site_ids,
        STRING_AGG(distinct cast(site_name  as string))as all_site_names,
        count(distinct site_id) as site_id_count,
        sum(case when site_id = 1070098 then 1 else 0 end) as targeted_against_default_site,

        STRING_AGG(distinct cast(zone_id as string)) as all_zone_ids,
        STRING_AGG(distinct cast(zone_name as string)) as all_zone_names,
        count(distinct zone_id) as zone_id_count,
        sum(case when zone_id = 217995 then 1 else 0 end) as targeted_against_default_zone

    from {tmp_table}
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14)

select
    DATE("{now_date.to_iso8601_string()}") AS submission_date,
    CAST(ad_id AS INT64) AS ad_id,
    CAST(advertiser_id AS INT64) AS advertiser_id,
    trim(sponsor) as advertiser_name, --rename from original JSON for parity with the advertiser ID field,
    nullif(all_site_ids, '') as all_site_ids, --LISTAGG has weird handling of null values to empty strings, convert any empty values back to null
    nullif(all_site_names, '') as all_site_names, --LISTAGG has weird handling of null values to empty strings, convert any empty values back to null,
    CAST(campaign_id AS INT64) AS campaign_id,
    campaign_name,
    content_url,
    CAST(creative_id AS INT64) AS creative_id,
    creative_title,
    creative_url,
    CAST(flight_id AS INT64) AS flight_id,
    flight_name,
    image_url,
    CAST(price AS FLOAT64) AS price,
    rate_type,
    CAST(case when site_id_count = 1 then cast(all_site_ids as integer) --if one site_id, attribute to that site_id
        when site_id_count > 1 and targeted_against_default_site = 1 then 1070098 --if one of the sites is the Firefox Production site, attribute the ad_id to that site
        when site_id_count > 1 and targeted_against_default_site = 0 then NULL end as INT64) as site_id, --if the ad_id has 2+ sites and one is not the default Firefox Production site, we cannot attribute the ad to a single site_id
    CAST(site_id_count AS INT64) AS site_id_count,
    case when site_id_count = 1 then all_site_names --if one site_id, attribute to that site
        when site_id_count > 1 and targeted_against_default_site = 1 then 'Firefox Production' --if one of the sites is the Firefox Production site, attribute the ad_id to that site
        when site_id_count > 1 and targeted_against_default_site = 0 then 'Multiple Sites' end as site_name, --if the ad_id has 2+ sites and one is not the default Firefox Production site, we cannot attribute the ad to a single site,
    CAST(targeted_against_default_site AS INT64) AS targeted_against_default_site,
    CAST(targeted_against_default_zone AS INT64) AS targeted_against_default_zone,
    CAST(    case when zone_id_count = 1 then cast(all_zone_ids as integer) --if one zone_id, attribute to that zone_id
        when zone_id_count > 1 and targeted_against_default_zone = 1 then 217995 --if one of the zones is the 3x7 zone, attribute the ad_id to that zone
        when zone_id_count > 1 and targeted_against_default_zone = 0 then NULL end as INT64) AS zone_id, --if the ad_id has 2+ zones and one is not the default 3x7 zone, we cannot attribute the ad to a single zone_id
    CAST(zone_id_count AS INT64) AS zone_id_count,
                case when zone_id_count = 1 then all_zone_names --if one zone_id, attribute to that zone
        when zone_id_count > 1 and targeted_against_default_zone = 1 then '3x7' --if one of the zones is the 3x7 zone, attribute the ad_id to that zone
        when zone_id_count > 1 and targeted_against_default_zone = 0 then 'Multiple Zones' end as zone_name, --if the ad_id has 2+ zones and one is not the default 3x7 zone, we cannot attribute the ad to a single zone
from prep

--filter out some bad data that did not produce any impressions in a production environment
where flight_id is not null
    and advertiser_id is not null
    and sponsor is not null""",
        job_config=bigquery.QueryJobConfig(
            destination=f"{history_table_name}${now_date.strftime('%Y%m%d')}",
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    ).result()

    # Finally, we clean up after ourselves.
    bqclient.delete_table(tmp_table)


def json_convert(obj_list):
    str_list = [json.dumps(item) + "\n" for item in obj_list]
    return str_list


def __parse_creative_template(creative_dets):
    if "TemplateValues" in creative_dets:
        jsonstr = creative_dets.get("TemplateValues")
        return json.loads(jsonstr) if jsonstr else None


def __parse_friendly_name(ctTitle):
    parts = ctTitle.split("|||")
    if len(parts) > 1:
        content_url = parts[1].strip()
        return content_url


@click.command()
@click.option(
    "--env",
    help="production or dev",
    type=click.Choice(["production", "dev"]),
    required=True,
)
@click.option("--api-key", help="kevel API key", required=True)
@click.option("--project", help="GCP project id")
@click.option("--bucket", help="GCP bucket name")
def main(project, bucket, api_key, env):
    ADZERK_API_HEADER["X-Adzerk-ApiKey"] = api_key

    prod_bucket = "moz-fx-data-prod-external-data"
    prod_project = "moz-fx-data-shared-prod"

    if env == "production": # pragma: no cover
        project = prod_project
        bucket = prod_bucket
    elif env == "dev": # pragma: no cover
        if None in [project, bucket]:
            raise Exception("project and bucket values are required in dev!")

    campaign_name_map = get_campaign_name_map()
    zone_map = _get_zone_mapping()
    site_map = _get_site_mapping()
    ad_details = get_all_active_creative_maps()
    ad_map = decorate_with_info(ad_details, site_map, "site_name", "site_id")
    ad_map = decorate_with_info(
        ad_map, zone_map, "zone_name", "zone_id", suppress_warning=True
    )
    ad_map = decorate_with_info(
        ad_map, campaign_name_map, "campaign_name", "campaign_id"
    )
    ads_buffer = json_convert(ad_map)
    blob_path = upload_to_storage(ads_buffer, project, bucket, env)
    upload_to_bq(project, bucket, blob_path)


if __name__ == "__main__":
    main()
