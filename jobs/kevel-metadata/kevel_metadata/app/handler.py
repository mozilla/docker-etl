import json
import logging
import os

import pytz
import requests

from app.exceptions.AdZerkException import AdZerkApiSecretException
from app.exceptions.AdZerkException import \
    AdZerkFlightException

"""
DEFINED CONSTANTS
"""
ADZERK_ALL_FLIGHTS = 'https://api.adzerk.net/v1/flight?isActive=true'
# https://dev.kevel.com/reference/list-flights
ADZERK_FLIGHT = 'https://api.adzerk.net/v1/flight/{}'
# https://dev.kevel.com/reference/get-flight
ADZERK_ZONES = 'https://api.adzerk.net/v1/zone'
# https://dev.kevel.com/reference/list-zones

ADZERK_ALL_CAMPAIGNS = 'https://api.adzerk.net/v1/campaign'
# https://dev.kevel.com/reference/list-campaigns
ADZERK_ADVERTISER = 'https://api.adzerk.net/v1/advertiser/{0}'
# https://dev.kevel.com/reference/get-advertisers
ADZERK_FLIGHT_IN_CAMPAIGN = 'https://api.adzerk.net/v1/campaign/{0}/flight'
# https://dev.kevel.com/reference/list-flights-for-campaign-id
ADZERK_BULK_INSTANT_REPORT = 'https://api.adzerk.net/v1/instantcounts/bulk'
# https://dev.kevel.com/reference/get-bulk-counts
ADZERK_SITES = 'https://api.adzerk.net/v1/site'
# https://dev.kevel.com/reference/list-sites
MAX_PUT_BATCH_RECORDS = 500
ADZERK_API_HEADER = {'X-Adzerk-ApiKey': None}
tz = pytz.timezone('America/Los_Angeles')
date_format = '%Y-%m-%dT%H:%M:%S'
# https://dev.kevel.com/docs/flights#flight-fields
# map to convert kevel Ids for each rate type to the human readable value
RATE_TYPE = {1: 'Flat', 2: 'CPM', 3: 'CPC', 4: 'CPA View', 5: 'CPA Click', 6: 'CPA View & Click'}

"""
END CONSTANTS
"""


def __get_api_key():
    secret_name = os.environ.get('ADZERK_SECRET_NAME')
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name='us-east-1'
    )
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret['ADZERK_API_KEY']
    except Exception as e:
        raise AdZerkApiSecretException(e)


def _get_site_mapping():
    site_mapping = dict()
    resp = requests.get(url=ADZERK_SITES, headers=ADZERK_API_HEADER)
    if resp.status_code == 200:
        sites = resp.json()['items']
        if sites:
            for s in sites:
                if not s['IsDeleted']:
                    site_mapping[s['Id']] = s['Title']
        return site_mapping
    else:
        logging.error(f'Failed request, status {resp.status_code} while obtaining site information')


def _get_zone_mapping():
    zone_mapping = dict()
    resp = requests.get(url=ADZERK_ZONES, headers=ADZERK_API_HEADER)
    if resp.status_code == 200:
        zones = resp.json()['items']
        if zones:
            for z in zones:
                if not z['IsDeleted']:
                    zone_mapping[z['Id']] = z['Name']
        return zone_mapping
    else:
        logging.error(f'Failed request, status {resp.status_code} while obtaining zone information')


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
        if 'items' in resp.json():
            flights = resp.json()['items']
            for flight in flights:
                all_flight_ads.extend(get_single_flight(flight))
    else:
        raise AdZerkFlightException('Error calling for all active flights: {0}'.format(json.dumps(resp.json())))

    return all_flight_ads


def main_handler(event, lambda_context, testing=False):
    ADZERK_API_HEADER['X-Adzerk-ApiKey'] = __get_api_key()

    campaign_name_map = get_campaign_name_map()
    zone_map = _get_zone_mapping()
    site_map = _get_site_mapping()
    ad_details = get_all_active_creative_maps()
    ad_map = decorate_with_info(ad_details, site_map, 'site_name', 'site_id')
    ad_map = decorate_with_info(ad_map, zone_map, 'zone_name', 'zone_id', suppress_warning=True)
    ad_map = decorate_with_info(ad_map, campaign_name_map, 'campaign_name', 'campaign_id')

    if testing:
        return {'statusCode': 200, 'body': {'ads': ad_map}}
    else:
        ads_buffer = json_convert(ad_map)
        push_to_firehose(ads_buffer, os.environ['ADS_FIREHOSE'])


def get_single_flight(flight):
    flight_id = flight['Id']
    resp = requests.get(url=ADZERK_FLIGHT.format(flight_id), headers=ADZERK_API_HEADER)
    if resp.status_code == 200:
        try:
            flight = resp.json()
            # if flight has no creatives, default to an empty list
            flight_creatives = flight.get('CreativeMaps', [])
            if not flight_creatives:
                flight_creatives = []

            flight_ads = _arrange_ad_details(flight_creatives, flight.get('SiteZoneTargeting'))
            flight_ads = decorate_with_info(flight_ads, flight, 'flight_name', 'Name', ad_value=False)
            flight_ads = decorate_with_info(flight_ads, flight, 'price', 'Price', ad_value=False)
            flight_ads = decorate_with_info(flight_ads, flight, 'rate_type', 'RateType', ad_value=False)
            flight_ads = decorate_with_info(flight_ads, RATE_TYPE, 'rate_type', 'rate_type')
            return flight_ads
        except Exception as err:
            logging.error('Error calling for flight {0}, data: {1}, error: ${2}'.format(flight_id, resp.json(), err))

    else:
        logging.error('Error calling for flight {0}, error: {1}'.format(flight_id, resp.json()))
    return []


def get_campaign_name_map():
    resp = requests.get(url=ADZERK_ALL_CAMPAIGNS, headers=ADZERK_API_HEADER)
    campaign_map = {}
    if resp.status_code == 200:
        campaigns = resp.json()['items']
        for item in campaigns:
            campaign_map[item['Id']] = item['Name']

        return campaign_map
    else:
        logging.error('Error calling for campaigns: {0}'
                        .format(resp.json()))
        return {}


def _arrange_ad_details(creative_map, site_zone_targeting):
    ad_list = []
    # https://dev.kevel.com/docs/flights#sitezone-targeting
    # flights do not have to specify site or zone targeting
    site_zones = [] if not site_zone_targeting else [{'site_id': x['SiteId'], 'zone_id': x['ZoneId']} for x in site_zone_targeting]
    for creative in creative_map:
        ad_list.extend(_get_ad_details(creative, site_zones))

    return ad_list


def _get_ad_details(ad_object, site_zone_mapping):
    template_values = __parse_creative_template(ad_object['Creative'])
    ad_details = {
        'flight_id': ad_object['FlightId'],
        'campaign_id': ad_object['CampaignId'],
        'ad_id': ad_object['Id'],
        'creative_id': ad_object['Creative']['Id'],
        'advertiser_id': ad_object['Creative']['AdvertiserId'],
        # parse out template variables, but use .get in case creatives are created with a different
        # template that has different fields.
        'sponsor': None if not template_values else template_values.get('ctSponsor'),
        'creative_title': None if not template_values else template_values.get('ctTitle'),
        'creative_url': None if not template_values else template_values.get('ctUrl'),
        'content_url': __parse_friendly_name(ad_object['Creative']['Title']),
        'image_url': ad_object['Creative']['ImageLink'],
    }

    ad_zone_list = []
    if not site_zone_mapping:
        ad_details_ = ad_details.copy()
        ad_zone_list.append(ad_details_)
    else:
        for sitezone in site_zone_mapping:
            ad_details_ = ad_details.copy()
            ad_details_['site_id'] = sitezone['site_id']
            ad_details_['zone_id'] = sitezone['zone_id']
            ad_zone_list.append(ad_details_)

    return ad_zone_list


def decorate_with_info(ad_map_details, info_map, key, value, ad_value=True, suppress_warning=False):
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
        except KeyError:
            if not suppress_warning:
                logging.warning('Value for {} does not exist in info map for key {}'.format(getter_, key))
            ad_[key] = None

        decorated_ad_list.append(ad_)

    return decorated_ad_list


def push_to_firehose(obj_list, stream_name):
    client = boto3.client('firehose')
    to_insert = len(obj_list)
    records_inserted_total = 0

    for index in range(0, len(obj_list), MAX_PUT_BATCH_RECORDS):
        mini_batch = obj_list[index:index + MAX_PUT_BATCH_RECORDS]
        response = client.put_record_batch(
            DeliveryStreamName=stream_name,
            Records=[{'Data': x} for x in mini_batch]
        )
        records_inserted_total += len(response['RequestResponses'])
        # if there's an error in one of the sub-batches, short circuit and return
        if 'FailedPutCount' in response and response['FailedPutCount'] > 0:
            failed_records = response['FailedPutCount']
            err_msg = {'statusCode': 500,
                       'msg': f'failed to insert {failed_records} records out of {to_insert} total records'}
            print(json.dumps(err_msg))

    # else return last response
    if records_inserted_total != to_insert:
        err_msg = {'statusCode': 500, 'msg': f'inserted {records_inserted_total} but there were {to_insert} in total'}
        print(json.dumps(err_msg))
    else:
        ret_msg = {'statusCode': 200, 'msg': f'{to_insert} inserted into {stream_name}'}
        print(json.dumps(ret_msg))


def json_convert(obj_list):
    str_list = [json.dumps(item) + '\n' for item in obj_list]
    return str_list


def __parse_creative_template(creative_dets):
    if 'TemplateValues' in creative_dets:
        jsonstr = creative_dets.get('TemplateValues')
        return json.loads(jsonstr) if jsonstr else None


def __parse_friendly_name(ctTitle):
    parts = ctTitle.split('|||')
    if len(parts) > 1:
        content_url = parts[1].strip()
        return content_url
