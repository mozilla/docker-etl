import logging
import re
import boto3

from .secrets_util import config as util_config


class LocalConfig(object):
    def __init__(self):
        pass

    def __getattr__(self, attr):
        return util_config[attr]


_config = LocalConfig()


def set_up_logging(level):
    log_level = logging.INFO
    if re.match("^debug$", level, flags=re.IGNORECASE):
        log_level = logging.DEBUG
    elif re.match("^info$", level, flags=re.IGNORECASE):
        log_level = logging.INFO
    elif re.match("^warn", level, flags=re.IGNORECASE):
        log_level = logging.WARNING
    elif re.match("^err", level, flags=re.IGNORECASE):
        log_level = logging.ERROR
    elif re.match("^crit", level, flags=re.IGNORECASE):
        log_level = logging.CRITICAL
    logging.basicConfig(
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=log_level,
        encoding="utf-8",
        # format="[%(asctime)s] %(name)s [%(levelname)s]: %(message)s", level=log_level
    )


def postal_to_coords_and_timezone(loc):
    from .classes.mozgeo import MozGeo

    geo = MozGeo(_config)
    coords = geo.postal_to_coords(loc)
    if coords != (None, None):
        tz = geo.coords_to_timezone(coords)
    else:
        tz = None
    return (coords, tz)

def verify_email_identity():
    ses_client = boto3.client("ses", region_name="us-west-2")
    response = ses_client.verify_email_identity(
        EmailAddress="jmoscon@mozilla.com"
    )
def send_email(source, destination, subject, body):
    client = boto3.client("ses", region_name="us-west-2")
    client.send_email(
        Source=source,
        Destination={
            "ToAddresses": destination,
            "CcAddresses": [],
        },
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {"Html": {"Data": body, "Charset": "UTF-8"}},
        },
)