import os

config = {
    "link": os.environ.get("NETSUITE_INTEG_WORKDAY_LISTING_OF_WORKERS_LINK", ""),
    "username": os.environ.get("NETSUITE_INTEG_WORKDAY_USERNAME", ""),
    "password": os.environ.get("NETSUITE_INTEG_WORKDAY_PASSWORD", ""),
    "links": {
        "wd_listing_of_workers_link": os.environ.get("NETSUITE_INTEG_WORKDAY_LISTING_OF_WORKERS_LINK", ""),
        "wd_international_transfers_link": os.environ.get("NETSUITE_INTEG_WORKDAY_INTERNATIONAL_TRANSFER_LINK", ""),
    },
    "timeout": 30,
}
