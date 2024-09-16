import os

config = {
    "link": os.environ.get("wd_listing_of_workers_link", ""),
    "username": os.environ.get("wd_username", ""),
    "password": os.environ.get("wd_pass", ""),
    "links": {
        "wd_listing_of_workers_link": os.environ.get("wd_listing_of_workers_link", ""),
    },
    "timeout": 10,
}