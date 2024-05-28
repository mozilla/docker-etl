import os

config = {
    "proxies": {},
    "xm_username": os.environ.get("XMATTERS_USERNAME", ""),
    "xm_password": os.environ.get("XMATTERS_PASSWORD", ""),
    "url": os.environ.get("XMATTERS_URL", ""),
    "supervisor_id": os.environ.get("XMATTERS_SUPERVISOR_ID", ""),
}
