import os

config = {
    "proxies": {},

    "xmatters_integration": {
        "username": os.environ.get("XMATTERS_INTEG_WORKDAY_USERNAME", ""),
        "password": os.environ.get("XMATTERS_INTEG_WORKDAY_PASSWORD", ""),
        "sites_url":  "https://services1.myworkday.com/ccx/service/\
customreport2/vhr_mozilla/ISU_RAAS/Mozilla_BusContSites?format=json",
        "people_url": "https://services1.myworkday.com/ccx/service/\
customreport2/vhr_mozilla/ISU_RAAS/Mozilla_BusContUsers?format=json",
    },
}
