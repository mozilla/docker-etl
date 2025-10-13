import os

config = {
    "proxies": {},
    "docusign_integration": {
        "username": os.environ.get("DOCUSIGN_INTEG_WORKDAY_USERNAME", ""),
        "password": os.environ.get("DOCUSIGN_INTEG_WORKDAY_PASSWORD", ""),
        "host": "https://services1.myworkday.com/",
        "datawarehouse_worker_endpoint": "ccx/service/customreport2/vhr_mozilla/ISU%20Report%20Owner/DataWarehouse_Worker_Full_\
File?format=csv",
        "worker_url_csv": "https://services1.myworkday.com/ccx/service/\
customreport2/vhr_mozilla/ISU%20Report%20Owner/DataWarehouse_Worker_Full_\
File?format=csv",
    }
}
