import os

config = {
    "proxies": {}, 
    "everfi_integration": {
        "username": os.environ.get("EVERFI_INTEG_WORKDAY_USERNAME", ""),
        "password": os.environ.get("EVERFI_INTEG_WORKDAY_PASSWORD", ""),
        "host": "https://wd2-impl-services1.workday.com/",
        "datawarehouse_worker_endpoint": "ccx/service/customreport2/vhr_mozilla1/ISU%20Report%20Owner/DataWarehouse_Worker_Full_\
File?format=csv",
        "worker_url_csv": "https://wd2-impl-services1.workday.com/ccx/service/\
customreport2/vhr_mozilla1/ISU%20Report%20Owner/DataWarehouse_Worker_Full_\
File?format=csv",
    }
}
