import os

config = {
    "proxies": {},
    "xmatters_integration_dev": {
        "username": os.environ.get("DEV_XMATTERS_INTEG_WORKDAY_USERNAME", ""),
        "password": os.environ.get("DEV_XMATTERS_INTEG_WORKDAY_PASSWORD", ""),
        "sites_url":  "https://wd2-impl-services1.workday.com/ccx/service/customreport2/vhr_mozilla1/ISU_RAAS/Mozilla_BusContSites?format=json",
        "people_url": "https://wd2-impl-services1.workday.com/ccx/service/customreport2/vhr_mozilla1/ISU_RAAS/Mozilla_BusContUsers?format=json",
    },
   
    "xmatters_integration_prod": {
        "username": os.environ.get("XMATTERS_INTEG_WORKDAY_USERNAME", ""),
        "password": os.environ.get("XMATTERS_INTEG_WORKDAY_PASSWORD", ""),
        "sites_url":  "https://services1.myworkday.com/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/Mozilla_BusContSites?format=json",
        "people_url": "https://services1.myworkday.com/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/Mozilla_BusContUsers?format=json",
    },
    "seating": {
        "username": os.environ.get("SEATING_WORKDAY_USERNAME", ""),
        "password": os.environ.get("SEATING_WORKDAY_PASSWORD", ""),
        "url": "https://services1.myworkday.com/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/WPR_Worker_Space_Number?format=json",
    },
    "hr_dashboard": {
        "username": os.environ.get("HR_DASHBOARD_WORKDAY_USERNAME", ""),
        "password": os.environ.get("HR_DASHBOARD_WORKDAY_PASSWORD", ""),
        "urls": {
            "headcount": "https://services1.myworkday.com/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/intg__Tableau_Employee_Details_Report?format=csv",
            "hires": "https://services1.myworkday.com/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/intg__Tableau_Hires_-_Date_Range?Business_Processes%21WID=cd09c92e446c11de98360015c5e6daf6!cd09b970446c11de98360015c5e6daf6&Transaction_Status%21WID=b90bc51be01d4ae99b603b02b073714d&format=csv",
            "terminations": "https://services1.myworkday.com/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/intg__Tableau_Terminations_-_Date_Range?Business_Processes%21WID=cd09d6c6446c11de98360015c5e6daf6!cd09bb46446c11de98360015c5e6daf6&Transaction_Status%21WID=b90bc51be01d4ae99b603b02b073714d&format=csv",
            "promotions": "https://services1.myworkday.com/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/intg__Tableau_Promotions_-_Date_Range?Business_Processes%21WID=c24592468ed147b2ac6d0de4d699a7da!cd09bc22446c11de98360015c5e6daf6!cd0dc65a446c11de98360015c5e6daf6&Transaction_Status%21WID=b90bc51be01d4ae99b603b02b073714d&format=csv",
        },
    },
    "ta_dashboard": {
        "username": os.environ.get("HR_DASHBOARD_WORKDAY_USERNAME", ""),
        "password": os.environ.get("HR_DASHBOARD_WORKDAY_PASSWORD", ""),
        "urls": {
            "hires": "https://services1.myworkday.com/ccx/service/customreport2/vhr_mozilla/candersen%40mozilla.com/intg__Tableau_Hires_-_Talent_-_Date_Range?Business_Processes%21WID=c24592468ed147b2ac6d0de4d699a7da!cd09c92e446c11de98360015c5e6daf6!cd09b970446c11de98360015c5e6daf6&Transaction_Status%21WID=b90bc51be01d4ae99b603b02b073714d&format=csv",
            #'hires': 'https://wd2-impl-services1.workday.com/ccx/service/customreport2/vhr_mozilla/candersen%40mozilla.com/intg__Tableau_Hires_-_Talent_-_Date_Range?Business_Processes%21WID=c24592468ed147b2ac6d0de4d699a7da!cd09c92e446c11de98360015c5e6daf6!cd09b970446c11de98360015c5e6daf6&Transaction_Status%21WID=b90bc51be01d4ae99b603b02b073714d&format=csv',
        },
    },
}
