from argparse import ArgumentParser
import logging
from workday_netsuite.api.workday import WorkDayRaaService
from workday_netsuite.api.netsuite import NetSuiteRestlet
from api.util import Util, APIAdaptorException
import sys
from workday_netsuite.api.netsuite import NetSuiteRestletException


def fix_none(x):
    return '' if not x else x.strip()
class NetSuite():
    def __init__(self) -> None:
        self.ns_restlet = NetSuiteRestlet()
        self.logger = logging.getLogger(self.__class__.__name__)

    def format_date(self, date_str):
        try:
            data_lst = date_str.split('/')
            return f"{data_lst[2]}-{data_lst[1]}-{data_lst[0]}"
        except Exception:
            return ""
        
    def build_comparison_string(self, ns_worker):
            return (
                fix_none(ns_worker.get('External ID',''))
                + "|"
                + fix_none(ns_worker.get('Employee Type',''))
                + "|"
                + self.format_date(fix_none(ns_worker.get('Original Hire Date','')))
                + "|"
                + fix_none(ns_worker.get('Company',''))
                + "|"
                + fix_none(ns_worker.get('Manager ID',''))
                + "|"
                + fix_none(ns_worker.get('Cost Center',''))
                + "|"
                + fix_none(ns_worker.get('Cost Center ID',''))
                + "|"
                + fix_none(ns_worker.get('Product','').split(':')[-1].strip())
                + "|"
                + fix_none(ns_worker.get('Email - Primary Work',''))
                + "|"
                + fix_none(ns_worker.get('First Name',''))
                + "|"
                + fix_none(ns_worker.get('Last Name',''))
                + "|"
                + fix_none(ns_worker.get('Country',''))
                + "|"
                + fix_none(ns_worker.get('Termination Date',''))
            )

    def get_employees(self):
        ret = self.ns_restlet.get_employees()
        ret_active = [x for x in ret.data if x.get('Employee Status - Active?')=='Actively Employed']
        return ret_active, {x.get('External ID'):self.build_comparison_string(x) for x in ret_active}

    def compare_users(self, wd_comp, ns_comp):
        import numpy as np

        add_list = []
        del_list = []
        upd_list = []
        wd_users_emails = list(wd_comp.keys())
        ns_users_emails = list(ns_comp.keys())
        add_list = np.setdiff1d(wd_users_emails, ns_users_emails)
        del_list = np.setdiff1d(ns_users_emails, wd_users_emails)
        intersect_list = np.intersect1d(wd_users_emails, ns_users_emails)

        for upd_email in intersect_list:
            if wd_comp.get(upd_email,'') != ns_comp.get(upd_email,''):
                upd_list.append(upd_email)
        return add_list, del_list, upd_list

    def map_country(self, country):
        if country =="United States of America":
            return "United States"
        elif country == "Czechia":
            return "Czech Republic"
        else:
            return country

    def map_payment_method(self, country):
        mcountry = self.map_country(country)
        if mcountry in ["Belgium","Finland", "France", "Germany",
                        "Netherlands","Poland", "Spain", "Sweden",
                        "Denmark"]:
            return "SEPA"
        elif mcountry in ["Austria", "Czech Republic","Greece", "Italy",
                            "United States"]:
            return "ACH"
        elif mcountry in ["United Kingdom"]:
            return "BACS"
        else:
            return None

    def map_currency(self, country):
        if country in ["Belgium",  "Finland",
                        "France", "Germany",
                        "Netherlands", "Spain"]:
            return "EUR"
        elif country in ["Australia"]:
            return "AUD"
        elif country in ["Canada"]:
            return "CAD"
        elif country in ["Poland","Denmark"]:
            return "DKK"
        elif country in ["United Kingdom"]:
            return "GBP"
        elif country in ["New Zealand"]:
            return "NZD"
        elif country in ["Sweden"]:
            return "SEK"
        elif country in ["Austria", "Czech Republic","Greece",
                        "Italy","United States"]:
            return "USD"
        else:
            return None

    def map_class(self, product,cost_center):

        if product == "Advertising": return 8
        elif product == "Emails": return 113
        elif product == "Emails Dedicated": return 114
        elif product == "Emails Standard": return 14
        elif product == "Fakespot": return 130
        elif product == "In-App/Web": return 15
        elif product == "MDN Advertising": return 126
        elif product == "Native Desktop": return 110
        elif product == "Native Mobile": return 129
        elif product == "Tiles Desktop": return 11
        elif product == "Tiles Direct Sell": return 108
        elif product == "Tiles Mobile": return 112
        elif product == "Business Support": return 27
        elif product == "All-Hands 2023": return 104
        elif product == "All-Hands 2024": return 133
        elif product == "China": return 24
        elif product == "Content": return 134
        elif product == "Firefox Other": return 26
        elif product == "Hubs Other": return 25
        elif product == "Innovation BI": return 118
        elif product == "Innovation General": return 119
        elif product == "Innovation MEICO": return 116
        elif product == "Innovation Mradi": return 111
        elif product == "Innovation Studio": return 120
        elif product == "MozSocial": return 132
        elif product == "Pocket Other": return 121
        elif product == "Firefox ESR": return 4
        elif product == "Keyword Search Desktop": return 2
        elif product == "Keyword Search Mobile": return 3
        elif product == "Suggest Desktop": return 9
        elif product == "Suggest Mobile": return 10
        elif product == "Vertical Desktop": return 6
        elif product == "Vertical Mobile": return 7
        elif product == "FPN": return 20
        elif product == "Hubs Subscription": return 107
        elif product == "MDN Subscription": return 22
        elif product == "Monitor": return 128
        elif product == "Pocket Premium": return 17
        elif product == "PXI Other": return 18
        elif product == "Relay": return 21
        elif product == "Relay Bundle Email": return 106
        elif product == "Relay Bundle Phone": return 122
        elif product == "VPN": return 19
        elif product == "VPN Relay Bundle": return 105
        elif product == "VPN Relay Bundle Email": return 124
        elif product == "VPN Relay Bundle Phone": return 125
        elif product == "VPN Relay Bundle VPN": return 123
        else:
            return None

    def update(self, wd_workers,
               workers_dict, max_limit,
               newEmployee = False,
               ns_workers = None,
               wd_comp=None,
               ns_comp=None):

        for i, wd_worker in enumerate(wd_workers):
            ns_country = self.map_country(wd_worker.Country)
            manager = workers_dict[wd_worker.Manager_ID]
            if workers_dict[wd_worker.Manager_ID].get('Preferred_Full_Name',''):
                manager_full_name = f"{wd_worker.Manager_ID} - {manager['First_Name']} {manager['Last_Name']} {workers_dict[wd_worker.Manager_ID].get('Preferred_Full_Name','')}".strip()
            else:
                manager_full_name = f"{wd_worker.Manager_ID} - {manager['First_Name']} {manager['Last_Name']} {manager['First_Name']} {manager['Last_Name']}"
            employee_data = {
                    "employees": [
                        {
                            "External ID": wd_worker.Employee_ID,
                            "Employee ID": f"{wd_worker.Employee_ID} - {wd_worker.First_Name} {wd_worker.Last_Name}",
                            "Last Name": wd_worker.Last_Name,
                            "First Name": wd_worker.First_Name,
                            "Original Hire Date": wd_worker.Original_Hire_Date,
                            "Most Recent Hire Date": wd_worker.Most_Recent_Hire_Date,
                            "Termination Date": wd_worker.termination_date,
                            "Employee Type": wd_worker.Employee_Type,
                            "Employee Status - Active?": 'Actively Employed' if wd_worker.Employee_Status=='1'else 'Terminated'  ,
                            "Email - Primary Work": wd_worker.primaryWorkEmail,
                            "Manager": manager_full_name,
                            "Manager ID": wd_worker.Manager_ID,
                            "Manager E-mail": wd_worker.Manage_Email_Address,
                            "Cost Center - ID": wd_worker.Cost_Center_ID,
                            "Cost Center": wd_worker.Cost_Center,
                            "Product": wd_worker.Product,
                            "Country": ns_country,
                            "Company": wd_worker.Company,
                            "DEFAULT CURRENCY FOR EXP. REPORT": self.map_currency(ns_country),
                            "Payment Method": self.map_payment_method(ns_country),
                            "Class": self.map_class(wd_worker.Product, wd_worker.Cost_Center),
                            "newEmployee" : newEmployee
                        }
                    ]
                }
            try:
                self.ns_restlet.update(employee_data)
            except NetSuiteRestletException as e:
                self.logger.info(f"Employee ID:{wd_worker.Employee_ID} ")
                self.logger.info(f"error {e.args[0].data}")

            except Exception as e:
                self.logger.info(f"error {e}")
                continue

class WorkdayToNetsuiteIntegration():
    """Integration class for syncing data from Workday to Netsuite.

    Args:
        args (Args): Arguments for the integration.
    """
    def __init__(self,) -> None:
        self.workday_service = WorkDayRaaService()
        self.netsuite = NetSuite()

        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, max_limit):
        #tests
        # Util.verify_email_identity()
        # Util.send_email(source="mcastelluccio@data.mozaws.net",
        #                 destination=["jmoscon@mozilla.com"],
        #                 subject="Test", body="email test")
        """Run all the steps of the integration"""


        # ========================================================
        # Step 1: Getting Workday Data
        # ========================================================
        try:
            self.logger.info("Step 1: Getting Workday Data")
            wd_workers, workers_dict, wd_comp = self.workday_service.get_listing_of_workers()

            self.logger.info(f"Number of Worday employees {len(wd_workers)}.")
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed while Getting Workday data...")
            sys.exit(1)

        # ========================================================
        # Step 2: Getting NetSuite Data
        # ========================================================
        try:
            self.logger.info("Step 2: Getting NetSuite Data")
            ns_workers, ns_comp = self.netsuite.get_employees()
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed while Getting NetSuite data...")
            sys.exit(1)

        # import csv
        # with open('ns_workers.csv', 'w', newline='',encoding="utf-8") as myfile:
        #     wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        #     wr.writerow(ns_workers[0].keys())
        #     for work in ns_workers:
        #         wr.writerow(work.values())
        #self.netsuite.map_data(wd_workers, workers_dict, max_limit)

        # ========================================================
        # Step 3: Compare Workday and Netsuite data
        # ========================================================
        self.logger.info("Step 3: Compare Workday and Netsuite data")
        add_list, del_list, upd_list = self.netsuite.compare_users(wd_comp=wd_comp, ns_comp=ns_comp)

        # ========================================================
        # Step 4: Add new employees
        # ========================================================
        # self.logger.info("Step 4: Add new employees")
        # wd_workers_add = [x for x in wd_workers if x.Employee_ID in add_list]

        # self.netsuite.update(wd_workers=wd_workers_add,
        #                      workers_dict=workers_dict,
        #                      max_limit=max_limit,
        #                      newEmployee=True,
        #                      ns_workers=ns_workers
        #                      )

        # ========================================================
        # Step 5: Update employees
        # ========================================================

        self.logger.info("Step 5: Update employees")
        wd_workers_upd = [x for x in wd_workers if x.Employee_ID in upd_list]
        self.netsuite.update(wd_workers=wd_workers_upd,
                             workers_dict=workers_dict,
                             max_limit=max_limit,
                             newEmployee=False,
                             ns_workers=ns_workers,
                             wd_comp=wd_comp, 
                             ns_comp=ns_comp
                             )
        self.logger.info("End of Integration.")

if __name__ == "__main__":
    parser = ArgumentParser(description="Slack Channels Integration ")

    parser.add_argument(
        "-l",
        "--level",
        action="store",
        help="log level (debug, info, warning, error, or critical)",
        type=str,
        default="info",
    )

    parser.add_argument(
        "-f",
        "--max_limit",
        action="store",
        type=int,
        help="limit the number of changes",
        default=40
    )
    args = None
    args = parser.parse_args()

    log_level = Util.set_up_logging(args.level)

    logger = logging.getLogger(__name__)

    logger.info("Starting...")
    logger.info(f"max_limit={args.max_limit}")

    WD = WorkdayToNetsuiteIntegration()

    logger = logging.getLogger("main")
    logger.info('Starting Workday to Netsuite Integratiogitn ...')

    WD.run(args.max_limit)
