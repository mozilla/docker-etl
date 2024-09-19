from argparse import ArgumentParser
import logging
from workday_netsuite.api.workday import WorkDayRaaService
from workday_netsuite.api.netsuite import NetSuiteRestlet
from api.util import Util
import json
from workday_netsuite.api.netsuite import NetSuiteRestletException
class NetSuite():
    def __init__(self) -> None:
        self.ns_restlet = NetSuiteRestlet()
        self.logger = logging.getLogger(self.__class__.__name__)

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
    
    def get_product(self, cost_center):
        if cost_center in ["10000 - Office of CEO",
                    "10001 - Executive Assistants",
                    "10010 - Office of Chair",
                    "10040 - CEO Strategic Reserve",
                    "10050 - Corp Consolidation & Elimination",
                    "10100 - Strategy & Operations",
                    "10101 - Strategy",
                    "10102 - BAIR",
                    "10103 - Trust & Safet",
                    "10104 - AdOps",
                    "28000 - Customer Experience",
                    "11000 - Legal & Policy",
                    "11100 - Global Policy",
                    "11200 - Legal",
                    "12000 - Finance",
                    "12110 - Accounting",
                    "12120 - Finance",
                    "12130 - Payroll",
                    "12140 - Procurement",
                    "12160 - Tax",
                    "13000 - People",
                    "13100 - IDEA",
                    "13200 - People Operations",
                    "13300 - Total Rewards",
                    "13400 - People Business Partners",
                    "13500 - Recruiting",
                    "13600 - Employee Experience",
                    "13710 - Sustainability",
                    "13810 - Workplace Experience",
                    "14000 - MozProd Infrastructure",
                    "14100 - MozProd Infrastructure Leadership",
                    "14140 - Infrastructure Operations",
                    "14150 - Site Reliability Engineering",
                    "14200 - Core Services",
                    "14300 - Information Technology Services",
                    "14400 - Enterprise Application Management",
                    "14610 - Data Science & Analytics",
                    "14650 - Data Engineering",
                    "11901 - Security Assurance",
                    "15000 - Business Development",
                    "15100 - Business Development",
                    "15101 - Mozilla Sales",
                    "20000 - Marketing",
                    "21000 - General Marketing",
                    "22000 - Marketing & Policy Communications",
                    "23000 - Global Product Marketing",
                    "24000 - North America Marketing",
                    "25000 - Marketing Strategy & Operations",
                    "26000 - Global Brand Marketing",
                    "27000 - EU Marketing",
                    "51200 - PXI Ecosystem",
                    "59000 - TPM",
                    "59100 - Program Management",
                    "62000 - ProdOps",
                    "62100 - MozProdOps"]:
            return "Business Support"
         
        elif cost_center in ["62201 - Fakespot Sales",
                            "62202 - Fakespot R&D"]:
            return "Fakespot"
        
        elif cost_center in ["50000 - Firefox"
                    "50100 - Advanced Technology",
                    "52100 - Web Platform Engineering",
                    "52300 - Engineering Effectiveness",
                    "55000 - Firefox Leadership",
                    "57000 - User Experience",
                    "58000 - Product Management"]:
            return "Firefox Other"
        
        elif cost_center in ["31100 - Hubs Product"]:
            return "Hubs Subscription"
        
        elif cost_center in ["30000 - New Products", 
                            "31200 - Rally",
                            "31300 - Innovation Ecosystem"]:
            return "Innovation General"
        
        elif cost_center in ["31400 - Africa Mradi"]:
            return "Innovation Mradi"
        
        elif cost_center in ["40000 - PXI"]:
            return "PXI"
        
        elif cost_center in ["41000 - PXI Protection"]:
            return  "PXI Other"
        
        elif cost_center in ["51000 - Desktop Engineering"]:
            return "Keyword Search Desktop"
        
        elif cost_center in ["54000 - Mobile Engineering"]:
            return "Keyword Search Mobile"
        elif cost_center in ["60000 - Pocket",
                            "61000 - Pocket Leadership",
                            "61700 - Engineering",
                            "61800 - Product"]:
            return "Pocket Other"
        elif cost_center in ["61400 - Content"]:
            return "Content"
        elif cost_center in ["62200 - Advertising"]:
            return "62200 - Advertising"
        elif cost_center in ["62300 - MDN"]:
            return "MDN Subscription"
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
            return "CAN"
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
        if not product:
            product = self.get_product(cost_center)

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

    def map_data(self, wd_workers, workers_dict, max_limit):
    
        for i, wd_worker in enumerate(wd_workers):
            ns_country = self.map_country(wd_worker.Country)
            manager = workers_dict[wd_worker.Manager_ID]
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
                            "Manager": f"{wd_worker.Manager_ID} - {manager['First_Name']} {manager['Last_Name']}",
                            "Manager ID": wd_worker.Manager_ID,
                            "Manager E-mail": wd_worker.Manage_Email_Address,
                            "Cost Center - ID": wd_worker.Cost_Center_ID,
                            "Cost Center": wd_worker.Cost_Center,
                            "Product": wd_worker.Product,
                            "Country": ns_country,
                            "Company": wd_worker.Company,
                            "DEFAULT CURRENCY FOR EXP. REPORT": self.map_currency(ns_country),
                            "Payment Method": self.map_payment_method(ns_country),
                            "Class": self.map_class(wd_worker.Product, wd_worker.Cost_Center)
                        }
                    ]
                }
            try:
                self.ns_restlet.update(employee_data) 
            except NetSuiteRestletException as e:
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
        Util.verify_email_identity()
        Util.send_email(source="mcastelluccio@data.mozaws.net", 
                        destination=["jmoscon@mozilla.com"], 
                        subject="Test", body="email test")
        """Run all the steps of the integration"""
        # self.netsuite.ns_restlet.get_employees()
        # Step 1: Get list of workers from workday
        self.logger.info("Step 1: Gathering data to run the transformations. ")
        wd_workers, workers_dict = self.workday_service.get_listing_of_workers()
        self.logger.info(f"{len(wd_workers)} workers returned from Workday.")

        # Step 2: Perform data transformations
        self.logger.info("Step 2: Transforming Workday data.")

        #self.netsuite.map_data(wd_workers, workers_dict, max_limit)


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
