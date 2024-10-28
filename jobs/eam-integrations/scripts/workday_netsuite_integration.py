from argparse import ArgumentParser
import logging
import re
import sys
from datetime import datetime

from workday_netsuite.api.workday import WorkDayRaaService, Worker, InternationalTransfer
from workday_netsuite.api.netsuite import NetSuiteRestlet
from api.util import Util, APIAdaptorException
from workday_netsuite.api.netsuite import NetSuiteRestletException


def fix_none(x):
    return '' if (not x or x=='- None -') else x.strip()
class Workday:
    def __init__(self,) -> None:
        self.workday_service = WorkDayRaaService()
        self.mapping = DataMapping()

    def add_update_address_test(self, worker):
        worker['Postal'] = '19335'
        worker['State'] = 'Pennsylvania'
        worker['City'] = 'Downingtown'
        worker['Primary_Address'] = '430 Creekside Drive'
        return worker
    
    def add_create_employee_test(self, worker):
        worker['Employee_ID'] = '282828'
        worker['First_Name'] = 'Jc'
        worker['Last_Name'] = 'Moc'
        worker['primaryWorkEmail'] = 'jcmoc@mozilla.com'
        return worker
    
    def add_international_transfer_test(self, worker):
        worker['Country'] = "United States of America"
        return worker
    
    def get_international_transfers(self, ns_workers):
        #2024-01-01-08
        today = datetime.today() 
        start_date = f"{today.year}-01-01"
        end_date = f"{today.year}-12-31"
        wd_data = self.workday_service.get_international_transfers(start_date,end_date)
        worker_list = []
        #check if the employee in NetSuite was already transfered 
        for worker in wd_data["Report_Entry"]:
            intern_transfer = InternationalTransfer(**worker)
            ns_worker = ns_workers.get(intern_transfer.Employee_ID,'')
            if self.mapping.map_country(intern_transfer.New_Country) != ns_worker.get('Country'):
                worker_list.append(InternationalTransfer(**worker))
        
        return worker_list
    
    def get_listing_of_workers(self):
 
        wd_data = self.workday_service.get_listing_of_workers()
        worker_dict = {}
        worker_list = []
        wd_comp = {}
        for worker in wd_data["Report_Entry"]:
            worker['Cost_Center_ID'] = worker.pop('Cost_Center_-_ID')
            
            #Update test
            if worker["Employee_ID"] =='200841':
                worker['Employee_Type'] = 'Student Worker (Fixed Term)'
                worker['First_Name'] = 'Ronald1'
                worker['Last_Name'] = 'Mman1'
                worker['Primary_Address'] = '111 Westsite Village Loop '
                worker['City'] = 'Windermere'
                worker['State'] = 'Berlin'
                worker['Postal'] = '12131'
                worker['Most_Recent_Hire_Date'] = '2024-11-01'
                worker['Manager_ID'] = 'E201024'
                #worker['Country'] = 'Germany'
                worker['Product'] = 'Relay'
                worker['Cost_Center_ID'] = '14400 - Enterprise Application Management'
                worker['termination_date'] = '2019-10-22'
                worker['Employee_Status'] = '2'

            # Terminated employee
            if worker['Employee_ID'] == '205716':
                worker['Employee_Status']= '2'
                worker['termination_date'] = '2024-10-21'

             #200837 international transfer
            if worker['Employee_ID'] == '200837':
                # update address test
                worker = self.add_update_address_test(worker=worker)
                
                # create employee test
                from copy import deepcopy
                new_worker = deepcopy(worker)
                worker2 = self.add_create_employee_test(worker=new_worker)
                worker2 = self.add_international_transfer_test(worker=worker2)
                worker_list.append(Worker(**worker2))
                worker_dict[worker2['Employee_ID']] = worker2
                wd_comp[worker2['Employee_ID']] = self.build_comparison_string(worker2)

            worker_list.append(Worker(**worker))
            worker_dict[worker['Employee_ID']] = worker
            wd_comp[worker['Employee_ID']] = self.build_comparison_string(worker)

        return worker_list,worker_dict, wd_comp
    
    def build_comparison_string(self, wd_worker):
            ns_country = self.mapping.map_country(wd_worker.get('Country',''))
            if wd_worker.get('Employee_ID','') == 200013:
                print("a")
            state = wd_worker.get('State','') if ns_country!="Canada" else wd_worker.get('Province','')
            company = self.mapping.map_company(ns_country)
            return (
                wd_worker.get('Employee_ID','')
                + "|" 
                + wd_worker.get('Employee_Type','')
                + "|" 
                + wd_worker.get('Most_Recent_Hire_Date','')
                + "|"
                + company
                + "|"
                + wd_worker.get('Manager_ID','')
                + "|"
                + wd_worker.get('Cost_Center_ID','')              
                + "|"
                + wd_worker.get('Product','')    
                + "|"
                + wd_worker.get('primaryWorkEmail','')
                + "|"
                + wd_worker.get('First_Name','')   
                + "|"
                + wd_worker.get('Last_Name','') 
                + "|"
                + self.mapping.map_country(wd_worker.get('Country',''))
                + "|"
                + wd_worker.get('termination_date','') 
                + "|"
                + wd_worker.get('Employee_Status','') 
                + "|"
                + wd_worker.get('Primary_Address','') 
                + "|"
                + state
                + "|"
                + wd_worker.get('City','') 
                + "|"
                + wd_worker.get('Postal','')
                + "|"
                + self.mapping.map_payment_method(ns_country)
                + "|"
                + self.mapping.map_currency(ns_country)
                + "|"
                + str(self.mapping.map_class(wd_worker.get('Product','')))
                                 
            )

class DataMapping():

    def map_country_codes(self, ns_country):
        ditct = {
            'Albania': 'AL',
            'American Samoa': 'AS',
            'Argentina': 'AR',
            'Armenia': 'AM',
            'Azerbaijan': 'AZ',
            'Bahrain': 'BH',
            'Barbados': 'BB',
            'Belarus': 'BY',
            'Bolivia': 'BO',
            'Botswana': 'BW',
            'Brazil': 'BR',
            'Cameroon': 'CM',
            'Chile': 'CL',
            'Colombia': 'CO',
            'Costa Rica': 'CR',
            'Dominican Republic': 'DO',
            'Ecuador': 'EC',
            'Egypt': 'EG',
            'El Salvador': 'SV',
            'Georgia': 'GE',
            'Guam': 'GU',
            'Guatemala': 'GT',
            'Honduras': 'HN',
            'Hong Kong': 'HK',
            'India': 'IN',
            'Indonesia': 'ID',
            'Israel': 'IL',
            'Jamaica': 'JM',
            'Japan': 'JP',
            'Jordan': 'JO',
            'Kazakhstan': 'KZ',
            'Kenya': 'KE',
            'Korea, Republic of': 'KR',
            'Kosovo': 'XK',
            'Kuwait': 'KW',
            'Kyrgyzstan': 'KG',
            'Malaysia': 'MY',
            'Mauritius': 'MU',
            'Mexico': 'MX',
            'Moldova, Republic of': 'MD',
            'Mozambique': 'MZ',
            'Namibia': 'NA',
            'Nicaragua': 'NI',
            'Nigeria': 'NG',
            'Pakistan': 'PK',
            'Peru': 'PE',
            'Philippines': 'PH',
            'Puerto Rico': 'PR',
            'Qatar': 'QA',
            'ROW': 'US',
            'Asia Pacific (APAC)': 'US',
            'Australia': 'AU',
            'China': 'CN',
            'New Zealand': 'NZ',
            'Taiwan': 'TW',
            'EMEA': 'US',
            'Austria': 'AT',
            'Belgium': 'BE',
            'Bosnia and Herzegovina': 'BA',
            'Bulgaria': 'BG',
            'Croatia': 'HR',
            'Cyprus': 'CY',
            'Czech Republic': 'CZ',
            'Denmark': 'DK',
            'Estonia': 'EE',
            'Finland': 'FI',
            'France': 'FR',
            'Germany': 'DE',
            'Berlin Office': 'DE',
            'Greece': 'GR',
            'Hungary': 'HU',
            'Iceland': 'IS',
            'Ireland': 'IE',
            'Italy': 'IT',
            'Latvia': 'LV',
            'Lithuania': 'LT',
            'Luxembourg': 'LU',
            'Macedonia, Republic of North': 'MK',
            'Malta': 'MT',
            'Netherlands': 'NL',
            'Norway': 'NO',
            'Poland': 'PL',
            'Portugal': 'PT',
            'Romania': 'RO',
            'Russian Federation': 'RU',
            'Serbia': 'RS',
            'Slovakia': 'SK',
            'Slovenia': 'SI',
            'Spain': 'ES',
            'Sweden': 'SE',
            'Switzerland': 'CH',
            'United Kingdom': 'GB',
            'Non-US Americas': 'US',
            'Canada': 'CA',
            'Toronto Office': 'CA',
            'Saint Lucia': 'LC',
            'Saudi Arabia': 'SA',
            'Senegal': 'SN',
            'Singapore': 'SG',
            'South Africa': 'ZA',
            'Thailand': 'TH',
            'Turkey': 'TR',
            'Uganda': 'UG',
            'Ukraine': 'UA',
            'United Arab Emirates': 'AE',
            'Uruguay': 'UY',
            'USA': 'US',
            'United States': 'US',
            'San Francisco Office': 'US',
            'Uzbekistan': 'UZ',
            'Vietnam': 'VN',
            'Zimbabwe': 'ZW'
            }
        return ditct.get(ns_country,'')

    def map_country(self, country):
        if country =="United States of America":
            return "United States"
        elif country == "Czechia":
            return "Czech Republic"
        else:
            return country
    def map_company(self, country):
        dict = {
            'Australia':'Moz 2008 Corporation (Australia)',
            'Austria':'Mozilla Corporation',
            'Belgium':'MZ Denmark ApS, Belgium Branch',
            'Canada':'MZ Canada Internet ULC (Canada)',
            'Czech Republic':'Mozilla Corporation',
            'Finland':'MZ Denmark ApS, filall Finland',
            'France':'MZ Denmark (France)',
            'Germany':'MZ Denmark GmbH (Germany)',
            'Greece':'Mozilla Corporation',
            'Italy':'Mozilla Corporation',
            'Netherlands':'MZ Netherlands B.V.',
            'New Zealand':'Moz 2008 Corporation (New Zealand)',
            'Poland':'MZ Denmark ApS',
            'Spain':'Denmark ApS, Sucursal en Espana (Spain)',
            'Sweden':'MZ Denmark ApS Danmark) filial (Sweden)',		
            'Taiwan':'Moz 2008 Corporation (Taiwan)',		
            'United Kingdom':'MZ Denmark (UK)',
            'United States':'Mozilla Corporation',		
            'Denmark':'MZ Denmark ApS',
            }
        return dict.get(country,'')
    
    def map_payment_method(self, country):
        mcountry = self.map_country(country)
        if mcountry in ["Belgium","Finland", "France", "Germany",
                        "Netherlands","Poland", "Spain", "Sweden",
                        "Denmark","Canada"]:
            return "SEPA"
        elif mcountry in ["Austria", "Czech Republic","Greece", "Italy",
                            "United States"]:
            return "ACH"
        elif mcountry in ["United Kingdom"]:
            return "BACS"
        elif mcountry in ["Australia","New Zealand"]:
            return "Wire"
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

    def map_class(self, product):

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
class NetSuite():
    def __init__(self) -> None:
        self.ns_restlet = NetSuiteRestlet()
        self.mapping = DataMapping()
        self.logger = logging.getLogger(self.__class__.__name__)

    def format_date(self, date_str):
        try:
            data_lst = date_str.split('/')
            return f"{data_lst[2]}-{data_lst[0]}-{data_lst[1]}"
        except Exception:
            return ""
        
    def build_comparison_string(self, ns_worker):
            
            if ns_worker.get("External ID")=='200221':
                print('s')
            if '__RANDOM_ID__' in fix_none(ns_worker.get('External ID','')):
                # external_id = fix_none(ns_worker.get('Employee ID')).split('-')[0].strip()
                external_id = self.extract_employee_id(ns_worker.get('Employee ID'))
                # external_id = re.findall(r'^\d+',ns_worker.get('Employee ID'))[0]
            else:
                external_id= fix_none(ns_worker.get('External ID',''))
            return (
                external_id
                + "|"
                + fix_none(ns_worker.get('Employee Type',''))
                + "|"
                + self.format_date(fix_none(ns_worker.get('Original Hire Date','')))
                + "|"
                + fix_none(ns_worker.get('Company',''))
                + "|"
                + fix_none(ns_worker.get('Manager ID',''))
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
                + self.format_date(fix_none(ns_worker.get('Termination Date','')))
                + "|"
                + fix_none('1' if ns_worker.get("Employee Status - Active?",'')=="Actively Employed" else '2')
                + "|"
                + fix_none(ns_worker.get('Address1','')) 
                + "|"
                + fix_none('' if ns_worker.get('State','')=='- None -' else ns_worker.get('State','')) 
                + "|"
                + fix_none(ns_worker.get('City','')) 
                + "|"
                + fix_none(ns_worker.get('Zipcode',''))
                + "|"
                + ns_worker.get("DEFAULT CURRENCY FOR EXP. REPORT")
                + "|"
                + ns_worker.get("Payment Method")
                + "|"
                + ns_worker.get("Class")
                
            )
    def extract_employee_id(self,employee_id):
        if employee_id=='E101374 - Mark Hammond':
            print('s')

        _employee_id = re.findall(r'^\d+',employee_id)
                
        if len(_employee_id)>0:
            return fix_none(_employee_id[0])     
        else: 
            return fix_none(employee_id).split(' ')[0].strip()    
        
    def get_employees(self):
        def fixEmployeeID(ns_worker):
            import re
            if '208454' in ns_worker.get('Employee ID',''):
                print('s')
            if '__RANDOM_ID__' in fix_none(ns_worker.get('External ID','')):
                #return fix_none(ns_worker.get('Employee ID')).split(' ')[0].strip()
                return self.extract_employee_id(ns_worker.get('Employee ID'))
                employee_id = re.findall(r'^\d+',ns_worker.get('Employee ID'))
                
                if len(employee_id)>0:
                    return fix_none(employee_id[0])     
                else: 
                    return fix_none(ns_worker.get('Employee ID')).split(' ')[0].strip()            
                return fix_none(re.findall(r'^\d+',ns_worker.get('Employee ID'))[0])
            else:
                return fix_none(ns_worker.get('External ID',''))
 
        ret = self.ns_restlet.get_employees()
        #ret_active = [x for x in ret.data if x.get('Employee Status - Active?')=='Actively Employed']
        ret_active = {fixEmployeeID(x):x for x in ret.data if x.get('Employee Status - Active?')=='Actively Employed'}
        return ret_active, {fixEmployeeID(x):self.build_comparison_string(x) for x in ret_active.values()}
        #return ret.data, {x.get('External ID'):self.build_comparison_string(x) for x in ret.data}

    def compare_users(self, wd_comp, ns_comp):
        import numpy as np

        add_list = []
        del_list = []
        upd_list = []
        int_transfer_list = []
        wd_users_emails = list(wd_comp.keys())
        ns_users_emails = list(ns_comp.keys())
        add_list = np.setdiff1d(wd_users_emails, ns_users_emails)
        del_list = np.setdiff1d(ns_users_emails, wd_users_emails)
        intersect_list = np.intersect1d(wd_users_emails, ns_users_emails)

        for upd_email in intersect_list:
            if wd_comp.get(upd_email,'') != ns_comp.get(upd_email,''):
                upd_list.append(upd_email)
            
            wd_country = wd_comp.get(upd_email,'').split('|')[10]
            ns_country = ns_comp.get(upd_email,'').split('|')[10]
            if wd_country != ns_country:
                int_transfer_list.append(upd_email)

        return add_list, del_list, upd_list,int_transfer_list

    

    def update(self, wd_workers,
               workers_dict, max_limit,
               newEmployee = False,
               reHire = False,
               internationalTransfer = False,
               ns_workers = None,
               wd_comp=None,
               ns_comp=None,
               error_lst=[]):
        import time

        for i, wd_worker in enumerate(wd_workers):
            start = time.time()
            ns_country = self.mapping.map_country(wd_worker.Country)
                     
            # print(wd_comp[wd_worker.Employee_ID])
            # print(ns_comp[wd_worker.Employee_ID])
            employee_data = {
                    "employees": [
                        {
                            "External ID": wd_worker.Employee_ID,
                            "Employee ID": f"{wd_worker.Employee_ID} - {wd_worker.First_Name} {wd_worker.Last_Name}",
                            "Last Name": wd_worker.Last_Name,
                            "First Name": wd_worker.First_Name,
                            "Original Hire Date": wd_worker.Most_Recent_Hire_Date,
                            "Most Recent Hire Date": wd_worker.Most_Recent_Hire_Date,
                            "Termination Date": wd_worker.termination_date if not reHire else None,
                            "Employee Type": wd_worker.Employee_Type,
                            "Employee Status - Active?": 'Actively Employed' if wd_worker.Employee_Status=='1'else 'Terminated'  ,
                            "Email - Primary Work": wd_worker.primaryWorkEmail,
                            "Manager ID": wd_worker.Manager_ID,
                            "Cost Center - ID": wd_worker.Cost_Center_ID,
                            "Product": wd_worker.Product,
                            "Address1": wd_worker.Primary_Address,
                            "Address2": None,
                            "State": wd_worker.State if wd_worker.State else wd_worker.Province,
                            "City": wd_worker.City,
                            "Zipcode": wd_worker.Postal,
                            "Country": self.mapping.map_country_codes(ns_country),
                            "CountryName": ns_country,
                            "Company": self.mapping.map_company(ns_country),
                            "DEFAULT CURRENCY FOR EXP. REPORT": self.mapping.map_currency(ns_country),
                            "Payment Method": self.mapping.map_payment_method(ns_country),
                            "Class": self.mapping.map_class(wd_worker.Product),
                            "newEmployee" : newEmployee,
                            "Rehire" : True if reHire else None,
                            "InternationalTransfer" : True if internationalTransfer else None, 
                            "oldCountryCode" : self.mapping.map_country_codes(ns_workers[wd_worker.Employee_ID].get('Country'))
                                           if internationalTransfer else None,
                            "oldCountryName" : ns_workers[wd_worker.Employee_ID].get('Country')
                                           if internationalTransfer else None,
                        }
                    ]
                }
            try:
                ret = self.ns_restlet.update(employee_data)
                if ret:
                    error_lst.append((employee_data,ret))
                end = time.time()
                print(end - start)

                
            except NetSuiteRestletException as e:
                self.logger.info(f"Employee ID:{wd_worker.Employee_ID} ")
                self.logger.info(f"error {e.args[0].data}")

            except Exception as e:
                self.logger.info(f"error {e}")
                continue


        return error_lst

class WorkdayToNetsuiteIntegration():
    """Integration class for syncing data from Workday to Netsuite.

    Args:
        args (Args): Arguments for the integration.
    """
    def __init__(self,) -> None:
        #self.workday_service = WorkDayRaaService()
        self.workday = Workday()
        self.netsuite = NetSuite()

        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, max_limit):
        #tests
        Util.verify_email_identity()
        Util.send_email(source="mcastelluccio@data.mozaws.net",
                        destination=["jmoscon@mozilla.com"],
                        subject="Test", body="email test")
        return
        """Run all the steps of the integration"""
        
        error_lst = []
        # ========================================================
        # Step 1: Getting Workday Data
        # ========================================================
        try:
            self.logger.info("Step 1: Getting Workday Data")
            wd_workers, workers_dict, wd_comp = self.workday.get_listing_of_workers()
 
            self.logger.info(f"Number of Worday employees {len(wd_workers)}.")
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed on Step 1: Getting Workday Data")
            sys.exit(1)

        # import csv
        # with open('wd_workers.csv', 'w', newline='',encoding="utf-8") as myfile:
        #     wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        #     wr.writerow(wd_workers[0].__dict__.keys())
        #     for work in wd_workers:
        #         wr.writerow(work.__dict__.values())

        # ========================================================
        # Step 2: Getting NetSuite Data
        # ========================================================
        try:
            self.logger.info("Step 2: Getting NetSuite Data")
            ns_workers, ns_comp = self.netsuite.get_employees()
            self.logger.info(f"Number of Worday employees {len(ns_workers)}.")
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed on Step 2: Getting NetSuite Data")
            sys.exit(1)

        # import csv
        # with open('ns_workers.csv', 'w', newline='',encoding="utf-8") as myfile:
        #     wr = csv.writer(myfile, quoting=csv.QUOTE_ALL)
        #     wr.writerow(ns_workers[0].keys())
        #     for work in ns_workers:
        #         wr.writerow(work.values())
        # self.netsuite.map_data(wd_workers, workers_dict, max_limit)

        # ========================================================
        # Step 3: Compare Workday and Netsuite data
        # ========================================================
        self.logger.info("Step 3: Compare Workday and Netsuite data")
        add_list, del_list, upd_list, int_transfer_list = self.netsuite.compare_users(wd_comp=wd_comp, ns_comp=ns_comp)

        # ========================================================
        #  Step 4: Add rehires
        # ========================================================
        # find rehires
        try:
            # diff_hire_dates that are in the add_list
            rehires = [x for x in wd_workers 
                                if x.Most_Recent_Hire_Date != x.Original_Hire_Date
                                and x.Employee_ID in add_list]
            rehires = []
            
            error_lst = self.netsuite.update(wd_workers=rehires,
                                workers_dict=workers_dict,
                                max_limit=max_limit,
                                newEmployee=False,
                                reHire=True,
                                ns_workers=ns_workers,
                                wd_comp=wd_comp, 
                                ns_comp=ns_comp,
                                error_lst=error_lst
                                )      
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed on Step 4: Add rehires")
             
        
        # ========================================================
        # Step 5: Add new employees
        # ========================================================
        try:
            self.logger.info("Step 4: Add new employees")
            # remove the rehires from the add_list
            add_list = [x for x in add_list if x not in [x.Employee_ID for x in rehires]]
            wd_workers_add = [x for x in wd_workers if x.Employee_ID in add_list]
            wd_workers_add = [x for x in wd_workers_add if x.Employee_ID == '282828']
            # wd_workers_add = []
            error_lst = self.netsuite.update(wd_workers=wd_workers_add,
                                workers_dict=workers_dict,
                                max_limit=max_limit,
                                newEmployee=True,
                                ns_workers=ns_workers,
                                error_lst=error_lst
                                )
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed on Step 5: Add new employees")

        # ========================================================
        # Step 6: International Transfers
        # ========================================================
        # try:
        #     self.logger.info("Step 6: International Transfers")
        #     ret = self.workday.get_international_transfers(ns_workers)

        #     wd_workers_upd = [x for x in wd_workers if x.Employee_ID in [x.Employee_ID for x in ret]] 
        #     # wd_workers_upd = [x for x in wd_workers_upd if x.Employee_ID == '282828']
        #     wd_workers_upd = []
        #     error_lst = self.netsuite.update(wd_workers=wd_workers_upd,
        #                         workers_dict=workers_dict,
        #                         max_limit=max_limit,
        #                         newEmployee=False,
        #                         internationalTransfer=True,
        #                         ns_workers=ns_workers,
        #                         wd_comp=wd_comp, 
        #                         ns_comp=ns_comp,
        #                         error_lst=error_lst
        #                         )
        # except (APIAdaptorException, Exception) as e:
        #     self.logger.error(str(e))
        #     self.logger.critical("Failed on Step 6: International Transfers")

        # ========================================================
        # Step 7: Update employees
        # ========================================================

        try:
            self.logger.info("Step 5: Update employees")
            wd_workers_upd = [x for x in wd_workers if x.Employee_ID in upd_list]
            wd_workers_upd = [x for x in wd_workers if x.Employee_ID == '200841']
            
            error_lst = self.netsuite.update(wd_workers=wd_workers_upd,
                                workers_dict=workers_dict,
                                max_limit=max_limit,
                                newEmployee=False,
                                ns_workers=ns_workers,
                                wd_comp=wd_comp, 
                                ns_comp=ns_comp,
                                error_lst=error_lst
                                )
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed on Step 7: Update employees")
        
        
        self.logger.info("End of Integration.")

def new_func(__name__, WorkdayToNetsuiteIntegration):
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
    logger.info('Starting Workday to Netsuite Integration ...')

    WD.run(args.max_limit)

if __name__ == "__main__":
    new_func(__name__, WorkdayToNetsuiteIntegration)
