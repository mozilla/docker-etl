from argparse import ArgumentParser
import logging
import re
import sys
from datetime import datetime
import json
from enum import Enum

from workday_netsuite.api.workday import WorkDayRaaService, Worker, InternationalTransfer
from workday_netsuite.api.netsuite import NetSuiteRestlet
from api.util import Util, APIAdaptorException
from workday_netsuite.api.netsuite import NetSuiteRestletException

class Operations(Enum):
    update_employee = 1
    add_new_hire = 2
    add_new_manager = 3    
    international_transfer = 4
    rehired = 5

def fix_none(x):
    return '' if (not x or x=='- None -') else x.strip()


class Workday:
    def __init__(self,) -> None:
        self.workday_service = WorkDayRaaService()
        self.mapping = DataMapping()


    def get_international_transfers(self, ns_workers,workers_dict):
        today = datetime.today() 
        start_date = f"{today.year-2}-01-01"
        end_date = datetime.today().strftime('%Y-%m-%d')
        wd_data = self.workday_service.get_international_transfers(start_date,end_date)
        worker_list = []
        #check if the employee in NetSuite was already transfered 
        for worker in wd_data["Report_Entry"]:
            intern_transfer = InternationalTransfer(**worker)
            ns_worker = ns_workers.get(intern_transfer.Employee_ID,'')
            wd_worker = workers_dict.get(intern_transfer.Employee_ID,'') 
            if ns_worker:
                if self.mapping.map_company(wd_worker.get('Country','')) != ns_worker.get('Company'):
                    worker_list.append(InternationalTransfer(**worker))

        
        return worker_list
    
    def get_listing_of_workers(self):
 
        wd_data = self.workday_service.get_listing_of_workers()
        worker_dict = {}
        worker_list = []
        wd_comp = {}
        for worker in wd_data["Report_Entry"]:
            worker['Cost_Center_ID'] = worker.pop('Cost_Center_-_ID')
            worker_list.append(Worker(**worker))
            worker_dict[worker['Employee_ID']] = worker
            wd_comp[worker['Employee_ID']] = self.build_comparison_string(worker)

        return worker_list,worker_dict, wd_comp
    
    def build_comparison_string(self, wd_worker):
            ns_country = self.mapping.map_country(wd_worker.get('Country',''))
            #Try Province if State is empty
            state = wd_worker.get('State','') if wd_worker.get('State','').strip()!="" else wd_worker.get('Province','')
            company = self.mapping.map_company(ns_country)

            if not wd_worker.get('Preferred_Full_Name'):
                First_Name = wd_worker.get('First_Name')
                Last_Name = wd_worker.get('Last_Name')
            else:
                 # check if there are Chinese chars
                if re.findall(r'[\u4e00-\u9fff]+', wd_worker.get('Preferred_Full_Name')):
                    First_Name = wd_worker.get('First_Name')
                    Last_Name = wd_worker.get('Last_Name')
                else:

                    First_Name = (' ').join(wd_worker.get('Preferred_Full_Name').split(' ')[0:-1])
                    Last_Name = wd_worker.get('Preferred_Full_Name').split(' ')[-1]
 
            return (
                fix_none(wd_worker.get('Employee_ID',''))
                + "|" 
                + fix_none(wd_worker.get('Employee_Type',''))
                + "|" 
                + fix_none(wd_worker.get('Most_Recent_Hire_Date',''))
                + "|"
                + fix_none(company)
                + "|"
                + fix_none(wd_worker.get('Manager_ID',''))
                + "|"
                + fix_none(wd_worker.get('Cost_Center_ID',''))
                + "|"
                + fix_none(wd_worker.get('primaryWorkEmail',''))
                + "|"
                + fix_none(First_Name)
                + "|"
                + fix_none(Last_Name)
                + "|"
                + fix_none(self.mapping.map_country(wd_worker.get('Country','')))
                + "|"
                + fix_none(wd_worker.get('Employee_Status',''))
                + "|"
                + fix_none(wd_worker.get('Primary_Address',''))
                + "|"
                + fix_none(state)
                + "|"
                + fix_none(wd_worker.get('City',''))
                + "|"
                + fix_none(wd_worker.get('Postal',''))
                + "|"
                + fix_none(self.mapping.map_payment_method(ns_country))
                + "|"
                + fix_none(self.mapping.map_currency(ns_country))
                + "|"
                + fix_none(str(self.mapping.product_class_map_dict.get(wd_worker.get('Product',''))))
                                 
            )

class DataMapping():
    def __init__(self):
 
        ret = NetSuiteRestlet().get_product_class_mapping()
        self.product_class_map_dict = {x.get('externalId'):x.get('internalId') 
                                      for x in ret.data}
        
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
        
    def map_company_id(self, company):
            dict = {
	                'Moz 2008 Corporation (Australia)'.upper():'27',
					'Mozilla Corporation'.upper():'1',
					'MZ Denmark ApS, Belgium Branch'.upper():'22',
					'MZ Canada Internet ULC (Canada)'.upper():'30',
					'Mozilla Corporation'.upper():'1',
					'MZ Denmark ApS, filall Finland'.upper():'24',
					'MZ Denmark (France)'.upper():'32',
					'MZ Denmark GmbH (Germany)'.upper():'33',
					'Mozilla Corporation'.upper():'1',
					'Mozilla Corporation'.upper():'1',
					'MZ Netherlands B.V.'.upper():'41',
					'Moz 2008 Corporation (New Zealand)'.upper():'26',
					'MZ Denmark ApS'.upper():'35',
					'MZ Denmark ApS, Sucursal en Espana (Spain)'.upper():'36',
					'MZ Denmark ApS Danmark) filial (Sweden)'.upper():'37',		
					'Moz 2008 Corporation (Taiwan)'.upper():'28',		
					'MZ Denmark (UK)'.upper():'38',
					'Mozilla Corporation'.upper():'1',		
					'MZ Denmark ApS'.upper():'21',
                }
            return dict.get(company.upper(),'')

    def map_company(self, country, index=1):
        dict = {
            'Australia':('Moz 2008 Corporation (Australia)','27'),
            'Austria':('Mozilla Corporation','1'),
            'Belgium':('MZ Denmark ApS, Belgium Branch','22'),
            'Canada':('MZ Canada Internet ULC (Canada)','30'),
            'Czech Republic':('Mozilla Corporation','1'),
            'Finland':('MZ Denmark ApS, filall Finland','24'),
            'France':('MZ Denmark (France)','32'),
            'Germany':('MZ Denmark GmbH (Germany)','33'),
            'Greece':('Mozilla Corporation','1'),
            'Italy':('Mozilla Corporation','1'),
            'Netherlands':('MZ Netherlands B.V.','41'),
            'New Zealand':('Moz 2008 Corporation (New Zealand)','26'),
            'Poland':('MZ Denmark ApS','21'),
            'Spain':('Denmark ApS, Sucursal en Espana (Spain)','36'),
            'Sweden':('MZ Denmark ApS Danmark) filial (Sweden)','37'),		
            'Taiwan':('Moz 2008 Corporation (Taiwan)','28'),		
            'United Kingdom':('MZ Denmark (UK)','38'),
            'United States':('Mozilla Corporation','1'),
            'United States of America':('Mozilla Corporation','1'),
            'Denmark':('MZ Denmark ApS','21'),
            }
        ret = dict.get(country,'')
        if not ret:
            return ''
        
        return ret[index]
    
    def map_payment_method(self, country):
        mcountry = self.map_country(country)
        if mcountry in ["Belgium","Finland", "France", "Germany",
                        "Netherlands","Poland", "Spain", "Sweden",
                        "Denmark", "Canada"]:
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
        self.error_lst = []

    def get_product_class_map(self, product):
        return self.mapping.product_class_map_dict.get(product,'')
        
    
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
                
            external_id = self.extract_employee_id(ns_worker.get('Employee ID'))
            return (
                external_id
                + "|"
                + fix_none(ns_worker.get('Employee Type',''))
                + "|"
                + self.format_date(fix_none(ns_worker.get('Original Hire Date','')))
                + "|"
                + fix_none(ns_worker.get('Company',''))
                + "|"
                + fix_none(self.extract_employee_id((ns_worker.get('Manager ID',''))))
                + "|"
                + fix_none(ns_worker.get('Cost Center ID',''))
                + "|"
                + fix_none(ns_worker.get('Email - Primary Work',''))
                + "|"
                + fix_none(ns_worker.get('First Name',''))
                + "|"
                + fix_none(ns_worker.get('Last Name',''))
                + "|"
                + fix_none(ns_worker.get('Country',''))
                # + "|"
                # + self.format_date(fix_none(ns_worker.get('Termination Date','')))
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
        _employee_id = re.findall(r'^\d+',employee_id)
                
        if len(_employee_id)>0:
            return fix_none(_employee_id[0])     
        else: 
            return fix_none(employee_id).split(' ')[0].strip()    
        
    def get_employees(self):
        def fixEmployeeID(ns_worker):
            import re
            return self.extract_employee_id(ns_worker.get('Employee ID'))
        
        ret = self.ns_restlet.get_employees()
        
        ret_active = {fixEmployeeID(x):x for x in ret.data if x.get('Employee Status - Active?')=='Actively Employed'}
        return ret_active, {fixEmployeeID(x):self.build_comparison_string(x) for x in ret_active.values()}
        
    def compare_users(self, wd_comp, ns_comp):
        import numpy as np

        add_list = []        
        upd_list = []        
        wd_users_emails = list(wd_comp.keys())
        ns_users_emails = list(ns_comp.keys())
        add_list = np.setdiff1d(wd_users_emails, ns_users_emails)   
        del_list = np.setdiff1d(ns_users_emails, wd_users_emails)     
        intersect_list = np.intersect1d(wd_users_emails, ns_users_emails)

        for upd_email in intersect_list:
            if wd_comp.get(upd_email,'') != ns_comp.get(upd_email,''):
                upd_list.append(upd_email)

        return add_list,  upd_list, del_list
    
    def post_error_report(self, operation):
        output_data = None
        
        for record in self.error_lst:
            employees_data = record[0].get('employees', [])
            error_description = record[1]
            row_id = record[2]
            
            for employee in employees_data:                
                output_data = [{
                    "row_id": row_id,
                    "error_description": error_description,
                    "operation": operation,  
                    "External ID": employee.get("External ID"),
                    "Employee ID": employee.get("Employee ID"),
                    "Last Name": employee.get("Last Name"),
                    "First Name": employee.get("First Name"),
                    "Original Hire Date": employee.get("Original Hire Date"),
                    "Most Recent Hire Date": employee.get("Most Recent Hire Date"),
                    "Termination Date": employee.get("Termination Date"),
                    "Employee Type": employee.get("Employee Type"),
                    "Employee Status - Active?": employee.get("Employee Status - Active?"),
                    "Email - Primary Work": employee.get("Email - Primary Work"),
                    "Manager ID": employee.get("Manager ID"),
                    "Cost Center - ID": employee.get("Cost Center - ID"),
                    "Address1": employee.get("Address1"),
                    "Address2": employee.get("Address2"),
                    "State": employee.get("State"),
                    "City": employee.get("City"),
                    "Zipcode": employee.get("Zipcode"),
                    "Country": employee.get("Country"),
                    "CountryName": employee.get("CountryName"),
                    "Company": employee.get("Company"),
                    "DEFAULT CURRENCY FOR EXP. REPORT": employee.get("DEFAULT CURRENCY FOR EXP. REPORT"),
                    "Payment Method": employee.get("Payment Method"),
                    "Class": employee.get("Class", ""),  
                    "newEmployee": employee.get("newEmployee"),
                    "Rehire": employee.get("Rehire"),
                    "InternationalTransfer": False if not employee.get("InternationalTransfer") else employee.get("InternationalTransfer"),
                    "oldCountryCode": employee.get("oldCountryCode"),
                    "oldCountryName": employee.get("oldCountryName")
                }]
                ret = self.ns_restlet.post_error_report(output_data)
                 
        self.error_lst = []
        
       
    def update(self, wd_workers,
               workers_dict, 
               newEmployee = False,
               reHire = False,
               internationalTransfer = False,
               ns_workers = None,
               wd_comp=None,
               ns_comp=None,
               operation=None,
               max_limit=1
               ):
        import time
        time_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        num_updates = 0
        for i, wd_worker in enumerate(wd_workers):            
            ns_country = self.mapping.map_country(wd_worker.Country)
            
            if not wd_worker.Preferred_Full_Name:
                First_Name = wd_worker.First_Name
                Last_Name = wd_worker.Last_Name
            else:
                # check if there are Chinese chars
                if re.findall(r'[\u4e00-\u9fff]+', wd_worker.Preferred_Full_Name):
                    First_Name = wd_worker.First_Name
                    Last_Name = wd_worker.Last_Name
                else:
                    First_Name = (' ').join(wd_worker.Preferred_Full_Name.split(' ')[0:-1])
                    Last_Name = wd_worker.Preferred_Full_Name.split(' ')[-1]

            #set new contractors' product as 27
            if newEmployee and wd_worker.Employee_Type in ['Elance Contractor', 
                                                           'Independent Contractor',
                                                           'Vendor']:
                class_ = 27 #Business Support
            else:
                class_ = self.get_product_class_map(wd_worker.Product)
                
            # print(wd_comp[wd_worker.Employee_ID])
            # print(ns_comp[wd_worker.Employee_ID])
            employee_data = {
                    "employees": [
                        {
                            "External ID": wd_worker.Employee_ID,
                            "Employee ID": f"{wd_worker.Employee_ID}",
                            "Last Name": Last_Name,
                            "First Name": First_Name,
                            "Legal Name": f"{wd_worker.First_Name} {wd_worker.Last_Name}",
                            "Original Hire Date": wd_worker.Most_Recent_Hire_Date,
                            "Most Recent Hire Date": wd_worker.Most_Recent_Hire_Date,
                            "Termination Date": wd_worker.termination_date if not reHire else None,
                            "Employee Type": wd_worker.Employee_Type,                                                                                 
                            "Employee Status - Active?": 'Actively Employed' if wd_worker.Employee_Status=='1'else 'Terminated'  ,
                            "Email - Primary Work": wd_worker.primaryWorkEmail,
                            "Manager ID": None if wd_worker.Manager_ID==wd_worker.Employee_ID else wd_worker.Manager_ID,
                            "Cost Center - ID": wd_worker.Cost_Center_ID,
                            #"Product": wd_worker.Product,
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
                            "Class": class_,
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
                if len(ret)==0:
                    num_updates+=1
                    self.error_lst.append((employee_data,'success',time_stamp))
                else:                 
                    self.error_lst.append((employee_data,ret,time_stamp))
                    self.logger.info(f"Error while updating Employee ID:{wd_worker.Employee_ID}  error:{ret}") 
                       
                if num_updates>=max_limit:
                    break
            except NetSuiteRestletException as e:
                self.logger.info(f"Employee ID:{wd_worker.Employee_ID} ")
                self.logger.info(f"error {e.args[0].data}")

            except Exception as e:
                self.logger.info(f"error {e}")
                continue
 
        return

    
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
 
    def compare_dates(self, date1: str, date2: str) -> bool:
        # Convert both strings to datetime objects
        date1_obj = datetime.strptime(date1, "%Y-%m-%d")
        date2_obj = datetime.strptime(date2, "%Y-%m-%d")
        if not date2_obj: # date2 expected to be the termination date
            return True
        
        return date1_obj > date2_obj
    
    def transform_data(self, input_tuple):
        result = []
        for index, (employee_data, error_description, timestamp) in enumerate(input_tuple, start=1):
            # Concatenate the timestamp with the row number
            row_id = f"{timestamp}_{index}"
            
            # Create the dictionary with required keys and values
            result.append(
                {'row_id': row_id,'error_description': error_description}| employee_data['employees'][0]
            )
        return self.convert_to_json(result)
     
    def convert_to_json(self, data, filename="output.json"):
        """
        Converts a Python data structure to JSON and saves it to a specified file.

        Parameters:
        - data (list): The Python data structure to convert, typically a list of dictionaries.
        - filename (str): The name of the file to save the JSON data (default is "output.json").
        """
        try:
            with open(filename, 'w', encoding='utf-8') as file:
                json.dump(data, file, indent=4, ensure_ascii=False)
            print(f"Data successfully saved to {filename}")
        except Exception as e:
            print(f"An error occurred while saving to JSON: {e}")

    
    
    def run(self, max_limit):

        """Run all the steps of the integration"""
        operations = [
                Operations.update_employee,
                Operations.rehired,
                Operations.add_new_manager,
                Operations.add_new_hire,
                Operations.international_transfer,
                ]
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

        # ========================================================
        # Step 3: Compare Workday and Netsuite data
        # ========================================================
        self.logger.info("Step 3: Compare Workday and Netsuite data")
        add_list, upd_list, del_list = self.netsuite.compare_users(wd_comp=wd_comp, ns_comp=ns_comp)
        self.logger.critical(f"Diff list {del_list}.")
        # remove terminated employees from add_list
        terminated = [x.Employee_ID for x in wd_workers if x.Employee_Status=='2']
        add_list = [x for x in add_list if x not in terminated]

        # add terminated records to update
        upd_list = upd_list + terminated
        # ========================================================
        #  Step 4: Add rehires
        # ========================================================
        
        try:
            # diff_hire_dates that are in the add_list
            rehires = [x for x in wd_workers if self.compare_dates(x.Most_Recent_Hire_Date, x.Original_Hire_Date)
                        and x.Employee_ID in add_list and x.Employee_Status=='1']           
            
            if Operations.rehired in operations:
                self.logger.critical("Step 4: Add rehires")
                self.netsuite.update(wd_workers=rehires,
                                     max_limit=max_limit,
                                    workers_dict=workers_dict,                                    
                                    newEmployee=False,
                                    reHire=True,
                                    ns_workers=ns_workers,
                                    wd_comp=wd_comp, 
                                    ns_comp=ns_comp,
                                    operation=Operations.rehired
                                    )      
                self.netsuite.post_error_report( "Adding rehires")
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed on Step 4: Add rehires")
             
        # ========================================================
        # Step 5: Add new employees
        # ========================================================
        try:
            
            # remove the rehires from the add_list
            add_list = [x for x in add_list if x not in [x.Employee_ID for x in rehires]]
            wd_workers_add = [x for x in wd_workers if x.Employee_ID in add_list and
                                                    x.Employee_Status =='1']
            # Add managers first
            wd_workers_add_managers = [x for x in wd_workers_add if x.Employee_ID 
                                        in [x.Manager_ID for x in wd_workers_add]]    
            if Operations.add_new_manager in operations:
                self.logger.info("Step 4: Add new employees")
                self.netsuite.update(wd_workers=wd_workers_add_managers,
                                                max_limit=max_limit,
                                                workers_dict=workers_dict,                                                
                                                newEmployee=True,
                                                ns_workers=ns_workers,
                                                operation=Operations.add_new_manager                                            
                                                )
                self.netsuite.post_error_report("Adding new managers")  

            # Adding non managers
            wd_workers_add = [x for x in wd_workers_add if x.Employee_ID 
                            not in [x.Manager_ID for x in wd_workers_add]]            
            
            if Operations.add_new_hire in operations:
                self.netsuite.update(wd_workers=wd_workers_add,
                                     max_limit=max_limit,
                                    workers_dict=workers_dict,                                    
                                    newEmployee=True,
                                    ns_workers=ns_workers,
                                    operation=Operations.add_new_hire                               
                                    )
                self.netsuite.post_error_report("Adding new employees")
                
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed on Step 5: Add new employees")

        # ========================================================
        # Step 6: International Transfers
        # ========================================================
        try:            
            ret = self.workday.get_international_transfers(ns_workers, workers_dict)
            wd_workers_upd = [x for x in wd_workers if x.Employee_ID in [x.Employee_ID for x in ret]]
            
            if Operations.international_transfer in operations:
                self.logger.info("Step 6: International Transfers")
                self.netsuite.update(wd_workers=wd_workers_upd,
                                    newEmployee=False,
                                    max_limit=max_limit,
                                    workers_dict=workers_dict,  
                                    internationalTransfer=True,
                                    ns_workers=ns_workers,
                                    wd_comp=wd_comp, 
                                    ns_comp=ns_comp,
                                    operation=Operations.international_transfer            
                                    )
                self.netsuite.post_error_report("International Transfers")
            
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed on Step 6: International Transfers")

        # ========================================================
        # Step 7: Update employees
        # ========================================================
        try:
            # compare_and_save_data(wd_workers, upd_list, terminated, wd_comp, ns_comp)
            wd_workers_upd = [x for x in wd_workers if x.Employee_ID in upd_list] 
            # wd_workers_upd = [x for x in wd_workers_upd if x.Employee_ID == '205032']
            if Operations.update_employee in operations:
                self.logger.info("Step 7: Update employees")
                self.netsuite.update(wd_workers=wd_workers_upd,
                                     max_limit=max_limit,
                                    workers_dict=workers_dict,                                    
                                    newEmployee=False,
                                    ns_workers=ns_workers,
                                    wd_comp=wd_comp, 
                                    ns_comp=ns_comp,
                                    operation=Operations.update_employee
                                    )
                self.netsuite.post_error_report("Updating employes")
            
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed on Step 7: Update employees")
        
        
        self.logger.info("End of Integration.")

def compare_and_save_data(wd_workers, upd_list, terminated, wd_comp, ns_comp):
            data = []
            wd_workers_upd = [x for x in wd_workers if x.Employee_ID in upd_list]
            wd_workers_upd = [x for x in wd_workers_upd if x.Employee_ID not in terminated]
            for i, wd_worker in enumerate(wd_workers_upd): 
                
                print("---------------------------------------")
                print(wd_comp[wd_worker.Employee_ID])
                print(ns_comp[wd_worker.Employee_ID])                
                # Input strings
                string1 = wd_comp[wd_worker.Employee_ID]
                string2 = ns_comp[wd_worker.Employee_ID]

                # Split the strings by '|'
                list1 = string1.split('|')
                list2 = string2.split('|')

                # Compare the lists
                differences1 = [""]*20
                differences1[0] = wd_worker.Employee_ID                
                differences1[1] = "workday"
                differences2 = [""]*20
                differences2[0] = wd_worker.Employee_ID
                differences2[1] = "netsuite"
                
                for index, (item1, item2) in enumerate(zip(list1, list2)):
                    if item1 != item2:
                        #differences.append((index, item1, item2))
                        differences1[index+2] = item1
                        differences2[index+2] = item2

                data.append(differences1)
                data.append(differences2)            

            csv_file_path = 'employees_upd.csv'  # Path to save the CSV file
            data_to_csv(data, csv_file_path)
            
def data_to_csv(data, csv_file_path):
    import numpy as np
    """
    Converts a data structure to a CSV file using numpy with specific column names.

    Args:
        data (list of lists): The data to be written to the CSV file. Each inner list is a row of data.
        csv_file_path (str): Path to save the CSV file.
    """
    # Define the column names dictionary
    column_names_dict = {0:"external_id", 1:"Employee Type", 2:"Original Hire Date", 3:"Company",
                    4:"Manager ID", 5:"Cost Center", 6:"Email", 7:"First Name", 8:"Last Name",
                    9:"Country", 10:"Employ Status", 11:"Address 1",
                    12:"State", 13:"City", 14:"Zipcode", 15:"Default Currency", 16:"Payment Method",
                    17:"Class"}

    # Extract column names from the dictionary
    column_names = [column_names_dict[i] for i in range(len(column_names_dict))]
    column_names = ["employee ID", "source"] + column_names
    
    # Convert the data to a numpy array
    data_array = np.array(data,dtype="object")

    # Prepend the column names to the data
    data_with_headers = np.vstack([column_names, data_array])

    # Save the data to a CSV file
    np.savetxt(csv_file_path, data_with_headers, fmt='%s', delimiter='|',encoding='utf-8')
    
    print(f"CSV file created successfully at: {csv_file_path}")
    
def main(__name__, WorkdayToNetsuiteIntegration):
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
        default=10
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
    main(__name__, WorkdayToNetsuiteIntegration)
