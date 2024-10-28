import json
import logging
import requests
from typing import TypedDict
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List
from .secrets import config
from typing import List, Optional

@dataclass
class Worker:
    """ Dataclass to be used to unpack the results of get_listing_of_workers WD RaaS service. """
    External_ID: Optional[int] = None
    Original_Hire_Date: Optional[datetime] = None
    Employee_Type: Optional[str] = None
    Company: Optional[str] = None
    Manage_Email_Address: Optional[str] = None
    Manager_ID: Optional[int] = None
    Cost_Center: Optional[str] = None
    Employee_ID: Optional[str] = None
    Product: Optional[str] = None
    Cost_Center_ID: Optional[int] = None
    Manager: Optional[str] = None
    primaryWorkEmail: Optional[str] = None
    First_Name: Optional[str] = None
    Employee_Status: Optional[int] = None
    Last_Name: Optional[str] = None
    Most_Recent_Hire_Date: Optional[datetime] = None
    Country: Optional[str] = None
    termination_date: Optional[datetime] = None
    Preferred_Full_Name: Optional[str] = None
    City: Optional[str] = None
    Home_Country: Optional[str] = None
    State: Optional[str] = None
    Primary_Address: Optional[str] = None
    Postal: Optional[str] = None
    Province: Optional[str] = None


@dataclass
class InternationalTransfer:
    Full_Name: Optional[str]
    New_Country: Optional[str]
    Employee_Type: Optional[str]
    Old_Country: Optional[str]
    Employee_ID: Optional[str]
    Manager: Optional[str]
    Intl_Transfer_Date: Optional[str]

@dataclass
class Report:
    Report_Entry: Optional[List[InternationalTransfer]]


class WorkDayRaaService():
    """Workday RaaS service implementation""" 
    def __init__(self):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)

    def build_comparison_string(self, wd_worker):
            return (
                wd_worker.get('Employee_ID','')
                + "|" 
                + wd_worker.get('Employee_Type','')
                + "|" 
                + wd_worker.get('Original_Hire_Date','')
                + "|"
                + wd_worker.get('Company','')
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
                + wd_worker.get('Country','')
                + "|"
                + wd_worker.get('termination_date','') 
 
            )

    def get_listing_of_workers(self) -> list[Worker]:

        """Get  listing of workers report data from WorkDay"""
        self.logger.info('Getting listing of workers from WorkDay.')
        result = requests.get(self.config['links']['wd_listing_of_workers_link'],
                              auth=(self.config['username'],
                                    self.config['password']),
                              timeout=self.config['timeout'])
        
        return json.loads(result.text)
    
        worker_dict = {}
        wd_data = json.loads(result.text)
        worker_list = []
        wd_comp ={}
        for worker in wd_data["Report_Entry"]:
            worker['Cost_Center_ID'] = worker.pop('Cost_Center_-_ID')
            worker_list.append(Worker(**worker))
            worker_dict[worker['Employee_ID']] = worker
            wd_comp[worker['Employee_ID']] = self.build_comparison_string(worker)
        return worker_list,worker_dict, wd_comp
    
    def get_international_transfers(self, begin_date, end_date):
        self.logger.info('Getting listing of workers from WorkDay.')
        link = self.config['links']['wd_international_transfers_link']
        result = requests.get(link.format(end_date=end_date, begin_date=begin_date),
                              auth=(self.config['username'],
                                    self.config['password']),
                              timeout=self.config['timeout'])
        
        return json.loads(result.text)