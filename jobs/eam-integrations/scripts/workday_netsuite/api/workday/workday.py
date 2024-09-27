import json
import logging
import requests
from typing import TypedDict
from datetime import datetime
from dataclasses import dataclass
from typing import Optional
from .secrets import config

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


class WDLink(TypedDict):
    """ TypedDict hints for WorkDay RaaS service."""
    wd_listing_of_workers_link: str


class WorkDayRaaSConfig(TypedDict):
    """TypedDict hints for WorkDay RaaS service."""
    username: str
    password: str
    links: list[WDLink]
    timeout: int


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
                + wd_worker.get('Cost_Center','')
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