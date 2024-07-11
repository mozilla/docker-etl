# %%


#from workday_everfi.api import Workday as WorkdayAPI
from workday_everfi.api.Workday import WorkdayAPI
from api.util import Util, cache_pickle

from workday_everfi.api.Everfi import EverfiAPI

import argparse
import logging

def cal_user_location(wd_user, locs,loc_map_table):

    loc = ""
    location_country = wd_user.get("location_country", "")

    if location_country == "Canada":
        loc = loc_map_table.get(wd_user.get("location_province", ""), "")
        if not loc:
            loc = "Federal (Canada)"
    elif location_country == "United States of America":
        #if wd_user.get("location_state", "") == "New York":
        loc = loc_map_table.get(wd_user.get("location_state", ""), "")

        if not loc:
            loc = "United States"
    else:
        loc = "Default"
    
    id = locs.get(loc)["id"]
    if not id:
        id = locs.get("Default")["Id"]
    
    logger.debug(f"Location id={id} mapped for user {wd_user.get('primary_work_email','')} loc = {loc}")
    return id


class Everfi():
    def __init__(self) -> None:
        self.everfi_api = EverfiAPI()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    # @cache_pickle
    def get_everfi_users(self,locs, loc_map_table):
        fields = "email,first_name,last_name,sso_id,employee_id,student_id,location_id,active,user_rule_set_roles,category_labels" 
        return self.everfi_api.get_users(fields, filter, locs, loc_map_table)

    def get_locations_mapping_table(self):     
        return self.everfi_api.get_locations_mapping_table()
    
    def upd_everfi_users(self, hire_date_category_id, hire_dates, locs, upd_list_keys, wd_users , everfi_users, loc_map_table):
        errors_list = []
        
        for email in upd_list_keys:
            wd_user = wd_users[email][1]
            loc_id = cal_user_location(wd_user, locs, loc_map_table)
            self.logger.info(f"Updating user {email}")
            json_data = {
                "data": {
                    "type": "registration_sets",
                    "id": everfi_users[email]['id'],
                    "attributes": {
                        "registrations": [
                            {
                                "rule_set": "user_rule_set",
                                "first_name": wd_user.get("preferred_first_name", ""),
                                "last_name": wd_user.get("preferred_last_name", ""),
                                "location_id": loc_id,
                                "employee_id":  wd_user.get("employee_id", ""),
                                "sso_id":  wd_user.get("employee_id", ""),            
                                                    
                            },
                            {
                                "rule_set": "cc_learner",
                                "role": "supervisor"
                                if wd_user.get("is_manager", "")
                                else "non_supervisor",
                            },
                        ],
                    },
                },
            }
            try:
                r = self.everfi_api.upd_user(everfi_users[email]['id'], json_data)
            except Exception as e:
                self.logger.exception(e)
                errors_list.append(e)
    
    def get_hire_date_id(self, wd_hire_date, hire_date_category_id, hire_dates):
        wd_hire_date = wd_hire_date.split('-')
        wd_hire_date = wd_hire_date[1] + "-" + wd_hire_date[0]
        hire_date_id = hire_dates.get(wd_hire_date)
        if not hire_date_id:
            #add new hire date
            r = self.everfi_api.add_hire_date(name=wd_hire_date, category_id=hire_date_category_id)
            id = r.data.get('data').get('id')
            hire_dates[wd_hire_date] = id
            return r.data.get('data').get('id')
        return hire_date_id
        
    def add_everfi_users(self, hire_date_category_id, hire_dates, locs, add_list_keys, wd_users,loc_map_table):
        errors = []
        
        for email in add_list_keys:
            wd_user = wd_users[email][1]
            loc_id = cal_user_location(wd_user, locs, loc_map_table)
            json_data = {
                "data": {
                    "type": "registration_sets",
                    "attributes": {
                        "registrations": [
                            {
                                "rule_set": "user_rule_set",
                                "first_name": wd_user.get("preferred_first_name", ""),
                                "last_name": wd_user.get("preferred_last_name", ""),
                                "email": wd_user.get("primary_work_email", ""),
                                "sso_id": wd_user.get("employee_id", ""),
                                "employee_id": wd_user.get("employee_id", ""),
                                "location_id": loc_id
                            },
                            {
                                "rule_set": "cc_learner",
                                "role": "supervisor"
                                if wd_user.get("is_manager", "")
                                else "non_supervisor",
                            },
                        ],
                    },
                },
            }
            try:
                r = self.everfi_api.add_user(json_data)
            except Exception as e:
                self.logger.exception(e)
                errors.append(e)
                continue

            logger.info(f"Setting hire data for user {email}")
            
            hire_date_id = self.get_hire_date_id(wd_users[email][1]['hire_date'], hire_date_category_id, hire_dates)
            
            try:
                self.everfi_api.assign_label_user(r.data.get('data').get('id'), hire_date_id)
            except Exception as e:
                self.logger.exception(e)
                errors.append(e)

            self.logger.info(f"New user { wd_user.get('primary_work_email','')} created.")
            # r = everfi_api.api_adapter.post(endpoint=endpoint,headers=headers,data=json_data)


class Workday():
    
    def build_comparison_string(self,wd_row,locs,loc_map_table):
        loc_id = cal_user_location(wd_row, locs,loc_map_table)
        is_manager = "supervisor" if wd_row.get("is_manager", "") else "non_supervisor"
        return wd_row['primary_work_email'] + "|" +\
                wd_row['preferred_first_name'] + "|" +\
                wd_row['preferred_last_name'] + "|" +\
                wd_row['employee_id'] + "|" +\
                loc_id + "|" +\
                is_manager
                
                
    def get_wd_users(self,locs,loc_map_table):
        import pandas as pd
        import io 
            
        # The API is not returning all fields in the json
        # but the csv is, so we will use the csv version
        #wd_users_csv = WorkdayAPI.get_datawarehouse_workers_csv()
        workday_api = WorkdayAPI()
        wd_users_csv = workday_api.get_datawarehouse_workers_csv()
        df = pd.read_csv(io.StringIO(wd_users_csv), sep=",")
        filtered = df[
            (df["currently_active"] == True)
            & (df["moco_or_mofo"] == "MoCo")
            & (df["worker_type"] == "Employee")
        ]
        #filtered = filtered[(filtered["primary_work_email"] == "daabel@mozilla.com")]
        #filtered.to_csv('file1.csv')
        comp = {x[1]['primary_work_email']:self.build_comparison_string(x[1],locs,loc_map_table) for x in filtered.iterrows()}
        return comp, {x[1]["primary_work_email"]: x for x in filtered.iterrows()}


class WorkdayEverfiIntegration():

    def __init__(self) -> None:
        self.workday = Workday()
        self.everfi = Everfi()        
        self.logger = logging.getLogger(self.__class__.__name__)

    def compare_users(self, wd_comp, everfi_comp, wd_users, everfi_users):
        import numpy as np

        add_list = []
        del_list = []
        upd_list = []
        wd_users_emails = list(wd_users.keys())
        everfi_users_emails = list(everfi_users.keys())
        add_list = np.setdiff1d(wd_users_emails, everfi_users_emails)
        del_list = np.setdiff1d(everfi_users_emails, wd_users_emails)
        intersect_list = np.intersect1d(wd_users_emails, everfi_users_emails)
        
        for upd_email in intersect_list:
            if everfi_comp[upd_email] != wd_comp[upd_email]:
                upd_list.append(upd_email)
        
        # TODO remove jmoscon(@mozilla.com")        
        del_list = np.delete(del_list, np.where(np.isin(del_list,["jmoscon@mozilla.com","jcmoscon@mozilla.com"])))
        return add_list, del_list, upd_list

    def run(self):
        hire_date_category_id, hire_dates = self.everfi.everfi_api.get_hire_dates()
        
        #========================================================
        # Getting Everfi locations and locations mapping table ...
        #========================================================
        self.logger.info("Getting Everfi locations ...")
        
        locs = self.everfi.everfi_api.get_locations()
        loc_map_table = self.everfi.everfi_api.get_locations_mapping_table()
                
        #========================================================
        # Getting Workday users...
        #========================================================
        self.logger.info("Getting Workday users...")
        wd_comp, wd_users = self.workday.get_wd_users(locs,loc_map_table)

        #========================================================
        # Getting Everfi users...
        #========================================================
        self.logger.info("Getting Everfi users...")
        everfi_comp, everfi_users = self.everfi.get_everfi_users(locs,loc_map_table)

        #========================================================
        # Comparing users...
        #========================================================
        self.logger.info("Comparing users...")
        add_list, del_list, upd_list = integration.compare_users(wd_comp,everfi_comp, wd_users, everfi_users)

        #========================================================
        # Deleting Everfi users ...
        #========================================================
        self.logger.info("Deleting Everfi users ...")
        self.everfi.everfi_api.deactivate_users(del_list, everfi_users)
        
        #========================================================
        # Adding Everfi users ...
        #========================================================
        self.logger.info("Adding Everfi users ...")    
        self.everfi.add_everfi_users(hire_date_category_id, hire_dates, locs, add_list, wd_users, loc_map_table)

        #========================================================
        # Updating Everfi users ...
        #========================================================
        self.logger.info("Updating Everfi users ...")        
        self.everfi.upd_everfi_users(hire_date_category_id, hire_dates, locs, upd_list, wd_users, everfi_users, loc_map_table)
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync up XMatters with Workday")

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
        "--force",
        action="store_true",
        help="force changes even if there are a lot",
    )

    args = parser.parse_args()

    log_level = Util.set_up_logging(args.level)
    
    logger = logging.getLogger(__name__)
 
    
    logger.info("Starting...")

    integration = WorkdayEverfiIntegration()

    integration.run()
