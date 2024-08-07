
from workday_everfi.api.Workday import WorkdayAPI
from workday_everfi.api.Everfi import EverfiAPI
from api.util import Util, APIAdaptorException
import argparse
import logging
import sys

def cal_user_location(wd_user, locs, loc_map_table):
    loc = ""
    location_country = wd_user.get("location_country", "")

    if location_country == "Canada":
        loc = loc_map_table.get(wd_user.get("location_province", ""), "")
        if not loc:
            loc = "Federal (Canada)"
    elif location_country == "United States of America":
        # if wd_user.get("location_state", "") == "New York":
        loc = loc_map_table.get(wd_user.get("location_state", ""), "")

        if not loc:
            loc = "United States"
    else:
        loc = "Default"

    id = locs.get(loc)["id"]
    if not id:
        id = locs.get("Default")["Id"]

    logger.debug(
        f"Location id={id} mapped for user {wd_user.get('primary_work_email','')} loc = {loc}"
    )
    return id


class Everfi:
    def __init__(self) -> None:
        self.everfi_api = EverfiAPI()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def get_everfi_users(self, locs, loc_map_table, hire_dates):
        filter = {'filter[active]': 'true'}
        fields = {'fields[users]': 'email,first_name,last_name,sso_id,employee_id,student_id,location_id,active,user_rule_set_roles,category_labels'}
        return self.everfi_api.get_users(fields, filter, locs, loc_map_table, hire_dates)

    def deactivate_users(self, del_list, everfi_users):
        count = 0
        
        for email in del_list:
            id = everfi_users[email].get('id')
            self.everfi_api.deactivate_user(id)
            if '@' in email:
                n = email.split("@")[0]
            else:
                n = email
            self.logger.info(f"{n[:4]} .. {n[-1]} deleted")
            count += 1
            if count % 20 == 0:
                self.logger.info(f"[{count} of {len(del_list)}] users deactivated.")
        return count
        
    def activate_user(self, id):
        self.everfi_api.set_active(id,True)
        
    def get_locations_mapping_table(self):
        return self.everfi_api.get_locations_mapping_table()

    def upd_everfi_users(
        self,
        hire_date_category_id,
        hire_dates,
        locs,
        upd_list_keys,
        wd_users,
        everfi_users,
        loc_map_table,
    ):
        errors_list = []
        count_upd = 0
        loc_id_dict = {x.get('id'):x.get('attributes').get('name') for x in locs.values()}
        
        for email in upd_list_keys:
            wd_user = wd_users[email][1]
            loc_id = cal_user_location(wd_user, locs, loc_map_table)    
            if int(loc_id) != everfi_users[email].get('attributes').get('location_id'):
                if '@' in email:
                    n = email.split("@")[0]
                else:
                    n = email
                self.logger.info(f"User {n[:4]} .. {n[-1]} changed location from {loc_id_dict[str(everfi_users[email].get('attributes').get('location_id'))]} to {loc_id_dict[loc_id]}")
            json_data = {
                "data": {
                    "type": "registration_sets",
                    "id": everfi_users[email]["id"],
                    "attributes": {
                        "registrations": [
                            {
                                "rule_set": "user_rule_set",
                                "first_name": wd_user.get("preferred_first_name", ""),
                                "last_name": wd_user.get("preferred_last_name", ""),
                                "location_id": loc_id,
                                "employee_id": wd_user.get("employee_id", ""),
                                "sso_id": email,
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
                r = self.everfi_api.upd_user(everfi_users[email]["id"], json_data)
            except Exception as e:
                self.logger.exception(e)
                errors_list.append(e)
            
            cat_label_user_id = self.get_category_label_user_id(everfi_users[email]["id"])
            if cat_label_user_id:
                self.delete_category_label_user(cat_label_user_id)

            #wd_users[email][1]["hire_date"] = '2024-07-10'
            hire_date_id = self.get_hire_date_id(
                wd_users[email][1]["hire_date"], hire_date_category_id, hire_dates
            )

            try:
                r = self.everfi_api.assign_label_user(
                    r.data.get("data").get("id"), hire_date_id
                )
            except Exception as e:
                self.logger.exception(e)
                errors_list.append(e)
            
            if count_upd % 20 == 0:
                self.logger.info(f"[{count_upd} of {len(upd_list_keys)}] users updated.")
                
            count_upd += 1
    
        return count_upd
    
    def get_category_label_user_id(self, id):
        ret = self.everfi_api.get_category_label_user_id(id)   
        if len(ret.data.get('data',''))>0:
            return ret.data.get('data','')[0].get('id','')
        else:
            return None
    
    def delete_category_label_user(self, id):
        ret = self.everfi_api.delete_category_label_user(id)               
        return ret
    
    def bulk_clear_category_id(self, ids, category_id,category_label):
        return self.everfi_api.bulk_clear_category_id(ids, category_id,category_label)

    def get_hire_date_id(self, wd_hire_date, hire_date_category_id, hire_dates):
        wd_hire_date = wd_hire_date.split("-")
        wd_hire_date = wd_hire_date[1] + "-" + wd_hire_date[0]
        hire_date_id = hire_dates.get(wd_hire_date)
        if not hire_date_id:
            # add new hire date
            r = self.everfi_api.add_hire_date(
                name=wd_hire_date, category_id=hire_date_category_id
            )
            id = r.data.get("data").get("id")
            hire_dates[wd_hire_date] = id
            return r.data.get("data").get("id")
        return hire_date_id

    def add_everfi_users(
        self,
        hire_date_category_id,
        hire_dates,
        locs,
        add_list_keys,
        wd_users,
        loc_map_table,
    ):
        errors = []
        count_add = 0
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
                                "sso_id": email,
                                "employee_id": wd_user.get("employee_id", ""),
                                "location_id": loc_id,
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
                self.logger.info("Trying to activate user and update ")
                if (e.args[0][0].get('id','')=='user_rule_set'):
                    # try to active user
                    # find user by email and then update the user with current data
                    filter = {'filter[email]': wd_user.get("primary_work_email", "")}
                    fields = {'fields[users]': 'id,email'}                    
                    #find user id
                    user = self.everfi_api.search_user(fields, filter)
                    id = user.get(email,'').get('id', '')
                    if id:
                        #self.activate_user(id)
                        json_data['data']['id'] = id
                        json_data['data']['attributes']['registrations'][0]['active'] = True 
                        #active user and update fields
                        r = self.everfi_api.upd_user(id, json_data) 
                        #remove hire date custom field
                       
                        #hd = wd_users[email][1]["hire_date"].split('-') 
                        cat_label_user_id = self.get_category_label_user_id(id)
                        if cat_label_user_id:
                            self.delete_category_label_user(cat_label_user_id)
                        #self.bulk_clear_category_id([id], hire_date_category_id, hd[1] + '-' + hd[0])
                    else:                        
                        errors.append(e)
                        continue

            
            #wd_users[email][1]["hire_date"] = '2024-07-10'
            hire_date_id = self.get_hire_date_id(
                wd_users[email][1]["hire_date"], hire_date_category_id, hire_dates
            )

            try:
                r = self.everfi_api.assign_label_user(
                    r.data.get("data").get("id"), hire_date_id
                )
            except Exception as e:
                self.logger.exception(e)
                errors.append(e)

            count_add += 1
            
            if '@' in email:
                n = email.split("@")[0]
            else:
                n = email
            self.logger.info(f"{n[:4]} .. {n[-1]} added")
            
            if count_add % 20 == 0:
                self.logger.info(f"[{count_add} of {len(add_list_keys)}] users added.")
            
            
        
        return count_add

class Workday:
    def build_comparison_string(self, wd_row, locs, loc_map_table):
        loc_id = cal_user_location(wd_row, locs, loc_map_table)
        hire_date = wd_row['hire_date'].split('-')
        
        is_manager = "supervisor" if wd_row.get("is_manager", "") else "non_supervisor"
        return (
            wd_row["primary_work_email"]
            + "|"
            + wd_row["preferred_first_name"]
            + "|"
            + wd_row["preferred_last_name"]
            + "|"
            + wd_row["employee_id"]
            + "|"
            + loc_id
            + "|"
            + is_manager
            + "|"
            + hire_date[1] + "-" + hire_date[0]
            + "|"
            + wd_row["primary_work_email"]
        )

    def get_wd_users(self, locs, loc_map_table):
        import pandas as pd
        import io

        # The API is not returning all fields in the json
        # but the csv is, so we will use the csv version
        # wd_users_csv = WorkdayAPI.get_datawarehouse_workers_csv()
        workday_api = WorkdayAPI()
        wd_users_csv = workday_api.get_datawarehouse_workers_csv()
        df = pd.read_csv(io.StringIO(wd_users_csv), sep=",")
        filtered = df[
            (df["currently_active"] == True)
            & (df["moco_or_mofo"] == "MoCo")
            & (df["worker_type"] == "Employee")
            | (df['primary_work_email'] == "jmoscon@mozilla.com")
        ]

        comp = {
            x[1]["primary_work_email"]: self.build_comparison_string(
                x[1], locs, loc_map_table
            )
            for x in filtered.iterrows()
        }
        return comp, {x[1]["primary_work_email"]: x for x in filtered.iterrows()}


class WorkdayEverfiIntegration:
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

 
        return add_list, del_list, upd_list

    def run(self, limit):
        # ========================================================
        # Getting Everfi hire dates, locations and locations mapping table ...
        # ========================================================        
        try:
            self.logger.info("Getting everfi hire dates")
            hire_date_category_id, hire_dates = self.everfi.everfi_api.get_hire_dates()           
            self.logger.info(f"Number of hire dates: {len(hire_dates)}")

            self.logger.info("Getting everfi locations")
            locs = self.everfi.everfi_api.get_locations()
            self.logger.info(f"Number of locations: {len(locs)}")

            self.logger.info("Getting everfi mapping table")
            loc_map_table = self.everfi.everfi_api.get_locations_mapping_table()
            self.logger.info(f"Number of mappins: {len(loc_map_table)}")

        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed while Getting Everfi hire dates,locations and locations mapping table ...")            
            sys.exit(1)

        # ========================================================
        # Getting Workday users...
        # ========================================================
        self.logger.info("Getting Workday users...")
        try:
            wd_comp, wd_users = self.workday.get_wd_users(locs, loc_map_table)
            self.logger.info(f"Number of wd users: {len(wd_users)}")
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed while Getting Workday users...")           
            sys.exit(1)

        # ========================================================
        # Getting Everfi users...
        # ========================================================
        self.logger.info("Getting Everfi users...")
        try:
            everfi_comp, everfi_users = self.everfi.get_everfi_users(locs, loc_map_table, hire_dates)
            self.logger.info(f"Number of Everfi users: {len(everfi_users)}")
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed while Getting Everfi users...")
            sys.exit(1)
            
        # ========================================================
        # Comparing users...
        # ========================================================
        self.logger.info("Comparing users...")
        try:
            add_list, del_list, upd_list = integration.compare_users(
                wd_comp, everfi_comp, wd_users, everfi_users
            )

            self.logger.info(f"Number of users to delete w/o limit={len(del_list)} with limit={len(del_list[:limit])}")
            self.logger.info(f"Number of users to add w/o limit={len(add_list)} with limit={len(add_list[:limit])}")
            self.logger.info(f"Number of users to update w/o limit={len(upd_list)} with limit={len(upd_list[:limit])}")

            del_list = del_list[:limit]
            add_list = add_list[:limit]
            upd_list = upd_list[:limit]
  
        except (Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed while Comparing users...")
            sys.exit(1)
<<<<<<< HEAD
=======
        
>>>>>>> main
            
        # ========================================================
        # Deleting Everfi users ...
        # ========================================================
        self.logger.info("Deleting Everfi users ...")        
        try:
             
            count_dels = self.everfi.deactivate_users(del_list, everfi_users)
            self.logger.info(f"Number of users deleted {count_dels}")
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Faile while Deleting Everfi users ...")
            sys.exit(1)
            
        # ========================================================
        # Adding Everfi users ...
        # ========================================================
        self.logger.info("Adding Everfi users ...")
        try:
            count_add = self.everfi.add_everfi_users(
                hire_date_category_id, hire_dates, locs, add_list, wd_users, loc_map_table
            )
            self.logger.info(f"Number of users added {count_add}")            
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed while Adding Everfi users ...")
            sys.exit(1)
        # ========================================================
        # Updating Everfi users ...
        # ========================================================
        self.logger.info("Updating Everfi users ...")
        
        try:
            count_upd = self.everfi.upd_everfi_users(
                hire_date_category_id,
                hire_dates,
                locs,
                upd_list,
                wd_users,
                everfi_users,
                loc_map_table,
            )
            self.logger.info(f"Number of users updated {count_upd}")
        except (APIAdaptorException, Exception) as e:
            self.logger.error(str(e))
            self.logger.critical("Failed while Updating Everfi users ...")
            sys.exit(1)
        
        self.logger.info("End of integration")

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
        "-m",
        "--max_limit", 
        action="store",
        type=int,
        help="limit the number of changes in Everfi",        
        default=10
    )
    args = None
    args = parser.parse_args()
    
    log_level = Util.set_up_logging(args.level)

    logger = logging.getLogger(__name__)

    logger.info("Starting...")
    logger.info(f"max_limit={args.max_limit}")

    integration = WorkdayEverfiIntegration()

    integration.run(args.max_limit)
