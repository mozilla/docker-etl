import argparse
from copyreg import pickle
import logging
from workday_docsign.Workday import WorkdayAPI
from workday_docsign.DocuSign import DocuSignAPI


class WorkdayDocusignIntegration():
    def __init__(self):
        self.docusignAPI = DocuSignAPI()
        self.workdayAPI = WorkdayAPI()
        self._logger = logging.getLogger(__name__)  
    

    def compare_users(self, wd_comp, ds_comp, wd_users, ds_users):
        import numpy as np

        add_list = []
        del_list = []
        upd_list = []
        wd_users_emails = list(wd_users.keys())
        ds_users_emails = list(ds_users.keys())
        add_list = np.setdiff1d(wd_users_emails, ds_users_emails)
        del_list = np.setdiff1d(ds_users_emails, wd_users_emails)
        intersect_list = np.intersect1d(wd_users_emails, ds_users_emails)

        for upd_email in intersect_list:
            if ds_comp[upd_email] != wd_comp[upd_email]:
                upd_list.append(upd_email)
 
        return add_list, del_list, upd_list
    
    def run(self):
        
        ################################################
        # 1) Get Workday Users
        ################################################       
        self._logger.info('Getting Workday Users...')
        try:      
            wd_comp, wd_users = self.workdayAPI.get_wd_users()
        except Exception as e:
            self._logger.error(f"Failed while getting Workday Users: {e}")
            self._logger.error("Exiting...")
            exit(1)
 
        ################################################
        # 2) Get DocuSign Contacts
        ################################################
        self._logger.info('Getting DocuSign Contacts...')
        try:        
            ds_comp, ds_contacts = self.docusignAPI.get_contacts()              
        except Exception as e:
            self._logger.error(f"Failed while getting DocuSign Users: {e}")
            self._logger.error("Exiting...")
            exit(1)

        ################################################
        # 3) Compare and add missing contacts
        ################################################
        self._logger.info('Comparing Users...')
        add_list, del_list, upd_list = integration.compare_users(
                wd_comp, ds_comp, wd_users, ds_contacts
            )
        ################################################
        # 4) Add contacts to DocuSign
        ################################################
        self._logger.info(f'Adding {len(add_list)} contacts, Deleting {len(del_list)} contacts, Updating {len(upd_list)} contacts...')
        for email in add_list:
            try:
                contact = wd_users.get(email, None)
                if email not in ds_contacts.keys():
                    self.docusignAPI.add_contact(email, contact[1].get("legal_first_name",''), contact[1].get("legal_last_name",''))
            except Exception as e:
                self._logger.error(f"Error adding contact {email}: {e}")

        ##############################################
        # 5) Delete contacts from DocuSign
        ##############################################
        self._logger.info(f'Deleting {len(del_list)} contacts...')
        for email in del_list:
            contact = ds_contacts.get(email, None)
            if contact:
                try:
                    self.docusignAPI.delete_contact(contact.get('contactId',''))
                except Exception as e:
                    self._logger.error(f"Error deleting contact {email}: {e}")

        ###############################################
        # 6) Update contacts in DocuSign
        ###############################################
        self._logger.info(f'Updating {len(upd_list)} contacts...')
        for email in upd_list:
            try:
                self.docusignAPI.delete_contact(ds_contacts[email].get('contactId',''))
                self.docusignAPI.add_contact(email, wd_users[email][1].get("legal_first_name",''), wd_users[email][1].get("legal_last_name",''))
            except Exception as e:
                self._logger.error(f"Error updating contact {email}: {e}")
            
        self._logger.info('End of integration run')
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Workday DocuSign Integration ")
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

    #log_level = Util.set_up_logging(args.level)
    logging.basicConfig(filename="workday_docusign.log",
                    filemode='a',
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging.INFO,
        encoding="utf-8")

    logger = logging.getLogger(__name__)

    logger.info("Starting...")
  
    integration = WorkdayDocusignIntegration()
    integration.run()
    
    
    