#https://support.panopto.com/resource/APIDocumentation/Help/html/922f471b-b3e6-d46a-8157-b7a73fc607c1.htm
from zeep import Client
import os
import sys
from argparse import ArgumentParser
from api.util import Util, APIAdaptorException

from gmail_panopto.api.Panopto import PanoptoAPI
from gmail_panopto.api.Gmail import GmailAPI
import logging


class GmailPanoptoIntegration():
    def __init__(self) -> None:
        self.panopto_api = PanoptoAPI() 
        self.gmail_api = GmailAPI()
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def run(self):        
        
        gmail_group_name = os.getenv('gmail_panopto_gmail_group_name') 
        if not gmail_group_name:
            self.logger.error('GMAIL group name not set in env variable gmail_panopto_gmail_group_name. Halting the integration.')
            sys.exit(1)
            
        panopto_group_name = os.getenv('gmail_panopto_panopto_group_name') 
        if not panopto_group_name:
            self.logger.error('Panopto group name not set in env variable gmail_panopto_panopto_group_name. Halting the integration.')
            sys.exit(1)
        
        #############################################################
        # Step 1: Get emails from Gmail user group
        #############################################################
        self.logger.info("Step 1: Get emails from Gmail user group")
        try:
            
            gmail_groups = self.gmail_api.get_group(gmail_group_name)
            self.logger.info("Gmail group found")
        except Exception as e:
            self.logger.error(str(e))
            self.logger.info(f"Step 1: Failed to get gmail group {gmail_group_name}. Halting the integration.")
            sys.exit(1)
            
        gmail_members = gmail_groups.get('members', [])
        gmail_members_emails = {x.get('email'):x for x in gmail_members if x.get('status') == 'ACTIVE' and x.get('type')=='USER'}
        self.logger.info(f"Step 1: Found {len(gmail_members_emails)} members in the gmail group {gmail_group_name}")
        
        #############################################################
        # Step 2: Get Panopto Users
        #############################################################
        self.logger.info("Step 2: Get dict of all users from Panopto and user group")
        #panopto_dict_user_id, panopto_dict_email = self.panopto_api.get_users_dict()
        
        try:            
            group_id = self.panopto_api.get_groups_by_name(panopto_group_name)
        except Exception as e:
            self.logger.error(str(e))
            self.logger.info(f"Step 2: Failed while running get_groups_by_name for group {panopto_group_name}. Halting the integration.")
            sys.exit(1) 
            
        if group_id:
            panopto_user_group_users = self.panopto_api.get_users_in_group(group_id[0])            
            self.logger.info(f"Step 2: Found {len(panopto_user_group_users)} users in the Panopto group {panopto_group_name}")
            
        else:
            self.logger.error('Panopto group id not found')
            self.logger.info("Step 2: Failed to find Panopto group id. Halting the integration.")
            sys.exit(1)
            
        #############################################################
        # Step 3: Build Add and Del list
        #############################################################
        self.logger.info("Step 3: Build Add list")        
        # gmail emails found in panopto
        panopto_gmail_users = []
        add_list = []
        del_list = []        
        # Build Add list
        for email in gmail_members_emails:
            panopto_user = self.panopto_api.get_user_by_key(f'MozillaMain\\{email}')
            #panopto_user = panopto_dict_email.get(email)
            if panopto_user.UserId != '00000000-0000-0000-0000-000000000000':
                panopto_gmail_users.append(panopto_user)
                if panopto_user.UserId not in panopto_user_group_users:
                    add_list.append(panopto_user.UserId)
            else:
                self.logger.info(f"Email {email} not found in Panopto")
        #Build Del list
        for user in self.panopto_api.get_users(panopto_user_group_users):
            #user = panopto_dict_user_id.get(email)
            if user.Email not in gmail_members_emails:            
                del_list.append(user.UserId)
        
        # self.panopto_api.create_user('jmoscon@mozilla.com')
        
        #############################################################
        # Step 4: Remove user from Panopto user group
        #############################################################
        if del_list:
            try:
                self.panopto_api.remove_members_from_internal_group(group_id[0],del_list)
            except Exception as e:
                self.logger.error(str(e))
                self.logger.info("Step 4: Failed while running remove_members_from_internal_group. Halting the integration.")
                sys.exit(1)
        
        #############################################################
        # Step 5: Add users to Panopto user group
        #############################################################
        if add_list:
            try:
                self.panopto_api.add_members_to_internal_group(group_id[0],add_list)
            except Exception as e:
                self.logger.error(str(e))
                self.logger.info("Step 5: Failed while running add_members_to_internal_group. Halting the integration.")
                sys.exit(1)

        self.logger.info("Integration run complete")
        
def main(__name__):
    
    
    parser = ArgumentParser(description="Gmail Panopto Integration ")

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
        default=100
    )
    args = None
    args = parser.parse_args()
    log_level = Util.set_up_logging(args.level)

    logging.basicConfig(filename="gmail_panopto_integration.log",
                filemode='a',
    format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
    level=logging.INFO,
    encoding="utf-8")

    logger = logging.getLogger(__name__)
    
    integration = GmailPanoptoIntegration()
    integration.run()
    
if __name__ == "__main__":
    main(__name__)
