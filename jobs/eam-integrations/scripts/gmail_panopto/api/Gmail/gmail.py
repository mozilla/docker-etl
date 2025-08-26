from google.oauth2 import service_account
from api.util.base import BaseAPI
from googleapiclient.discovery import build


class GmailAPI(BaseAPI):
    def __init__(self) -> None:
        super().__init__(__file__)
        self.SCOPES=["https://www.googleapis.com/auth/admin.directory.group.readonly","https://www.googleapis.com/auth/apps.groups.settings"]
        self.secrets_dict['private_key'] = self.secrets_dict['private_key'].replace("\\n", "\n")
        self.credentials = service_account.Credentials.from_service_account_info( self.secrets_dict, 
                                                                     scopes=self.SCOPES, 
                                                                     subject=self.secrets_dict['client_email'])
        self.service_directory = build("admin", "directory_v1", credentials=self.credentials)

    def get_group(self, group_key):
        return self.service_directory.members().list(groupKey=group_key).execute()
        