from requests_oauthlib import OAuth1
from .secrets import config
from api.util import APIAdaptor
import logging

 
class DocuSignAPI():
    def __init__(self) -> None:
        self.api_adapter = APIAdaptor(host=config.get('token_host'))
        self.token = self.get_auth_token()
        self.api_adapter.url = config.get('host')
        self.config_id = config.get('account_id')
        
    def get_auth_token(self):
        params ={
            'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
            'assertion': config.get('docusign_jwt')
        }
        endpoint = "oauth/token"
        ret = self.api_adapter.post(endpoint=endpoint, params=params)
        return ret.data.get('access_token','')
    
    def get_users(self):
        headers = {
            "Accept" : "application/json",
            "Authorization" : f"Bearer {self.token}"
        }
        endpoint = f"accounts/{self.config_id}/users"
        
        return self.api_adapter.get(endpoint=endpoint, headers=headers)
    
    def get_contacts(self):
        headers = {
            "Accept" : "application/json",
            "Authorization" : f"Bearer {self.token}"
        }
        endpoint = f"accounts/{self.config_id}/contacts"
        start_position = 0
        ret_dict = {}
        comp = {}
        while True:
            parameters = {
                "count": 500,
                "start_position": start_position    
            }
            ret =  self.api_adapter.get(endpoint=endpoint, headers=headers, params=parameters)
            ret_dict.update({x.get('emails')[0]:x for x in ret.data.get('contacts',[])})
            comp.update({x.get('emails')[0]:x.get('name') for x in ret.data.get('contacts')})
            
            if int(ret.data.get('totalSetSize')) > len(ret_dict):
                start_position = int(ret.data.get('endPosition',0)) + 1
            else:
                break 
         
        return comp, ret_dict

    def add_contact(self, email, first_name, last_name):
        headers = {
            "Accept" : "application/json",
            "Authorization" : f"Bearer {self.token}",
        }
        data = {
                "contactList": [
                    {
                    "emails": [
                        email
                    ],
                    "shared": "true",
                    "name": first_name + " " + last_name,
                    "organization": "Mozilla"
                    }
                ]
                }
        endpoint = f"accounts/{self.config_id}/contacts"

        return self.api_adapter.post(endpoint=endpoint, headers=headers, data=data)
    
    def delete_contact(self, contact_id):
        headers = {
            "Accept" : "application/json",
            "Authorization" : f"Bearer {self.token}"
        }
        endpoint = f"accounts/{self.config_id}/contacts/{contact_id}"

        return self.api_adapter.delete(endpoint=endpoint, headers=headers)