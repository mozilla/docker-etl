
from requests_oauthlib import OAuth1
from .secrets import config
from api.util import APIAdaptor
import logging

class NetSuiteRestletException(Exception):
    pass


class NetSuiteRestlet():
    def __init__(self) -> None:
        self.api_adapter = APIAdaptor(host=config.get('host'))
        self.auth = self.createAuth()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.headers = {"Content-Type": "application/json"}
        self.endpoint = f"/app/site/hosting/restlet.nl"

    def createAuth(self):
        auth = OAuth1(
            client_key=config.get("consumer_key"),
            client_secret=config.get("consumer_secret"),
            resource_owner_key=config.get("token_id"),
            resource_owner_secret=config.get("token_secret"),
            realm=config.get("oauth_realm"),
            signature_method="HMAC-SHA256",
        )

        return auth

    def run_get(self, params):
        return  self.api_adapter.get(endpoint=self.endpoint, auth=self.auth, params=params,
                              headers=self.headers) 
        
    def get_product_class_mapping(self):
        params = {'script':2026,'deploy':1}
        return self.run_get(params=params)

    def get_employees(self):
        headers = {"Content-Type": "application/json"}
        endpoint = f"/app/site/hosting/restlet.nl"
        params = {'script':2024,'deploy':1}
        ret =  self.api_adapter.get(endpoint=endpoint, auth=self.auth, params=params,
                              headers=headers)
    
        return ret

    def post_error_report(self, body_data):
        headers = {"Content-Type": "application/json"}
        endpoint = f"/app/site/hosting/restlet.nl"
        params = {'script':2025,'deploy':1}
        ret =  self.api_adapter.post(endpoint=endpoint, auth=self.auth, params=params,
                              headers=headers, data=body_data)
        return ret
    
    def update(self, body_data):
        headers = {"Content-Type": "application/json"}
        endpoint = f"/app/site/hosting/restlet.nl"
        params = {'script':2024,'deploy':1}
        ret =  self.api_adapter.post(endpoint=endpoint, auth=self.auth, params=params,
                              headers=headers, data=body_data)
        if len(ret.data)==0:
            print("empty return")
            self.logger.info(f"POST returned empty string. INPUT {body_data}")
            return ""  
        if 'successfully' in ret.data.lower():
            self.logger.info(f"{ret.data}")
            return "" 
        if isinstance(ret.data,str):
            print(ret.data)
            return ret.data
        if ret.data.get("type", "") == 'error.SuiteScriptError':
            return ret.data
            raise NetSuiteRestletException(ret)
