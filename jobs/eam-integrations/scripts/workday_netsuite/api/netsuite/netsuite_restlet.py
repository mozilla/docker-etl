
from requests_oauthlib import OAuth1
from .secrets import config
from api.util import APIAdaptor

class NetSuiteRestletException(Exception):
    pass


class NetSuiteRestlet():
    def __init__(self) -> None:
        self.api_adapter = APIAdaptor(host=config.get('host'))
        self.auth = self.createAuth()
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

    def get_employees(self):
        headers = {"Content-Type": "application/json"}
        endpoint = f"/app/site/hosting/restlet.nl"
        params = {'script':1823,'deploy':1}
        ret =  self.api_adapter.get(endpoint=endpoint, auth=self.auth, params=params,
                              headers=headers)
    
        return ret

    def update(self, body_data):
        headers = {"Content-Type": "application/json"}
        endpoint = f"/app/site/hosting/restlet.nl"
        params = {'script':1823,'deploy':1}
        ret =  self.api_adapter.post(endpoint=endpoint, auth=self.auth, params=params,
                              headers=headers, data=body_data)
        if len(ret.data)==0:
            return #inactive
        if 'Successfully' in ret.data:
            return #inactive + active
        if ret.data.get("type", "") == 'error.SuiteScriptError':
            raise NetSuiteRestletException(ret)
