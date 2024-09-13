
from requests_oauthlib import OAuth1
from .secrets import config
from api.util import APIAdaptor


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
        endpoint = f"/services/rest/record/v1/employee"
        self.api_adapter.get(endpoint=endpoint, auth=self.auth,
                              headers=headers)

    def update(self, body_data):
        headers = {"Content-Type": "application/json"}
 
        endpoint = f"/app/site/hosting/restlet.nl"

        params = {'script':2017,'deploy':1}
         
        self.api_adapter.post(endpoint=endpoint, auth=self.auth, params=params,
                              headers=headers, data=body_data)
