import requests
import json
import logging
from typing import Dict, List
from requests.structures import CaseInsensitiveDict

class Result:
    def __init__(self, status_code: int, headers: CaseInsensitiveDict,
                 message: str = '', data: List[Dict] = None):
        self.status_code = int(status_code)
        self.headers = headers
        self.message = str(message)
        self.data = data if data else []


class APIAdaptorException(Exception):
    pass


class APIAdaptor:
    def __init__(
        self, host: str, timeout: int = 30
    ):
        self.url = host
        self._logger = logging.getLogger(__name__)
        self.timeout = timeout
    # TODO review this func
    
    def _request(
        self,
        http_method: str,
        endpoint: str,
        headers: Dict = None,
        params: Dict = None,
        data: Dict = None,
        timeout: int = 10,
        auth=None,
        response_json=True
         
    ):
        full_url = self.url + endpoint

        try:
            response = requests.request(
                http_method,
                full_url,
                headers=headers,
                params=params,
                json=data,
                timeout=timeout,
                auth=auth
            )

        except APIAdaptorException as e:
            self._logger.error(msg=(str(e)))
            raise APIAdaptorException("Request failed") from e

        try:
            if response_json:
                if response.text != '':                
                    data_out = response.json()
                else:
                    data_out = ''
            else:
                data_out = response.text
        except (ValueError, json.JSONDecodeError) as e:
            raise APIAdaptorException(f"Bad JSON in response." +
                f"Response.text = {response.text} " +
                f"Response.status_code = {response.status_code}") from e

        is_success = 299 >= response.status_code >= 200     # 200 to 299 is OK
        if is_success:
            return Result(response.status_code, headers=response.headers,
                          message=response.reason, data=data_out)
        raise Exception(data_out)

    def get(self, endpoint: str, params: Dict = None, headers: str = None, auth=None, timeout=20,response_json=True):
        return self._request(http_method="GET", endpoint=endpoint, params=params, headers=headers, auth=auth,timeout=timeout,response_json=response_json)

    def post(self, endpoint: str, params: Dict = None,  headers: str = None, data: Dict = None):
        return self._request(
            http_method="POST", endpoint=endpoint, params=params, data=data, headers=headers
        )

    def patch(self, endpoint: str, params: Dict = None, headers: str = None, data: Dict = None):
        return self._request(http_method="PATCH", endpoint=endpoint, params=params,  data=data, headers=headers)

    def delete(self, endpoint: str, params: Dict = None, headers: str = None, data: Dict = None):
        return self._request(http_method="DELETE", endpoint=endpoint, params=params, data=data, headers=headers)
