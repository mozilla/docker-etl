import logging
from .secrets_workday import config as wd_config
from api.util import APIAdaptor



logger = logging.getLogger(__name__)

class LocalConfig(object):
    def __getattr__(self, attr):
        return wd_config[attr]


class WorkdayAPI:
    def __init__(self, page_size: int = 100, timeout: int = 10):
        self._config = LocalConfig()
        everfi_integration = getattr(self._config, "everfi_integration")

        self.api_adapter = APIAdaptor(host=everfi_integration["host"])

    
    def get_datawarehouse_workers_csv(self):
        everfi_integration = getattr(self._config, "everfi_integration")

        auth = (
                everfi_integration["username"],
                everfi_integration["password"],
            )
        timeout = 360
        endpoint = everfi_integration["datawarehouse_worker_endpoint"]

        result = self.api_adapter.get(endpoint=endpoint, auth=auth, timeout=timeout,
                                      response_json=False)

        return result.data
