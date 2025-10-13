import logging
from .secrets_workday import config as wd_config
from api.util import APIAdaptor
#from api.util import cache_pickle


logger = logging.getLogger(__name__)

class LocalConfig(object):
    def __getattr__(self, attr):
        return wd_config[attr]


class WorkdayAPI:
    def __init__(self, page_size: int = 100, timeout: int = 10):
        self._config = LocalConfig()
        docusign_integration = getattr(self._config, "docusign_integration")

        self.api_adapter = APIAdaptor(host=docusign_integration["host"])

    #@cache_pickle
    def get_datawarehouse_workers_csv(self):
        docusign_integration = getattr(self._config, "docusign_integration")

        auth = (
                docusign_integration["username"],
                docusign_integration["password"],
            )
        timeout = 360
        endpoint = docusign_integration["datawarehouse_worker_endpoint"]

        result = self.api_adapter.get(endpoint=endpoint, auth=auth, timeout=timeout,
                                      response_json=False)

        return result.data

    def get_wd_users(self):
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
                x[1])
            for x in filtered.iterrows()
        }
        return  comp, {x[1]["primary_work_email"]: x for x in filtered.iterrows()}
    
    def build_comparison_string(self, wd_row):
                 
        return (
            wd_row["legal_first_name"] + " " + wd_row["legal_last_name"] 
        )