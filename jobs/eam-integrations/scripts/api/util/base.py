from abc import ABC
import os
import json

class BaseAPI(ABC)        :
    def __init__(self, path, config_filename='secrets.json'):
        __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(path)))        
        secrets_json = open(os.path.join(__location__, config_filename))
        self.secrets_dict = json.load(secrets_json)
        # the json should be flat for this to work
        self.secrets_dict = {key:os.path.expandvars(self.secrets_dict[key]) for (key,value) in self.secrets_dict.items() }