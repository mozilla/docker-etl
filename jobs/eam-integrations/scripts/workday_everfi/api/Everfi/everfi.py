from api.util import APIAdaptor, cache_pickle
import logging
import requests
from .secrets_everfi import config

logger = logging.getLogger(__name__)

class EverfiAPIExceptionNoCategory(Exception):
    pass


class EverfiAPI():
    # todo fix the host and api_key parameters
    def __init__(self, page_size: int = 100, timeout: int = 10):
        
        self.api_adapter = APIAdaptor(host=config.get('host'))
        self.token = self.get_token()
        self.headers = {'Content-Type': 'application/json',
                  'Accept': 'application/json',
                  'Authorization': 'Bearer %s' % self.token}     

    def get_token(self):
        params = {'grant_type':'client_credentials',                 
                  'client_id': config.get('username',''),
                  'client_secret': config.get('password','')}        

        result = self.api_adapter.post(endpoint="oauth/token",params=params)
        return result.data['access_token']
      
    # =============================================================
    # Category
    # =============================================================    
    def get_category(self, category_name):
        endpoint = 'v1/admin/categories/'
        
        result = self.api_adapter.get(endpoint=endpoint, headers=self.headers)
        cat_id =""
        for rec in result.data.get('data',[]):
            if rec['attributes']['name'] == category_name:
                cat_id = rec['id']
                break        
        if not cat_id:
            raise EverfiAPIExceptionNoCategory(f"Category {category_name} not found.")
        
        endpoint = f'v1/admin/categories/{cat_id}'
        params = {'include':'category_labels'}
        result = self.api_adapter.get(endpoint=endpoint, headers=self.headers, params=params)
                    
        return result
    
    # =============================================================
    # Hire Dates Category
    # =============================================================     
    def get_hire_dates(self):
        
        result = self.get_category('Hire Date')
       
        included = result.data.get('included')
        category_id = result.data.get('data').get('id')
        hire_dates = {}
        for hire_date in included:
            hire_dates[hire_date.get('attributes','').get('name')] = hire_date.get('id','')
    
        return category_id, hire_dates
    
    def add_hire_date(self, name, category_id):
        endpoint = 'v1/admin/category_labels/'
        json_data = {
            'data': {
                'type': 'category_labels',
                'attributes': {
                    'name': name,
                    'category_id': category_id,
                },
            },
        }
        return self.api_adapter.post(endpoint=endpoint, headers=self.headers,data=json_data)
        

    # =============================================================
    # USERS
    # =============================================================
    
    def get_users(self, fields,filter, locs, loc_map_table):
        def fix_none(x):
            return '' if not x else x
        def build_comparison_string(rec, locs, loc_map_table):
            cc_learner = [x for x in rec.get('attributes',{}).get('user_rule_set_roles','[]')if x.get('rule_set','')=='cc_learner']            
            
            if not cc_learner:
                is_manager ='non_supervisor'
            else:
                is_manager = fix_none(cc_learner[0].get('role',''))
                
            return fix_none(rec.get('attributes',{}).get('email','')) + "|"+\
                   fix_none(rec.get('attributes',{}).get('first_name','')) + "|"+\
                   fix_none(rec.get('attributes',{}).get('last_name','')) + "|"+\
                   fix_none(rec.get('attributes',{}).get('employee_id','')) + "|"+\
                   fix_none(str(rec.get('attributes',{}).get('location_id',''))) + "|"+\
                   is_manager 
        
        users_dict = {}
        comp = {}
        curr_page = 1
        params = {'page[per_page]': 100,
                  'filter[active]': 'true',
                  'fields[users]': 'email,first_name,last_name,sso_id,employee_id,student_id,location_id,active,user_rule_set_roles,category_labels'}                                   
        while True:
            params['page[page]'] = curr_page
            result = self.api_adapter.get(endpoint='v1/admin/users', params=params,headers=self.headers)
            if len(result.data.get('data', [])) == 0:
                return comp, users_dict

            for rec in result.data.get('data',[]):
                email = rec.get('attributes',{}).get('email','')                                          
                users_dict[email] = rec
                comp[email] = build_comparison_string(rec, locs, loc_map_table)

            curr_page += 1
            
    def deactivate_users(self, del_list,everfi_users):
        
        for email in del_list:    
            id = everfi_users[email].get('id')
            endpoint = f'v1/admin/registration_sets/{id}'  
            json_data = {
                'data': {
                    'type': 'registration_sets',
                    'id': id,
                    'attributes': {
                        'registrations': [
                            {
                                "rule_set": "user_rule_set",
                                'active': False,
                            }
                        ],
                    },
                },
            }
        
            r = self.api_adapter.patch(endpoint=endpoint, headers=self.headers, data= json_data)
        
        
    def upd_user(self, id, json_data):
        endpoint = f'v1/admin/registration_sets/{id}'
  
        return self.api_adapter.patch(endpoint=endpoint, headers=self.headers, data= json_data)
    
    def add_user(self, json_data):
        endpoint = 'v1/admin/registration_sets'
        return self.api_adapter.post(endpoint=endpoint, headers=self.headers, data= json_data)
    
    def assign_label_user(self, user_id, category_label_id):
        endpoint = 'v1/admin/category_label_users'
        json_data = {
            'data': {
                'type': 'category_label_users',
                'attributes': {
                    'user_id': '%s' % user_id,
                    'category_label_id': category_label_id,
                },
            },
        }
        return self.api_adapter.post(endpoint=endpoint, headers=self.headers,data=json_data)


    # =============================================================
    # LOCATIONS
    # =============================================================
    def get_locations_mapping_table(self):
        # Get all categories and find loc_map_table category        
        result = self.get_category('Locations Mapping Table')
        map = {}
        for rec in result.data.get('included'):
            fields = rec.get('attributes').get('name').split("|")
            if len(fields)!=2:
                continue
            map[fields[0]] = fields[1]

        return map

    def get_locations(self, page_size=10000):

            locs = {}
        
            params = {'page[size]':page_size}
            curr_page = 1
            
            params['page[page]'] = curr_page
            result = self.api_adapter.get(endpoint='v1/admin/locations', params=params,headers=self.headers)
                        
            for rec in result.data.get('data',[]):
                name = rec.get('attributes',{}).get('name','')                                          
                locs[name] = rec
                
            return locs
    

