from zeep import Client
from .secrets import config

from api.util.decorators import wait
from api.util.base import BaseAPI
class PanoptoAPI():
    def __init__(self):        
        self.AUTH = {
            "UserKey": config['gmail_panopto_user'],
            "Password": config['gmail_panopto_pass']
        }
        self.client = Client(f'https://{config["gmail_panopto_host"]}/Panopto/PublicAPI/4.0/UserManagement.svc?singleWsdl')
    
    def get_groups_by_name(self, group_name):
        return  self.client.service.GetGroupsByName(self.AUTH, group_name)
    
    def get_users_in_group(self,group_id):        
        users_ids = []
        if group_id:
            group_id = group_id.Id
            users_ids = self.client.service.GetUsersInGroup(self.AUTH,group_id)
            
        return users_ids
    
    def get_user_by_key(self, userKey):        
        return self.client.service.GetUserByKey (self.AUTH, userKey)
        
    def get_users(self, users_ids):
        guids = self.client.get_type('ns3:ArrayOfguid')
        return self.client.service.GetUsers(self.AUTH,guids(users_ids))
    
    def get_users_dict(self):
        page_number = 0
        max_num_results = 500
        panopto_dict_user_id = {}
        panopto_dict_email = {}
        while True:        
            list_users = self.client.service.ListUsers(self.AUTH,searchQuery='', parameters={'Pagination':{'MaxNumberResults':max_num_results, 'PageNumber':page_number}})        
            if page_number>= list_users.TotalResultCount:
                break
            for x in list_users.PagedResults.User:
                if x.Email:
                    panopto_dict_user_id[x.UserId] = x
                    panopto_dict_email[x.Email] = x
                    
            page_number+=1
        
        return (panopto_dict_user_id, panopto_dict_email)
    
    def remove_members_from_internal_group(self, group_id, member_ids):        
        guids = self.client.get_type('ns3:ArrayOfguid')
        self.client.service.RemoveMembersFromInternalGroup(self.AUTH,group_id.Id,  guids(member_ids))
    
    def add_members_to_internal_group(self, group_id, member_ids):
        guids = self.client.get_type('ns3:ArrayOfguid')
        self.client.service.AddMembersToInternalGroup(self.AUTH,group_id.Id, guids(member_ids))
        
    def create_user(self,email,group_id):
        user = self.client.get_type('ns2:User')        
        guids = self.client.get_type('ns3:ArrayOfguid')
        new_user = user(
            Email=email,
            EmailSessionNotifications=True,            
            GroupMemberships={'guid': guids(group_id)},  # empty group list            
            UserKey=f'MozillaMain@\{email}',  # Unique ID from your system            
        )
        self.client.service.CreateUser(self.AUTH,new_user)