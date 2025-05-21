import os
from api.util import APIAdaptor
from .secrets import config
import time
from api.util.decorators import wait

def retry(count):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for _ in range(count):
                try:
                    ret = func(*args, **kwargs)
                    return ret

                except Exception as e:
                    print('retry except')
                    if 'ratelimited' in e.args[0].get('error'):
                        time.sleep(5)
                        continue
        return wrapper
    return decorator

class SlackAPI:
    def __init__(self):
        self.api_adapter = APIAdaptor(host=config['slack_host'])
        self._token = config['slack_token']

    @retry(10)
    def get_conversation_info(self, channel_id):
        params = {'channel': channel_id,'include_num_members':True}
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/conversations.info"

        return self.api_adapter.get(endpoint=endpoint, 
                                    headers=headers,
                                    params=params)
        
    @retry(10)
    def get_teams_list(self):
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/auth.teams.list"
        return self.api_adapter.get(endpoint=endpoint, 
                            headers=headers)
        
    @retry(10)
    def get_conversations_list(self, types,team_id):
        channels_dict = {}
        params = {'limit': 1000,
                  'types': types,
                  'team_id': team_id, 
                }
        
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/conversations.list"        
        
        while (True):
            data = self.api_adapter.get(endpoint=endpoint,
                                        headers=headers,
                                        params=params)

            if not data.data.get('ok'):
                raise Exception(data.data)
            # channels_dict = {x.get('id'):x for x in data.data.get('channels',[])}
            for x in data.data.get('channels',''):
                channels_dict[x.get('id')] = x

            if data.data.get('response_metadata').get('next_cursor', ''): 
                params['cursor'] = data.data.get('response_metadata').get('next_cursor', '')
            else:
                break

        return channels_dict

    @retry(10)
    def get_conversations_history(self, params):
        
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/conversations.history"

        return self.api_adapter.get(endpoint=endpoint, 
                                    headers=headers,
                                    params=params)

    @retry(10)
    def conversations_archive(self, channel_id):
        params = {'channel': channel_id}
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/conversations.archive"

        return self.api_adapter.post(endpoint=endpoint,
                                     headers=headers,
                                     params=params)
    @retry(10)
    def conversations_delete(self, channel_id):
        params = {'channel_id': channel_id}
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/admin.conversations.delete"
        return self.api_adapter.post(endpoint=endpoint,
                                            headers=headers,
                                            params=params)

    @retry(10)
    def leave_channel(self, channel_id):
        params = {'channel': channel_id}
        endpoint = "api/conversations.leave"
        headers = {'Authorization': f'Bearer {self._token }'}

        return self.api_adapter.post(endpoint=endpoint,
                                     headers=headers,
                                     params=params)
        
    @retry(10)
    def join_channel(self, channel_id):
        params = {'channel': channel_id}
        endpoint = "api/conversations.join"
        headers = {'Authorization': f'Bearer {self._token }'}

        return self.api_adapter.post(endpoint=endpoint,
                                     headers=headers,
                                     params=params)
    @retry(10)
    def chat_post_message(self, channel_id, text):
        params = {'channel': channel_id,
                  'text': text}
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/chat.postMessage"

        return self.api_adapter.post(endpoint=endpoint,
                                     headers=headers,
                                     params=params)
        
