import os
from api.util import APIAdaptor
from .secrets import config


class SlackAPI:
    def __init__(self):
        self.api_adapter = APIAdaptor(host=config['slack_host'])  
        self._token = os.getenv('SLACK_CHANNEL_TOKEN')

    def get_conversations_list(self, types):
        channels_dict = {}
        params = {'limit': 1,
                  'types': types}
        
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/conversations.list"
        
        while (True):
            data = self.api_adapter.get(endpoint=endpoint, 
                                        headers=headers,
                                        params=params)

            # channels_dict = {x.get('id'):x for x in data.data.get('channels',[])}
            for x in data.data.get('channels',''):
                channels_dict[x.get('id')] = x

            if data.data.get('response_metadata').get('next_cursor', ''): 
                params['cursor'] = data.data.get('response_metadata').get('next_cursor', '')
            else:
                break

        return channels_dict

    def get_conversations_history(self, channel_id):
        params = {'limit': 1, 'channel': channel_id}
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/conversations.history"

        return self.api_adapter.get(endpoint=endpoint, 
                                    headers=headers,
                                    params=params)

    def conversations_archive(self, channel_id):
        params = {'channel': channel_id}
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/conversations.archive"

        return self.api_adapter.post(endpoint=endpoint,
                                     headers=headers,
                                     params=params)
        
    def chat_post_message(self, channel_id, text):
        params = {'channel': channel_id,
                  'text': text}
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/chat.postMessage"

        return self.api_adapter.post(endpoint=endpoint,
                                     headers=headers,
                                     params=params)
        
