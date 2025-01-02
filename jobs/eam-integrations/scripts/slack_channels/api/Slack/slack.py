import os
from api.util import APIAdaptor
from .secrets import config


class SlackAPI:
    def __init__(self):
        self.api_adapter = APIAdaptor(host=config['slack_host'])
        self._token = config['slack_token']

    def get_conversations_list(self, types):
        channels_dict = {}
        params = {'limit': 100,
                  'types': types,
                  'team_id': 'T07JXFQU132',
                  # 'last_message_activity_before': 1724426147
        }
        
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/conversations.list"
        #endpoint = "api/admin.conversations.lookup"
        
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

    def get_conversations_history(self, params):
        
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

    def conversations_delete(self, channel_id):
        params = {'channel_id': channel_id}
        headers = {'Authorization': f'Bearer {self._token }'}
        endpoint = "api/admin.conversations.delete"

        return self.api_adapter.post(endpoint=endpoint,
                                     headers=headers,
                                     params=params)


    def join_channel(self, channel_id):
        params = {'channel': channel_id}
        endpoint = "api/conversations.join"
        headers = {'Authorization': f'Bearer {self._token }'}

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
        
