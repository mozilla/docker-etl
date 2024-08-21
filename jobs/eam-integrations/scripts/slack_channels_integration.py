import argparse
import logging
import os
import requests
import datetime

from slack_channels.api.Slack import SlackAPI
from api.util import Util


class Slack:
    def __init__(self):
        self._slackAPI = SlackAPI()

    def get_conversations_list(self):
        types = 'public_channel,private_channel'
        channels_dict = self._slackAPI.get_conversations_list(types)

        integration_report = [x for x in channels_dict 
                              if channels_dict[x].get('name') == 'integrations-report']
        
        archived = [x for x in channels_dict
                    if channels_dict[x].get('is_archived')]

        non_archived = [x for x in channels_dict
                        if channels_dict[x].get('is_archived') is False
                        and channels_dict[x].get('name') != 'integrations-report']

        return non_archived, archived, integration_report, channels_dict

    def get_conversations_history(self, channel_id):
        data = self._slackAPI.get_conversations_history(channel_id)
        return data

    def conversations_archive(self, channel_id):
        data = self._slackAPI.conversations_archive(channel_id=channel_id)
        return data

    def is_ts_older_than(self, days, unix_timestamp):

        timestamp_date = datetime.datetime.fromtimestamp(float(unix_timestamp))
        current_date = datetime.datetime.now()
        days_ago = current_date - datetime.timedelta(seconds=days)
        return timestamp_date < days_ago

    def chat_post_message(self, channel_id, text):
        self._slackAPI.chat_post_message(channel_id=channel_id,text=text)
        
class SlackIntegration:
    def __init__(self):
        self._slack = Slack()

    def run(self, force):

        # get all Slack channels
        non_archived, archived, integration_report, channels_dict = self._slack.get_conversations_list()
        print(integration_report)
        report = "_*SLACK CHANNELS INTEGRATION REPORT:*_\r"
        report += f"_*`Number of Channels:`*_ `{len(channels_dict.keys())}`\r"
        report += f"_*`Number of archived channels:`*_ `{len(archived)}`\r"
        report += f"_*`Number of active channels:`*_ `{len(non_archived)}`\r"
        
        # For non-archived channels: Select channels where the last message
        # was sent six or more months ago, and archive them.
        # Six months is our message retention period.
        report += "_*`Names of channels to be archived:`*_\r "
        for channel_id in non_archived:
            data = self._slack.get_conversations_history(channel_id=channel_id)
            ts = data.data.get('messages')[0].get('ts')
            days = 1

            if (self._slack.is_ts_older_than(days=days, unix_timestamp=ts)):
                print(f'`channel {channels_dict[channel_id].get("name")} msg is old')
                report += f"* `{channels_dict[channel_id].get('name')}`\r"
                # r = self._slack.conversations_archive(channel_id)

        report += "Archived channels to be deleted: "
        # For archived channels: Select channels that have been archived 
        # for at least one month and delete them.
        for channel_id in archived:
            data = self._slack.get_conversations_history(channel_id=channel_id)
            ts = data.data.get('messages')[0].get('ts')
            days = 1
            if (self._slack.is_ts_older_than(days=days, unix_timestamp=ts)):
                print(f'channel {channels_dict[channel_id].get("name")} msg is old')
                report += f"* `{channels_dict[channel_id].get('name')}`\r"

        self._slack.chat_post_message(integration_report, report)
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync up XMatters with Workday")

    parser.add_argument(
        "-l",
        "--level",
        action="store",
        help="log level (debug, info, warning, error, or critical)",
        type=str,
        default="info",
    )
   
    parser.add_argument(
        "-f",
        "--force", 
        action="store",
        type=int,
        help="If true, the script will run and delete and archive channels, otherwise it will only report the channels",
        default=40
    )
    args = None
    args = parser.parse_args()
    
    log_level = Util.set_up_logging(args.level)

    logger = logging.getLogger(__name__)

    logger.info("Starting...")
    logger.info(f"force={args.force}")

    integration = SlackIntegration()
    integration.run(args.force)

# # Replace 'your-slack-bot-token' with your actual bot token
# slack_token = os.environ.get("SLACK_CHANNEL_TOKEN")
# headers = {
#     'Authorization': f'Bearer {slack_token}'
# }

# # The endpoint URL to list all channels (conversations.list method)
# url = 'https://slack.com/api/conversations.list'

# # Parameters for the request
# params = {
#     'limit': 1000,  # You can adjust the limit as needed
#     'types': 'public_channel,private_channel'  # Adjust to include other types if needed
# }

# response = requests.get(url, headers=headers, params=params)

# if response.status_code == 200:
#     data = response.json()
#     if data.get('ok', False):
#         channels = data.get('channels', [])
#         for channel in channels:
#             print(f"Channel ID: {channel['id']}, Channel Name: {channel['name']}")
#     else:
#         print(f"Error fetching channels: {data.get('error')}")
# else:
#     print(f"Request failed with status code {response.status_code}")

