import argparse
import logging
import sys
import datetime

from slack_channels.api.Slack import SlackAPI
from api.util import Util


class SlackAPIException(Exception):
    pass


class Slack:
    def __init__(self):
        self._slackAPI = SlackAPI()
        self.integration_report_channel = 'integrations-report'

    def get_conversations_list(self, days):
        types = 'public_channel,private_channel'
        channels_dict = self._slackAPI.get_conversations_list(types)
        days_ago = datetime.datetime.now() - datetime.timedelta(days=days)
                
        integration_report = [x for x in channels_dict
                              if channels_dict[x].get('name') == self.integration_report_channel 
                              ]

        archived = [x for x in channels_dict
                    if channels_dict[x].get('is_archived')]

        # excluding channels created within the past X days (from input)
        non_archived = [x for x in channels_dict
                        if channels_dict[x].get('is_archived') is False
                        and channels_dict[x].get('name') not in ['general',
                                                                 self.integration_report_channel]
                        and float(channels_dict[x].get('created')) < days_ago.timestamp()]


        return non_archived, archived, integration_report, channels_dict

    def get_conversations_history(self, channel_id):
        data = self._slackAPI.get_conversations_history(channel_id)
        if not data.data.get('ok'):
            raise SlackAPIException(data)

        return data

    def conversations_archive(self, channel_id):
        data = self._slackAPI.conversations_archive(channel_id=channel_id)
        if not data.data.get('ok'):
            raise SlackAPIException(data)        
        return data
    
    def conversations_delete(self, channel_id):
        data = self._slackAPI.conversations_delete(channel_id=channel_id)
        if not data.data.get('ok'):
            raise SlackAPIException(data)        
 
        return data

    def is_ts_older_than(self, days, unix_timestamp):

        timestamp_date = datetime.datetime.fromtimestamp(float(unix_timestamp))
        current_date = datetime.datetime.now()

        days_ago = current_date - datetime.timedelta(days=days)
        return timestamp_date < days_ago

    def chat_post_message(self, channel_id, text):
        data = self._slackAPI.chat_post_message(channel_id=channel_id,text=text)
        if not data.data.get('ok'):
            raise SlackAPIException(data)
        return data


class SlackIntegration:
    def __init__(self):
        self._slack = Slack()
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, force):
        
        # ==================================================================================
        # 1 - Getting all Slack channels (public and private).
        # ==================================================================================
        days = 1
        try:
            non_archived, archived, integration_report, channels_dict = self._slack.get_conversations_list(days)
            if not integration_report:
                self.logger.info(f"The {self._slack.integration_report_channel} channel was not found.")

        except Exception as e:
            self.logger.error(str(e))
            self.logger.info("Failed while getting all Slack channels (public and private).")
            
            # trying to post the error msg in the integration report channel
            self._slack.chat_post_message(integration_report, "Slack Channels integration failed during step 1")
            
            sys.exit(1)

        report = "_*SLACK CHANNELS INTEGRATION REPORT:*_\r"
        report += f"_*`Number of Channels:`*_ `{len(channels_dict.keys())}`\r"
        report += f"_*`Number of archived channels:`*_ `{len(archived)}`\r"
        report += f"_*`Number of active channels:`*_ `{len(non_archived)}`\r"

        # ==================================================================================
        # 2 - Selecting non-archived channels
        #     Business Rule: For non-archived channels: Select channels where the last
        #                    message was sent six or more months ago, and archive them.
        #                    Six months is our message retention period.
        # ==================================================================================
        report += "_*`Names of channels to be archived:`*_\r "
        for channel_id in non_archived:
            try:
                data = self._slack.get_conversations_history(channel_id=channel_id)
            except Exception as e:
                self.logger.info(e.args[0].data)
                continue
            ts = data.data.get('messages')[0].get('ts')
             

            if (self._slack.is_ts_older_than(days=days, unix_timestamp=ts)):
                self.logger.info(f'The channel {channels_dict[channel_id].get("name")} set to be archived')
                report += f"* `{channels_dict[channel_id].get('name')}`\r"
                r = self._slack.conversations_archive(channel_id)

        report += "Archived channels to be deleted: "
        
        # ==================================================================================
        # 3 - Selecting archived channels to be deleted
        #     Business Rule: For archived channels: Select channels that have been archived 
        #     for at least one month and delete them.
        # ==================================================================================
        for channel_id in archived:
            try:
                data = self._slack.get_conversations_history(channel_id=channel_id)
            except Exception as e:
                self.logger.info(e.args[0].data)
                continue

            ts = data.data.get('messages')[0].get('ts')
            days = 1
            if (self._slack.is_ts_older_than(days=days, unix_timestamp=ts)):
                self.logger.info(f'channel {channels_dict[channel_id].get("name")} set to be deleted')
                report += f"* `{channels_dict[channel_id].get('name')}`\r"
                r = self._slack.conversations_delete(channel_id=channel_id)

        # ==================================================================================
        # 4 - Posting the report message to the integration channel
        # ==================================================================================
        try:
            self._slack.chat_post_message(integration_report, report)
        except Exception as e:
            self.logger.info(e.args[0].data)

        self.logger.info("End of integration")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Slack Channels Integration ")


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

