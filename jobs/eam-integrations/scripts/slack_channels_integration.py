import argparse
import logging
import sys
import datetime
from slack_channels.api.Slack import SlackAPI
from api.util import Util


class SlackAPIException(Exception):
    pass

class Slack:
    def __init__(self, donot_delete_lst):
        self._slackAPI = SlackAPI()
        self.integration_report_channel = 'integrations-report'
        self.donot_delete_lst = ['C07NW6GV1V2','C07RXPTAH9S']

    def get_conversations_list(self, days):

        #types = 'public_channel,private_channel'
        types = 'public_channel'
        channels_dict = self._slackAPI.get_conversations_list(types)
        days_ago = datetime.datetime.now() - datetime.timedelta(seconds=days)

                
        integration_report = [x for x in channels_dict
                              if channels_dict[x].get('name') == self.integration_report_channel 
                              ]

        archived = [x for x in channels_dict
                    if channels_dict[x].get('is_archived')
                    and channels_dict[x].get('id') not in self.donot_delete_lst]

        # excluding channels created within the past X days (from input)
        non_archived = [x for x in channels_dict
                        if channels_dict[x].get('is_archived') is False
                        and channels_dict[x].get('name') not in ['general',
                                                                 self.integration_report_channel]
                        and float(channels_dict[x].get('created')) < days_ago.timestamp()
                        and channels_dict[x].get('id') not in self.donot_delete_lst]
 

        return non_archived, archived, integration_report, channels_dict

    def get_conversations_history(self, channel_id, limit):
        params = {'limit': limit, 'channel': channel_id}
        data = self._slackAPI.get_conversations_history(params)

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

        days_ago = current_date - datetime.timedelta(seconds=days)
        return timestamp_date < days_ago

    def chat_post_message(self, channel_id, text):
        data = self._slackAPI.chat_post_message(channel_id=channel_id,text=text)
        if not data.data.get('ok'):
            raise SlackAPIException(data)
        return data
    
    def join_channel(self, channel_id):
        data = self._slackAPI.join_channel(channel_id=channel_id)
        if not data.data.get('ok'):
            raise SlackAPIException(data)
        return data


class SlackIntegration:
    def __init__(self):
        self._slack = Slack(['content-admin-test'])
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, max_limit, operations = []):
        num_deleted = 0
        num_archived = 0
        num_warnings = 0

        # operations: delete archived no members, 
        #             warning msg, 
        #             archive, delete after warning msgs 
        # ==================================================================================
        # 1 - Getting all Slack channels (public).
        # ==================================================================================
        self.logger.info("1 - Getting all Slack channels (public channels only).")
        lst_msg_secs = 60*60*24*166 # 166 days
        lst_msg_secs = 1
        try:
            non_archived, archived, integration_report, channels_dict = self._slack.get_conversations_list(lst_msg_secs)

            if not integration_report:
                self.logger.info(f"The {self._slack.integration_report_channel} channel was not found.")

        except Exception as e:
            self.logger.error(str(e))
            self.logger.info("Failed while getting all Slack channels (public and private).")
            
            # trying to post the error msg in the integration report channel
            self._slack.chat_post_message(integration_report, "Slack Channels integration failed during step 1")
            
            sys.exit(1)

        # ==================================================================================
        # 2 - Selecting non-archived channels
        #     Business Rule: For non-archived channels: Select channels where the last
        #                    message was sent six or more months ago, and archive them.
        #                    Six months is our message retention period.
        # ==================================================================================
        self.logger.info("2 - Selecting non-archived channels")
        msg_archived = """Hello, in 14 days this channel will be automatically archived by the Slack admins due to inactivity. Then it will be deleted in 30 days. If you wish to continue using it, please post a message. If you wish to unarchive it after the 14th day please ask in #servicedesk. We cannot restore a channel once it's deleted."""

        for channel_id in non_archived[:max_limit]:
            try:
                #return the last limit messages
                data = self._slack.get_conversations_history(channel_id=channel_id, limit=10)
            except Exception as e:
                self.logger.info(e.args[0].data)
                continue
            #filter only msgs sent by users
            msgs = [x for x in data.data.get('messages')
                    if 'subtype' not in x.keys()
                    or 'channel_unarchive' in x.values()] # unarchive msgs considered as a normal msg
            
            # no msgs
            if len(msgs) == 0:
                created = channels_dict.get(channel_id).get('created')
                
                # Channel has no members -> delete channel
                if (channels_dict.get(channel_id).get('num_members')==0):
                    # Channel older than N then Delete channel
                    if (self._slack.is_ts_older_than(days=lst_msg_secs, unix_timestamp=created)):
                        r = self._slack.conversations_delete(channel_id=channel_id)
                        num_deleted += 1
                        self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was deleted [no msgs and no members]')

                # Channel has members -> post warning msg
                else:
                    # Channel older than N then Archive it
                    if (self._slack.is_ts_older_than(days=lst_msg_secs, unix_timestamp=created)):
                        self._slack.join_channel(channel_id=channel_id)
                        self._slack.chat_post_message(channel_id=channel_id,text=msg_archived)
                        num_warnings +=1
                        #r = self._slack.conversations_archive(channel_id)
                        #num_archived +=1
                        #self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was archived')

            else:
                # Channel has msgs
                # Archive channel if last msg is from the Mozilla Service Account and is the warning msg
                # 
                ts = msgs[0].get('ts')
                # TODO put user in a data structure
                if (msgs[0].get('user')=='U07MZF1V34Y') and msgs[0].get('text')==msg_archived:
                    # Archive channel if ts >= 14 days
                    ts_14_days = 60*60*24*14
                    ts_14_days = 1
                    if (self._slack.is_ts_older_than(days=ts_14_days, unix_timestamp=ts)):
                        r = self._slack.conversations_archive(channel_id)
                        self.logger.info(f'The channel {channels_dict[channel_id].get("name")} was archived') 
                        num_archived +=1
                        continue
                
                # Channel has no members -> delete channel
                if (channels_dict.get(channel_id).get('num_members')==0):
                    r = self._slack.conversations_delete(channel_id=channel_id)
                    num_deleted += 1
                    self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was deleted [no msgs and no members]')
                else:
                    # Channel has members, send warning message.
                    if (self._slack.is_ts_older_than(days=lst_msg_secs, unix_timestamp=ts)):
                        
                        self._slack.join_channel(channel_id=channel_id)
                        self._slack.chat_post_message(channel_id=channel_id,text=msg_archived) 
                        num_warnings +=1
        
        # ==================================================================================
        # 3 - Selecting archived channels to be deleted
        #     Business Rule: For archived channels: Select channels that have been archived 
        #     for at least one month and delete them.
        # ==================================================================================
        archived_secs = 60*60*24*30
        archived_secs = 1
        for channel_id in archived[:max_limit]:
            try:

                # the updated field of an archived channel contains the date of when the 
                # channel was archived
                ts = channels_dict[channel_id].get("updated")

            except Exception as e:
                self.logger.info(e.args[0].data)
                continue

            if (self._slack.is_ts_older_than(days=archived_secs, unix_timestamp=ts/1000)):
                r = self._slack.conversations_delete(channel_id=channel_id)
                num_deleted +=1
                self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was deleted')

        # ==================================================================================
        # 4 - Posting the report message to the integration channel
        # ==================================================================================
        try:
            report = "_*SLACK CHANNELS INTEGRATION REPORT:*_\r"
            report += f"_*`Number of Public and Archived Channels (before integration):`*_ `{len(channels_dict)}`\r"
            report += f"_*`Warning messages posted by the integration:`*_ `{num_warnings}`\r"
            report += f"_*`Channels archived by the integration:`*_ `{num_archived}`\r"
            report += f"_*`Channels deleted by the integration:`*_ `{num_deleted}`\r"
            self._slack.join_channel(channel_id=integration_report)
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
        "--max_limit", 
        action="store",
        type=int,
        help="limit the number of changes",
        default=40
    )
    args = None
    args = parser.parse_args()

    log_level = Util.set_up_logging(args.level)

    logger = logging.getLogger(__name__)

    logger.info("Starting...")
  
    integration = SlackIntegration()
    integration.run(args.max_limit)

