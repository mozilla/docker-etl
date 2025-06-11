import argparse
import logging
import sys
import datetime
from slack_channels.api.Slack import SlackAPI, secrets
from api.util import Util
from enum import Enum



def unix_to_date(timestamp):
    from datetime import datetime
    # If timestamp is in milliseconds, convert to seconds
    if timestamp > 1e10:
        timestamp /= 1000
    
    # Convert to datetime object
    dt = datetime.utcfromtimestamp(timestamp)
    
    # Format as a readable string
    return dt.strftime('%Y-%m-%d %H:%M:%S UTC')

def read_file_to_list(file_path):    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()  # Read all lines into a list
        return [line.strip() for line in lines]  # Remove newline characters
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return []
    except Exception as e:
        print(f"An error occurred: {e}")
        return []
    
    
class SlackAPIException(Exception):
    pass

class Slack:
    def __init__(self, donot_delete_lst):
        self._slackAPI = SlackAPI()
        self.integration_report_channel = 'C08E9BKRJDU'
        self.donot_delete_lst = ['C08CZEL89LZ']

    def get_teams_list(self):
        return self._slackAPI.get_teams_list()
        
    def get_conversations_list(self, days, team_id):
        
        types = 'public_channel'
        #channels_dict = self._slackAPI.get_conversation_info(team_id)
        channels_dict = self._slackAPI.get_conversations_list(types,team_id)
        days_ago = datetime.datetime.now() - datetime.timedelta(seconds=days)

                
        integration_report = self.integration_report_channel

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

    def is_ts_older_than(self, secs, unix_timestamp):

        timestamp_date = datetime.datetime.fromtimestamp(float(unix_timestamp))
        current_date = datetime.datetime.now()

        days_ago = current_date - datetime.timedelta(seconds=secs)
        return timestamp_date <= days_ago

    def chat_post_message(self, channel_id, text):
        data = self._slackAPI.chat_post_message(channel_id=channel_id,text=text)
        if not data.data.get('ok'):
            raise SlackAPIException(data)
        return data

    def leave_channel(self, channel_id):
        data = self._slackAPI.leave_channel(channel_id=channel_id)
        if not data.data.get('ok'):
            raise SlackAPIException(data)
        return data
        
    def join_channel(self, channel_id):
        data = self._slackAPI.join_channel(channel_id=channel_id)
        if not data.data.get('ok'):
            raise SlackAPIException(data)
        return data

   
    
class Operations(Enum):
    delete_no_members_no_msgs = 1
    warning_msg = 2
    archive = 3
    delete_archived = 4

class SlackIntegration:
    def __init__(self):
        self._slack = Slack(['content-admin-test'])
        self.logger = logging.getLogger(self.__class__.__name__)

    def run(self, max_limit, operations = [], team = None):
        
        team_name = team[0]
        team_id = team[1]

        num_deleted = 0
        num_archived = 0
        num_warnings = 0
        
        num_vars = [num_deleted, num_archived, num_warnings,num_deleted]
        operations = [
                      Operations.delete_no_members_no_msgs,
                      Operations.warning_msg,
                      Operations.archive, 
                      Operations.delete_archived
                      ]
        max_limit = 5
        ts_07_days = 60*60*24*7
        lst_msg_secs = 60*60*24*173 # 173 days
        archived_secs = 60*60*24*30 # 30 days
        lst_msg_secs_full = 60*60*24*181 # 181 days

        # ==================================================================================
        # 1 - Getting all Slack channels (public).
        # ==================================================================================
        self.logger.info("1 - Getting all Slack channels (public channels only).")
        
        try:
            non_archived, archived, integration_report, channels_dict = self._slack.get_conversations_list(lst_msg_secs,team_id)            
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
        msg_archived = """This channel will be archived in 7 days due to inactivity and deleted 30 days after archiving. To keep the channel active a member will need to post a message. Members must unarchive the channel within the 30 day period and post a message to keep the channel active. Note: A channel cannot be restored after it is deleted."""
       
        for i, channel_id in enumerate(non_archived):
            #added 3/12
            channels_dict ={}
            channels_dict[channel_id] =  self._slack._slackAPI.get_conversation_info(channel_id).data.get('channel')
            if not channels_dict[channel_id]:
                self.logger.info(f'Channel {channel_id} not found')
                continue
            
            if channels_dict.get(channel_id).get('is_archived'):      
                self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was already archived')
                continue

            num_vars = [num_deleted, num_archived, num_warnings,num_deleted] 
            # only indexes of operations that are in the list of operations
            operations_indexes = [i for i,x in enumerate(num_vars) if i in [e.value-1 for e in operations]] 
            # break if all num_vars for the operations are greater than max_limit
            if all([num_vars[x]>=max_limit for x in operations_indexes]):
                break
            
            try:
                #return the last limit messages
                data = self._slack.get_conversations_history(channel_id=channel_id, limit=20)
            except Exception as e:
                self.logger.info(e.args[0].data)
                continue
            
            msgs = [x for x in data.data.get('messages')]                    
            
            if len([x for x in msgs if x.get('subtype') == 'tombstone']):
                self.logger.info(f'Channel {channels_dict[channel_id].get("name")} is tombstone. Skipping it.')
                continue
            
            if channels_dict.get(channel_id).get('is_shared'):    
                self.logger.info(f'Channel {channels_dict[channel_id].get("name")} is shared. Skipping it.')
                continue
                        
            msgs = [x for x in data.data.get('messages')
                    if x.get('subtype')=='bot_message' or
                    ('subtype' not in x.keys() 
                     or 'channel_unarchive' in x.values()
                     )
                    ] #
            
            created = channels_dict.get(channel_id).get('created')
            # no msgs
            if len(msgs) == 0:                                
                
                # No messages, no members: Delete channel
                if (channels_dict.get(channel_id).get('num_members')==0):                     
                    if (self._slack.is_ts_older_than(secs=lst_msg_secs_full, unix_timestamp=created)):
                      
                        if Operations.delete_no_members_no_msgs in operations and num_deleted < max_limit:                                                     
                            r = self._slack.conversations_delete(channel_id=channel_id)
                            num_deleted += 1                            
                            self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was deleted [no msgs and no members]')
                        else:
                            self.logger.info(f'Channel {channels_dict[channel_id].get("name")} WAS NOT deleted [no msgs and no members]. Operation delete_no_members_no_msgs not allowed')
                    else:
                        self.logger.info(f'Channel {channels_dict[channel_id].get("name")} skipped') 
                        
                else:
                    # Channel has members -> post warning msg
                    if (self._slack.is_ts_older_than(secs=lst_msg_secs, unix_timestamp=created)):
                        if Operations.warning_msg in operations and num_warnings < max_limit:
                            self._slack.join_channel(channel_id=channel_id)
                            self._slack.chat_post_message(channel_id=channel_id,text=msg_archived)
                            self._slack.leave_channel(channel_id=channel_id)
                            num_warnings +=1
                            self.logger.info(f'Warning message posted on Channel {channels_dict[channel_id].get("name")}')
                        else:
                            self.logger.info(f'Warning message was NOT posted on Channel {channels_dict[channel_id].get("name")}. Operations.warning_msg not allowed')    
                    else:
                        self.logger.info(f'Channel {channels_dict[channel_id].get("name")} skipped') 

            else:
                # Channel has msgs
                ts = msgs[0].get('ts')
                
                if (msgs[0].get('user')==secrets.config['slack_service_account']) and msgs[0].get('text')==msg_archived:                   
                    # Archive channel if last msg is from the Mozilla Service Account and is the warning msg    
                    if (self._slack.is_ts_older_than(secs=ts_07_days, unix_timestamp=ts)):
                        if Operations.archive in operations and num_archived < max_limit:
                            r = self._slack.conversations_archive(channel_id)
                            self.logger.info(f'The channel {channels_dict[channel_id].get("name")} was archived') 
                            num_archived +=1
                        else:
                            self.logger.info(f'The channel {channels_dict[channel_id].get("name")} was NOT archived. Operations.archive not allowed') 
                    else:
                        self.logger.info(f'Channel {channels_dict[channel_id].get("name")} skipped') 
                    
                    continue
                
                # Channel has no members -> delete channel
                if (channels_dict.get(channel_id).get('num_members')==0):
                    if (self._slack.is_ts_older_than(secs=lst_msg_secs_full, unix_timestamp=created)):
                        if Operations.delete_no_members_no_msgs in operations and num_deleted < max_limit:
                            r = self._slack.conversations_delete(channel_id=channel_id)
                            num_deleted += 1
                            self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was deleted [no msgs and no members]')
                        else:
                            self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was NOT deleted [no msgs and no members].Operations.delete_archived_no_members not allowed')
                    else:
                        self.logger.info(f'Channel {channels_dict[channel_id].get("name")} skipped') 
                else:
                    # Channel has members, send warning message.
                    if (self._slack.is_ts_older_than(secs=lst_msg_secs, unix_timestamp=ts)):
                        if Operations.warning_msg in operations and num_warnings < max_limit:
                            self._slack.join_channel(channel_id=channel_id)
                            self._slack.chat_post_message(channel_id=channel_id,text=msg_archived) 
                            self._slack.leave_channel(channel_id=channel_id)
                            num_warnings +=1
                            self.logger.info(f'Warning msg was sent to channel {channels_dict[channel_id].get("name")}')
                        else:
                            self.logger.info(f'Warning msg was NOT sent to channel {channels_dict[channel_id].get("name")}. Operations.warning_msg not allowed')
                    else:
                        if [x for x in msgs if x.get('user')==secrets.config['slack_service_account'] and x.get('text')==msg_archived]:
                            self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was NOT archived because somebody posted a msg after the warning msg')
                        self.logger.info(f'Channel {channels_dict[channel_id].get("name")} skipped') 

        # ==================================================================================
        # 3 - Selecting archived channels to be deleted
        #     Business Rule: For archived channels: Select channels that have been archived 
        #     for at least one month and delete them.
        # ==================================================================================

        for i, channel_id in enumerate(archived):
            try:

                # the updated field of an archived channel contains the date of when the 
                # channel was archived
                #add by julio
                channels_dict ={}
                channels_dict[channel_id] =  self._slack._slackAPI.get_conversation_info(channel_id).data.get('channel')
                
                ts = channels_dict[channel_id].get("updated")

            except Exception as e:
                self.logger.info(e.args[0].data)
                continue

            if not channels_dict[channel_id].get("is_archived"):
                self.logger.info(f"Channel not archived {channels_dict[channel_id].get('name')}")
                continue
                
            if (self._slack.is_ts_older_than(secs=archived_secs, unix_timestamp=ts/1000)):
                # Archived channel: Delete channel 30 days after archive date 
                try:
                    #return the last limit messages
                    data = self._slack.get_conversations_history(channel_id=channel_id, limit=20)
                except Exception as e:
                    self.logger.info(e.args[0].data)
                    continue
                msgs = [x for x in data.data.get('messages')]
                if len([x for x in msgs if x.get('subtype') == 'tombstone']):
                    self.logger.info(f'Archived channel {channels_dict[channel_id].get("name")} is tombstone. Skipping it.')
                    continue
                
                if channels_dict.get(channel_id).get('is_shared'):    
                    self.logger.info(f'Archived channel {channels_dict[channel_id].get("name")} is shared. Skipping it.')
                    continue
                                
                if Operations.delete_archived in operations and num_deleted < max_limit:
                    r = self._slack.conversations_delete(channel_id=channel_id)
                    num_deleted +=1
                    self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was deleted')
                else:
                    self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was NOT deleted. Operations.delete_archived not allowed')
            else:
                self.logger.info(f'Channel {channels_dict[channel_id].get("name")} was NOT delete because "updated" is {unix_to_date(ts)}')
        # ==================================================================================
        # 4 - Posting the report message to the integration channel
        # ==================================================================================
        try:
            report = "_*SLACK CHANNELS INTEGRATION REPORT:*_\r"
            report += f"_*`Number of Public and Archived Channels (before integration):`*_ `{len(non_archived)+ len(archived)}`\r"
            report += f"_*`Warning messages posted by the integration:`*_ `{num_warnings}`\r"
            report += f"_*`Channels archived by the integration:`*_ `{num_archived}`\r"
            report += f"_*`Channels deleted by the integration:`*_ `{num_deleted}`\r"
            # self._slack.join_channel(channel_id=integration_report)
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
        default=10
    )
    args = None
    args = parser.parse_args()

    #log_level = Util.set_up_logging(args.level)
    logging.basicConfig(filename="slack_channels_integration.log",
                    filemode='a',
        format="%(asctime)s:\t%(name)s.%(funcName)s()[%(filename)s:%(lineno)s]:\t%(levelname)s: %(message)s",
        level=logging.INFO,
        encoding="utf-8")

    logger = logging.getLogger(__name__)

    logger.info("Starting...")
  
    integration = SlackIntegration()
    teams = integration._slack.get_teams_list()
    
    
    
    for team in [(x.get('name'), x.get('id')) for x in teams.data.get('teams') if x.get('name') =="Mozilla"]:
        logger.info(f"Running integration for Slack Team: {team[0]} - {team[1]}")
        integration.run(args.max_limit, team=team)    
        

