import os

config = {
    "consumer_key": os.environ.get("wd_netsuite_consumer_key"),
    "consumer_secret": os.environ.get("wd_netsuite_consumer_secret", ""),    
    "token_id": os.environ.get("wd_netsuite_token_id", ""), 
    "token_secret": os.environ.get("wd_netsuite_token_secret", ""), 
    "oauth_realm": os.environ.get("wd_netsuite_token_oauth_realm"),
    "host": os.environ.get("wd_netsuite_host")
}
