import os

config = {
    "consumer_key": os.environ.get("NETSUITE_INTEG_NETSUITE_CONSUMER_KEY"),
    "consumer_secret": os.environ.get("NETSUITE_INTEG_NETSUITE_CONSUMER_SECRET", ""),    
    "token_id": os.environ.get("NETSUITE_INTEG_NETSUITE_TOKEN_ID", ""), 
    "token_secret": os.environ.get("NETSUITE_INTEG_NETSUITE_TOKEN_SECRET", ""), 
    "oauth_realm": os.environ.get("NETSUITE_INTEG_NETSUITE_TOKEN_OAUTH_REALM"),
    "host": os.environ.get("NETSUITE_INTEG_NETSUITE_HOST")
}
