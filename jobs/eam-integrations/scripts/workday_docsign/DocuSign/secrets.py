import os

config = {
    "docusign_jwt": os.environ.get("DOCUSIGN_INTEG_DOCUSIGN_JWT", ""),    
    "token_host": "https://account-d.docusign.com/",
    "host" : "https://demo.docusign.net/restapi/v2.1/",
    "account_id": os.environ.get("DOCUSIGN_INTEG_DOCUSIGN_ACCOUNT_ID", ""),
    
}
