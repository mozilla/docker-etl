import os

config = {
    "proxies": {},
    "username": os.environ.get("EVERFI_USERNAME", ""),    
    "password": os.environ.get("EVERFI_PASSWORD", ""), 
    "host": "https://api.fifoundry.net/"
}
