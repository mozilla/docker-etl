import os

config = {
    "proxies": {},
    "username": os.environ.get("EVERFI_USERNAME", ""),    
    "password": os.environ.get("EVERFI_PASSWORD", ""), 
    "host" : "http://api.fifoundry-sandbox.net/"
}