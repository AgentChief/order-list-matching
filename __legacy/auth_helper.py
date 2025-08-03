# auth_helper.py

import os
import logging
import requests
from dotenv import load_dotenv
from msal import PublicClientApplication, SerializableTokenCache

class InsecureSession(requests.Session):
    def request(self, *args, **kwargs):
        kwargs['verify'] = True
        logging.debug("⚠️ Using insecure session: SSL verification is disabled.")
        return super().request(*args, **kwargs)

# Load environment variables
load_dotenv()

CLIENT_ID = os.getenv("CLIENT_ID", "70c40022-9957-4db8-b402-f7d6d32beefb")
TENANT_ID = os.getenv("TENANT_ID", "95d7583a-1925-44b1-b78b-3483e00c5b46")
AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
SCOPES = ["Calendars.Read",  "User.Read"]
CACHE_FILE = ".msal_cache.bin"

def get_token():
    token_cache = SerializableTokenCache()
    if os.path.exists(CACHE_FILE):
        token_cache.deserialize(open(CACHE_FILE, "r").read())

    session = InsecureSession()
    app = PublicClientApplication(
        client_id=CLIENT_ID,
        authority=AUTHORITY,
        token_cache=token_cache,
        http_client=session
    )

    accounts = app.get_accounts()
    result = None

    if accounts:
        result = app.acquire_token_silent(SCOPES, account=accounts[0])

    if not result:
        flow = app.initiate_device_flow(scopes=SCOPES)
        print(flow["message"])
        input("Press Enter once you have completed login in the browser...")
        result = app.acquire_token_by_device_flow(flow)

    if "access_token" in result:
        with open(CACHE_FILE, "w") as f:
            f.write(token_cache.serialize())
        return result["access_token"]
    else:
        raise Exception(f"Failed to acquire token: {result.get('error_description')}")
    
