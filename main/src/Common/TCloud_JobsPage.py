import tableauserverclient as TSC
import requests
import os
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

# Load .env file from the main directory (two levels up from common)
load_dotenv(find_dotenv())
# -------------- CONFIGURATION --------------
TABLEAU_SERVER_URL = os.environ.get("TABLEAU_SERVER_URL")
TABLEAU_SITE_NAME = os.environ.get("TABLEAU_SITE_NAME")
PAT_NAME = os.environ.get("PAT_NAME")
PAT_SECRET = os.environ.get("PAT_SECRET")

# -------------- AUTHENTICATE WITH TSC --------------
auth = TSC.PersonalAccessTokenAuth(
    token_name=PAT_NAME,
    personal_access_token=PAT_SECRET,
    site_id=TABLEAU_SITE_NAME
)



server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)

with server.auth.sign_in(auth):
    print("✅ Signed into Tableau Cloud")

    # Get auth token and site ID
    auth_token = server.auth_token
    site_id = server.site_id
    api_version = server.version  # Get latest supported API version

    print(f"🔐 Auth Token: {auth_token[:10]}... (truncated)")
    print(f"🌐 Site ID: {site_id}")
    print(f"📦 API Version: {api_version}")

    # -------------- BUILD JOBS API REQUEST --------------
    jobs_url = f"{TABLEAU_SERVER_URL}/api/{api_version}/sites/{site_id}/jobs"

    headers = {
        "X-Tableau-Auth": auth_token,
        "Accept": "application/json"
    }

    # -------------- MAKE REST API CALL --------------
    response = requests.get(jobs_url, headers=headers)

    if response.status_code == 200:
        jobs_data = response.json()
        jobs = jobs_data.get("jobs", {}).get("job", [])

        print(f"\n📋 Found {len(jobs)} job(s):\n")

        for job in jobs:
            print(f"🆔 ID: {job.get('id')}")
            print(f"🔧 Type: {job.get('type')}")
            print(f"📊 Status: {job.get('status')}")
            print(f"⏱ Started At: {job.get('createdAt')}")
            print(f"🧍 User ID: {job.get('userId')}")
            print("-" * 40)

    else:
        print(f"❌ Failed to fetch jobs. Status Code: {response.status_code}")
        print(response.text)