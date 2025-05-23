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

# Debug: Print to verify values are loaded
print(f"TABLEAU_SERVER_URL: {TABLEAU_SERVER_URL}")
print(f"TABLEAU_SITE_NAME: {TABLEAU_SITE_NAME}")
print(f"PAT_NAME: {PAT_NAME}")
print(f"PAT_SECRET: {'*' * len(PAT_SECRET) if PAT_SECRET else 'None'}")