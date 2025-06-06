import requests
import json

import os
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

load_dotenv(find_dotenv())
# -------------- CONFIGURATION --------------
TABLEAU_SERVER_URL = os.environ.get("TABLEAU_SERVER_URL")
TABLEAU_SITE_NAME = os.environ.get("TABLEAU_SITE_NAME")
PAT_NAME = os.environ.get("PAT_NAME")
PAT_SECRET = os.environ.get("PAT_SECRET")
TABLEAU_CLOUD_POD = os.environ.get("TABLEAU_CLOUD_POD")



# Replace with the LUID of the published data source you want to query
datasource_luid = "8e0b3b5c-0548-4dd2-a51b-d03232d49b76" # e.g., "a1b2c3d4-e5f6-7890-1234-abcdefghijkl"

# --- Tableau REST API Sign-in (to get auth token) ---
signin_url = f"https://{TABLEAU_CLOUD_POD}.online.tableau.com/api/3.22/auth/signin" # Use appropriate API version
signin_payload = {
    "credentials": {
        "personalAccessTokenName": PAT_NAME,
        "personalAccessTokenSecret": PAT_SECRET,
        "site": {
            "contentUrl": TABLEAU_SITE_NAME
        }
    }
}
signin_headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}

print("Attempting to sign in to Tableau Cloud...")
try:
    signin_response = requests.post(signin_url, json=signin_payload, headers=signin_headers)
    signin_response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

    signin_data = signin_response.json()
    tableau_token = signin_data['credentials']['token']
    # site_id = signin_data['credentials']['site']['id'] # We already know the contentUrl (site_id) used for VDS endpoint

    print("Sign in successful.")

except requests.exceptions.RequestException as e:
    print(f"Error during Tableau Cloud sign-in: {e}")
    if hasattr(e, 'response') and e.response is not None:
        print(f"Response status code: {e.response.status_code}")
        try:
            print(f"Response body: {e.response.json()}")
        except json.JSONDecodeError:
             print(f"Response body (text): {e.response.text}")
    exit()

# --- VizQL Data Service Query Definition ---
# Define your query here. This is a basic example.
# Consult the official VizQL Data Service API documentation for more complex queries
# including filters, aggregations, calculations, etc.
vizql_query_payload = {
    "datasource": {
        "datasourceLuid": datasource_luid
    },
    "query": {
        "fields": [
            # Add fields you want to retrieve. Use the exact name or caption from the data source.
            # Example:
            { "fieldCaption": "CallType" },
            { "fieldCaption": "WaitTime", "function": "SUM" }, # Example with aggregation
            # { "fieldCaption": "Order Date", "function": "YEAR" } # Example with date function
        ],
        "filters": [
            # Add filters here if needed. Consult VDS API docs for filter types.
            # Example:
            # {
            #     "field": { "fieldCaption": "Region" },
            #     "filterType": "CATEGORICAL",
            #     "membership": ["East", "West"]
            # }
        ]
    },
    "options": {
        "returnFormat": "OBJECTS" # Or "CSV" if you prefer CSV output (requires different handling below)
        # Other options like "debug": true can be added here
    }
}

# --- Important: Populate the 'fields' list above with the fields you want! ---
# Example fields for a hypothetical Superstore-like data source:
#vizql_query_payload["query"]["fields"].append({ "fieldCaption": "CallType" })
#vizql_query_payload["query"]["fields"].append({ "fieldCaption": "Product Name" })
#vizql_query_payload["query"]["fields"].append({ "fieldCaption": "Sales", "function": "SUM" })
#vizql_query_payload["query"]["fields"].append({ "fieldCaption": "Quantity", "function": "SUM" })
#vizql_query_payload["query"]["fields"].append({ "fieldCaption": "Category" })
#vizql_query_payload["query"]["fields"].append({ "fieldCaption": "Sub-Category" })


# --- VizQL Data Service API Request ---
# The endpoint uses api/v1/vizql-data-service/query-datasource
vizql_url = f"https://{TABLEAU_CLOUD_POD}.online.tableau.com/api/v1/vizql-data-service/query-datasource"

vizql_headers = {
    "Content-Type": "application/json",
    "Accept": "application/json",
    "X-Tableau-Auth": tableau_token # Use the token obtained from sign-in
}

print(f"\nAttempting to query data source LUID: {datasource_luid}")
try:
    vizql_response = requests.post(vizql_url, json=vizql_query_payload, headers=vizql_headers)
    vizql_response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)

    vizql_data = vizql_response.json()

    print("Query successful. Received data:")
    # The structure of the response depends on the 'returnFormat' and the query.
    # For "OBJECTS" format, the data is typically in vizql_data['data']['data']
    if 'data' in vizql_data and 'data' in vizql_data['data']:
        for row in vizql_data['data']['data']:
            print(row)
    else:
        print("Unexpected response format:")
        print(json.dumps(vizql_data, indent=4)) # Print the full response structure

except requests.exceptions.RequestException as e:
    print(f"Error during VizQL Data Service query: {e}")
    if hasattr(e, 'response') and e.response is not None:
        print(f"Response status code: {e.response.status_code}")
        try:
            print(f"Response body: {e.response.json()}")
        except json.JSONDecodeError:
             print(f"Response body (text): {e.response.text}")

finally:
    # --- Tableau REST API Sign-out (Optional but good practice) ---
    # You can sign out using the REST API
    if 'tableau_token' in locals(): # Check if token was successfully obtained
        signout_url = f"https://{TABLEAU_CLOUD_POD}.online.tableau.com/api/3.22/auth/signout"
        signout_headers = {
            "X-Tableau-Auth": tableau_token
        }
        print("\nAttempting to sign out...")
        try:
            signout_response = requests.post(signout_url, headers=signout_headers)
            signout_response.raise_for_status()
            print("Sign out successful.")
        except requests.exceptions.RequestException as e:
            print(f"Error during Tableau Cloud sign-out: {e}")
            # Don't exit here, as the main task is done