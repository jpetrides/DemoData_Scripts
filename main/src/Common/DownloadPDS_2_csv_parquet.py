import requests
import os
import zipfile
import tempfile
import shutil
import json
import xml.etree.ElementTree as ET
from tableauhyperapi import HyperProcess, Connection, Telemetry, TableName
import pandas as pd
from pathlib import Path
import pantab

# ========== CONFIGURATION ==========
TABLEAU_SITE_URL = "https://10ay.online.tableau.com/"
TABLEAU_SITE_ID = "tableautth"  # Empty string if default
TABLEAU_TOKEN_NAME = "Petrides_Rest"
TABLEAU_TOKEN_SECRET = "+wYr44S4SOq6UMhDHZRO2A==:QCVfalFa0R1Fzoom6AnsZ43lhLsYfFs5"
DATASOURCE_NAME = "Prep_DL_Test"

# Output settings
USER_HOME = str(Path.home())
OUTPUT_DIR = os.path.join(USER_HOME, "Documents", "CCL_Test")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Output format
#OUTPUT_FORMAT = "parquet"  
OUTPUT_FORMAT = "csv"
# ===================================

def sign_in():
    url = f"{TABLEAU_SITE_URL}/api/3.18/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": TABLEAU_TOKEN_NAME,
            "personalAccessTokenSecret": TABLEAU_TOKEN_SECRET,
            "site": {"contentUrl": TABLEAU_SITE_ID}
        }
    }
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()  # Ensure we have a 200 OK

    raw = response.text
    print("Sign-in response text:", raw[:300])  # Optional: Preview only

    data = json.loads(raw)  # ✅ Safe and explicit
    token = data['credentials']['token']
    site_id = data['credentials']['site']['id']
    return token, site_id


def parse_xml_datasources(xml_content):
    """Parse XML response to extract datasources information"""
    root = ET.fromstring(xml_content)
    datasources = []
    
    # Find all datasource elements in the XML
    # The namespace might vary, so we'll try a few approaches
    ns = {'t': 'http://tableau.com/api'}
    
    # Try with namespace first
    ds_elements = root.findall('.//t:datasource', ns)
    
    # If no elements found, try without namespace
    if not ds_elements:
        ds_elements = root.findall('.//datasource')
    
    print(f"Found {len(ds_elements)} datasource elements in XML")
    
    # Extract data from each datasource element
    for ds in ds_elements:
        datasource = {}
        
        # Try with and without namespace for each attribute
        for attr in ['id', 'name', 'contentUrl']:
            # Try direct attribute
            value = ds.get(attr)
            if value is not None:
                datasource[attr] = value
                continue
                
            # Try with namespace prefix
            value = ds.get(f"{{http://tableau.com/api}}{attr}")
            if value is not None:
                datasource[attr] = value
        
        # Add to our list if we have at least an ID and name
        if 'id' in datasource and 'name' in datasource:
            datasources.append(datasource)
    
    return datasources


def find_datasource_id(token, site_id):
    url = f"{TABLEAU_SITE_URL}/api/3.25/sites/{site_id}/datasources?pageSize=1000"
    
    # Try to explicitly request JSON in headers
    headers = {
        'X-Tableau-Auth': token,
        'Accept': 'application/json'
    }
    
    print(f"Requesting datasources from: {url}")
    response = requests.get(url, headers=headers)
    
    # Print response status and headers for debugging
    print(f"Response status: {response.status_code}")
    print(f"Response content type: {response.headers.get('Content-Type', 'unknown')}")
    
    response.raise_for_status()
    
    content_type = response.headers.get('Content-Type', '').lower()
    
    # Handle different response formats
    if 'json' in content_type:
        print("Received JSON response")
        data = response.json()
        
        if 'datasources' in data and 'datasource' in data['datasources']:
            datasources = data['datasources']['datasource']
        else:
            print(f"Unexpected JSON structure. Keys: {list(data.keys())}")
            raise Exception("Response doesn't contain expected structure")
            
    elif 'xml' in content_type:
        print("Received XML response, parsing...")
        # Parse XML response
        datasources = parse_xml_datasources(response.text)
    else:
        # If content type is not explicitly JSON or XML, try to guess based on content
        print(f"Unknown content type: {content_type}, trying to detect format...")
        content_preview = response.text[:100].strip()
        
        if content_preview.startswith('{') or content_preview.startswith('['):
            print("Content appears to be JSON")
            data = response.json()
            if 'datasources' in data and 'datasource' in data['datasources']:
                datasources = data['datasources']['datasource']
            else:
                print(f"Unexpected JSON structure. Keys: {list(data.keys())}")
                raise Exception("Response doesn't contain expected structure")
        elif content_preview.startswith('<'):
            print("Content appears to be XML")
            datasources = parse_xml_datasources(response.text)
        else:
            print(f"Unable to determine response format. Content preview: {content_preview}")
            print(f"Full response: {response.text[:1000]}")
            raise Exception("Unknown response format")
    
    # Print found datasources for debugging
    print(f"Found {len(datasources)} datasources")
    for ds in datasources[:5]:  # Print first 5 only
        print(f"  - {ds.get('name', 'unnamed')} (ID: {ds.get('id', 'no-id')})")
    
    # Find our target datasource
    for datasource in datasources:
        if datasource['name'] == DATASOURCE_NAME:
            return datasource['id']
            
    # If we get here, datasource not found
    print(f"Available datasource names: {[ds.get('name', 'unnamed') for ds in datasources]}")
    raise Exception(f"Datasource '{DATASOURCE_NAME}' not found.")


def download_datasource(token, site_id, datasource_id):
    url = f"{TABLEAU_SITE_URL}/api/3.18/sites/{site_id}/datasources/{datasource_id}/content"
    headers = {'X-Tableau-Auth': token}
    response = requests.get(url, headers=headers, stream=True)
    response.raise_for_status()

    content_disp = response.headers.get('Content-Disposition')
    if content_disp and "filename=" in content_disp:
        filename = content_disp.split("filename=")[-1].strip('"')
    else:
        filename = f"{DATASOURCE_NAME}.unknown"

    file_path = os.path.join(OUTPUT_DIR, filename)
    with open(file_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Downloaded file: {file_path}")
    return file_path

def extract_hyper_from_tdsx(tdsx_path):
    temp_extract_dir = tempfile.mkdtemp()
    with zipfile.ZipFile(tdsx_path, 'r') as zip_ref:
        zip_ref.extractall(temp_extract_dir)

    hyper_files = []
    for root, dirs, files in os.walk(temp_extract_dir):
        for file in files:
            if file.endswith(".hyper"):
                hyper_files.append(os.path.join(root, file))

    if not hyper_files:
        raise Exception("No .hyper file found inside .tdsx package.")

    print(f"Extracted .hyper file: {hyper_files[0]}")
    return hyper_files[0], temp_extract_dir

def read_hyper_to_dataframe(hyper_path):
    """Read data from Hyper file using pantab & Hyper API for inspection"""
    print(f"Reading hyper file: {hyper_path}")

    # Use Hyper API to inspect tables
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(endpoint=hyper.endpoint, database=hyper_path) as connection:
            table_names = connection.catalog.get_table_names("Extract")
            print(f"Available tables: {table_names}")

            if not table_names:
                raise Exception("No tables found in the Hyper file.")

            table_name = table_names[0]  # Pick the first table

    # Now use pantab to read the table
    import pantab
    df = pantab.frame_from_hyper(hyper_path, table=table_name)
    print(f"Read {len(df)} rows with {len(df.columns)} columns from table {table_name}")
    return df

def save_dataframe(df, output_base_name):
    output_file = os.path.join(OUTPUT_DIR, f"{output_base_name}.{OUTPUT_FORMAT}")
    if OUTPUT_FORMAT == "parquet":
        df.to_parquet(output_file, index=False)
    else:
        df.to_csv(output_file, index=False)
    print(f"Saved file: {output_file}")

def main():
    try:
        print("Starting authentication...")
        token, site_id = sign_in()
        print(f"Successfully authenticated. Token: {token[:10]}... Site ID: {site_id}")
        
        try:
            print(f"Looking for datasource: {DATASOURCE_NAME}")
            datasource_id = find_datasource_id(token, site_id)
            print(f"Found datasource with ID: {datasource_id}")
            
            print(f"Downloading datasource...")
            downloaded_file = download_datasource(token, site_id, datasource_id)
            print(f"Download complete: {downloaded_file}")

            # Determine file type
            if downloaded_file.endswith(".hyper"):
                hyper_path = downloaded_file
                temp_extract_dir = None
                print("Direct .hyper file downloaded")
            elif downloaded_file.endswith(".tdsx"):
                print("Extracting .hyper from .tdsx package...")
                hyper_path, temp_extract_dir = extract_hyper_from_tdsx(downloaded_file)
                print(f"Extraction complete: {hyper_path}")
            else:
                raise Exception(f"Unexpected file type downloaded: {downloaded_file}")

            print("Reading data from Hyper file...")
            df = read_hyper_to_dataframe(hyper_path)
            print(f"Successfully read dataframe with shape: {df.shape}")
            
            print(f"Saving data in {OUTPUT_FORMAT} format...")
            save_dataframe(df, DATASOURCE_NAME)
            print("Processing complete!")

        finally:
            # Clean up session
            print("Signing out...")
            try:
                requests.post(
                    f"{TABLEAU_SITE_URL}/api/3.25/auth/signout",
                    headers={'X-Tableau-Auth': token}
                )
                print("Successfully signed out")
            except Exception as e:
                print(f"Error during sign out: {e}")
                
            # Clean temp dirs
            if 'temp_extract_dir' in locals() and temp_extract_dir and os.path.exists(temp_extract_dir):
                print(f"Cleaning up temporary directory: {temp_extract_dir}")
                shutil.rmtree(temp_extract_dir)
                
    except Exception as e:
        print(f"Error in main process: {e}")
        import traceback
        traceback.print_exc()
        return 1
        
    return 0

if __name__ == "__main__":
    main()