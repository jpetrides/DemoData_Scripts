import tableauserverclient as TSC
from tableauserverclient.models import TableauAuth

import os
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

load_dotenv(find_dotenv())
# -------------- CONFIGURATION --------------
TABLEAU_SERVER_URL = os.environ.get("TABLEAU_SERVER_URL")
TABLEAU_SITE_NAME = os.environ.get("TABLEAU_SITE_NAME")
PAT_NAME = os.environ.get("PAT_NAME")
PAT_SECRET = os.environ.get("PAT_SECRET")

project_name = 'Palonia_Inputs'  # Replace with your project name

# Path to your .hyper file
# also - note that our API can accept a parquet file instead of a hyper
#hyper_file_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Hotel/Dim_Property.hyper'
hyper_file_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Hotel/Dim_Property.parquet'

# Authenticate and create a server object
tableau_auth = TSC.PersonalAccessTokenAuth(PAT_NAME, PAT_SECRET, TABLEAU_SITE_NAME)
server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)

try:
    with server.auth.sign_in(tableau_auth):
        # Get the project ID
        all_projects, _ = server.projects.get()
        project = next((project for project in all_projects if project.name == project_name), None)
        
        if project is None:
            raise ValueError(f"Project '{project_name}' not found")

        # Create a datasource item
        datasource = TSC.DatasourceItem(project.id)

        # Publish the datasource
        datasource = server.datasources.publish(
            datasource,
            hyper_file_path,
            mode=TSC.Server.PublishMode.Overwrite
        )
        
        print(f"Datasource published successfully. ID: {datasource.id}")

except Exception as e:
    print(f"An error occurred: {str(e)}")