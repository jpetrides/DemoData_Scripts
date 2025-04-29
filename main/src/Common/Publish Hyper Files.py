import tableauserverclient as TSC
from tableauserverclient.models import TableauAuth
import os

# Tableau Cloud site and authentication details
server_url = 'https://10ay.online.tableau.com/'  # Replace with your Tableau Cloud URL if different
site_id = 'tableautth'  # Replace with your site ID
token_name = 'Petrides_REST'
token_value = '2i5nI1J6R8K2pHWFcqOdoQ==:xA8icfC7D8ZcpGXnyuzGJY9Ek3xqWoAr'
project_name = 'Palonia_Inputs'  # Replace with your project name

# Path to your .hyper file
# also - note that our API can accept a parquet file instead of a hyper
#hyper_file_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Hotel/Dim_Property.hyper'
hyper_file_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Hotel/Dim_Property.parquet'

# Authenticate and create a server object
tableau_auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site_id)
server = TSC.Server(server_url, use_server_version=True)

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