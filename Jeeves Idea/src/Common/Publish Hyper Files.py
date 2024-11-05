import tableauserverclient as TSC
from tableauserverclient.models import TableauAuth
import os

# Tableau Cloud site and authentication details
server_url = 'https://10ay.online.tableau.com/'  # Replace with your Tableau Cloud URL if different
site_id = 'tableautth'  # Replace with your site ID
token_name = 'Petrides_REST'
token_value = 'rCvnj38TQ5isqY00sW92fg==:v1wGMKIiOzFsLI0nIyDKn9rKXhCnirvJ'
project_name = 'Palonia_Inputs'  # Replace with your project name

# Path to your .hyper file
hyper_file_path = '/Users/jpetrides/Documents/Demo Data/Hotels/Jeeves Idea/data/Dim_Palonia_Cust.parquet'

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