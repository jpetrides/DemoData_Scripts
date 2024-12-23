import requests
import json

def add_user_to_databricks(workspace_url, access_token, email):
    # API endpoints
    scim_user_endpoint = f"{workspace_url}/api/2.0/preview/scim/v2/Users"
    scim_group_endpoint = f"{workspace_url}/api/2.0/preview/scim/v2/Groups"
    
    # Headers for API requests
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/scim+json'
    }
    
    # Create user payload
    user_payload = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "userName": email,
        "entitlements": [
            {
                "value": "allow-cluster-create"
            }
        ]
    }
    
    # Add user
    try:
        response = requests.post(scim_user_endpoint, headers=headers, json=user_payload)
        response.raise_for_status()
        print(f"Successfully added user: {email}")
        user_id = response.json().get('id')
    except requests.exceptions.RequestException as e:
        print(f"Error adding user: {str(e)}")
        return None
    
    # Get group ID for tableau_solutions_engineers
    try:
        response = requests.get(scim_group_endpoint, headers=headers)
        response.raise_for_status()
        groups = response.json().get('Resources', [])
        group_id = None
        current_members = None
        
        for group in groups:
            if group['displayName'] == 'tableau_solutions_engineers':
                group_id = group['id']
                current_members = group.get('members', [])
                break
                
        if not group_id:
            print("Group 'tableau_solutions_engineers' not found")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Error getting groups: {str(e)}")
        return None
    
    # Add the new user to the existing members
    current_members = current_members or []
    current_members.append({"value": user_id})
    
    # Simplified group update payload
    group_update_payload = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
        "members": current_members
    }
    
    try:
        group_url = f"{workspace_url}/api/2.0/preview/scim/v2/Groups/{group_id}"
        response = requests.put(group_url, 
                              headers=headers, 
                              json=group_update_payload)
        response.raise_for_status()
        print(f"Successfully added user to tableau_solutions_engineers group")
    except requests.exceptions.RequestException as e:
        print(f"Error adding user to group: {str(e)}")
        if response.text:
            print(f"Response content: {response.text}")
        return None
    
    return user_id


# Example usage
workspace_url = "https://dbc-6fcbae34-bdbb.cloud.databricks.com/"
access_token = "dapib0e3106a246e3bcee955a75da831c2a1"
email = "zzuberi@salesforce.com"

user_id = add_user_to_databricks(workspace_url, access_token, email)