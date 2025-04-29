import requests

# Replace these with your actual values
databricks_instance = "https://dbc-6fcbae34-bdbb.cloud.databricks.com/"  # e.g., https://<region>.azuredatabricks.net
access_token = "dapib0e3106a246e3bcee955a75da831c2a1"
user_email = "anesseth@salesforce.com"
group_name = "tableau_solutions_engineers"

# Headers for Databricks API
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

def add_user_to_workspace():
    """
    Adds a new user to the Databricks workspace.
    """
    url = f"{databricks_instance}/api/2.0/preview/scim/v2/Users"
    payload = {
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
        "userName": user_email
    }

    response = requests.post(url, json=payload, headers=headers)
    if response.status_code == 201:
        print(f"User {user_email} added successfully.")
        user_id = response.json().get("id")
        return user_id
    elif response.status_code == 409:
        print(f"User {user_email} already exists.")
        # Fetch the user ID of the existing user
        user_id = get_user_id()
        return user_id
    else:
        print(f"Error adding user: {response.text}")
        return None

def get_user_id():
    """
    Fetches the user ID of an existing user.
    """
    url = f"{databricks_instance}/api/2.0/preview/scim/v2/Users"
    params = {"filter": f"userName eq \"{user_email}\""}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        users = response.json().get("Resources", [])
        if users:
            return users[0].get("id")
        else:
            print(f"User {user_email} not found.")
            return None
    else:
        print(f"Error fetching user ID: {response.text}")
        return None

def add_user_to_group(user_id):
    """
    Adds a user to a specified group.
    """
    # Get the group ID
    group_id = get_group_id()
    if not group_id:
        print(f"Group {group_name} not found.")
        return

    url = f"{databricks_instance}/api/2.0/preview/scim/v2/Groups/{group_id}"
    payload = {
        "members": [
            {
                "value": user_id
            }
        ]
    }

    response = requests.patch(url, json=payload, headers=headers)
    if response.status_code == 204:
        print(f"User {user_email} added to group {group_name} successfully.")
    else:
        print(f"Error adding user to group: {response.text}")

def get_group_id():
    """
    Fetches the group ID of the specified group.
    """
    url = f"{databricks_instance}/api/2.0/preview/scim/v2/Groups"
    params = {"filter": f"displayName eq \"{group_name}\""}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        groups = response.json().get("Resources", [])
        if groups:
            return groups[0].get("id")
        else:
            print(f"Group {group_name} not found.")
            return None
    else:
        print(f"Error fetching group ID: {response.text}")
        return None

# Main execution
if __name__ == "__main__":
    user_id = add_user_to_workspace()
    if user_id:
        add_user_to_group(user_id)