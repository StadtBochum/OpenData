import os
import json
import requests

# Configuration
CKAN_API_KEY = os.getenv("RVR_BOCHUM_CKAN")
CKAN_BASE_URL = "https://bochum.opendata.ruhr"
CKAN_API_URL = f'{CKAN_BASE_URL}/api/3/action'
data_dir = os.path.join(os.getcwd(), "data/ENVI/ecowitt_gw2001")

# Function to create or update a CKAN dataset
def create_or_update_dataset(dataset_info):
    dataset_name = dataset_info['name']
    
    # Check if the dataset already exists
    response = requests.get(f"{CKAN_API_URL}/package_show", params={"id": dataset_name}, headers={"Authorization": CKAN_API_KEY})
    if response.status_code == 200:
        print(f"Dataset '{dataset_name}' exists. Updating dataset...")
        # Update dataset if it exists
        data = response.json()["result"]
        data.update(dataset_info)
        response = requests.post(f"{CKAN_API_URL}/package_update", json=data, headers={"Authorization": CKAN_API_KEY})
    else:
        print(f"Dataset '{dataset_name}' does not exist. Creating dataset...")
        # Create a new dataset if it doesn't exist
        response = requests.post(f"{CKAN_API_URL}/package_create", json=dataset_info, headers={"Authorization": CKAN_API_KEY})

    if response.status_code not in (200, 201):
        raise Exception(f"Failed to create/update dataset: {response.text}")
    print(f"Dataset '{dataset_name}' created/updated successfully.")
    return response.json()["result"]["id"]

# Function to update or create a resource in a CKAN dataset
def update_or_create_resource(dataset_id, file_path):
    filename = os.path.basename(file_path)
    response = requests.get(f"{CKAN_API_URL}/package_show", params={"id": dataset_id}, headers={"Authorization": CKAN_API_KEY})
    
    if response.status_code == 200:
        dataset = response.json()["result"]
        resources = dataset.get("resources", [])
        
        # Check if a resource exists
        if resources:
            resource = resources[0]  # Assuming there's only one resource per dataset
            resource_id = resource["id"]
            print(f"Updating resource '{resource_id}' for dataset '{dataset_id}'...")
            
            with open(file_path, "rb") as f:
                update_response = requests.post(
                    f"{CKAN_API_URL}/resource_update",
                    headers={"Authorization": CKAN_API_KEY},
                    data={
                        "id": resource_id,
                        "name": filename,
                        "format": filename.split('.')[-1].upper(),  # Extract format from file extension
                    },
                    files={"upload": f}
                )
            
            if update_response.status_code not in (200, 201):
                raise Exception(f"Failed to update resource: {update_response.text}")
            print(f"Resource '{resource_id}' updated successfully.")
        else:
            print(f"No existing resource found for dataset '{dataset_id}'. Creating a new resource...")
            upload_file_resource(dataset_id, file_path)
    else:
        raise Exception(f"Failed to fetch dataset '{dataset_id}': {response.text}")

# Function to upload a file resource to a CKAN dataset (used if no existing resource is found)
def upload_file_resource(dataset_id, file_path):
    filename = os.path.basename(file_path)
    print(f"Uploading new resource '{filename}' to dataset '{dataset_id}'...")
    with open(file_path, "rb") as f:
        response = requests.post(
            f"{CKAN_API_URL}/resource_create",
            headers={"Authorization": CKAN_API_KEY},
            data={
                "package_id": dataset_id,
                "name": filename,
                "format": filename.split('.')[-1].upper(),  # Extract format from file extension
            },
            files={"upload": f}
        )
    
    if response.status_code not in (200, 201):
        raise Exception(f"Failed to upload file resource: {response.text}")
    print(f"Resource '{filename}' uploaded successfully.")

# Function to process each MAC folder
def process_mac_directory(mac_dir):
    mac_path = os.path.join(data_dir, mac_dir)
    
    json_file = None
    csv_file = None

    # Loop through files in the MAC folder
    print(f"Processing directory: {mac_dir}")
    for file_name in os.listdir(mac_path):
        if file_name.endswith('.json'):
            json_file = os.path.join(mac_path, file_name)
        elif file_name.endswith('.csv'):
            csv_file = os.path.join(mac_path, file_name)

    if json_file:
        print(f"Found JSON file: {json_file}")
    else:
        raise Exception(f"No JSON file found in {mac_dir}. Skipping...")

    if csv_file:
        print(f"Found CSV file: {csv_file}")
    else:
        raise Exception(f"No CSV file found in {mac_dir}. Skipping...")

    with open(json_file, 'r') as f:
        dataset_info = json.load(f)
    print(f"Successfully loaded JSON data from {json_file}")

    # Create or update the dataset in CKAN
    dataset_id = create_or_update_dataset(dataset_info)

    # Update or create the resource (CSV file)
    update_or_create_resource(dataset_id, csv_file)

# Function to scan and process all MAC directories
def scan_and_process():
    for mac_dir in os.listdir(data_dir):
        mac_path = os.path.join(data_dir, mac_dir)
        if os.path.isdir(mac_path):
            process_mac_directory(mac_dir)

if __name__ == "__main__":
    print("Starting CKAN dataset processing...")
    scan_and_process()
    print("CKAN dataset processing completed.")
