import os
import requests
import pandas as pd
from datetime import datetime

# Retrieve the API keys from environment variables
application_key = os.getenv("BOCHUM_ECOWITT_APPLICATION_KEY")
api_key = os.getenv("BOCHUM_ECOWITT_API_KEY")

# Define the API endpoint and existing CSV URLs
api_endpoint = 'https://api.ecowitt.net/api/v3/device/'
location_csv_url = 'https://raw.githubusercontent.com/StadtBochum/OpenData/main/data/ENVI/ecowitt_gw2001/bochum_ecowitt_gw2001_locations.csv'
extended_csv_url = 'https://raw.githubusercontent.com/StadtBochum/OpenData/main/data/ENVI/ecowitt_gw2001/bochum_ecowitt_gw2001_extended.csv'

# Define the directory to save the CSV files
data_dir = os.path.join(os.getcwd(), "data/ENVI/ecowitt_gw2001")
os.makedirs(data_dir, exist_ok=True)

# Define the status CSV file path using data_dir
status_csv_path = os.path.join(data_dir, "bochum_ecowitt_gw2001_device_status.csv")

# Fetch the existing CSV data
locations_csv = pd.read_csv(location_csv_url)
extended_csv = pd.read_csv(extended_csv_url)

# Make the API request with parameters
response = requests.get(
    f"{api_endpoint}list",
    params={
        "application_key": application_key,
        "api_key": api_key,
        "limit": 20,
    },
    timeout=30
)

# Check if the request was successful
if response.status_code == 200:
    api_data = response.json().get('data', {}).get('list', [])
else:
    api_data = []

# Extract relevant data from API response for the locations CSV
api_df = pd.DataFrame(api_data)
api_relevant_df = api_df[['name', 'latitude', 'longitude', 'mac']].rename(
    columns={'name': 'Name', 'latitude': 'Latitude', 'longitude': 'Longitude', 'mac': 'Mac'}
)

# Merge the existing locations CSV data with the new API data based on the 'Mac' column
merged_locations_df = pd.merge(locations_csv, api_relevant_df, left_on='MAC', right_on='Mac', how='outer', suffixes=('_csv', '_api'))

# Use the API data if available, otherwise keep the existing CSV data
merged_locations_df['Name'] = merged_locations_df['Name_api'].combine_first(merged_locations_df['Name_csv'])
merged_locations_df['Latitude'] = merged_locations_df['Latitude_api'].combine_first(merged_locations_df['Latitude_csv'])
merged_locations_df['Longitude'] = merged_locations_df['Longitude_api'].combine_first(merged_locations_df['Longitude_csv'])

# Drop the temporary columns used for merging
merged_locations_df = merged_locations_df[['Name', 'Latitude', 'Longitude', 'MAC']]

# Sort the merged DataFrame by MAC
merged_locations_df = merged_locations_df.sort_values(by='MAC')

# Merge the existing extended CSV data with the new API data based on the 'Mac' column
merged_extended_df = pd.merge(extended_csv, api_df, on='mac', how='outer', suffixes=('_csv', '_api'))

# Use the API data if available, otherwise keep the existing CSV data
for col in api_df.columns:
    if col != 'mac':
        merged_extended_df[col] = merged_extended_df[f'{col}_api'].combine_first(merged_extended_df[f'{col}_csv'])

# Drop the temporary columns used for merging
merged_extended_df = merged_extended_df[api_df.columns]

# Sort the extended DataFrame by ID
merged_extended_df = merged_extended_df.sort_values(by='id')

# Save the merged locations data to a new CSV file
merged_locations_csv_path = os.path.join(data_dir, 'bochum_ecowitt_gw2001_locations.csv')
merged_locations_df.to_csv(merged_locations_csv_path, index=False)

# Save the merged extended data to a new CSV file
merged_extended_csv_path = os.path.join(data_dir, 'bochum_ecowitt_gw2001_extended.csv')
merged_extended_df.to_csv(merged_extended_csv_path, index=False)

# Prepare the device status data
current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
mac_addresses = locations_csv['MAC'].unique()
status_dict = {'datetime': [current_time]}

for mac in mac_addresses:
    status_dict[mac] = ['online' if mac in api_relevant_df['Mac'].values else 'offline']

status_df = pd.DataFrame(status_dict)

# Append the new status to the existing status CSV, if it exists, otherwise create it
if os.path.exists(status_csv_path):
    existing_status_df = pd.read_csv(status_csv_path)
    combined_status_df = pd.concat([existing_status_df, status_df], ignore_index=True)
else:
    combined_status_df = status_df

# Save the combined status data to the CSV file
combined_status_df.to_csv(status_csv_path, index=False)
