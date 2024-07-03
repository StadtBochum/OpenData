import os
import requests
import pandas as pd

# Retrieve the API keys from environment variables
application_key = os.getenv("BOCHUM_ECOWITT_APPLICATION_KEY")
api_key = os.getenv("BOCHUM_ECOWITT_API_KEY")

# Define the API endpoint and existing CSV URL
api_endpoint = 'https://api.ecowitt.net/api/v3/device/'
location_csv_url = 'https://raw.githubusercontent.com/StadtBochum/OpenData/main/data/ENVI/ecowitt_gw2001/bochum_ecowitt_gw2001_locations.csv'

# Fetch the existing CSV data
locations_csv = pd.read_csv(location_csv_url)

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

# Extract relevant data from API response for the first CSV
api_df = pd.DataFrame(api_data)
api_relevant_df = api_df[['name', 'latitude', 'longitude', 'mac']].rename(
    columns={'name': 'Name', 'latitude': 'Latitude', 'longitude': 'Longitude', 'mac': 'Mac'}
)

# Merge the existing CSV data with the new API data based on the 'Mac' column
merged_df = pd.merge(locations_csv, api_relevant_df, left_on='MAC', right_on='Mac', how='outer', suffixes=('_csv', '_api'))

# Use the API data if available, otherwise keep the existing CSV data
merged_df['Name'] = merged_df['Name_api'].combine_first(merged_df['Name_csv'])
merged_df['Latitude'] = merged_df['Latitude_api'].combine_first(merged_df['Latitude_csv'])
merged_df['Longitude'] = merged_df['Longitude_api'].combine_first(merged_df['Longitude_csv'])

# Drop the temporary columns used for merging
merged_df = merged_df[['Name', 'Latitude', 'Longitude', 'MAC']]

# Define the directory to save the CSV files
data_dir = os.path.join(os.getcwd(), "data/ENVI/ecowitt_gw2001")
os.makedirs(data_dir, exist_ok=True)

# Save the merged data to a new CSV file
merged_csv_path = os.path.join(data_dir, 'bochum_ecowitt_gw2001_locations.csv')
merged_df.to_csv(merged_csv_path, index=False)

# Save all API data to another CSV file
api_csv_path = os.path.join(data_dir, 'bochum_ecowitt_gw2001_extended.csv')
api_df.to_csv(api_csv_path, index=False)

# Show the column names of the new CSVs
print("Columns in the merged CSV:")
print(merged_df.columns)

print("\nColumns in the extended CSV:")
print(api_df.columns)
