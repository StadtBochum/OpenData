import os
import re
import time
import requests
import pytz
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


api_endpoint = 'https://api.ecowitt.net/api/v3/device/'
temp_unitid = 1
pressure_unitid = 3
wind_speed_unitid = 7
rainfall_unitid = 12

application_key = os.getenv("BOCHUM_ECOWITT_APPLICATION_KEY")
api_key = os.getenv("BOCHUM_ECOWITT_API_KEY")


def load_device_list():
    """
    Load the device list from the API and return a DataFrame
    """
    df = pd.DataFrame()
    time.sleep(1)
    response = requests.get(f"{api_endpoint}list",
                            params={
                                "application_key": application_key,
                                "api_key": api_key,
                                "limit": 20,
                            },
                            timeout=5)
    if response.status_code == 200:
        json_data = response.json()
        if json_data['code'] == 0:
            devices = json_data['data']['list']
            df = pd.DataFrame(devices)
        else:
            print("CAUTION: JSON Data malformed")
    return df

def create_directories_for_devices(base_dir, device_df):
    """
    Create directories named after device MAC addresses.
    Args:
        base_dir (str): The base directory where device directories will be created.
        device_df (pd.DataFrame): DataFrame containing device data including MAC addresses.
    """
    # Ensure the base directory exists
    os.makedirs(base_dir, exist_ok=True)

    # Iterate over the DataFrame rows
    for index, row in device_df.iterrows():
        # Create a directory name from the MAC address
        mac_address = row['mac'].replace(":", "")  # Remove colons if present
        dir_path = os.path.join(base_dir, mac_address)

        # Create the directory if it doesn't already exist
        os.makedirs(dir_path, exist_ok=True)
        #print(f"Directory created or exists: {dir_path}")

def process_historic_data(data, category):
    """
    Process the historic data
    """
    sensor_data = {}
    if category in data["data"]:
        category_data = data["data"][category]
        for sensor_key, sensor_values in category_data.items():
            if sensor_key != "unit":
                #print(sensor_key)
                #print(sensor_values)
                sensor_data[f"{category}_{sensor_key}"] = \
                    pd.Series(sensor_values["list"]).replace('-', np.nan).astype(float)
    return sensor_data


def load_historic_data(mac, start_date, end_date):
    """
    Load the historic data from the API and return a DataFrame
    """
    sensor_df = pd.DataFrame()
    params = {
        "application_key": application_key,
        "api_key": api_key,
        "mac": mac,
        "start_date": start_date,
        "end_date": end_date,
        "cycle_type": "auto",
        # old: "outdoor",
        "call_back": "outdoor,indoor,solar_and_uvi,rainfall_piezo,wind,pressure,battery",
        "temp_unitid": temp_unitid,
        "pressure_unitid": pressure_unitid,
        "wind_speed_unitid": wind_speed_unitid,
        "rainfall_unitid": rainfall_unitid,
    }
    time.sleep(2)
    response = requests.get(f"{api_endpoint}history",
                            params=params,
                            timeout=30)
    if response.status_code == 200:
        data = response.json()
        if "data" in data:
            for category in data["data"]:
                sensor_cat_df = pd.DataFrame(
                    process_historic_data(data, category))
                sensor_cat_df.index = pd.to_datetime(
                    sensor_cat_df.index, unit="s")
                sensor_df = pd.concat([sensor_df, sensor_cat_df], axis=1)
    return sensor_df


def clean_historic_data(historic):
    """
    Remove empty DataFrames from the dictionary
    """
    return {device_name: data for device_name, data in historic.items()
            if data is not None and not data.empty}


def load_historic_data_for_days(benches, days=1):
    """
    Load the historic data from the API for a specified number of days ending yesterday.
    If no number of days is specified, defaults to yesterday only.

    Args:
    benches (DataFrame): DataFrame containing device data including MAC addresses.
    days (int): Number of days to retrieve data for, default is 1 (yesterday).

    Returns:
    dict: A dictionary of DataFrames with MAC addresses as keys.
    """
    berlin_timezone = pytz.timezone('Europe/Berlin')
    current_time_berlin = datetime.now(berlin_timezone)

    # Calculate the end of the period as the start of today (midnight)
    end_of_period = current_time_berlin.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_period = end_of_period - timedelta(days=days)

    # Format times for API request
    start_date_str = start_of_period.strftime('%Y-%m-%d %H:%M:%S')
    end_date_str = (end_of_period - timedelta(seconds=1)).strftime('%Y-%m-%d %H:%M:%S')

    historic_data_all = {}

    for index, row in benches.iterrows():
        mac = row["mac"]
        historic_data = load_historic_data(mac, start_date_str, end_date_str)

        if historic_data is not None and not historic_data.empty:
            if mac not in historic_data_all:
                historic_data_all[mac] = historic_data
            else:
                historic_data_all[mac] = pd.concat(
                    [historic_data_all[mac], historic_data], ignore_index=False)

    return clean_historic_data(historic_data_all)


def load_historic_data_for_date(benches, specific_date):
    """
    Load the historic data from the API for a specified date.

    Args:
    benches (DataFrame): DataFrame containing device data including MAC addresses.
    specific_date (str): The specific date to retrieve data for, formatted as 'YYYY-MM-DD'.

    Returns:
    dict: A dictionary of DataFrames with MAC addresses as keys.
    """
    berlin_timezone = pytz.timezone('Europe/Berlin')
    specific_date_dt = datetime.strptime(specific_date, '%Y-%m-%d')

    # Set the datetime to use Berlin timezone
    specific_date_dt = berlin_timezone.localize(specific_date_dt)

    # Calculate the start and end of the specific date in Berlin timezone
    start_of_day = specific_date_dt
    end_of_day = start_of_day + timedelta(days=1, seconds=-1)
    #print(start_of_day)
    #print(end_of_day)

    # Format times for API request
    start_date_str = start_of_day.strftime('%Y-%m-%d %H:%M:%S')
    end_date_str = end_of_day.strftime('%Y-%m-%d %H:%M:%S')

    historic_data_all = {}

    for index, row in benches.iterrows():
        mac = row["mac"]
        #print(mac)
        #print(start_date_str)
        #print(end_date_str)
        historic_data = load_historic_data(mac, start_date_str, end_date_str)

        if historic_data is not None and not historic_data.empty:
            if mac not in historic_data_all:
                historic_data_all[mac] = historic_data
            else:
                historic_data_all[mac] = pd.concat(
                    [historic_data_all[mac], historic_data], ignore_index=False)

    return clean_historic_data(historic_data_all)

           
def write_dataframes_to_csv(dataframes, archive_dir):
    """
    Write the DataFrames to CSV files, organizing them into subdirectories by MAC address and then
    further into 'daily' or 'monthly' depending on the data span, adjusted for Berlin timezone.

    Args:
    dataframes (dict): A dictionary of DataFrames with MAC addresses as keys.
    archive_dir (str): The base directory where CSV files will be saved.
    """
    berlin_timezone = pytz.timezone('Europe/Berlin')
    pattern = r"[!@#$%^&*()_+={}\[\]:\";'<>?,./\\|`\s]"

    for mac, df in dataframes.items():
        if not df.empty:
            # Ensure DataFrame index is localized to UTC and then converted to Berlin time
            if df.index.tz is None:
                df.index = pd.to_datetime(df.index).tz_localize('UTC').tz_convert(berlin_timezone)

            # Normalize the MAC address to use as a folder name
            clean_mac = re.sub(pattern, '', mac)
            mac_dir = os.path.join(archive_dir, clean_mac)

            # Find the local day boundaries based on the first and last timestamps in Berlin time
            local_min_day = df.index.min().normalize()
            local_max_day = df.index.max().normalize()

            # Determine if the data is all from the same 'local' day
            if local_min_day == local_max_day:
                subfolder = "daily"
                date_str = local_min_day.strftime('%Y-%m-%d')
            else:
                subfolder = "monthly"
                date_str = local_min_day.strftime('%Y-%m')

            # Define the full path to the output file
            full_path = os.path.join(mac_dir, subfolder)
            os.makedirs(full_path, exist_ok=True)  # Ensure the directory exists
            output_file = os.path.join(full_path, f"bochum_ecowitt_gw2001_{clean_mac}_{date_str}.csv")

            # Write the DataFrame to the CSV file
            df.index.name = 'datetime'
            df.to_csv(output_file, mode='w', index=True)
            print(f"Data written for {mac} to {output_file}")
        else:
            print(f"No data available for {mac}")


def aggregate_daily_to_monthly(base_dir):
    """
    Aggregates all CSV files from 'daily' subdirectories within each MAC/device subdirectory
    in the base directory into separate monthly CSV files stored in a 'monthly' subdirectory.

    Args:
    base_dir (str): The path to the base directory containing device subdirectories.
    """
    for entry in os.listdir(base_dir):  # Iterate over each entry in the base directory
        device_dir = os.path.join(base_dir, entry)
        if os.path.isdir(device_dir):  # Check if the entry is a directory
            daily_dir = os.path.join(device_dir, 'daily')
            monthly_dir = os.path.join(device_dir, 'monthly')
            os.makedirs(monthly_dir, exist_ok=True)  # Create the monthly directory if it doesn't exist

            # Gather all data by month
            monthly_data_groups = {}

            if os.path.exists(daily_dir):
                # Process each CSV file within the 'daily' directory
                for daily_file in sorted(os.listdir(daily_dir)):
                    if daily_file.endswith('.csv'):
                        file_path = os.path.join(daily_dir, daily_file)
                        df = pd.read_csv(file_path)
                        month_year = pd.to_datetime(daily_file.split('_')[-1].split('.')[0]).strftime('%Y-%m')

                        if month_year not in monthly_data_groups:
                            monthly_data_groups[month_year] = []
                        monthly_data_groups[month_year].append(df)

                # Write each month's data to a separate CSV file
                for month_year, dfs in monthly_data_groups.items():
                    if dfs:
                        monthly_data = pd.concat(dfs, ignore_index=True)
                        monthly_file_name = f"bochum_ecowitt_gw2001_{entry}_{month_year}.csv"
                        monthly_file_path = os.path.join(monthly_dir, monthly_file_name)
                        monthly_data.to_csv(monthly_file_path, index=False)
                        print(f"Monthly data for {entry} in {month_year} aggregated and written to {monthly_file_path}")
                    else:
                        print(f"No data found for {month_year}")
            else:
                print(f"No 'daily' directory found for {entry}")
        else:
            print(f"Skipped {device_dir}, which is not a directory")


def aggregate_monthly_to_yearly(base_dir):
    """
    Aggregates all CSV files from 'monthly' subdirectories within each MAC/device subdirectory
    in the base directory into separate yearly CSV files stored in a 'yearly' subdirectory.

    Args:
    base_dir (str): The path to the base directory containing device subdirectories.
    """
    for entry in os.listdir(base_dir):  # Iterate over each entry in the base directory
        device_dir = os.path.join(base_dir, entry)
        if os.path.isdir(device_dir):  # Check if the entry is a directory
            monthly_dir = os.path.join(device_dir, 'monthly')
            yearly_dir = os.path.join(device_dir, 'yearly')
            os.makedirs(yearly_dir, exist_ok=True)  # Create the yearly directory if it doesn't exist

            if os.path.exists(monthly_dir):
                # Group files by year
                yearly_files = {}
                for monthly_file in sorted(os.listdir(monthly_dir)):
                    if monthly_file.endswith('.csv'):
                        year = monthly_file.split('_')[-1][:4]  # Extract year from filename
                        if year not in yearly_files:
                            yearly_files[year] = []
                        yearly_files[year].append(os.path.join(monthly_dir, monthly_file))

                # Aggregate data by year and write to yearly CSV files
                for year, files in yearly_files.items():
                    dfs = [pd.read_csv(file) for file in files]
                    yearly_data = pd.concat(dfs, ignore_index=True)
                    yearly_file_path = os.path.join(yearly_dir, f"bochum_ecowitt_gw2001_{entry}_{year}.csv")
                    yearly_data.to_csv(yearly_file_path, index=False)
                    print(f"Yearly data for {entry} in {year} aggregated and written to {yearly_file_path}")
            else:
                print(f"No 'monthly' directory found for {entry}")
        else:
            print(f"Skipped {device_dir}, which is not a directory")


def aggregate_yearly_to_one_file(base_dir):
    """
    Aggregates all yearly CSV files within each device's 'yearly' subdirectory in the base directory
    into a single CSV file per device.

    Args:
    base_dir (str): The path to the base directory containing device subdirectories with 'yearly' folders.
    """
    for device_entry in os.listdir(base_dir):  # Iterate over each device directory in the base directory
        device_dir = os.path.join(base_dir, device_entry)
        yearly_dir = os.path.join(device_dir, 'yearly')
        
        if os.path.isdir(yearly_dir):  # Make sure the 'yearly' directory exists
            all_yearly_data = []  # List to store DataFrames from all yearly files

            # Loop through all CSV files in the 'yearly' directory
            for yearly_file in sorted(os.listdir(yearly_dir)):
                if yearly_file.endswith('.csv'):
                    file_path = os.path.join(yearly_dir, yearly_file)
                    df = pd.read_csv(file_path)
                    all_yearly_data.append(df)

            # Concatenate all DataFrames into one, if any are found
            if all_yearly_data:
                combined_df = pd.concat(all_yearly_data, ignore_index=True)
                output_file_path = os.path.join(device_dir, f"bochum_ecowitt_gw2001_{device_entry}.csv")
                combined_df.to_csv(output_file_path, index=False)
                print(f"Combined yearly data for {device_entry} written to {output_file_path}")
            else:
                print(f"No yearly data files found for {device_entry}")
        else:
            print(f"No 'yearly' directory found for {device_entry}")
