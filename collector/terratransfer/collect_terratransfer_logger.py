import os
import sys
import re
import csv
import requests
from datetime import datetime
import logging
from utils.utils_terratransfer import (
    aggregate_daily_to_monthly,
    aggregate_monthly_to_yearly,
    aggregate_yearly_to_summary
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the working directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
working_directory = os.path.join(os.getcwd(), "data/ENVI/terratransfer")
os.makedirs(working_directory, exist_ok=True)

# Path to the CSV file containing logger information
logger_info_file = os.path.join(working_directory, 'bochum_terratransfer_logger.csv')

loggers_data = []

# Load data from the CSV file
with open(logger_info_file, mode='r') as file:
    reader = csv.DictReader(file)
    for row in reader:
        loggers_data.append({
            'logger_id': row['logger_id'],
            'name': row['name'],
            'latitude': float(row['latitude']),
            'longitude': float(row['longitude'])
        })

api_key = os.getenv("BOCHUM_TERRATRANSFER_API_KEY")
api_url = os.getenv('BOCHUM_TERRATRANSFER_API_URL')

# This will hold all the data for the summary CSV
all_rows = []

def fetch_and_save_data(logger_id, latitude, longitude, name):
    try:
        days_since_last_run = 1
        today_date = datetime.now().strftime('%Y-%m-%d')

        url = f'{api_url}?APIKEY={api_key}&LOGGER={logger_id}&DAYS={days_since_last_run}'
        logger.info(f"Fetching data from URL: {url}")
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad status codes
        data = response.json()

        pegel = data.get('PEGEL', [])
        wtemp = data.get('WTemp', [])
        o = data.get('O', [])
        do = data.get('DO', [])
        elL = data.get('elL', [])
        hkBat = data.get('HKBat', [])
        hkTemp = data.get('HKTemp', [])
        hkHum = data.get('HKHum', [])

        rows = []
        for entry in pegel:
            for timestamp, value in entry.items():
                row = {
                    'datetime': datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S'),
                    'logger_id': logger_id,
                    'name': name,
                    'latitude': latitude,
                    'longitude': longitude,
                    'pegel': value,
                    'wtemp': next((e.get(timestamp) for e in wtemp if timestamp in e), None),
                    'o': next((e.get(timestamp) for e in o if timestamp in e), None),
                    'do': next((e.get(timestamp) for e in do if timestamp in e), None),
                    'elL': next((e.get(timestamp) for e in elL if timestamp in e), None),
                    'hkBat': next((e.get(timestamp) for e in hkBat if timestamp in e), None),
                    'hkTemp': next((e.get(timestamp) for e in hkTemp if timestamp in e), None),
                    'hkHum': next((e.get(timestamp) for e in hkHum if timestamp in e), None)
                }
                rows.append(row)

        # Sort rows by datetime ascending
        rows_sorted_desc = sorted(rows, key=lambda x: x['datetime'], reverse=False)

        # Check if there's any data to save
        if rows_sorted_desc:
            # Define the folder structure
            sanitized_name = re.sub(r'\W+', '_', name).lower()
            logger_folder = os.path.join(working_directory, f'{logger_id}_{sanitized_name}', 'daily')
            os.makedirs(logger_folder, exist_ok=True)

            filename = os.path.join(logger_folder, f'bochum_terratransfer_{logger_id}_{sanitized_name}_{today_date}.csv')

            logger.info(f"Saving data to {filename}")
            with open(filename, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=[
                    'datetime', 'logger_id', 'name', 'latitude', 'longitude', 'pegel',
                    'wtemp', 'o', 'do', 'elL', 'hkBat', 'hkTemp', 'hkHum'
                ])
                writer.writeheader()
                writer.writerows(rows_sorted_desc)

            logger.info(f"Data successfully saved for logger {logger_id}")

            # Append sorted rows to the all_rows list for summary
            all_rows.extend(rows_sorted_desc)
        else:
            logger.info(f"No data fetched for logger {logger_id} on {today_date}. Skipping file creation.")

    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP error occurred: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

# Fetch and save data for all loggers
for logger_info in loggers_data:
    fetch_and_save_data(logger_info['logger_id'], logger_info['latitude'], logger_info['longitude'], logger_info['name'])

# Save the summarized data to a CSV file, sorted by datetime ascending
if all_rows:
    summary_filename = os.path.join(working_directory, 'bochum_terratransfer_summary.csv')
    logger.info(f"Saving summarized data to {summary_filename}")

    # Sort all rows by datetime ascending
    all_rows_sorted_desc = sorted(all_rows, key=lambda x: x['datetime'], reverse=False)

    with open(summary_filename, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=[
            'datetime', 'logger_id', 'name', 'latitude', 'longitude', 'pegel',
            'wtemp', 'o', 'do', 'elL', 'hkBat', 'hkTemp', 'hkHum'
        ])
        writer.writeheader()
        writer.writerows(all_rows_sorted_desc)

    logger.info(f"Summarized data successfully saved to {summary_filename}")

# Aggregate data
for logger_info in loggers_data:
    aggregate_daily_to_monthly(logger_info['logger_id'], logger_info['name'], working_directory)
    aggregate_monthly_to_yearly(logger_info['logger_id'], logger_info['name'], working_directory)
    aggregate_yearly_to_summary(logger_info['logger_id'], logger_info['name'], working_directory)
