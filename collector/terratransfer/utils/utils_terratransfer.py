import os
import re
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def aggregate_daily_to_monthly(logger_id, name, working_directory='terratransfer'):
    sanitized_name = re.sub(r'\W+', '_', name).lower()
    logger_folder = os.path.join(working_directory, f'{logger_id}_{sanitized_name}')
    daily_folder = os.path.join(logger_folder, 'daily')
    monthly_folder = os.path.join(logger_folder, 'monthly')
    os.makedirs(monthly_folder, exist_ok=True)

    # Grouping files by month
    monthly_data = {}
    for daily_file in os.listdir(daily_folder):
        if daily_file.endswith('.csv'):
            file_path = os.path.join(daily_folder, daily_file)
            df = pd.read_csv(file_path)
            df['datetime'] = pd.to_datetime(df['datetime'])  # Ensure datetime is parsed correctly
            df['year_month'] = df['datetime'].dt.strftime('%Y-%m')  # Extract YYYY-MM

            for month, group_df in df.groupby('year_month'):
                if month not in monthly_data:
                    monthly_data[month] = []
                monthly_data[month].append(group_df)

    for month, dfs in monthly_data.items():
        monthly_df = pd.concat(dfs).sort_values('datetime')
        monthly_file = os.path.join(monthly_folder, f'bochum_terratransfer_{logger_id}_{month}.csv')
        monthly_df.to_csv(monthly_file, index=False)
        logger.info(f"Aggregated daily files into monthly file: {monthly_file}")

def aggregate_monthly_to_yearly(logger_id, name, working_directory='terratransfer'):
    sanitized_name = re.sub(r'\W+', '_', name).lower()
    logger_folder = os.path.join(working_directory, f'{logger_id}_{sanitized_name}')
    monthly_folder = os.path.join(logger_folder, 'monthly')
    yearly_folder = os.path.join(logger_folder, 'yearly')
    os.makedirs(yearly_folder, exist_ok=True)

    # Grouping files by year
    yearly_data = {}
    for monthly_file in os.listdir(monthly_folder):
        if monthly_file.endswith('.csv'):
            file_path = os.path.join(monthly_folder, monthly_file)
            df = pd.read_csv(file_path)
            df['datetime'] = pd.to_datetime(df['datetime'])
            year = df['datetime'].dt.year.iloc[0]  # Extract YYYY
            if year not in yearly_data:
                yearly_data[year] = []
            yearly_data[year].append(df)

    for year, dfs in yearly_data.items():
        yearly_df = pd.concat(dfs).sort_values('datetime')
        yearly_file = os.path.join(yearly_folder, f'bochum_terratransfer_{logger_id}_{year}.csv')
        yearly_df.to_csv(yearly_file, index=False)
        logger.info(f"Aggregated monthly files into yearly file: {yearly_file}")

def aggregate_yearly_to_summary(logger_id, name, working_directory='terratransfer'):
    sanitized_name = re.sub(r'\W+', '_', name).lower()
    logger_folder = os.path.join(working_directory, f'{logger_id}_{sanitized_name}')
    yearly_folder = os.path.join(logger_folder, 'yearly')
    summary_file = os.path.join(logger_folder, f'bochum_terratransfer_{logger_id}_summary.csv')

    all_data = []
    for yearly_file in os.listdir(yearly_folder):
        if yearly_file.endswith('.csv'):
            file_path = os.path.join(yearly_folder, yearly_file)
            df = pd.read_csv(file_path)
            all_data.append(df)

    if all_data:
        summary_df = pd.concat(all_data).sort_values('datetime')
        summary_df.to_csv(summary_file, index=False)
        logger.info(f"Aggregated yearly files into summary file: {summary_file}")
