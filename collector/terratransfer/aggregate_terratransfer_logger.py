import os
import sys

from utils.utils_terratransfer import (
    get_terratransfer_logger,
    aggregate_daily_to_monthly,
    aggregate_monthly_to_yearly,
    aggregate_yearly_to_summary
)

# Define the working directory
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
working_directory = os.path.join(os.getcwd(), "data/ENVI/terratransfer")
os.makedirs(working_directory, exist_ok=True)

# Path to the CSV file containing logger information
logger_info_file = os.path.join(working_directory, 'bochum_terratransfer_logger.csv')

loggers_data = get_terratransfer_logger(logger_info_file)

# Aggregate data
for logger_info in loggers_data:
    aggregate_daily_to_monthly(logger_info['logger_id'], logger_info['name'], working_directory)
    aggregate_monthly_to_yearly(logger_info['logger_id'], logger_info['name'], working_directory)
    aggregate_yearly_to_summary(logger_info['logger_id'], logger_info['name'], working_directory)
