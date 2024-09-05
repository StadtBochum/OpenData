# Import libs
import sys
import os

from utils.utils_ecowitt_weather import (
    aggregate_daily_to_monthly,
    aggregate_monthly_to_yearly,
    aggregate_yearly_to_one_file
)

# Set variables
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(os.getcwd(), "data/ENVI/ecowitt_gw2001")

# Aggregate data from singlle csv files
aggregate_daily_to_monthly(data_dir)
aggregate_monthly_to_yearly(data_dir)
aggregate_yearly_to_one_file(data_dir)
