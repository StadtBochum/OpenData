import sys
import os
from datetime import datetime, timedelta
import pytz
import pandas as pd

from utils.utils_collect_ecowitt import load_device_list, \
    create_directories_for_devices, load_historic_data_for_days, \
    write_dataframes_to_csv, aggregate_daily_to_monthly, \
    load_historic_data_for_date, aggregate_monthly_to_yearly

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(os.getcwd(), "data/ENVI/ecowitt_gw2001")

berlin_timezone = pytz.timezone('Europe/Berlin')
current_time_berlin = datetime.now(berlin_timezone)
end_of_yesterday = current_time_berlin.replace(hour=0, minute=0, second=0, microsecond=0)  # Midnight at the start of today
start_of_yesterday = end_of_yesterday - timedelta(days=1)  # Midnight at the start of yesterday
start_of_yesterday_str = start_of_yesterday.strftime('%Y-%m-%d %H:%M:%S')
end_of_yesterday_str = end_of_yesterday.strftime('%Y-%m-%d %H:%M:%S')

ecowitt_gw2001_list = load_device_list()
#print(smart_benches)

create_directories_for_devices(data_dir, ecowitt_gw2001_list)

#data_yesterday = load_historic_data_for_days(ecowitt_gw2001_list)
#write_dataframes_to_csv(data_yesterday, data_dir)

start_date = "2024-04-01"
end_date = "2024-04-09"

dates_to_process = pd.date_range(start=start_date, end=end_date, freq='D').strftime('%Y-%m-%d')

if True == True:
    for date in dates_to_process:
        data_specific = load_historic_data_for_date(ecowitt_gw2001_list, date)
        write_dataframes_to_csv(data_specific, data_dir)

aggregate_daily_to_monthly(data_dir)
aggregate_monthly_to_yearly(data_dir)
