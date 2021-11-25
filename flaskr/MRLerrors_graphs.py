import pandas as pd
import pyodbc
from datetime import datetime
import numpy as np # Just for busisness days function!
import io
from glob import glob
from functools import lru_cache
import time

from zipfile import ZipFile
#import cv2
import os

@lru_cache(maxsize=1)
def get_MRLdashboard(this_SN):
    SN = []
    timestamps = []
    timestrings = []
    alert_levels = []
    messages = []
    error_codes = []
    systems = []
    filenames = []
    week_deltas = []
        
    if this_SN == '242':
        log_error_files = glob(r"\\gcgbprmrl01\VRMRL242\Export\log_errors\*.csv")
        ref_time = datetime(2021,1,1,0,0,0,0)
    else:
        log_error_files = glob(r"\\gcgbprmrl01\VRMRL243\Export\log_errors\*.csv")
        ref_time = datetime(2019,6,1,0,0,0,0)
    #print(log_error_files)
    # Don't do this bit as it takes forever with all the TDS logs
    for log_error_file in log_error_files:
        with open(log_error_file, encoding='utf-16') as f:
            lines = f.readlines()
            for line in lines:
                parts = line.split(";")
        
                message = parts[0].replace("Error Displayed to user"," ")
                message_time_object = datetime.strptime(parts[1], '%m/%d/%Y %H:%M:%S:%f') # Format 02/21/2021  10:05:59:020
                timestamp = message_time_object.timestamp()
                week_delta = int((message_time_object - ref_time).days/7)
                timestring = time.ctime(message_time_object.timestamp())
                if "(" in message and ")" in message:
                    error_code = message.split('(')[1].split(')')[0]
                else:
                    error_code = ""
                SN.append(this_SN)
                timestamps.append(timestamp)
                timestrings.append(timestring)
                messages.append(message)
                error_codes.append(error_code)
                if "Critical" in message:
                    alert_levels.append("Criticial")
                else:
                    alert_levels.append("Warning")
                filenames.append(parts[2])
                system = os.path.split(log_error_file)[1][:-4]
                systems.append(system)
                week_deltas.append(week_delta)

    d = {'Timestamp': timestamps, \
        'SN': SN, \
        'System': systems, \
        'Date and time': timestrings, \
        'Alert level': alert_levels, \
        'Error message': messages, \
        'Error code': error_codes, \
        'Filename': filenames, \
        'Week': week_deltas}

    df_MRL = pd.DataFrame(data=d)
    df_MRL.drop_duplicates(subset=['Timestamp'], keep='first', inplace=True)
    df_MRL['Timestamp'] = pd.to_numeric(df_MRL['Timestamp'])
    df_MRL.sort_values(by=['Timestamp'])

    df_critical = df_MRL.loc[df_MRL['Alert level'] == 'Criticial']
    #df_critical.Week.value_counts()
    error_count_by_week = df_critical.Week.value_counts()

    list1 = error_count_by_week.index.to_list()
    list2 = error_count_by_week.to_list()

    list1, list2 = (list(t) for t in zip(*sorted(zip(list1, list2))))

    return list1,  list2
if __name__ == "__main__":
    get_MRLdashboard()