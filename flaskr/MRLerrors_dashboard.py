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

def get_MRLdashboard(this_SN):
    SN = []
    timestamps = []
    timestrings = []
    alert_levels = []
    messages = []
    error_codes = []
    systems = []
    filenames = []

    if this_SN == '242':
        support_packages = glob(r"\\gcgbprmrl01\VRMRL242\SupportPackages\zip") + glob(r"\\gcgbprmrl01\VRMRL242\SupportPackages\**\*.zip")
    else:
        support_packages = glob(r"\\gcgbprmrl01\VRMRL243\SupportPackages\*.zip") + glob(r"\\gcgbprmrl01\VRMRL243\SupportPackages\**\*.zip")
    #print(support_packages)
    #files = glob(r"C:\Users\ben.george\OneDrive - GenesisCare\MRL\Projects\Dashboard-web\SupportPackages\*.zip")
    #print(files)
    for zip_file in support_packages:
        # Get the support package name
        file_name = os.path.split(zip_file)[1][:-4]
        # Time stamp (epoch time) and printable string
        timestamp = os.path.getmtime(zip_file)
        timestring = time.ctime(timestamp)

        # Append this data
        SN.append(this_SN)
        timestamps.append(timestamp)
        timestrings.append(timestring)
        messages.append(file_name)
        error_codes.append('')
        alert_levels.append('New Support Package')
        systems.append('')
        filenames.append(zip_file)

        with ZipFile(zip_file,'r') as zip_ref:
            list_of_files = zip_ref.namelist()
            for elem in list_of_files:
                if 'ViewRayRe' in elem:
                    with io.TextIOWrapper(zip_ref.open(elem), encoding="utf-8") as f:
                        lines = f.readlines()
                        for i, line in enumerate(lines):
                            line = line.rstrip()
                            if "Error Displayed to user" in line:
                                # Check if this is the end of the message
                                j = 1
                                while "</ExtraData>" not in line:
                                    line = line + lines[i + j]
                                    j = j + 1
                                timestamp_str = lines[i-1][15:-16]
                                time_object = datetime.strptime(timestamp_str, '%m/%d/%Y %H:%M:%S:%f')
                                timestring = time.ctime(time_object.timestamp())
                                timestamp = time_object.timestamp()
                                message_str = line.replace("Error Displayed to user"," ")

                                SN.append(this_SN)
                                systems.append("From support package")
                                timestamps.append(timestamp)
                                timestrings.append(timestring)
                                messages.append(message_str[12:-22])
                                try:
                                    error_codes.append(message_str.split('(')[1].split(')')[0])
                                except:
                                    error_codes.append('no code') # catch bug where some errors have no (CODE)
                                if "Critical" in line:
                                    alert_levels.append("Criticial")
                                else:
                                    alert_levels.append("Warning")
                                filenames.append(elem)
    
    if this_SN == '242':
        log_error_files = glob(r"\\gcgbprmrl01\VRMRL242\Export\log_errors\*.csv")
    else:
        log_error_files = glob(r"\\gcgbprmrl01\VRMRL243\Export\log_errors\*.csv")
    #print(log_error_files)
    # Don't do this bit as it takes forever with all the TDS logs
    """
    for log_error_file in log_error_files:
        with open(log_error_file, encoding='utf-16') as f:
            lines = f.readlines()
            for line in lines:
                parts = line.split(";")
        
                message = parts[0].replace("Error Displayed to user"," ")
                message_time_object = datetime.strptime(parts[1], '%m/%d/%Y %H:%M:%S:%f') # Format 02/21/2021  10:05:59:020
                timestamp = message_time_object.timestamp()
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
    """

    d = {'Timestamp': timestamps, \
        'SN': SN, \
        'System': systems, \
        'Date and time': timestrings, \
        'Alert level': alert_levels, \
        'Error message': messages, \
        'Error code': error_codes, \
        'Filename': filenames}

    df_MRL = pd.DataFrame(data=d)
    df_MRL.drop_duplicates(subset=['Timestamp'], keep='first', inplace=True)
    df_MRL['Timestamp'] = pd.to_numeric(df_MRL['Timestamp'])
    df_MRL.sort_values(by=['Timestamp'])

    df_format = pd.DataFrame().reindex_like(df_MRL)
    for col in df_format:
        df_format[col] = df_format[col].astype(str)
    
    #print(df_MRL)
    ind = df_MRL.index[df_MRL['Alert level'].map(lambda x: 'Criticial' in str(x))]
    df_format.loc[ind, 'Alert level'] = 'alert-critical'
    ind = df_MRL.index[df_MRL['Alert level'].map(lambda x: 'Warning' in str(x))]
    df_format.loc[ind, 'Alert level'] = 'alert-warning'
    ind = df_MRL.index[df_MRL['Alert level'].map(lambda x: 'New Support Package' in str(x))]
    df_format.loc[ind, 'Alert level'] = 'support-package'
    df_format.loc[ind, 'Timestamp'] = 'support-package'
    df_format.loc[ind, 'Error message'] = 'support-package'
    df_format.loc[ind, 'Error code'] = 'support-package'
    df_format.loc[ind, 'SN'] = 'support-package'
    df_format.loc[ind, 'Filename'] = 'support-package'
    df_format.loc[ind, 'System'] = 'support-package'
    df_format.loc[ind, 'Date and time'] = 'support-package'      

    return df_MRL.to_csv(), df_format.to_csv()

if __name__ == "__main__":
    get_MRLdashboard()