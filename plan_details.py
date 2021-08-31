import pandas as pd
import pyodbc
from datetime import datetime
import numpy as np # Just for busisness days function!
import time
import MRL_plan_data

def get_dashboard(Pat_IDA):

    tic = time.perf_counter()
    conn_str = (
                r'DRIVER={SQL Server};'
                r'SERVER=GCGBPRDW01;'
                r'DATABASE=GC_DosimetryDash_v2;'
                r'Trusted_Connection=yes;'
    )
    # Connect and get data
    #conn = pyodbc.connect(conn_str)
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(sql_query, conn)

    df = df[df['Pat_IDA'] == Pat_IDA]
    for activity in df['Activity']:
        if activity.find('MRL') > 0:

            PDS = []
            V100 = []
            MGI = []
            plan_data = MRL_plan_data.MRL_plan_data(Pat_IDA)
            # Planning and PSQA stats
            PDS.append(plan_data.Plan_Statistics['prescription_dose_spillage'])
            V100.append(plan_data.Plan_Statistics['target_coverage'])
            MGI.append(plan_data.Plan_Statistics['modified_gradient_index'])
            
            df['PDS'] = PDS
            df['V100'] = V100
            df['MGI'] = MGI

    df_format = pd.DataFrame().reindex_like(df)
    for col in df_format:
        df_format[col] = df_format[col].astype(str)
    
    return df.to_csv(), df_format.to_csv()
    
# Reduced SQL query to get only key data
sql_query = \
"""SELECT
    --convert(char(10),ScheduleData.App_DtTm ,103) As 'Date'
       ScheduleData.App_DtTm As 'Date'
      ,ScheduleData.IDA as 'Pat_IDA'
      ,ScheduleData.Short_Desc as 'Activity'
      ,convert(char(8),ScheduleData.App_DtTm ,108) As 'Time'
      ,ScheduleData.PAT_NAME as 'Pat'
FROM 
   ScheduleData"""
