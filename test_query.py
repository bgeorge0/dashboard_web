import time
import pandas as pd
import pyodbc

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

print(df)

#ind = df.index[df['Pat_IDA'] == 'CP004007'].tolist()
df = df[df['Pat_IDA'] == 'CP004007']
for activity in df['Activity']:
    print(activity)
