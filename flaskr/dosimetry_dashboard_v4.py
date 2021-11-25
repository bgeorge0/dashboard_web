import pandas as pd
import pyodbc
from datetime import datetime
import numpy as np # Just for busisness days function!
import time

'''
Provider=SQLOLEDB.1;
Integrated Security=SSPI;
Persist Security Info=True;
Initial Catalog=GC_DosimetryDash_v4;
Data Source=GCGBPRDW01;
Extended Properties="Trusted_Connection=True";
Use Procedure for Prepare=1;
Auto Translate=True;
Packet Size=4096;
Workstation ID=SQLDash;
Use Encryption for Data=False;
Tag with column collation when possible=False
'''

def get_custom_query(this_sql_query):
    # Connection string for connection to server
    conn_str = (
        r'DRIVER={SQL Server};'
        r'SERVER=GCGBPRDW01;'
        r'DATABASE=GC_DosimetryDash_v4;'
        r'Trusted_Connection=yes;'
    )

    # Connect and get data
    #conn = pyodbc.connect(conn_str)
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(this_sql_query, conn)
    #df.sort_values(['Pat_IDA','Date','Time','Seq'])
    return df

''' Get raw data '''
def get_raw_QCL_data():
    # Connection string for connection to server
    conn_str = (
        r'DRIVER={SQL Server};'
        r'SERVER=GCGBPRDW01;'
        r'DATABASE=GC_DosimetryDash_v4;'
        r'Trusted_Connection=yes;'
    )

    # Connect and get data
    #conn = pyodbc.connect(conn_str)
    #sql_query_all = '''SELECT * FROM  QCLData'''
    with pyodbc.connect(conn_str) as conn:
        df = pd.read_sql(sql_query, conn)
    #df.sort_values(['Pat_IDA','Date','Time','Seq'])
    return df

''' Get full dashboard, includnig all columns not for printing '''
def get_dashbord_full(Activity_Filter=None, Stage_Filter=None):
    # Get the raw QCLs dashboard
    df = get_raw_QCL_data()

    # Start processing
    df['StartDate'] = df['Date']

    # Dashboard_v4 specific fix - bring Activity2 data to Activity3, Staff2 to Staff etc. if there is content
    df['Activity3'] = df['Activity3'] + df['Activity2']
    df['Staff'] = df['Staff'] + df['Staff2']
    df['Type'] = df['Type'] + df['Type2']
    df['Complete'] = df['Complete'] + df['Complete2']


    # 'ROI/Fldsetup','VolumeRendering','InitialDosimetry','1stCheck','OSC','2ndCheck','PlanApproval'
    # Some major hacking around here to try and get things in the correct format - different spellings etc. for Mosaiq and Aria databases
    
    df['Activity3'].replace(to_replace=['ROI/Fld Setup_2', 'ROI/Fld setup       '], value='ROI/Fld setup', inplace=True)
    df['Activity3'].replace(to_replace=['VolumeRendering_2   ', 'VolumeRendering     '], value='VolumeRendering', inplace=True)
    df['Activity3'].replace(to_replace=['InitialDosimetry_2  ', 'InitialDosimetry    '], value='InitialDosimetry', inplace=True)
    df['Activity3'].replace(to_replace=['1stCheck_2          ', '1stCheck            '], value='1stCheck', inplace=True)
    df['Activity3'].replace(to_replace=['OSC_2', 'OSC                 '], value='OSC', inplace=True)
    df['Activity3'].replace(to_replace=['2ndCheck_2          ', '2nd check', '2ndCheck            '], value='2ndCheck', inplace=True)
    df['Activity3'].replace(to_replace=['PlanApproval        ', 'PlanApproval_2      '], value='PlanApproval', inplace=True)
    df['Activity3'].replace(to_replace=np.nan, value=None, inplace=True)
    
    # Now drop unnecessary columns
    df = df.drop(columns=['Activity2', 'Staff2', 'Type2', 'Complete2'])

    #print(df['Activity3'])
    #print(df.columns)

    # Assign unique key to each row based on the patient ID, the activty (Tx type) and start date-time
    # This will allow us to drop duplices based on this, so that each patient can have multiple Tx and be on the dashboard as long as they have different start date-times
    df = df.assign(
        code=df.index.to_series().groupby(
            [df.Pat_IDA, df.Activity, df.Date]
        ).transform('first').map('CODE{}'.format)
    )[['code'] + df.columns.tolist()]
    
    # Sort out the notes before we drop all the duplicates
    df.set_index('code',drop=False, inplace=True)
    df.sort_values(by='DueDate', ascending='True', inplace=True) # Most recent DueDate at top, so finds most recent note
    # Each code is a different treatment for a patient
    for code in df.index.unique():
        # Get the note in 'VolumeRendering' - whitespace is required here
        # Return an array (length 0 or 1)
        this_note = df.loc[(df['code'] == code) & (df['Activity3'] == 'VolumeRendering'), 'Notes'].values
        # Catch some edge cases
        if len(this_note) < 1:
            this_note = ''
        elif this_note[0] == 'RECORD TMT SITE HERE':
            this_note = ''
        else:
            this_note = this_note[0]
        # Set all rows which match this code to the same note, so when we drop the duplicates next we still have the note data
        df.loc[code,'Notes'] = this_note

    # Get QCL activity and completion status
    # Drop duplicate rows based on the 'code' key created above
    # This means that each patinet ID can have multiple activity entries in the table
    df2 = df.drop_duplicates(subset=['code', 'Activity3'], keep='first', inplace=False)
    df2.set_index('code', drop=False, inplace=True) # And reset the key index to the code key

    # Pivot this table to get the staff member responsible for each activity
    df_staff = df2.pivot(index='code', columns='Activity3', values='Staff')
    # Pivot a different way to get whether each activity is complete
    df_status = df2.pivot(index='code', columns='Activity3', values='RQCL Complete')
    # Pivot a third way to get the due date for each acqivity
    df_due = df2.pivot(index='code', columns='Activity3', values='DueDate')
    # Pivot a forth way to ge the date the QCL was completed
    df_completed = df2.pivot(index='code', columns='Activity3', values='RQCL')

    # Join these together to get the completion status of each QCL in one table    
    df_staff_status = df_staff.join(df_status, lsuffix='', rsuffix='_complete', how='outer')
    df_staff_status_due = df_staff_status.join(df_due, lsuffix='', rsuffix='_due', how='outer')
    df_staff_status_due_completed = df_staff_status_due.join(df_completed, lsuffix='', rsuffix='_completed', how='outer')
  
    # Get the other information for each patient and Tx
    # Drop duplicates based on the code key created earlier (e.g., patient and treatment type) - each patient will have at most one entry in this table per treatment
    df_IDA = df.drop_duplicates(subset=['code'], keep='first', inplace=False)
    # Select the columns we want from this dataframe
    df_data = df_IDA[['code', 'Pat_IDA', 'StartDate', 'Time', 'Pat', 'Activity', 'Dr' ,'2DSim_Dat', 'Notes', 'Dept', '2DSim_Dat_Stat', 'SysDefStatus', 'Status']]
    df_data.set_index('code', drop=False, inplace=True) # Reset the key index

    # Join all the dataframes together into a master table
    df_dashboard = df_staff_status_due_completed.join(df_data, on='code', rsuffix='_data', how='inner')

    # Stip some whitespace from the end of the columns - probably not needed any more as we consolidate all this above
    df_dashboard.columns = df_dashboard.columns.map(str)
    df_dashboard.columns = df_dashboard.columns.map(lambda s: s.replace(' ',''))

    # Rename to match old dashbarod format
    #df_dashboard = df_dashboard.rename(columns={'ROI/Fld setup': 'ROI/Fldsetup'})

    #print(df_dashboard.columns)

    # Planning and PSQA stats
    #df_dashboard['PDS'] = '' #df_dashboard['Pat_IDA'].map(lambda x: MRL_plan_data.get_plan_statistics(x)['prescription_dose_spillage'])
    #df_dashboard['V100'] = '' #df_dashboard['Pat_IDA'].map(lambda x: MRL_plan_data.get_plan_statistics(x)['target_coverage'])
    #df_dashboard['MGI'] = '' #df_dashboard['Pat_IDA'].map(lambda x: MRL_plan_data.get_plan_statistics(x)['modified_gradient_index'])
    #print(f"{time.perf_counter() - tic:0.4f} seconds - MRL-specific planning stats added")
    #df_dashboard['3n3'] = '' #df_dashboard['Pat_IDA'].map(lambda x: MRL_PSQA.getPSQAResults(x))
    #print(f"{time.perf_counter() - tic:0.4f} seconds - MRL-specific gamma pass rate added")

    # Format datetime nicely
    df_dashboard['Date_long'] = df_dashboard['StartDate']
    df_dashboard['Time_long'] = df_dashboard['Time']
    df_dashboard['StartDate'] = df_dashboard['Date_long'].map(lambda x: format_date_sortable(x))
    df_dashboard['Time'] = df_dashboard['Time_long'].map(lambda x: format_time_short(x))
    df_dashboard['pCT_Date'] = df_dashboard['2DSim_Dat'].map(lambda x: format_date_sortable(x))

    # Calculate sim-treat days
    #(df_dashboard['Date_long'] - df_dashboard['2DSim_Dat']).map(lambda x: x.days)
    #df_dashboard['Days'] = pd.date_range(df_dashboard['Date_long'], df_dashboard['2DSim_Dat'], freq=BDay())
    A = [d.date() for d in df_dashboard['Date_long']]
    B = [d.date() for d in df_dashboard['2DSim_Dat']]
    A = [datetime.now().date() if x != x else x for x in A]
    B = [datetime.now().date() if x != x else x for x in B]
    df_dashboard['Days'] = data=np.busday_count(B, A) - 1

    #print(df_dashboard.columns)

    # Format staff + date due for uncompleted QCLs
    # I have no idea why this doesn't work when run with this zip + loop
    df_dashboard['ROI/Fldsetup'] = df_dashboard['ROI/Fldsetup'] + ' ' + df_dashboard['ROI/Fldsetup_due'].map(lambda x: format_date_short(x))
    df_dashboard['VolumeRendering'] = df_dashboard['VolumeRendering'] + ' ' + df_dashboard['VolumeRendering_due'].map(lambda x: format_date_short(x))
    df_dashboard['InitialDosimetry'] = df_dashboard['InitialDosimetry'] + ' ' + df_dashboard['InitialDosimetry_due'].map(lambda x: format_date_short(x))
    df_dashboard['1stCheck'] = df_dashboard['1stCheck'] + ' ' + df_dashboard['1stCheck_due'].map(lambda x: format_date_short(x))
    df_dashboard['OSC'] = df_dashboard['OSC'] + ' ' + df_dashboard['OSC_due'].map(lambda x: format_date_short(x))
    df_dashboard['2ndCheck'] = df_dashboard['2ndCheck'] + ' ' + df_dashboard['2ndCheck_due'].map(lambda x: format_date_short(x))    
    df_dashboard['PlanApproval'] = df_dashboard['PlanApproval'] + ' ' + df_dashboard['PlanApproval_due'].map(lambda x: format_date_short(x))

    # Select base on activity filter
    if Activity_Filter is not None:
        df_dashboard = df_dashboard[df_dashboard['Activity'].str.contains(Activity_Filter)]
    
    # Append a column which will be a list of all the 'stages' this plan seems to be stuck at. Could be multiple ones
    df_dashboard['Stage'] = [[]] * df_dashboard.shape[0]
    # Some logic below
    # If simulation is complete, but ROIs not done, then this is at stage 0
    df_dashboard.loc[df_dashboard.index[(df_dashboard['2DSim_Dat_Stat'] == 'C') & (df_dashboard['ROI/Fldsetup_complete'] != 'C')], 'Stage'].apply(lambda x: x.append(0))
    # If ROIs complete, but volume rendering not completed, this is at stage 1
    df_dashboard.loc[df_dashboard.index[(df_dashboard['ROI/Fldsetup_complete'] == 'C') & (df_dashboard['VolumeRendering_complete'] != 'C')], 'Stage'].apply(lambda x: x.append(1))
    # If volume rendering complete, but initial dosimetry not complete, this is at stage 2
    df_dashboard.loc[df_dashboard.index[(df_dashboard['VolumeRendering_complete'] == 'C') & (df_dashboard['InitialDosimetry_complete'] != 'C')], 'Stage'].apply(lambda x: x.append(2))
    # If initial dosimetry is complete, but 1st check is not complete, this is at stage 3
    df_dashboard.loc[df_dashboard.index[(df_dashboard['InitialDosimetry_complete'] == 'C') & (df_dashboard['1stCheck_complete'] != 'C')], 'Stage'].apply(lambda x: x.append(3))
    # If 1st check complete, but 2nd check not complete, this is at stage 4
    df_dashboard.loc[df_dashboard.index[(df_dashboard['1stCheck_complete'] == 'C') & (df_dashboard['2ndCheck_complete'] != 'C')], 'Stage'].apply(lambda x: x.append(4))
    # If 2nd check complete, but plan approval not complete, this is at stage 5
    df_dashboard.loc[df_dashboard.index[(df_dashboard['2ndCheck_complete'] == 'C') & (df_dashboard['PlanApproval_complete'] != 'C')], 'Stage'].apply(lambda x: x.append(4))


    # Only pass on if held at Stage_Filter
    steps = ['ROI/Fldsetup','VolumeRendering','InitialDosimetry','1stCheck','2ndCheck','PlanApproval']
    if Stage_Filter is not None:
        #if steps.index(Stage_Filter) >= 0:
        #    df_dashboard = df_dashboard[df_dashboard['2DSim_Dat_Stat'] == 'C']
        if steps.index(Stage_Filter) == 0:
            df_dashboard = df_dashboard[df_dashboard['2DSim_Dat_Stat'] == 'C']
            df_dashboard = df_dashboard[df_dashboard['ROI/Fldsetup_complete'] != 'OK']
        if steps.index(Stage_Filter) == 1:
            df_dashboard = df_dashboard[df_dashboard['ROI/Fldsetup_complete'] == 'OK']
            df_dashboard = df_dashboard[df_dashboard['VolumeRendering_complete'] != 'OK']
        if steps.index(Stage_Filter) == 2:
            df_dashboard = df_dashboard[df_dashboard['VolumeRendering_complete'] == 'OK']
            df_dashboard = df_dashboard[df_dashboard['InitialDosimetry_complete'] != 'OK']
        if steps.index(Stage_Filter) == 3:
            df_dashboard = df_dashboard[df_dashboard['InitialDosimetry_complete'] == 'OK']
            df_dashboard = df_dashboard[df_dashboard['1stCheck_complete'] != 'OK']
        if steps.index(Stage_Filter) == 4:
            df_dashboard = df_dashboard[df_dashboard['1stCheck_complete'] == 'OK']
            df_dashboard = df_dashboard[df_dashboard['2ndCheck_complete'] != 'OK']
        

    return df_dashboard


''' Return tuple of dashboard data and formatting informaiotn as a csv '''
def get_dashboard(Activity_Filter=None, Stage_Filter=None):
    tic = time.perf_counter()

    # Get the full dashboard
    df_dashboard = get_dashbord_full(Activity_Filter, Stage_Filter)
    print(f"{time.perf_counter() - tic:0.4f} seconds - basic dashboard creation complete")
  
    # Create some formatting
    # Each cell is a css class for which the appropiate style will be applied
    # Initial cell format is blank
    df_format = pd.DataFrame().reindex_like(df_dashboard)
    for col in df_format:
        df_format[col] = df_format[col].astype(str)
    
    if df_dashboard.empty:
        # Reorder columns
        df_dashboard = df_dashboard[['StartDate','Time','Pat','Pat_IDA','Activity','Dr','pCT_Date','ROI/Fldsetup','VolumeRendering','InitialDosimetry', 'OSC', '1stCheck','2ndCheck','PlanApproval','Dept','Days','Notes', 'Status']]
        return df_dashboard.to_csv(), df_format.to_csv()

    # Set yellow if date overdue
    df_format.loc[df_dashboard.index[df_dashboard['ROI/Fldsetup_due'].map(lambda x: overdue(x))].tolist(), 'Fldsetup'] = 'status-overdue'
    df_format.loc[df_dashboard.index[df_dashboard['VolumeRendering_due'].map(lambda x: overdue(x))].tolist(), 'VolumeRendering'] = 'status-overdue'
    df_format.loc[df_dashboard.index[df_dashboard['InitialDosimetry_due'].map(lambda x: overdue(x))].tolist(), 'InitialDosimetry'] = 'status-overdue'
    df_format.loc[df_dashboard.index[df_dashboard['1stCheck_due'].map(lambda x: overdue(x))].tolist(), '1stCheck'] = 'status-overdue'
    df_format.loc[df_dashboard.index[df_dashboard['OSC_due'].map(lambda x: overdue(x))].tolist(), 'OSC'] = 'status-overdue'
    df_format.loc[df_dashboard.index[df_dashboard['2ndCheck_due'].map(lambda x: overdue(x))].tolist(), '2ndCheck'] = 'status-overdue'
    df_format.loc[df_dashboard.index[df_dashboard['PlanApproval_due'].map(lambda x: overdue(x))].tolist(), 'PlanApproval'] = 'status-overdue'

    # Set green if status complete/OK
    df_format.loc[df_dashboard.index[df_dashboard['ROI/Fldsetup_complete'] == 'OK'].tolist(), 'ROI/Fldsetup'] = 'status-complete'
    df_format.loc[df_dashboard.index[df_dashboard['VolumeRendering_complete'] == 'OK'].tolist(), 'VolumeRendering'] = 'status-complete'
    df_format.loc[df_dashboard.index[df_dashboard['InitialDosimetry_complete'] == 'OK'].tolist(), 'InitialDosimetry'] = 'status-complete'
    df_format.loc[df_dashboard.index[df_dashboard['1stCheck_complete'] == 'OK'].tolist(), '1stCheck'] = 'status-complete'
    df_format.loc[df_dashboard.index[df_dashboard['OSC_complete'] == 'OK'].tolist(), 'OSC'] = 'status-complete'
    df_format.loc[df_dashboard.index[df_dashboard['2ndCheck_complete'] == 'OK'].tolist(), '2ndCheck'] = 'status-complete'
    df_format.loc[df_dashboard.index[df_dashboard['PlanApproval_complete'] == 'OK'].tolist(), 'PlanApproval'] = 'status-complete'
    
    # Flag start date this week and next week
    df_format.loc[df_dashboard.index[df_dashboard['Date_long'].map(lambda x: isfourweeks(x))].tolist(), 'StartDate'] = 'four-weeks-away'
    df_format.loc[df_dashboard.index[df_dashboard['Date_long'].map(lambda x: isthreeweeks(x))].tolist(), 'StartDate'] = 'three-weeks-away'
    df_format.loc[df_dashboard.index[df_dashboard['Date_long'].map(lambda x: istwoweeks(x))].tolist(), 'StartDate'] = 'two-weeks-away'
    df_format.loc[df_dashboard.index[df_dashboard['Date_long'].map(lambda x: isnextweek(x))].tolist(), 'StartDate'] = 'one-week-away'
    df_format.loc[df_dashboard.index[df_dashboard['Date_long'].map(lambda x: isthisweek(x))].tolist(), 'StartDate'] = 'zero-weeks-away'
    df_format.loc[df_dashboard.index[df_dashboard['Date_long'].map(lambda x: istoday(x))].tolist(), 'StartDate'] = 'starts-today'
    
    # Flag pCT this week
    ind = df_dashboard.index[df_dashboard['2DSim_Dat'].map(lambda x: isthisweek(x))].tolist()
    df_format.loc[ind, 'pCT_Date'] = 'pCT-this-week'

    # Set green if pCT is captured
    ind = df_dashboard.index[df_dashboard['2DSim_Dat_Stat'].map(lambda x: 'C' in str(x))]
    df_format.loc[ind, 'pCT_Date'] = 'status-complete'

    # Set color based on dept
    for dept in {'OX4r', 'CM1', 'PO9', 'NG5', 'B74', 'ME19', 'MK14r', 'WD6', 'GU1', 'SO16', 'BS32', 'SW5', 'SL4', 'CB8'}:
        df_format.loc[df_dashboard.index[df_dashboard['Dept'].map(lambda x: dept in str(x))], 'Dept'] = dept
    
    # Set color based on Activity type
    for activity in {'SABR', 'CxTx', 'simple', 'VMAT', 'sIMRT', 'CxIMRT', 'MRL', 'IntTx'}:
        df_format.loc[df_dashboard.index[df_dashboard['Activity'].map(lambda x: activity in str(x.replace(" ", "")))], 'Activity'] = activity

    # Highlight SysDefStatus = O
    df_format.loc[df_dashboard.index[df_dashboard['SysDefStatus'].map(lambda x: 'O' in str(x))].tolist(), 'Pat'] = 'old-start'

    print(f"{time.perf_counter() - tic:0.4f} seconds - formatting complete")

    # Reorder columns
    df_dashboard = df_dashboard[['StartDate','Time','Pat','Pat_IDA','Activity','Dr','pCT_Date','ROI/Fldsetup','VolumeRendering','InitialDosimetry','1stCheck', 'OSC', '2ndCheck','PlanApproval','Dept','Days','Notes', 'Status', 'Stage']]
    print(df_dashboard)
    # TODO
    # Some other stuff

    # Return as csv for the datatable display    
    return df_dashboard.to_csv(), df_format.to_csv()

''' Returns date of format dd/mm from ISO format '''
def format_date_sortable(date_object):    
    return date_object.strftime('%Y-%m-%d') if not pd.isnull(date_object) else ''

''' Returns date in short format '''
def format_date_short(date_object):
    return date_object.strftime('%d-%m') if not pd.isnull(date_object) else ''

''' Returns time in short format '''
def format_time_short(time_object):
    return time_object[0:5]

''' Returns true if date is in previous days'''
def overdue(date_object):
    return (date_object - datetime.now()).days < 0

''' Returns true if the date is tomorrow '''
def istomorrow(date_object):
    return (date_object - datetime.now()).days < 2

''' Returns true if the date is today '''
def istoday(date_object):
    return (date_object - datetime.now()).days < 1

''' Returns true if the date is this week '''
def isthisweek(date_object):
    try:
        return (date_object.to_pydatetime().isocalendar()[1] - datetime.now().isocalendar()[1]) == 0
    except:
        return False

''' Returns true if the date is next week '''
def isnextweek(date_object):
    return (date_object.to_pydatetime().isocalendar()[1] - datetime.now().isocalendar()[1]) == 1

''' Returns true if the date is in two weeks '''
def istwoweeks(date_object):
    return (date_object.to_pydatetime().isocalendar()[1] - datetime.now().isocalendar()[1]) == 2

''' Returns true if the date is in three weeks away '''
def isthreeweeks(date_object):
    return (date_object.to_pydatetime().isocalendar()[1] - datetime.now().isocalendar()[1]) == 3

''' Returns true if the date is in three weeks away '''
def isfourweeks(date_object):
    return (date_object.to_pydatetime().isocalendar()[1] - datetime.now().isocalendar()[1]) == 4


''' SQL query to get data from Mosaiq '''
sql_query = """SELECT [Date]
      ,[Time]
      ,[Pat]
      ,[Pat_IDA]
      ,[Activity]
      ,[Dr]
      ,[RQCL]
      ,[RQCL Complete]
      ,[2DSim_Dat]
      ,[2DSim_Dat_Stat]
      ,[Sim_Act]
      ,[Seq]
      ,[Date_Tm]
      ,[Activity3]
      ,[Staff]
      ,[Type]
      ,[Complete]
      ,[Machine]
      ,[SysDefStatus]
      ,[MASK_App]
      ,[MASK_Sts]
      ,[MASK_Act]
      ,[NB_App]
      ,[NB_STS]
      ,[NB_Act]
      ,[Chg_ID]
      ,[Notes]
      ,[DueDate]
      ,[Status]
      ,[PlanCode]
      ,[Dept]
      ,[Seq2]
      ,[Date_Tm2]
      ,[Activity2]
      ,[Staff2]
      ,[Type2]
      ,[Complete2]
  FROM [GC_DosimetryDash_v4].[dbo].[DosimetryDashData]
ORDER BY (CASE WHEN [Time] IS NULL THEN 1 ELSE 0 END) DESC, [Time],[Pat_IDA] ,[Seq]"""