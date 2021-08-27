import pandas as pd
import pyodbc
from datetime import datetime
import numpy as np # Just for busisness days function!
import time

''' Return tuple of dashboard data and formatting informaiotn as a csv '''
def get_dashboard():
    tic = time.perf_counter()
    # Connection string for connection to server
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
    print(f"{time.perf_counter() - tic:0.4f} seconds - SQL Query complete")
    df.sort_values(['Pat_IDA','Date','Time','Seq'])    
    df['StartDate'] = df['Date']

    # Assign unique key to each row based on the patient ID, the activty (Tx type) and start date
    # This will allow us to drop duplices based on this, so that each patient can have multiple Tx and be on the dashboard as long as they start on different days
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
        this_note = df.loc[(df['code'] == code) & (df['Activity3'] == 'VolumeRendering     '), 'Notes'].values
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
    df_data = df_IDA[['code', 'Pat_IDA', 'StartDate', 'Time', 'Pat', 'Activity', 'Dr' ,'2DSim_Dat', 'Notes', 'Dept', '2DSim_Dat_Stat', 'SysDefStatus']]
    df_data.set_index('code', drop=False, inplace=True) # Reset the key index

    # Join all the dataframes together into a master table
    df_dashboard = df_staff_status_due_completed.join(df_data, on='code', rsuffix='_data', how='inner')

    # Stip some whitespace from the end of the columns
    df_dashboard.columns = df_dashboard.columns.map(str)
    df_dashboard.columns = df_dashboard.columns.map(lambda s: s.replace(' ',''))

    print(f"{time.perf_counter() - tic:0.4f} seconds - basic dashboard creation complete")

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

    # Format staff + date due for uncompleted QCLs
    # I have no idea why this doesn't work when run with this zip + loop
    df_dashboard['ROI/Fldsetup'] = df_dashboard['ROI/Fldsetup'] + ' ' + df_dashboard['ROI/Fldsetup_due'].map(lambda x: format_date_short(x))
    df_dashboard['VolumeRendering'] = df_dashboard['VolumeRendering'] + ' ' + df_dashboard['VolumeRendering_due'].map(lambda x: format_date_short(x))
    df_dashboard['InitialDosimetry'] = df_dashboard['InitialDosimetry'] + ' ' + df_dashboard['InitialDosimetry_due'].map(lambda x: format_date_short(x))
    df_dashboard['1stCheck'] = df_dashboard['1stCheck'] + ' ' + df_dashboard['1stCheck_due'].map(lambda x: format_date_short(x))
    df_dashboard['2ndCheck'] = df_dashboard['2ndCheck'] + ' ' + df_dashboard['2ndCheck_due'].map(lambda x: format_date_short(x))    
    df_dashboard['PlanApproval'] = df_dashboard['PlanApproval'] + ' ' + df_dashboard['PlanApproval_due'].map(lambda x: format_date_short(x))

    # Create some formatting
    # Each cell is a css class for which the appropiate style will be applied
    # Initial cell format is blank
    df_format = pd.DataFrame().reindex_like(df_dashboard)
    for col in df_format:
        df_format[col] = df_format[col].astype(str)

    # Set yellow if date overdue
    df_format.loc[df_dashboard.index[df_dashboard['ROI/Fldsetup_due'].map(lambda x: overdue(x))].tolist(), 'Fldsetup'] = 'status-overdue'
    df_format.loc[df_dashboard.index[df_dashboard['VolumeRendering_due'].map(lambda x: overdue(x))].tolist(), 'VolumeRendering'] = 'status-overdue'
    df_format.loc[df_dashboard.index[df_dashboard['InitialDosimetry_due'].map(lambda x: overdue(x))].tolist(), 'InitialDosimetry'] = 'status-overdue'
    df_format.loc[df_dashboard.index[df_dashboard['1stCheck_due'].map(lambda x: overdue(x))].tolist(), '1stCheck'] = 'status-overdue'
    df_format.loc[df_dashboard.index[df_dashboard['2ndCheck_due'].map(lambda x: overdue(x))].tolist(), '2ndCheck'] = 'status-overdue'
    df_format.loc[df_dashboard.index[df_dashboard['PlanApproval_due'].map(lambda x: overdue(x))].tolist(), 'PlanApproval'] = 'status-overdue'

    # Set green if status complete/OK
    df_format.loc[df_dashboard.index[df_dashboard['ROI/Fldsetup_complete'] == 'OK'].tolist(), 'ROI/Fldsetup'] = 'status-complete'
    df_format.loc[df_dashboard.index[df_dashboard['VolumeRendering_complete'] == 'OK'].tolist(), 'VolumeRendering'] = 'status-complete'
    df_format.loc[df_dashboard.index[df_dashboard['InitialDosimetry_complete'] == 'OK'].tolist(), 'InitialDosimetry'] = 'status-complete'
    df_format.loc[df_dashboard.index[df_dashboard['1stCheck_complete'] == 'OK'].tolist(), '1stCheck'] = 'status-complete'
    df_format.loc[df_dashboard.index[df_dashboard['2ndCheck_complete'] == 'OK'].tolist(), '2ndCheck'] = 'status-complete'
    df_format.loc[df_dashboard.index[df_dashboard['PlanApproval_complete'] == 'OK'].tolist(), 'PlanApproval'] = 'status-complete'
    
    # Flag start date this week and next week
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
    df_dashboard = df_dashboard[['StartDate','Time','Pat','Pat_IDA','Activity','Dr','pCT_Date','ROI/Fldsetup','VolumeRendering','InitialDosimetry','1stCheck','2ndCheck','PlanApproval','Dept','Days','Notes']]

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

''' SQL query to get data from Mosaiq '''
sql_query = """SELECT
      --convert(char(10),ScheduleData.App_DtTm ,103) As 'Date'
       ScheduleData.App_DtTm As 'Date'
      ,convert(char(8),ScheduleData.App_DtTm ,108) As 'Time'
      ,ScheduleData.PAT_NAME as 'Pat'
      ,ScheduleData.IDA as 'Pat_IDA'
      ,ScheduleData.Short_Desc as 'Activity'
      ,ScheduleData.MD_INITIALS as 'Dr'
      ,MAX(convert(char(10),VolData.Date_Tm ,103)) as 'RQCL'
      ,(Case WHEN VolData.Complete = '0' then 'Check' ELSE (Case WHEN VolData.Complete = '1' THEN 'OK' ELSE (Case WHEN VolData.Complete = '2' THEN 'Skip' Else '' END)END)END) as 'RQCL Complete'
      --,MAX((Case when isnull(convert(char(10),PlanData.Sim_App,103),'01/01/1900') <> '1900/01/01' tHEN  convert(char(10),PlanData.Sim_App,103) ELSE '' end)) as '2DSim_Dat'      
      --,MAX((Case when isnull(convert(char(10),PlanData.Sim_App,103),'01/01/1900') <> '1900/01/01' tHEN  PlanData.Sim_STS ELSE '' EnD)) as '2DSim_Dat_Stat'
      ,MAX(PlanData.Sim_App) as '2DSim_Dat' 
      ,MAX(PlanData.Sim_STS) as '2DSim_Dat_Stat'
      ,MAX(PlanData.Sim_Act) as 'Sim_Act'
	  --,MAX(VolData.Description) as 'Description'
      ,MAX(VolData.Item_Seq) as 'Seq'
      ,MAX(VolData.Date_Tm) as 'Date_Tm'
      ,MAX(VolData.Activity) as 'Activity3'
      ,MAX(VolData.Staff) as 'Staff'
      ,MAX(VolData.Type) as 'Type' 
      ,MAX(VolData.Complete) as 'Complete'
      ,ScheduleData.LOC_INITIALS as 'Machine'
      ,ScheduleData.SysDefStatus
      ,'1900/01/01' as 'MASK_App'
      ,'OLD' as 'MASK_Sts'
      ,'OLD' as 'MASK_Act'
      ,MAX((Case when isnull(convert(char(10),PlanData.NB_App,103),'01/01/1900') <> '1900/01/01' tHEN  convert(char(10),PlanData.NB_App,103) ELSE '' end)) as 'NB_App'
      ,MAX(PlanData.NB_STS) as 'NB_STS'
      ,MAX(PlanData.NB_Act) as 'NB_Act'
      ,ISNULL(Voldata.Hsp_Code,'') as 'Chg_ID'
      ,MAX(VolData.Notes) as 'Notes'
      ,MAX(VolData.Due_DtTm) as 'DueDate'
      ,ScheduleData.UserDefStatus as 'Status'
      ,MAX(PlanData.CHK2_Code) as 'PlanCode'
      ,ScheduleData.DEPT as 'Dept'
      ,MAX(VolData.Item_Seq2) as 'Seq2'
      ,MAX(VolData.Date_Tm2) as 'Date_Tm2'
      ,MAX(VolData.Activity2) as 'Activity2'
      ,MAX(VolData.Staff2) as 'Staff2'
      ,MAX(VolData.Type2) as 'Type2' 
      ,MAX(VolData.Complete2) as 'Complete2'

FROM 
   ScheduleData
            
      LEFT OUTER JOIN
      (SELECT
				 Case WHEN QCLData.Activity in ('ROI/FLD SETUP','VOLUME RENDERED','VOLUME RENDERING', 'VOLUMERENDERING', 'INITIAL DOSIMETRY', 'INITIALDOSIMETRY', 'PLANAPPROVAL', 'PLAN APPROVAL', 'PLAN APPROVED', 'PLANAPPROVED', '1ST CHECK', '1STCHECK', '2ND CHECK', '2NDCHECK', 'OSC')   Then QCLData.Item_Seq ELSE '' EnD as 'Item_Seq'
				 ,Case WHEN QCLData.Activity in ('ROI/FLD SETUP','VOLUME RENDERED','VOLUME RENDERING', 'VOLUMERENDERING', 'INITIAL DOSIMETRY', 'INITIALDOSIMETRY', 'PLANAPPROVAL', 'PLAN APPROVAL', 'PLAN APPROVED', 'PLANAPPROVED', '1ST CHECK', '1STCHECK', '2ND CHECK', '2NDCHECK', 'OSC')   Then QCLData.Edit_DtTm ELSE '' EnD as 'Date_Tm'
				 ,Case WHEN QCLData.Activity in ('ROI/FLD SETUP','VOLUME RENDERED','VOLUME RENDERING', 'VOLUMERENDERING', 'INITIAL DOSIMETRY', 'INITIALDOSIMETRY', 'PLANAPPROVAL', 'PLAN APPROVAL', 'PLAN APPROVED', 'PLANAPPROVED', '1ST CHECK', '1STCHECK', '2ND CHECK', '2NDCHECK', 'OSC')   Then QCLData.Activity ELSE '' EnD as 'Activity'
				 ,Case WHEN QCLData.Activity in ('ROI/FLD SETUP','VOLUME RENDERED','VOLUME RENDERING', 'VOLUMERENDERING', 'INITIAL DOSIMETRY', 'INITIALDOSIMETRY', 'PLANAPPROVAL', 'PLAN APPROVAL', 'PLAN APPROVED', 'PLANAPPROVED', '1ST CHECK', '1STCHECK', '2ND CHECK', '2NDCHECK', 'OSC')   Then QCLData.Initials ELSE '' EnD as 'Staff'
				 ,Case WHEN QCLData.Activity in ('ROI/FLD SETUP','VOLUME RENDERED','VOLUME RENDERING', 'VOLUMERENDERING', 'INITIAL DOSIMETRY', 'INITIALDOSIMETRY', 'PLANAPPROVAL', 'PLAN APPROVAL', 'PLAN APPROVED', 'PLANAPPROVED', '1ST CHECK', '1STCHECK', '2ND CHECK', '2NDCHECK', 'OSC')   Then QCLData.Type ELSE '' EnD as 'Type'
				 ,Case WHEN QCLData.Activity in ('ROI/FLD SETUP','VOLUME RENDERED','VOLUME RENDERING', 'VOLUMERENDERING', 'INITIAL DOSIMETRY', 'INITIALDOSIMETRY', 'PLANAPPROVAL', 'PLAN APPROVAL', 'PLAN APPROVED', 'PLANAPPROVED', '1ST CHECK', '1STCHECK', '2ND CHECK', '2NDCHECK', 'OSC')   Then QCLData.Complete ELSE '' EnD as 'Complete'
				 ,Case WHEN QCLData.Activity in ('ROI/FLD SETUP_2','VOLUME RENDERED','VOLUME RENDERING_2', 'VOLUMERENDERING_2', 'INITIAL DOSIMETRY_2', 'INITIALDOSIMETRY_2', 'PLANAPPROVAL_2', 'PLAN APPROVAL_2', 'PLAN APPROVED_2', 'PLANAPPROVED_2', '1ST CHECK_2', '1STCHECK_2', '2ND CHECK_2', '2NDCHECK_2', 'OSC_2')   Then QCLData.Item_Seq ELSE '' EnD as 'Item_Seq2'
				 ,Case WHEN QCLData.Activity in ('ROI/FLD SETUP_2','VOLUME RENDERED','VOLUME RENDERING_2', 'VOLUMERENDERING_2', 'INITIAL DOSIMETRY_2', 'INITIALDOSIMETRY_2', 'PLANAPPROVAL_2', 'PLAN APPROVAL_2', 'PLAN APPROVED_2', 'PLANAPPROVED_2', '1ST CHECK_2', '1STCHECK_2', '2ND CHECK_2', '2NDCHECK_2', 'OSC_2')   Then QCLData.Edit_DtTm ELSE '' EnD as 'Date_Tm2'
				 ,Case WHEN QCLData.Activity in ('ROI/FLD SETUP_2','VOLUME RENDERED','VOLUME RENDERING_2', 'VOLUMERENDERING_2', 'INITIAL DOSIMETRY_2', 'INITIALDOSIMETRY_2', 'PLANAPPROVAL_2', 'PLAN APPROVAL_2', 'PLAN APPROVED_2', 'PLANAPPROVED_2', '1ST CHECK_2', '1STCHECK_2', '2ND CHECK_2', '2NDCHECK_2', 'OSC_2')   Then QCLData.Activity ELSE '' EnD as 'Activity2'
				 ,Case WHEN QCLData.Activity in ('ROI/FLD SETUP_2','VOLUME RENDERED','VOLUME RENDERING_2', 'VOLUMERENDERING_2', 'INITIAL DOSIMETRY_2', 'INITIALDOSIMETRY_2', 'PLANAPPROVAL_2', 'PLAN APPROVAL_2', 'PLAN APPROVED_2', 'PLANAPPROVED_2', '1ST CHECK_2', '1STCHECK_2', '2ND CHECK_2', '2NDCHECK_2', 'OSC_2')   Then QCLData.Initials ELSE '' EnD as 'Staff2'
				 ,Case WHEN QCLData.Activity in ('ROI/FLD SETUP_2','VOLUME RENDERED','VOLUME RENDERING_2', 'VOLUMERENDERING_2', 'INITIAL DOSIMETRY_2', 'INITIALDOSIMETRY_2', 'PLANAPPROVAL_2', 'PLAN APPROVAL_2', 'PLAN APPROVED_2', 'PLANAPPROVED_2', '1ST CHECK_2', '1STCHECK_2', '2ND CHECK_2', '2NDCHECK_2', 'OSC_2')   Then QCLData.Type ELSE '' EnD as 'Type2'
				 ,Case WHEN QCLData.Activity in ('ROI/FLD SETUP_2','VOLUME RENDERED','VOLUME RENDERING_2', 'VOLUMERENDERING_2', 'INITIAL DOSIMETRY_2', 'INITIALDOSIMETRY_2', 'PLANAPPROVAL_2', 'PLAN APPROVAL_2', 'PLAN APPROVED_2', 'PLANAPPROVED_2', '1ST CHECK_2', '1STCHECK_2', '2ND CHECK_2', '2NDCHECK_2', 'OSC_2')   Then QCLData.Complete ELSE '' EnD as 'Complete2'
				 ,QCLData.Pat_ID1
				 ,QCLData.Hsp_Code
				 ,QCLData.Due_DtTm
				 ,QCLData.Notes
	  FROM
				 QCLData
      
      )as VolData ON
            scheduleData.Pat_ID1 = VolData.Pat_ID1
            
      LEFT OUTER JOIN
      (SELECT 
                  SimData.Pat_ID1 as 'DataID'
                  ,Case WHEN Activity in ('RT00017','RT00025','ReCT','ReC-CT')  Then SimData.App_DtTm ELSE '' EnD as 'Sim_App'
                  ,ISNULL(Case WHEN Activity in ('RT00017','RT00025','ReCT','ReC-CT')  Then SysDefStatus ELSE '' EnD,'') as 'Sim_STS'
                  ,ISNULL(Case WHEN Activity in ('RT00017','RT00025','ReCT','ReC-CT') Then Activity  ELSE  '' END ,'') as 'Sim_Act'
                  ,Case WHEN SimData.Activity in ('1DSX','1RDOS','IDOS','IRDOS') Then STF_INITIALS ELSE '' EnD as 'PLAN_STF'
                  ,Case WHEN SimData.Activity in ('1DSX','1RDOS','IDOS','IRDOS') Then (Case WHEN ISNULL(SysDefStatus,'') <> '' THEN SysDefStatus ELSE ISNULL(UserDefStatus,'') END) ELSE '' EnD as 'PLAN_STS'
                  ,Case WHEN SimData.Activity in ('2DOS','2DSX','2RDOS','CHK1DOS','CHK1RDOS') Then STF_INITIALS ELSE '' EnD as 'CHK1_STF'
                  ,Case WHEN SimData.Activity in ('2DOS','2DSX','2RDOS','CHK1DOS','CHK1RDOS') Then (Case WHEN ISNULL(SysDefStatus,'') <> '' THEN SysDefStatus ELSE ISNULL(UserDefStatus,'') END) ELSE '' EnD as 'CHK1_STS'
                  ,Case WHEN Activity in ('RT00012','RT00013','RT00015','RT00016','RT00029','RT00030','RT00031','RT00032','RT00033') Then Activity ELSE '' EnD as 'CHK2_Code'
                  ,Case WHEN Activity in ('RT00012','RT00013','RT00015','RT00016','RT00029','RT00030','RT00031','RT00032','RT00033')  Then STF_INITIALS ELSE '' EnD as 'CHK2_STF'
                  ,Case WHEN Activity in ('RT00012','RT00013','RT00015','RT00016','RT00029','RT00030','RT00031','RT00032','RT00033')  Then (Case WHEN ISNULL(SysDefStatus,'') <> '' THEN SysDefStatus ELSE ISNULL(UserDefStatus,'') END) ELSE '' EnD as 'CHK2_STS'
                  ,Case WHEN SimData.Activity in ('RT00017','RT00025') Then Max(SimData.App_DtTm) ELSE '' EnD as 'NB_App'
                  ,ISNULL(Case WHEN SimData.Activity in ('RT00017','RT00025','ReCT','ReC-CT') Then SysDefStatus ELSE '' EnD,'') as 'NB_STS'
                  ,ISNULL(Case WHEN SimData.Activity in ('RT00017','RT00025','ReCT','ReC-CT') Then SimData.Activity  ELSE  '' EnD,'') as 'NB_Act' 
	   FROM 
                                    
                 SimData

	 INNER JOIN 
			(
			SELECT 
				Max(SimDataX.App_DtTm) as App_DtTm,SimDataX.Pat_ID1
				FROM                           
				SimData SimDataX GROUP BY SimDataX.Pat_ID1)SimDataONE ON (SimData.Pat_ID1 = SimDataONE.Pat_ID1 AND SimData.App_DtTm = SimDataONE.App_DtTm)
                   
      GROUP BY SimData.Pat_ID1 ,SimData.App_DtTm,SimData.Activity, SimData.Capture, SimData.SysDefStatus, SimData.STF_INITIALS,SimData.UserDefStatus
      ) as PlanData ON
                        ScheduleData.Pat_ID1 = PlanData.DataID
            
GROUP BY  ScheduleData.App_DtTm, ScheduleData.IDA,ScheduleData.PAT_NAME,ScheduleData.Short_Desc,ScheduleData.MD_INITIALS, 
VolData.Complete,ScheduleData.Pat_ID1, ScheduleData.LOC_INITIALS, VolData.Item_Seq,VolData.Date_Tm,VolData.Activity,
VolData.Staff,VolData.Type,VolData.Complete,VolData.Item_Seq2,VolData.Date_Tm2,VolData.Activity2,
VolData.Staff2,VolData.Type2,VolData.Complete2,ScheduleData.SysDefStatus,Hsp_Code, ScheduleData.DEPT, ScheduleData.UserDefStatus
--, PlanData.Sim_App, PlanData.Sim_STS, PlanData.PLAN_STF,PlanData.PLAN_STS, PlanData.CHK1_STF,PlanData.CHK1_STS, PlanData.CHK2_STF,PlanData.CHK2_STS
ORDER BY (CASE WHEN ScheduleData.App_DtTm IS NULL THEN 1 ELSE 0 END) DESC, ScheduleData.App_DtTm,ScheduleData.IDA ,VolData.Item_Seq"""