import pandas as pd
import pyodbc
from datetime import datetime
import numpy as np # Just for busisness days function!
import time
import dosimetry_dashboard
import io

def get_dashboard():
    df_dashboard = dosimetry_dashboard.get_dashbord_full()
    
    # Select only MRL activities
    df_MRL = df_dashboard.loc[df_dashboard.index[df_dashboard['Activity'].map(lambda x: 'MRL' in str(x))]]

    #2DSim_Dat
    df_MRL['SimWeeksAway'] = df_MRL['Date_long'].map(lambda x: weeks_away(x))
    print(df_MRL[['Pat_IDA', 'SimWeeksAway']])
    sims_by_week = df_MRL.SimWeeksAway.value_counts()
    print(sims_by_week)
    list1 = sims_by_week.index.to_list()
    list2 = sims_by_week.to_list()

    list1, list2 = (list(t) for t in zip(*sorted(zip(list1, list2))))

    return list1,  list2


def weeks_away(date_object):
    return date_object.to_pydatetime().isocalendar()[1] - datetime.now().isocalendar()[1]