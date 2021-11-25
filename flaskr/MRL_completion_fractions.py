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

#@lru_cache(maxsize=1)
def get_dashboard():

    week_deltas = []
    completion_fractions = []
    completion_fractions = []
    ref_time = datetime(2019,6,1,0,0,0,0)

    D = glob("V:\Export\*\Treatment\*_*.00\PlanOverview.txt")
    for d in D:
        parts = os.path.split(d)
        s = parts[0].split('.')[0] + '*'
        dd = os.path.join(s + '*', 'PlanOverview.txt')
        FractionParts = len(glob(dd))
        delivery_date = datetime.fromtimestamp(os.path.getmtime(d))
        week_delta = int((delivery_date - ref_time).days/7)
        if FractionParts > 1:
            week_deltas.append(week_delta)
            completion_fractions.append(FractionParts)
        
    d = {'CompletionFractions': completion_fractions,\
            'Week': week_deltas}
    df_MRL = pd.DataFrame(data=d)
    df_MRL.sort_values(by=['Week'])
    completions_per_week = df_MRL.Week.value_counts()
    
    #print(df_MRL)

    list1 = completions_per_week.index.to_list()
    list2 = completions_per_week.to_list()

    list1, list2 = (list(t) for t in zip(*sorted(zip(list1, list2))))

    return list1,  list2

if __name__ == "__main__":
    get_dashboard()