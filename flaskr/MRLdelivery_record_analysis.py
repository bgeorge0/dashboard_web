from glob import glob
import re
from tqdm import tqdm
import os
from datetime import datetime
import pandas as pd
from functools import lru_cache

def MRL_fraction_count():
    df_MRL = delivery_record_analysis()
    #df_MRL_fractions = df_MRL.loc[df_MRL['Fraction'] == '1']

    list1 = df_MRL['Date_Delta'].to_list()
    list2 = df_MRL['Date_Delta'].index.to_list()

    return list1, list2

def MRL_patient_count():
    df_MRL = delivery_record_analysis()
    df_MRL_fractions = df_MRL.loc[df_MRL['Fraction'] == '1']
    df_MRL = df_MRL.sort_values(by=['Date_Delta'], ignore_index=True)

    list1 = df_MRL_fractions['Date_Delta'].to_list()
    list2 = df_MRL_fractions['Date_Delta'].index.to_list()

    return list1, list2

@lru_cache(maxsize=1)
def delivery_record_analysis():
    D_OX4 = glob("V:\Physics\DeliveryRecords\Delivery Record TDS-SN-243 *.txt")
    D_SW5 = glob("W:\Physics\DeliveryRecords\Delivery Record TDS-SN-242 *.txt")

    D = D_OX4 + D_SW5

    ref_time = datetime(2019,6,1,0,0,0,0)

    DATE = []
    ID = []
    TX = []
    FX = []
    SN = []
    RX = []
    TotalFX = []
    DOB = []
    CTVVOL = []
    DATE_DELTA = []

    pbar = tqdm(D)
    for d in pbar:
        #print(d)
        content = ''
        with open(d) as file:
            content = file.read()
        
        if not re.findall('N/A - QA Mode', content) and re.findall('Prescription Fraction Number: \d\n', content):
            candidate_ID = re.findall('((?:GC\d|CP\d|GCHX)\d{5})', content)        
            candidate_date = re.findall('Delivered on: ([^\n\r]*)', content)
            candidate_Fx = re.findall('Prescription Fraction Number: (\d)\n', content)
            candidate_totalFx = re.findall('Total Fractions (Rx): (\d)\n', content)
            candidate_Rx = re.findall('Rx Dose for \'.*\': (.*) Gy to .* in (.*) fractions,', content)
            candidate_DOB = re.findall('Patient Date of Birth: (.*)', content)
            candidate_SN = re.findall('Serial Number: (\d+)\n', content)
            candidate_CTVvol = re.findall('  CTV                 : (.*)', content)
            id = ''
            Fx = ''
            totalFx = ''
            Rx = ''
            tx = ''
            dob = ''
            CTVvol = 0
            sn = ''

            if len(candidate_ID) > 0:
                id = candidate_ID[0]
            if len(candidate_date) > 0:
                date = datetime.strptime(candidate_date[0], '%b %d %Y %H:%M:%S')
            if len(candidate_Fx) > 0:
                Fx = candidate_Fx[0]
            #if len(candidate_Rx) > 0:
            #    totalFx = candidate_totalFx[0][1]
            if len(candidate_Rx) > 0:
                Rx = candidate_Rx[0][0]
            if len(candidate_DOB) > 0:
                dob = candidate_DOB[0]
            if len(candidate_CTVvol) > 0:
                CTVvol = candidate_CTVvol[0]
            if len(candidate_SN) > 0:
                sn = candidate_SN[0]
            
            if len(id) > 0 and len(Fx) > 0:
                if SN == '243':
                    PO = glob(os.path.join("V:\DoseQA", id, "*_Original\PlanningData\PlanOverview.txt"))
                else:
                    PO = glob(os.path.join("W:\DoseQA", id, "*_Original\PlanningData\PlanOverview.txt"))
                if len(PO) > 0:
                    content = ''
                    with open(PO[0]) as file:
                        content = file.read()
                    candidate_Tx = re.findall('Plan Name: ([A-Za-z]+)_[Oo]riginal', content)
                    if len(candidate_Tx) > 0:
                            tx = candidate_Tx[0]
                
                date_delta = int((date - ref_time).days)

                DATE.append(date)
                ID.append(id)
                TX.append(tx)
                FX.append(Fx)
                SN.append(sn)
                RX.append(Rx)
                TotalFX.append(totalFx)
                DOB.append(dob)
                CTVVOL.append(CTVvol)
                DATE_DELTA.append(date_delta)

    d = {'Date': DATE, 'Pat_IDA': ID, 'TxSite': TX, 'Fraction': FX, 'SN': SN, 'RxDose': RX, 'TotalFx': TotalFX, 'DOB': DOB, 'CTVvol': CTVVOL, 'Date_Delta': DATE_DELTA}

    df_MRL = pd.DataFrame(data=d)
    df_MRL = df_MRL.sort_values(by=['Date_Delta'], ignore_index=True)
    df_MRL['Date_Delta'] = df_MRL['Date_Delta'] - min(df_MRL['Date_Delta'])

    return df_MRL

if __name__ == "__main__":
    MRL_patient_count()