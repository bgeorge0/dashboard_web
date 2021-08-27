import pandas as pd

data = pd.read_excel('S:\HQ Treatment sites shared\Physics\IMRT\ArcCHECK\OX4 MR Linac\OX4 MRL ArcCheck Patient QA Results_v17.xlsx',skiprows=6)

def getPSQAResults(Pat_IDA):
    ind = data.index[data['GC No.'] == Pat_IDA].tolist()
    if len(ind) > 0:
        return data.iat[ind[-1], 28]
    else:
        return ''
    