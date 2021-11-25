import logging
from glob import glob
import re
from tqdm import tqdm
import os
from datetime import datetime
import pydicom

D_OX4 = glob("V:\Physics\DeliveryRecords\Delivery Record TDS-SN-243 *.txt")
D_SW5 = glob("W:\Physics\DeliveryRecords\Delivery Record TDS-SN-242 *.txt")

D = D_OX4 + D_SW5

fractions = open(r"C:\Users\ben.george\OneDrive - GenesisCare\MRL\Projects\Dashboard-web\xlsm_dashboard\data\fractions.csv", 'a+')
patients = open(r"C:\Users\ben.george\OneDrive - GenesisCare\MRL\Projects\Dashboard-web\xlsm_dashboard\data\patients.csv", 'a+')

already_scanned = r"C:\Users\ben.george\OneDrive - GenesisCare\MRL\Projects\Dashboard-web\xlsm_dashboard\src\py\already_scanned.csv"
with open(already_scanned, 'r') as f:
    already_scanned_content = f.read()

logging.basicConfig(filename='update_dashboard.log', filemode='a', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())

logging.info('Found ' + str(len(D)))

pbar = tqdm(D)
for d in pbar:

    if already_scanned_content.find(d) > 0:
        # Done this one already
        pass
    else:

        #print(d)
        content = ''
        with open(d) as f:
            content = f.read()
        with open(already_scanned, 'a+') as f:
            f.write(d + '\n')
        
        if not re.findall('N/A - QA Mode', content) and re.findall('Prescription Fraction Number: \d\n', content):
            candidate_ID = re.findall('((?:GC\d|CP\d|GCHX)\d{5})', content)        
            candidate_date = re.findall('Delivered on: ([^\n\r]*)', content)
            candidate_Fx = re.findall('Prescription Fraction Number: (\d)\n', content)
            candidate_totalFx = re.findall('Total Fractions (Rx): (\d)\n', content)
            candidate_Rx = re.findall('Rx Dose for \'.*\': (.*) Gy to .* in (.*) fractions,', content)
            candidate_DOB = re.findall('Patient Date of Birth: (.*)', content)
            candidate_SN = re.findall('Serial Number: (\d+)\n', content)
            candidate_CTVvol = re.findall('  CTV                 : (.*)', content)
            candidate_GTVvol = re.findall('  GTV                 : (.*)', content)
            id = ''
            Fx = ''
            totalFx = ''
            Rx = ''
            tx = ''
            DOB = ''
            CTVvol = 0
            GTVvol = 0
            SN = ''
            sim_date = ''

            if len(candidate_ID) > 0:
                id = candidate_ID[0]
            if len(candidate_date) > 0:
                date = datetime.strptime(candidate_date[0], '%b %d %Y %H:%M:%S')
            if len(candidate_Fx) > 0:
                Fx = candidate_Fx[0]
            if len(candidate_Rx) > 0:
                totalFx = candidate_Rx[0][1]
            if len(candidate_Rx) > 0:
                Rx = candidate_Rx[0][0]
            if len(candidate_DOB) > 0:
                DOB = candidate_DOB[0]
            if len(candidate_CTVvol) > 0:
                CTVvol = candidate_CTVvol[0]
            if len(candidate_GTVvol) > 0:
                GTVvol = candidate_GTVvol[0]
            if len(candidate_SN) > 0:
                SN = candidate_SN[0]
            
            if len(id) > 0 and len(Fx) > 0:
                if SN == '243':
                    PO = glob(os.path.join("V:\DoseQA", id, "*_Original\PlanningData\PlanOverview.txt"))
                else:
                    PO = glob(os.path.join("W:\DoseQA", id, "*_Original\PlanningData\PlanOverview.txt"))
                if len(PO) > 0:
                    content = ''
                    with open(PO[0]) as file:
                        content = file.read()
                    #print(content)
                    tx_line = re.findall('Plan Name: (.*)', content)
                    #print(tx_line[0])
                    candidate_Tx = re.findall('([A-Za-z]+)_[Oo]riginal', tx_line[0])
                    if len(candidate_Tx) > 0:
                        tx = candidate_Tx[0]
                    else:
                        tx = tx_line[0]
                    #print(tx)

                if Fx == '1':
                    # Try and get sim date
                    if SN == '243':
                        MR = glob(os.path.join("V:\DoseQA", id, "*_Original\PlanningData\MR2*.dcm"))
                    else:
                        MR = glob(os.path.join("W:\DoseQA", id, "*_Original\PlanningData\MR2*.dcm"))
                    if len(MR) > 0:
                        with pydicom.dcmread(MR[0]) as ds:
                            if hasattr(ds,'AcquisitionDate'):
                                sim_date = datetime.strptime(ds.AcquisitionDate, '%Y%m%d')
                            elif hasattr(ds, 'InstanceCreationDate'):
                                sim_date = datetime.strptime(ds.InstanceCreationDate, '%Y%m%d')
                    
                fractions.write(str(date) + ',' + id + ',' + tx + ',' + Fx + ',' + SN + '\n')

                if Fx == '1':
                    patients.write(str(date) + ',' + id + ',' + tx + ',' + Fx + ',' + Rx + ',' + totalFx + ',' + DOB + ',' + str(CTVvol) + ',' + str(GTVvol) + ','  + SN + ',' + str(sim_date) + '\n')

fractions.close()
patients.close()

logging.info('Finished updating factions and patients csv')