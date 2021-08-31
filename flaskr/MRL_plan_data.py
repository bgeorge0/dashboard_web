import numpy as np
import glob
from tqdm import tqdm
import os
import re
import pandas as pd

class MRL_plan_data():

    Pat_IDA = ''
    Plan_Statistics = ''
    PSQA_Results = ''
    DVHcontent = ''
    RxTarget = ''
    RxDose = 0.0
    Skin_cc = 0.0
    PTV_cc = 0.0

    def __init__(self, Pat_IDA):
        self.Pat_IDA = Pat_IDA
        self.DVHcontent = self._get_DVHcontent()
        self.Plan_Statistics = self._get_plan_statistics()
        #self.PSQA_Results = self._get_PSAQA_results()

    def _get_PSAQA_results(self):
        data = pd.read_excel('S:\HQ Treatment sites shared\Physics\IMRT\ArcCHECK\OX4 MR Linac\OX4 MRL ArcCheck Patient QA Results_v18.xlsx',skiprows=6)

        ind = data.index[data['GC No.'] == self.Pat_IDA].tolist()
        if len(ind) > 0:
            return data.iat[ind[-1], 28]
        else:
            return ''    

    def _npDVH_from_DVHcontent(self, DVHcontent, structure):
        StructDVHTXT = ''
        inStruct = False
        for line in DVHcontent:
            #print(line)
            if line.find('#') > -1:
                inStruct = False
            if inStruct:
                StructDVHTXT = StructDVHTXT + line[0:-2] + '\n'
            if line.find(structure + ';') > -1:
                inStruct = True
        return np.genfromtxt(StructDVHTXT.splitlines(True), delimiter=',')

    def _get_plan_statistics(self):
        plan_statistics = {}
        plan_statistics['prescription_dose_spillage'] = self._get_prescription_dose_spillage()
        plan_statistics['target_coverage'] = self._get_target_coverage()
        plan_statistics['modified_gradient_index'] = self._get_modified_gradient_index()
        return plan_statistics

    def _get_modified_gradient_index(self):           
        SkinDVH = self._npDVH_from_DVHcontent(self.DVHcontent, 'Skin')
        PTVDVH = self._npDVH_from_DVHcontent(self.DVHcontent, self.RxTarget)

        # Skin_V50/PTV_V100
        dose100 = float(self.RxDose) * 100/100 # 100%
        dose50 = float(self.RxDose) *50/100 # 50%
        SkinV50 = float(self.Skin_cc) * np.interp(dose50, SkinDVH[:,0], SkinDVH[:,1]) / 100
        PTVV100 = float(self.PTV_cc) * np.interp(dose100, PTVDVH[:,0], PTVDVH[:,1]) / 100
        return "{:.2f}".format(SkinV50/PTVV100)

    def _get_prescription_dose_spillage(self):
        SkinDVH = self._npDVH_from_DVHcontent(self.DVHcontent, 'Skin')
        PTVDVH = self._npDVH_from_DVHcontent(self.DVHcontent, self.RxTarget)

        dose = float(self.RxDose) * 100/100 # R100
        SkinV100 = float(self.Skin_cc) * np.interp(dose, SkinDVH[:,0], SkinDVH[:,1]) / 100
        PTVV100 = float(self.PTV_cc) * np.interp(dose, PTVDVH[:,0], PTVDVH[:,1]) / 100
        return "{:.2f}".format(SkinV100/PTVV100)

    def _get_target_coverage(self):
        SkinDVH = self._npDVH_from_DVHcontent(self.DVHcontent, 'Skin')
        PTVDVH = self._npDVH_from_DVHcontent(self.DVHcontent, self.RxTarget)

        dose = float(self.RxDose) * 100/100 # R100
        PTVV100 = float(self.PTV_cc) * np.interp(dose, PTVDVH[:,0], PTVDVH[:,1]) / 100
        return "{:.2f}".format(float(PTVV100)/float(self.PTV_cc))
    
    def _get_DVHcontent(self):
        pth = os.path.join("V:DoseQA", self.Pat_IDA, "*\PlanningData\DVHData.txt")
        D = glob.glob(pth)

        if len(D) == 0:
            return ''
        else:
            d = D[0] # Just take the first for now and assume that this is the correct DVH file
            DVHpath = d
            POpath = os.path.join(os.path.split(d)[0], 'PlanOverview.txt')

            POcontent = ''
            with open(POpath) as file:
                POcontent = file.read()
            
            DVHcontent = ''
            with open(DVHpath) as file:
                DVHcontent = file.readlines()
            
            try:
                self.RxTarget = re.findall("Rx Dose for \'(.*)\': ", POcontent)[0]
                self.RxDose = re.findall("Rx Dose for '" + self.RxTarget + "': (\S*)", POcontent)[0]
                self.Skin_cc = re.findall("  Skin                : (.*)", POcontent)[0]
                self.PTV_cc = re.findall("  " + self.RxTarget +  "\s.*: (.*)", POcontent)[0]
            except Exception as e:
                print(e)
                print('Errors processing path : {}'.format(os.path.split(d)[0]))
                return ''
            return DVHcontent
            
