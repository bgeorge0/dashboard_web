import numpy as np
import glob
from tqdm import tqdm
import os
import re

def npDVH_from_DVHcontent(DVHcontent, structure):
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

def get_plan_statistics(Pat_IDA):
    plan_statistics = {}
    plan_statistics['prescription_dose_spillage'] = get_prescription_dose_spillage(Pat_IDA)
    plan_statistics['target_coverage'] = get_target_coverage(Pat_IDA)
    plan_statistics['modified_gradient_index'] = get_modified_gradient_index(Pat_IDA)
    return plan_statistics

def get_modified_gradient_index(Pat_IDA):
    pth = os.path.join("V:DoseQA", Pat_IDA, "*\PlanningData\DVHData.txt")
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
            RxTarget = re.findall("Rx Dose for \'(.*)\': ", POcontent)[0]
            RxDose = re.findall("Rx Dose for '" + RxTarget + "': (\S*)", POcontent)[0]
            Skin_cc = re.findall("  Skin                : (.*)", POcontent)[0]
            PTV_cc = re.findall("  " + RxTarget +  "\s.*: (.*)", POcontent)[0]
        except:
            print('Errors processing path : {}'.format(os.path.split(d)[0]))
            return ''
        
        SkinDVH = npDVH_from_DVHcontent(DVHcontent, 'Skin')
        PTVDVH = npDVH_from_DVHcontent(DVHcontent, RxTarget)

        # Skin_V50/PTV_V100
        dose100 = float(RxDose) * 100/100 # 100%
        dose50 = float(RxDose) *50/100 # 50%
        SkinV50 = float(Skin_cc) * np.interp(dose50, SkinDVH[:,0], SkinDVH[:,1]) / 100
        PTVV100 = float(PTV_cc) * np.interp(dose100, PTVDVH[:,0], PTVDVH[:,1]) / 100
        return "{:.2f}".format(SkinV50/PTVV100)

def get_prescription_dose_spillage(Pat_IDA):
    pth = os.path.join("V:DoseQA", Pat_IDA, "*\PlanningData\DVHData.txt")
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
            RxTarget = re.findall("Rx Dose for \'(.*)\': ", POcontent)[0]
            RxDose = re.findall("Rx Dose for '" + RxTarget + "': (\S*)", POcontent)[0]
            Skin_cc = re.findall("  Skin                : (.*)", POcontent)[0]
            PTV_cc = re.findall("  " + RxTarget +  "\s.*: (.*)", POcontent)[0]
        except:
            print('Errors processing path : {}'.format(os.path.split(d)[0]))
            return ''
        
        SkinDVH = npDVH_from_DVHcontent(DVHcontent, 'Skin')
        PTVDVH = npDVH_from_DVHcontent(DVHcontent, RxTarget)

        dose = float(RxDose) * 100/100 # R100
        SkinV100 = float(Skin_cc) * np.interp(dose, SkinDVH[:,0], SkinDVH[:,1]) / 100
        PTVV100 = float(PTV_cc) * np.interp(dose, PTVDVH[:,0], PTVDVH[:,1]) / 100
        return "{:.2f}".format(SkinV100/PTVV100)

def get_target_coverage(Pat_IDA):
    pth = os.path.join("V:DoseQA", Pat_IDA, "*\PlanningData\DVHData.txt")
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
            RxTarget = re.findall("Rx Dose for \'(.*)\': ", POcontent)[0]
            RxDose = re.findall("Rx Dose for '" + RxTarget + "': (\S*)", POcontent)[0]
            Skin_cc = re.findall("  Skin                : (.*)", POcontent)[0]
            PTV_cc = re.findall("  " + RxTarget +  "\s.*: (.*)", POcontent)[0]
        except:
            print('Errors processing path : {}'.format(os.path.split(d)[0]))
            return ''
        
        SkinDVH = npDVH_from_DVHcontent(DVHcontent, 'Skin')
        PTVDVH = npDVH_from_DVHcontent(DVHcontent, RxTarget)

        dose = float(RxDose) * 100/100 # R100
        PTVV100 = float(PTV_cc) * np.interp(dose, PTVDVH[:,0], PTVDVH[:,1]) / 100
        return "{:.2f}".format(float(PTVV100)/float(PTV_cc))
