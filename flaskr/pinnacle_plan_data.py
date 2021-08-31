import numpy as np
import glob
from tqdm import tqdm
import os
import re

def npDVH_from_DVHcontent(DVHcontent, structure):
    StructDVHTXT = ''
    inStruct = False
    content_iterator = iter(DVHcontent)
    for line in content_iterator:
        #print(line)
        if line.find('Roi:	' + structure) > -1:
            inStruct = True
            _ = next(line) # Discard this
            vol = next(line).split(' ')[-1]
            print(vol)
        if inStruct:
            StructDVHTXT = StructDVHTXT + line + '\n'
        if line.find('ROI: ') > -1:
            inStruct = True
    return np.genfromtxt(StructDVHTXT.splitlines(True), delimiter=' ')


DVHpath = 'PinnacleDVH\GC049681.txt'
DVHcontent = ''
with open(DVHpath) as file:
    DVHcontent = file.readlines()

