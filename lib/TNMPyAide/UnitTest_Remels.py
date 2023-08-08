# -*- coding: utf-8 -*-
"""
Created on Wed Feb  1 18:46:03 2023

@author: aaron.hastings
"""

from Compute_TNM_Ref_Max_Levels import Compute_REMELs as remels

# UT01: 1236 Auto at 61.97206154 mph on Avg Pavement

spl = remels.Compute_SPL('at',1236,61.97206154,'avg','cruise')

print('SPL for 1236 auto at 61.97206154 mph over avg pavement should be 73.70')
print('Should match Auto SPL row 9, col x of "Leq Worst HOur Calculations" Tab in original spreadsheet and original sample data')
print('Actual SPL = ' + str(round(spl,2)))
if round(spl,2)==73.70:
    print('UT01 Passes')
else:
    print('UT01 Fails')
print('')

# UT02: 286 HTs at 57.54548571 mph on PCC Pavement
spl = remels.Compute_SPL('ht',286,57.54548571,'avg','cruise')

print('SPL for 286 Hts at 57.54548571 mph over AVG pavement should be 76.29')
print('Should match HT SPL row 9, col z of "Leq Worst HOur Calculations" Tab in original spreadsheet and original sample data')
print('Actual SPL = ' + str(round(spl,2)))
if round(spl,2)==76.29:
    print('UT02 Passes')
else:
    print('UT02 Fails')
print('')