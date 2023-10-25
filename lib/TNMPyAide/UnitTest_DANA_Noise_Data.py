# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 17:17:12 2022

@author: aaron.hastings
"""

import pandas as pd
from DANA_Noise_Data import Link_Sound_Data as LSD
import time


filePath = 'C:/Users/aaron.hastings/OneDrive - DOT OST/Code/Python/FHWA-DANATool/lib/UnitTests/'
#fileName = 'Sample Data - Required Inputs - No Missing Data.csv'
#fileName = 'Sample Data - Required Inputs - Non-Leap Year.csv'
#fileName = 'Sample Data - Required Inputs - One Month Two Links.csv'
#fileName = 'Sample Data - Required Inputs - Missing Speeds - 1 month.csv'
#fileName = 'Sample Data - Required Inputs - Missing Volumes - 1 month.csv'
#fileName = 'Sample Data - 101+05209 No average worst hour.csv'
#fileName = 'test_csv_20220623.csv'
#fileName = 'Sample Data - Required Inputs - Missing Speeds.csv'
#fileName = 'Sample Data - 111+04600 - Fails on DATE 24 hour metrics.csv'

Test_Case_To_Run = 4
###############################################################################
# SHOULD PASS

start_time = time.time()

# NEED TO CREATE UNIT TEST COMPARE HT +8 Grade and -8 Grade -- manual test works

# Test for case with all good inputs and df_DANA has full data for non leap year
if Test_Case_To_Run == 1:
    print('Test for case with all good inputs and df_DANA has full data for non leap year')
    fileName = 'Sample Data - Required Inputs - Non-Leap Year.csv' # two links
    df_DANA = pd.read_csv(filePath + fileName)
    link_grade = (-8.0,-1.0) # float at limit, float as float                                                                  
    lsd_obj = LSD(df_DANA, link_grade)    


# Test for case with all good inputs and df_DANA has full data for non leap year
if Test_Case_To_Run == 2:
    print('Test for case with all good inputs and df_DANA has full data for non leap year')
    fileName = 'Sample Data - Required Inputs - Non-Leap Year.csv' # two links
    df_DANA = pd.read_csv(filePath + fileName)
    link_grade = (8.0,-1.0) # float at limit, float as float                                                                  
    lsd_obj = LSD(df_DANA, link_grade, robust_speeds=True) # O > 230 times slower

# Test for three TMCs, int as float
if Test_Case_To_Run == 3:
    print('Test for three TMCs, int as float')
    fileName = 'Sample Data - Required Inputs - Three TMCs.csv' # more than two Links
    df_DANA = pd.read_csv(filePath + fileName)
    link_grade = (8,-8.0, 1)  # float at limit, int as float, TMC pairs should have same median_width and opposite link_grades 
    lsd_obj = LSD(df_DANA, link_grade)                  

# Test for one TMCs
if Test_Case_To_Run == 4:
    print('Test for one TMC')
    fileName = 'Sample Data - Required Inputs - One TMC.csv' # one link
    df_DANA = pd.read_csv(filePath + fileName)
    link_grade = (4.5)  # correct number of grades - note, this is just a float
    lsd_obj = LSD(df_DANA, link_grade)                  

# Test for float as list
if Test_Case_To_Run == 5:
    print('Test for float as list')
    fileName = 'Sample Data - Required Inputs - One TMC.csv' # one link
    df_DANA = pd.read_csv(filePath + fileName)
    link_grade = 4.5  # correct number of grades
    lsd_obj = LSD(df_DANA, link_grade)                  


###############################################################################
# SHOULD FAIL

# Test for too many roadway grades for a single TMC
if Test_Case_To_Run == 6:
    print('Test for too many roadway grades for a single TMC')
    fileName = 'Sample Data - Required Inputs - One TMC.csv' # one link
    df_DANA = pd.read_csv(filePath + fileName)
    link_grade = (8.0,-8.0, 0.0) # too many grades
    lsd_obj = LSD(df_DANA, link_grade)                  

stop_time = time.time()

print('Elapsed Time: ', (stop_time - start_time), ' sec')