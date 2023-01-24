# -*- coding: utf-8 -*-
"""
Created on Mon Nov 14 17:17:12 2022

@author: aaron.hastings
"""

import pandas as pd
from DANA_Noise_Data import DANA_Noise_Data as DND

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

###############################################################################
# SHOULD PASS

# Test for case with all good inputs and df_DANA has full data for non leap year
fileName = 'Sample Data - Required Inputs - Non-Leap Year.csv' # two links
df_DANA = pd.read_csv(filePath + fileName)
median_width = 10.0 # float as float
link_grade = (8.0,-1.0) # float at limit, float as float                                                                  
robust_speeds = False
dnd_obj = DND(df_DANA, median_width, link_grade, robust_speeds)                  

# Test for three TMCs, int as float
fileName = 'Sample Data - Required Inputs - Three TMCs.csv' # more than two Links
df_DANA = pd.read_csv(filePath + fileName)
link_grade = (8,-8.0, 1)  # float at limit, int as float 
#dnd_obj = DND(df_DANA, median_width, link_grade, robust_speeds)               

# Test for one TMCs
fileName = 'Sample Data - Required Inputs - One TMC.csv' # one link
df_DANA = pd.read_csv(filePath + fileName)
link_grade = (4.5)  # correct number of grades
#dnd_obj = DND(df_DANA, median_width, link_grade, robust_speeds)           

# Test for float as list
fileName = 'Sample Data - Required Inputs - One TMC.csv' # one link
df_DANA = pd.read_csv(filePath + fileName)
link_grade = 4.5  # correct number of grades
#dnd_obj = DND(df_DANA, median_width, link_grade, robust_speeds)    


###############################################################################
# SHOULD FAIL

# UT03:
# Test for too many roadway grades for a single TMC
fileName = 'Sample Data - Required Inputs - One TMC.csv' # one link
df_DANA = pd.read_csv(filePath + fileName)
link_grade = (8.0,-8.0, 0.0) # too many grades
# dnd_obj = DND(df_DANA, median_width, link_grade, robust_speeds)        

