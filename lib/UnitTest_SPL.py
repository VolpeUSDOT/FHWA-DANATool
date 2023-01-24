# -*- coding: utf-8 -*-
"""
Created on Tue Nov  8 18:27:43 2022

@author: aaron.hastings
"""

import pandas as pd
import numpy as np
from Sound_Pressure_Level_Metrics import Sound_Pressure_Level as spl

###############################################################################
# COMPUTE FOR VALID INPUTS
###############################################################################

# UT 01:
# Pass a numpy array with 2 float elements, one at 60 and one at 70
# Result AVG = 67.40362689
nparray = np.array([60, 70])
print('UT 01: Pass a numpy array with 2 float elements, one at 60 and one at 70.')
if round(spl.Compute_LOG_AVG(nparray), 4) == round(67.40362689, 4):
    print('LOG_AVG = PASS')
else:
    print('LOG_AVG = FAIL')

# UT 02:
# Pass a numpy array with 24 float elements, all at 60
# This is the intended object type and format for calculations
# Result Leq_24_hour = 60, Ldn = 66.40978057, Lden = 66.67012337           
nparray = 60 * np.ones(24)
print('\nUT 02: Pass a numpy array with 24 float elements, all at 60.')
if spl.LEQ_24_HR(nparray) == 60:
    print('Leq_24_Hour = PASS')
else:
    print('Leq_24_Hour = FAIL')
if round(spl.LDN(nparray), 4) == round(66.40978057, 4):
    print('Ldn = PASS')
else:
    print('Ldn = FAIL')
if round(spl.LDEN(nparray), 4) == round(66.67012337, 4):
    print('Lden = PASS')
else:
    print('Lden = FAIL')

# UT 03:
# Pass a df with 24 rows and one column that has integer data, all at 60
# Result Leq_24_hour = 60, Ldn = 66.40978057, Lden = 66.67012337           
times = pd.date_range("00:00", "23:00", freq="60min")
df = pd.DataFrame(60*np.ones(len(times)), index=times, columns=['spl'])
print('\nUT 03: Pass a df with 24 rows and one column that has integer spl data, all at 60.')
if spl.LEQ_24_HR(df, 'spl') == 60:
    print('Leq_24_Hour = PASS')
else:
    print('Leq_24_Hour = FAIL')
if round(spl.LDN(df, 'spl'), 4) == round(66.40978057, 4):
    print('Ldn = PASS')
else:
    print('Ldn = FAIL')
if round(spl.LDEN(df, 'spl'), 4) == round(66.67012337, 4):
    print('Lden = PASS')
else:
    print('Lden = FAIL')

# UT 04:
# Pass a list with 24 elements all strings of '60'
# This is the farthest removed object from the intended numpy array that should work
# Result Leq_24_hour = 60, Ldn = 66.40978057, Lden = 66.67012337           
list_obj = ['60','60','60','60','60','60','60','60','60','60','60','60','60','60',
     '60','60','60','60','60','60','60','60','60','60']
print('\nUT 04: Pass a list with 24 strings, all at 60.')
if spl.LEQ_24_HR(list_obj) == 60:
    print('Leq_24_Hour = PASS')
else:
    print('Leq_24_Hour = FAIL')
if round(spl.LDN(list_obj), 4) == round(66.40978057, 4):
    print('Ldn = PASS')
else:
    print('Ldn = FAIL')
if round(spl.LDEN(list_obj), 4) == round(66.67012337, 4):
    print('Lden = PASS')
else:
    print('Lden = FAIL')
    
# UT 05:
# Pass a tuple with 24 elements all strings of '60'
# This is also the farthest removed object from the intended numpy array that should work
# Result Leq_24_hour = 60, Ldn = 66.40978057, Lden = 66.67012337 
tuple_obj = ('60','60','60','60','60','60','60','60','60','60','60','60','60','60',
     '60','60','60','60','60','60','60','60','60','60')
print('\nUT 05: Pass a tuple with 24 strings, all at 60.')
if spl.LEQ_24_HR(tuple_obj) == 60:
    print('Leq_24_Hour = PASS')
else:
    print('Leq_24_Hour = FAIL')
if round(spl.LDN(tuple_obj), 4) == round(66.40978057, 4):
    print('Ldn = PASS')
else:
    print('Ldn = FAIL')
if round(spl.LDEN(tuple_obj), 4) == round(66.67012337, 4):
    print('Lden = PASS')
else:
    print('Lden = FAIL')
    
# UT 06:
# Pass a tuple with 24 elements a mix of strings, floats and ints all = 60
# This is also the farthest removed object from the intended numpy array that should work
# Result Leq_24_hour = 60, Ldn = 66.40978057, Lden = 66.67012337 
tuple_obj = ('60','60','60','60',60,'60','60',60.0,'60',60,'60','60','60','60',
     '60','60','60','60','60','60','60','60','60','60')
print('\nUT 06: Pass a tuple with 24 valid but mixed types, all at 60.')
if spl.LEQ_24_HR(tuple_obj) == 60:
    print('Leq_24_Hour = PASS')
else:
    print('Leq_24_Hour = FAIL')
if round(spl.LDN(tuple_obj), 4) == round(66.40978057, 4):
    print('Ldn = PASS')
else:
    print('Ldn = FAIL')
if round(spl.LDEN(tuple_obj), 4) == round(66.67012337, 4):
    print('Lden = PASS')
else:
    print('Lden = FAIL')

###############################################################################
# CATCH EXCEPTIONS
###############################################################################

# UT 07:
# Not enough data for 24 hour metric
list_obj = [60, 60, 60, 60,'60']
print('\nUT 07: Pass a list with only 4 values, all at 60.')
if spl.LEQ_24_HR(list_obj) is None:
    print('Leq_24_Hour = CAUGHT EXCEPTION')
else:
    print('Leq_24_Hour = MISSED EXCEPTION')
if spl.LDN(list_obj) is None:
    print('Ldn = CAUGHT EXCEPTION')
else:
    print('Ldn = MISSED EXCEPTION')
if spl.LDEN(list_obj) is None:
    print('Lden = CAUGHT EXCEPTION')
else:
    print('Lden = MISSED EXCEPTION')

# UT 08:
# Pass a tuple with 24 elements, some cannot be converted to numeric
# This is also the farthest removed object from the intended numpy array that should work
# Result Leq_24_hour = 60, Ldn = 66.40978057, Lden = 66.67012337 
tuple_obj = ('60','60','60','60',60,'60','60',60.0,'60',60,'60','60','60','60',
     '60','60','60','60','apple','fred','60','60','60','60')
print('\nUT 08: Pass a tuple with 24 elements, some cannot be converted to numeric.')
if spl.LEQ_24_HR(tuple_obj) is None:
    print('Leq_24_Hour = CAUGHT EXCEPTION')
else:
    print('Leq_24_Hour = MISSED EXCEPTION')
if spl.LDN(tuple_obj) is None:
    print('Ldn = CAUGHT EXCEPTION')
else:
    print('Ldn = MISSED EXCEPTION')
if spl.LDEN(tuple_obj) is None:
    print('Lden = CAUGHT EXCEPTION')
else:
    print('Lden = MISSED EXCEPTION')