# -*- coding: utf-8 -*-
"""
Created on Tue Feb 21 11:58:33 2023

@author: aaron.hastings
"""

import pandas as pd
#from DANA_Noise_Data import DANA_Noise_Data as DND
import time
from collections import namedtuple
from TNMPyAide import TNMPyAide


filePath = 'C:/Users/aaron.hastings/OneDrive - DOT OST/Code/Python/FHWA-DANATool/lib/UnitTests/'
detailed_log = False

Test_Case_To_Run = 1
###############################################################################
# TEST 1 to 4
# TESTING THAT ONE AND TWO LINKS FOR LEAP AND NON-LEAP YEARS RUN

start_time = time.time()

# Test for case with all good inputs and df_DANA has full data for non leap year - Two Links
if Test_Case_To_Run == 1:
    print('Test 01: Test for case with all good inputs and df_DANA has full data for non leap year, 2 Links')
    fileName = 'v21_TNMPyAide_Test_01.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = (0, 0)
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', 'NC', 'MECKLENBURG')
    
    print('TNMPyAide should run with no errors --- standby.')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log)   
    
    LAEq_Alltime_Worst_Hour = 87.12258613863014
    LAEq_Average_Worst_Hour = 86.46145179386103
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')
            
    
    
# Test for case with all good inputs and df_DANA has full data for non leap year - One Link
if Test_Case_To_Run == 2:
    print('Test 02: Test for case with all good inputs and df_DANA has full data for non leap year, 1 Link')
    fileName = 'v21_TNMPyAide_Test_02.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = 0
        
    meta_data = namedtuple('meta_data', 'L1_name L1_tmc state county')
    meta = meta_data('I-77 SB', '125N04779', 'NC', 'MECKLENBURG')
    
    print('TNMPyAide should run with no errors --- standby.')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, detailed_log = detailed_log)   
    
    LAEq_Alltime_Worst_Hour = 84.75667213150864
    LAEq_Average_Worst_Hour = 84.09553778673953
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')
    
# Test for case with all good inputs and df_DANA has full data for leap year - Two Links
if Test_Case_To_Run == 3:
    print('Test 03: Test for case with all good inputs and df_DANA has full data for leap year, 2 Links')
    fileName = 'v21_TNMPyAide_Test_03.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = (0, 0)
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', 'NC', 'MECKLENBURG')
    
    print('TNMPyAide should run with no errors --- standby.')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log) 
    
    LAEq_Alltime_Worst_Hour = 91.73988711712357
    LAEq_Average_Worst_Hour = 86.46085584553761
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')
    
# Test for case with all good inputs and df_DANA has full data for leap year - One Link
if Test_Case_To_Run == 4:
    print('Test 04: Test for case with all good inputs and df_DANA has full data for leap year, 1 Link')
    fileName = 'v21_TNMPyAide_Test_04.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = 0
        
    meta_data = namedtuple('meta_data', 'L1_name L1_tmc state county')
    meta = meta_data('I-77 SB', '125N04779', 'NC', 'MECKLENBURG')
    
    print('TNMPyAide should run with no errors --- standby.')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, detailed_log = detailed_log)   
    
    LAEq_Alltime_Worst_Hour = 91.08671195363206
    LAEq_Average_Worst_Hour = 84.09494183841613
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')
        
        

###############################################################################
# TEST 5 to 7
# TESTING SPECIFIC WORST AVG HOUR AND WORST HOUR OF ALL TIME (1 AND 2 LINKS)
                                  
# Non leap year - One Link, Hour 12 is worst on average, hour 3 on Jan 3 worst of all time
if Test_Case_To_Run == 5:
    print('Test 05: Test for case with all good inputs and df_DANA has full data for non leap year, 1 Link')
    fileName = 'v21_TNMPyAide_Test_05.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = 0
        
    meta_data = namedtuple('meta_data', 'L1_name L1_tmc state county')
    meta = meta_data('I-77 SB', '125N04779', 'NC', 'MECKLENBURG')
    
    print('Worst hour on average should = 12. Worst hour of all time should equal 3 am on Jan 3')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, detailed_log = detailed_log)   
    
    LAEq_Alltime_Worst_Hour = 86.19126951050475
    LAEq_Average_Worst_Hour = 102.25938835395972
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')
        
        

# Non leap year - Two Links, Hour 12 is worst on average, hour 3 on Jan 3 worst of all time
# Worst hours driven by link 1
if Test_Case_To_Run == 6:
    print('Test 06: Test for case with all good inputs and df_DANA has full data for non leap year, 2 Links')
    fileName = 'v21_TNMPyAide_Test_06.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = (0, 0)
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', 'NC', 'MECKLENBURG')
    
    print('Worst hour on average should = 12. Worst hour of all time should equal 3 am on Jan 3')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log)   

    LAEq_Alltime_Worst_Hour = 102.26652662443747
    LAEq_Average_Worst_Hour = 87.71819268978678
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')

                              

# Non leap year - Two Links, Hour 12 is worst on average, hour 3 on Jan 3 worst of all time
# Worst hours driven by link 2
if Test_Case_To_Run == 7:
    print('Test 07: Test for case with all good inputs and df_DANA has full data for non leap year, 2 Links')
    fileName = 'v21_TNMPyAide_Test_07.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = (0.0, 0.0) 
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', 'NC', 'MECKLENBURG')
    
    print('Worst hour on average should = 12. Worst hour of all time should equal 3 am on Jan 3')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log)
    
    LAEq_Alltime_Worst_Hour = 100.87166294933448
    LAEq_Average_Worst_Hour = 87.35062799999832
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')
        
        
    
###############################################################################
# TEST 8 to 9
# TESTING WHEN ONE LINK IS DOMINANT

# Non leap year - Two Links
# Link 1 Dominant
if Test_Case_To_Run == 8:
    print('Test 08: Test for case with all good inputs and df_DANA has full data for non leap year, 2 Links')
    fileName = 'v21_TNMPyAide_Test_08.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = (0.0, 0.0) 
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', 'NC', 'MECKLENBURG')
    
    print('Link 1 is Dominant, but should be similar to results when Link 2 is Dominant')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log) 
    
    LAEq_Alltime_Worst_Hour = 84.75918758326179
    LAEq_Average_Worst_Hour = 84.09805323849268
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')
        
        
    
# Non leap year - Two Links
# Link 2 Dominant
if Test_Case_To_Run == 9:
    print('Test 09: Test for case with all good inputs and df_DANA has full data for non leap year, 2 Links')
    fileName = 'v21_TNMPyAide_Test_09.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = (0.0, 0.0) 
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', 'NC', 'MECKLENBURG')
    
    print('Link 2 is Dominant, but should be similar to results when Link 1 is Dominant')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log) 
    
    LAEq_Alltime_Worst_Hour = 83.36014161619792
    LAEq_Average_Worst_Hour = 82.69900727142877
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')
        
        
    
    
###############################################################################
# TEST 10 to 12
# TESTING RESULTS

# Non leap year - Two Links
if Test_Case_To_Run == 10:
    print('Test 10: Test results full data for non leap year, 2 Links - 1 lane each way, no median, 0 grade')
    fileName = 'v21_TNMPyAide_Test_10.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = (0.0, 0.0) 
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', 'NC', 'MECKLENBURG')
    
    print('Link 2 is Dominant, but should be similar to results when Link 1 is Dominant')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log) 
    
    LAEq_Alltime_Worst_Hour = 87.12258613863014
    LAEq_Average_Worst_Hour = 86.46145179386103
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')
        
        
    
    # Non leap year - Two Links
if Test_Case_To_Run == 11:
    print('Test 11: Test results full data for non leap year, 2 Links - 4 lanes each way, 20 ft median, 0 grade')
    fileName = 'v21_TNMPyAide_Test_10.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = (0.0, 0.0) 
    median_width = 20
    number_of_lanes = (4, 4)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', 'NC', 'MECKLENBURG')
    
    print('Link 2 is Dominant, but should be similar to results when Link 1 is Dominant')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log) 
    
    LAEq_Alltime_Worst_Hour = 86.0714260423835
    LAEq_Average_Worst_Hour = 85.41029169761437
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')
        
        
    
    # Non leap year - Two Links
if Test_Case_To_Run == 12:
    print('Test 12: Test results full data for non leap year, 2 Links - 1 lane each way, no median, +/-8 grade')
    fileName = 'v21_TNMPyAide_Test_10.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    
    link_grade = (8.0, -8.0) 
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', 'NC', 'MECKLENBURG')
    
    print('Link 2 is Dominant, but should be similar to results when Link 1 is Dominant')    
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log) 
    
    LAEq_Alltime_Worst_Hour = 85.03076177233521 
    LAEq_Average_Worst_Hour = 86.6245246916261
    err1 = tnmPyAide.worst_day.worst_hour - LAEq_Alltime_Worst_Hour
    err2 = tnmPyAide.average_day.worst_hour - LAEq_Average_Worst_Hour
    if err1 < 0.1 and err2 < 0.1:
        print('\nLAeq Worst Hour Metrics agree with expected results to within 0.1 dB')
        print('PASS\n')
    else:
        print('\nLAeq Worst Hour Metrics do not agree with expected results to within 0.1 dB')
        print('FAIL\n')
        
        
    
###############################################################################
# TEST 100 to ZZ
# TESTS THAT SHOULD FAIL

# Test for case with all good inputs and df_DANA has full data for non leap year but 3 links
if Test_Case_To_Run == 100:
    print('Test 100: Test for case with all 3 Links specified - should fail\n')
    fileName = 'v21_TNMPyAide_Test_100.csv' # MORE THAN TWO LINKS
    df_DANA = pd.read_csv(filePath + fileName)
    link_grade = (0, 0)
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc L3_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', '3rdTMC_should_fail', 'NC', 'MECKLENBURG')
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log)                                     

# No Matching TMC Links in data compared to meta 
if Test_Case_To_Run == 101:
    print('Test 101: Test for case with no matching Links specified - should fail\n')
    fileName = 'v21_TNMPyAide_Test_100.csv' # MORE THAN TWO LINKS
    df_DANA = pd.read_csv(filePath + fileName)
    link_grade = (0, 0)
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', 'TMC1', 'TMC2', 'NC', 'MECKLENBURG')
    tnmPyAide = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log)   

###############################################################################

stop_time = time.time()

print('Elapsed Time: ', (stop_time - start_time), ' sec')