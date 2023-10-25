# -*- coding: utf-8 -*-
"""
Created on Wed May 17 08:05:12 2023

@author: aaron.hastings
"""

import pandas as pd
import time
from collections import namedtuple
from TNMPyAide import TNMPyAide
import numpy as np

from DANAPlot import DANAPlot as dp


filePath = 'C:/Users/aaron.hastings/OneDrive - DOT OST/Code/Python/FHWA-DANATool/lib/UnitTests/'
detailed_log = False
start_time = time.time()

Test_Case_To_Run = 2

# Basic Test Case Using TNMPyAide Data
if Test_Case_To_Run == 1:
    # Generate Data to Plot
    fileName = 'v21_TNMPyAide_Test_200.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    link_grade = (0, 0)
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', 'NC', 'MECKLENBURG')
    tnmAideData = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log)   
    df_Traffic_Noise = tnmAideData.df_Traffic_Noise
    df_avg_day = tnmAideData.df_avg_day
    df_worst_day = tnmAideData.df_worst_day
    
    x = np.array(df_avg_day.HOUR)
    y = np.array(df_avg_day.SPD_ALL_L1)
    
    # Show Bar Plot
    fig, ax = dp.Bar_Plot(x, y)
    fig, ax = dp.Bar_Plot(x, y, 'Hour', 'Speed, MPH', 'Average Hourly Speed - All')
    
    # Show single line
    fig, ax = dp.Line_Plot(x, y)
    fig, ax = dp.Line_Plot(x, y, 'Hour', 'Speed, MPH', 'Average Hourly Speed')
    
    # Show multiple lines
    x = np.array([df_avg_day.HOUR, df_avg_day.HOUR, df_avg_day.HOUR, df_avg_day.HOUR, df_avg_day.HOUR, df_avg_day.HOUR])
    y = np.array([df_avg_day.SPD_ALL_L1, df_avg_day.SPD_AT_L1, df_avg_day.SPD_HT_L1, df_avg_day.SPD_ALL_L2, df_avg_day.SPD_AT_L2, df_avg_day.SPD_HT_L2]) 
    fig, ax = dp.Line_Plot(x, y, 'Hour', 'Speed, MPH', 'Average Hourly Speed')
    labels = ['All(L1)','AT(L1)','HT(L1)','All(L2)','AT(L2)','HT(L2)']
    dp.Add_Legend(fig, ax, labels)

    # Show hourly histogram
    bin_centers = np.array(range(0,105,5))
    data = np.array([df_Traffic_Noise.SPD_ALL_L1])
    dp.Histogram(data, bin_centers, xlabel = 'Speed, MPH', title = 'Speed Distribution (All, L1)')

# TNMPYAide Test Case
if Test_Case_To_Run == 2:
    # Generate Data to Plot
    fileName = 'v21_TNMPyAide_Test_200.csv'
    df_DANA = pd.read_csv(filePath + fileName)
    link_grade = (0, 0)
    median_width = 0
    number_of_lanes = (1, 1)
    meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
    meta = meta_data('I-77 SB', 'I-77 NB', '125N04779', '125P04779', 'NC', 'MECKLENBURG')
    tnmAideData = TNMPyAide(df_DANA, link_grade, meta, median_width, number_of_lanes, detailed_log = detailed_log)   
    
    # Plot from TNMPYaide
    figs1, axs1 = tnmAideData.Plot_Avg_Day_Hourly_Speed()
    figs2, axs2 = tnmAideData.Plot_Avg_Day_Hourly_SPL()
    figs3, axs3 = tnmAideData.Plot_Hourly_Speed_Histograms()
    figs4, axs4 = tnmAideData.Plot_Hourly_SPL_Histograms()
    
if Test_Case_To_Run == 3:
    # Test multiple subplots on same figure
    x = np.array([1.0, 2.0, 3.0])
    y = np.array([1.0, 2.0, 3.0])
    fig, ax = dp.Line_Plot(x, y) # Figure 1

    fig, ax = dp.Line_Plot(x, y) # Figure 2

    y2 = y * y    
    fig, ax = dp.Line_Plot(x, y2, fig = fig, ax = ax) # Figure 2, updated
    
    fig, ax = dp.Line_Plot(x, y, ax = 311) # Figure 3 with subplot specified
    
    fig, ax = dp.Line_Plot(x, y2, fig = fig, ax = 312) # Figure 3 with new subplot specified
    
stop_time = time.time()
print('\nExecution Time: ' + str(round(stop_time - start_time,2)) + ' sec')