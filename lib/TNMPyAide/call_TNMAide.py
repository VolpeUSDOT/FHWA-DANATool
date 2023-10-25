# -*- coding: utf-8 -*-
"""
Created By: Volpe National Transportation Systems Center

@author: Aaron.Hastings
"""

from TNMAide import TNMAide
import pandas as pd
import datetime

t1 = datetime.datetime.now()

filePath = 'C:/Users/aaron.hastings/OneDrive - DOT OST/Code/Python/FHWA-DANATool/lib/UnitTests/'
#fileName = 'Sample Data - Required Inputs - Leap Year.csv'
fileName = 'v20_MA_Composite_Emissions_select_has_all_speeds_1_link_leap_year.csv'
#fileName = 'Sample Data - Required Inputs - One Month Two Links.csv'
#fileName = 'Sample Data - Required Inputs - Missing Speeds - 1 month.csv'
#fileName = 'Sample Data - Required Inputs - Missing Volumes - 1 month.csv'
#fileName = 'Sample Data - 101+05209 No average worst hour.csv'
#fileName = 'test_csv_20220623.csv'
#fileName = 'Sample Data - Required Inputs - Missing Speeds.csv'
#fileName = 'Sample Data - 111+04600 - Fails on DATE 24 hour metrics.csv'

df = pd.read_csv(filePath + fileName)

linkResults = TNMAide(df, 6, 50.0, 0.0, do_two_lanes = False, robust_speeds = False)


print("SUMMARY TABLES")
print("-------------------------------------")
print("df_day_WORST_HOUR_DATE = ")
print(linkResults.df_day_WORST_HOUR_DATE)
print(" ")

print("df_day_AVG_DAY = ")
print(linkResults.df_day_AVG_DAY)
print(" ")


print("TRAFFIC VOLUMES")
print("-------------------------------------")
print("AADT = " + str(linkResults.AADT))
print("Auto_Overall_Fraction = " + str(round(linkResults.Auto_Overall_Fraction,3)))
print("MT_Overall_Fraction = " + str(round(linkResults.MT_Overall_Fraction,3)))
print("HT_Overall_Fraction = " + str(round(linkResults.HT_Overall_Fraction,3)))
print("BUS_Overall_Fraction = " + str(round(linkResults.BUS_Overall_Fraction,3)))
print("MC_Overall_Fraction = " + str(round(linkResults.MC_Overall_Fraction,3)))
Total_Fraction = linkResults.Auto_Overall_Fraction + linkResults.MT_Overall_Fraction + \
    linkResults.HT_Overall_Fraction + linkResults.BUS_Overall_Fraction + \
    linkResults.MC_Overall_Fraction
print("Total of Overall Fractions = " + str(round(Total_Fraction,3)))
print(" ")


print("WORST HOUR")
print("-------------------------------------")
print("WORST_HOUR_DATE = " + str(linkResults.WORST_HOUR_DATE))
print("WORST_HOUR = " + str(round(linkResults.WORST_HOUR,2)))
print("LAeq_WORST_HOUR = " + str(round(linkResults.LAeq_WORST_HOUR,2)))
print("LAeq_24hrs_WORST_HOUR_DATE = " + str(round(linkResults.LAeq_24hrs_WORST_HOUR_DATE,2)))
print("Ldn_WORST_HOUR_DATE = " + str(round(linkResults.Ldn_WORST_HOUR_DATE,2)))
print("Lden_WORST_HOUR_DATE = " + str(round(linkResults.Lden_WORST_HOUR_DATE,2)))
print(" ")


print("WORST HOUR AVG DAY")
print("-------------------------------------")
print("WORST_HOUR_AVG = " + str(round(linkResults.WORST_HOUR_AVG,2)))
print("LAeq_WORST_HOUR_AVG = " + str(round(linkResults.LAeq_WORST_HOUR_AVG,2)))
print("LAeq_24hrs_AVG_DAY = " + str(round(linkResults.LAeq_24hrs_AVG_DAY,2)))
print("Ldn_AVG_DAY = " + str(round(linkResults.Ldn_AVG_DAY,2)))
print("Lden_AVG_DAY = " + str(round(linkResults.Lden_AVG_DAY,2)))
print(" ")

print("PERCENT MISSING SPEED DATA")
print("-------------------------------------")
print("AUTOS: " + str(linkResults.percent_missing_auto_speeds) + " %")
print("MTS: " + str(linkResults.percent_missing_mt_speeds) + " %")
print("HTS: " + str(linkResults.percent_missing_ht_speeds) + " %")
print("BUSES: " + str(linkResults.percent_missing_bus_speeds) + " %")
print("MCS: " + str(linkResults.percent_missing_mc_speeds) + " %")


t2 = datetime.datetime.now()    
tdiff = t2-t1
print("Time Elapsed: " + str(round(tdiff.total_seconds(),2)) + " seconds")   

# linkResults.Compute_Future_Metrics()
# df1 =  copy.deepcopy(linkResults)

# linkResults.Compute_Future_Metrics(Auto_Fractions = [0.50, 0.15], \
#                                                     MT_Fractions = [0.25, 0.10], \
#                                                     HT_Fractions = [0.00, 0.00], \
#                                                     BUS_Fractions = [0.0, 0.0], \
#                                                     MC_Fractions = [0.0, 0.0])
# df2 = copy.deepcopy(linkResults)

# linkResults.Compute_Future_Metrics(Auto_Fractions = [0.50, 0.05, 0.10], \
#                                                     MT_Fractions = [0.05, 0.02, 0.03], \
#                                                     HT_Fractions = [0.10, 0.05, 0.10], \
#                                                     BUS_Fractions = [0.0, 0.0, 0.0], \
#                                                     MC_Fractions = [0.0, 0.0, 0.0])
# df3 = copy.deepcopy(linkResults)