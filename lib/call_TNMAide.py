# -*- coding: utf-8 -*-
"""
Created By: Volpe National Transportation Systems Center

@author: Aaron.Hastings
"""

from TNMAide import TNMAide
import pandas as pd
import datetime

t1 = datetime.datetime.now()

filePath = 'D:/Project/DANA/Data/TNMAide Input Samples/'
fileName = 'Sample Data - Required Inputs - Non-Leap Year.csv'
fileName = 'Sample Data - Required Inputs - Leap Year.csv'
fileName = 'Sample Data - Required Inputs - One Month Two Links.csv'

df = pd.read_csv(filePath + fileName)

linkResults = TNMAide(df, 6, 50.0, 0.0)


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
print("Auto_Fraction = " + str(round(linkResults.Auto_Fraction,3)))
print("MT_Fraction = " + str(round(linkResults.MT_Fraction,3)))
print("HT_Fraction = " + str(round(linkResults.HT_Fraction,3)))
print("BUS_Fraction = " + str(round(linkResults.BUS_Fraction,3)))
print("MC_Fraction = " + str(round(linkResults.MC_Fraction,3)))
Total_Fraction = linkResults.Auto_Fraction + linkResults.MT_Fraction + \
    linkResults.HT_Fraction + linkResults.BUS_Fraction + linkResults.MC_Fraction
print("Total Fractions = " + str(round(Total_Fraction,3)))
print(" ")


print("WORST HOUR")
print("-------------------------------------")
print("WORST_HOUR_DATE = " + linkResults.WORST_HOUR_DATE)
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


t2 = datetime.datetime.now()    
tdiff = t2-t1
print("Time Elapsed: " + str(round(tdiff.total_seconds(),2)) + " seconds")   


