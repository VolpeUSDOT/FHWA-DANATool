# -*- coding: utf-8 -*-
"""
Created By: Volpe National Transportation Systems Center

"""

from lib import NTD_00_TMAS
from lib import NTD_01_NPMRDS
from lib import NTD_02_MOVES
from lib import NTD_03_SPEED
from lib import NTD_04_NOISE
import os
import sys
import datetime as dt

# Basic Input Parameters
step = 0

testOption = 1


if testOption == 1:
    tmas_year = 2015
    npmrds_year = 2018
    state  = 'MA'
    county = 'Middlesex'
elif testOption == 2:
    tmas_year = 2019
    npmrds_year = 2017
    state  = 'DC'
    county = 'District'
elif testOption == 3:
    tmas_year = 2019
    npmrds_year = 2019
    state  = 'VA'
    county = 'Alexandria'
elif testOption == 4:
    tmas_year = 2019
    npmrds_year = 2018
    state = 'OR'
    county = 'Marion'
elif testOption == 5:
    tmas_year = 2019
    npmrds_year = 2021
    state = 'LA'
    county = 'LaSalle'
elif testOption == 6:
    tmas_year = 2019
    npmrds_year = 2021
    state = 'VA'
    county = '3Counties'
elif testOption == 7:
    tmas_year = 2019
    npmrds_year = 2021
    state = 'VA'
    county = 'FairfaxCity'    
elif testOption == 8:
    tmas_year = 2019
    npmrds_year = 2019
    state = 'CA'
    county = 'LosAngeles'    
elif testOption == 9:
    tmas_year = 2019
    npmrds_year = 2019
    state = 'IL'
    county = 'State'
elif testOption == 10:
    tmas_year = 2019
    npmrds_year = 2019
    state = 'MD'
    county = 'State'
    
elif testOption == 11:
    tmas_year = 2021
    npmrds_year = 2019
    state = 'RI'
    county = 'Providence'
    
elif testOption == 12:
    tmas_year = 2019
    npmrds_year = 2019
    state = 'OK'
    county = 'Oklahoma'
    
elif testOption == 13:
    tmas_year = 2020
    npmrds_year = 2020
    state = 'VA'
    county = 'Richmond'
    
elif testOption == 14:
    tmas_year = 2019
    npmrds_year = 2020
    state = 'NJ'
    county = 'Morris'
elif testOption == 15:
    tmas_year = 2020
    npmrds_year = 2020
    state = 'MA'
    county = '1TMC'
    dateStart = dt.date(2020, 3, 20)
    dateEnd = dt.date(2020, 3, 22)

SELECT_TMC = ['129+04374', '129P09003']
SELECT_STATE = state

if not sys.warnoptions: # Write "False and" after "if" to display warnings during runtime
    import warnings
    warnings.simplefilter("ignore") 

# Setup Path Prefixes Based on Users Computer
computerName = os.environ['ComputerName']
if computerName == 'VOLSLBOS-43873':
    pathPrefix1 = 'D:/Repos/FHWA-DANATool/Default Input Files'
    pathPrefix2 = 'D:/Repos/FHWA-DANATool/User Input Files/Example_MiddlesexCounty_Massachusetts/2018 NPMRDS Data'
    pathPrefix3 = 'D:/Repos/FHWA-DANATool/NPMRDS_Intermediate_Output'
elif computerName == 'OFFICEDESKTOP':
    pathPrefix1 = 'G:/Repos/FHWA-DANATool/Default Input Files'
    pathPrefix2 = 'G:/Repos/FHWA-DANATool/User Input Files/Example_MiddlesexCounty_Massachusetts/2018 NPMRDS Data'
    pathPrefix3 = 'G:/Repos/FHWA-DANATool/NPMRDS_Intermediate_Output'
elif computerName in ('VOLSLBOS-06756', 'TSCPDBOS-05790'):
    pathPrefix1 = 'C:/Users/William.Chupp/OneDrive - DOT OST/Documents/DANAToolTesting/FHWA-DANATool/Default Input Files'
    pathPrefix2 = 'H:/TestData/{}_{}'.format(county, state)
    pathPrefix3 = 'C:/Users/William.Chupp/OneDrive - DOT OST/Documents/DANAToolTesting/FHWA-DANATool/Final Output'


# Set File Paths for Calling DANA Scripts
PATH_TMAS_STATION = pathPrefix1 + '/TMAS Data/TMAS {}/tmas_station_{}.csv'.format(tmas_year, tmas_year)
PATH_TMAS_CLASS_CLEAN = pathPrefix1 + '/TMAS Data/TMAS {}/tmas_class_clean_{}.csv'.format(tmas_year, tmas_year)
PATH_FIPS = pathPrefix1 + '/FIPS_County_Codes.csv'
PATH_NEI = pathPrefix1 + '/NEI2017_RepresentativeCounties.csv'

PATH_npmrds_raw_all = pathPrefix2 + '/NPMRDS Data/{}_{}_{}_ALL.csv'.format(state.upper(), county.upper(), npmrds_year)
PATH_npmrds_raw_pass = pathPrefix2 + '/NPMRDS Data/{}_{}_{}_PASSENGER.csv'.format(state.upper(), county.upper(),npmrds_year)
PATH_npmrds_raw_truck = pathPrefix2 + '/NPMRDS Data/{}_{}_{}_TRUCKS.csv'.format(state.upper(), county.upper(),npmrds_year)
PATH_tmc_identification = pathPrefix2 + '/NPMRDS Data/TMC_Identification.csv'

PATH_tmc_shp = 'lib/ShapeFiles/'
PATH_emission = pathPrefix1 + '/NEI2017_RepresentativeEmissionsRates.parquet'

PATH_HPMS  = pathPrefix2 + '/HPMS Data/{}_HPMS_{}.csv'.format(state.upper(), npmrds_year-2) # Need to confirm - ALH
PATH_VM2 = pathPrefix1 + '/Statewide Functional Class VMT/State_VMT_by_Class_{}.csv'.format(tmas_year) # Need to confirm - ALH
PATH_COUNTY_MILEAGE = pathPrefix1 + '/HPMS County Road Mileage/County_Road_Mileage_{}.csv'.format(tmas_year) # Need to confirm - ALH

PATH_NPMRDS = pathPrefix3 + '/Process1_LinkLevelDataset/{}_Composite_Emissions.parquet'.format(state) # Need to confirm - ALH

if __name__ == '__main__':
    if step == 0:
        NTD_00_TMAS.TMAS(SELECT_STATE, PATH_TMAS_STATION, 
                         r'H:\DANATool\TMAS 2021\TMAS_Class_2021_1.dat', PATH_FIPS, PATH_NEI, PREREADSTATION = True)
    if step == 1:
        NTD_01_NPMRDS.NPMRDS(SELECT_STATE, PATH_tmc_identification, PATH_npmrds_raw_all, PATH_npmrds_raw_pass, PATH_npmrds_raw_truck, PATH_emission, PATH_TMAS_STATION, PATH_TMAS_CLASS_CLEAN, PATH_FIPS, PATH_NEI, AUTO_DETECT_DATES=True)
    elif step == 2:
        #NTD_02_MOVES.MOVES(SELECT_STATE, PATH_NPMRDS, PATH_HPMS, PATH_VM2, PATH_COUNTY_MILEAGE)
        pass
    elif step == 3:
        NTD_03_SPEED.SPEED(SELECT_STATE, PATH_NPMRDS)  
    elif step == 4:
        NTD_04_NOISE.NOISE(SELECT_STATE, SELECT_TMC, PATH_NPMRDS)

    
