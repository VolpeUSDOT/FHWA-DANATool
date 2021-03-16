# -*- coding: utf-8 -*-
"""
Created By: Volpe National Transportation Systems Center

"""

from lib import NTD_01_NPMRDS
from lib import NTD_02_MOVES
from lib import NTD_03_SPEED
from lib import NTD_04_NOISE
import os
import sys

# Basic Input Parameters
step = 1

testOption = 5
if testOption == 1:
    tmas_year = 2018
    npmrds_year = 2018
    state  = 'MA'
    county = 'Middlesex'
elif testOption == 2:
    tmas_year = 2017
    npmrds_year = 2017
    state  = 'DC'
    county = 'District'
elif testOption == 3:
    tmas_year = 2016
    npmrds_year = 2018
    state  = 'VA'
    county = 'Alexandria'
elif testOption == 4:
    tmas_year = 2018
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
elif computerName == 'VOLSLBOS-06756':
    pathPrefix1 = 'C:/Users/William.Chupp/Documents/DANAToolTesting/FHWA-DANATool/Default Input Files'
    pathPrefix2 = 'C:/Users/William.Chupp/Documents/DANAToolTesting/FHWA-DANATool/User Input Files/{}_{}'.format(county, state)
    pathPrefix3 = 'C:/Users/William.Chupp/Documents/DANAToolTesting/FHWA-DANATool/NPMRDS_Intermediate_Output'


# Set File Paths for Calling DANA Scripts
PATH_TMAS_STATION = pathPrefix1 + '/TMAS Data/TMAS {}/tmas_station_{}.csv'.format(tmas_year, tmas_year)
PATH_TMAS_CLASS_CLEAN = pathPrefix1 + '/TMAS Data/TMAS {}/tmas_class_clean_{}.csv'.format(tmas_year, tmas_year)
PATH_FIPS = pathPrefix1 + '/FIPS_County_Codes.csv'
PATH_NEI = pathPrefix1 + '/NEI_Representative_Counties.csv'

PATH_npmrds_raw_all = pathPrefix2 + '/NPMRDS Data/{}_{}_{}_ALL.csv'.format(state.upper(), county.upper(), npmrds_year)
PATH_npmrds_raw_pass = pathPrefix2 + '/NPMRDS Data/{}_{}_{}_PASSENGER.csv'.format(state.upper(), county.upper(),npmrds_year)
PATH_npmrds_raw_truck = pathPrefix2 + '/NPMRDS Data/{}_{}_{}_TRUCKS.csv'.format(state.upper(), county.upper(),npmrds_year)
PATH_tmc_identification = pathPrefix2 + '/NPMRDS Data/TMC_Identification.csv'

PATH_tmc_shp = 'lib/ShapeFiles/'
PATH_emission = pathPrefix1 + '/NEI_National_Emissions_Rates_Basis.csv'

PATH_HPMS  = pathPrefix2 + '/HPMS Data/{}_HPMS_{}.csv'.format(state.upper(), tmas_year) # Need to confirm - ALH
PATH_VM2 = pathPrefix2 + '/HPMS Data/VM2_{}.csv'.format(tmas_year) # Need to confirm - ALH
PATH_COUNTY_MILEAGE = pathPrefix1 + '/HPMS County Road Mileage/County_Road_Mileage_{}.csv'.format(tmas_year) # Need to confirm - ALH

PATH_NPMRDS = pathPrefix3 + '/{}_Composite_Emissions.parquet'.format(state) # Need to confirm - ALH



if step == 1:
    NTD_01_NPMRDS.NPMRDS(SELECT_STATE, PATH_tmc_identification, PATH_npmrds_raw_all, PATH_npmrds_raw_pass, PATH_npmrds_raw_truck, PATH_emission, PATH_TMAS_STATION, PATH_TMAS_CLASS_CLEAN, PATH_FIPS, PATH_NEI)
elif step == 2:
    NTD_02_MOVES.MOVES(SELECT_STATE, PATH_TMAS_CLASS_CLEAN, PATH_HPMS, PATH_VM2, PATH_COUNTY_MILEAGE)
elif step == 3:
    NTD_03_SPEED.SPEED(SELECT_STATE, PATH_NPMRDS)  
elif step == 4:
    NTD_04_NOISE.NOISE(SELECT_STATE, SELECT_TMC, PATH_NPMRDS)

    
