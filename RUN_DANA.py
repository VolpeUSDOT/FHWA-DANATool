# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 10:34:05 2020

@author: wchupp
"""

from lib import NTD_01_NPMRDS
from lib import NTD_02_MOVES
from lib import NTD_03_SPEED
from lib import NTD_04_NOISE

# Basic Input Parameters
step = 1
year = 2017
state  = 'MA'
county = 'Middlesex'

SELECT_TMC = ['129+04374', '129P09003']
SELECT_STATE = state 


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
elif computerName == 'TBD'
    pathPrefix1 = 'TBD'
    pathPrefix2 = 'TBD'
    pathPrefix3 = 'TBD'


# Set File Paths for Calling DANA Scripts
PATH_TMAS_STATION = pathPrefix1 + '/TMAS Data/TMAS {}/tmas_station_{}.csv'.format(year, year)
PATH_TMAS_CLASS_CLEAN = pathPrefix1 + '/TMAS Data/TMAS {}/tmas_class_clean_{}.csv'.format(year, year)
PATH_FIPS = pathPrefix1 + '/FIPS_County_Codes.csv'
PATH_NEI = pathPrefix1 + '/NEI_Representative_Counties.csv'

PATH_npmrds_raw_all = pathPrefix2 + '/{}_{}_2018_ALL.csv'.format(state.upper(), county.upper())
PATH_npmrds_raw_pass = pathPrefix2 + '/{}_{}_2018_PASSENGER.csv'.format(state.upper(), county.upper())
PATH_npmrds_raw_truck = pathPrefix2 + '/{}_{}_2018_TRUCKS.csv'.format(state.upper(), county.upper())
PATH_tmc_identification = pathPrefix2 + '/TMC_Identification.csv'

PATH_tmc_shp = pathPrefix1 + '/National TMC Shapefile/NationalMerge.shp'
PATH_emission = pathPrefix1 + '/NEI_National_Emissions_Rates_Basis.csv'

PATH_HPMS  = pathPrefix2 + '/HPMS/{}_HPMS_{}.csv'.format(state.upper(), year) # Need to confirm - ALH
PATH_VM2 = pathPrefix2 + '/HPMS/VM2_{}.csv'.format(year) # Need to confirm - ALH
PATH_COUNTY_MILEAGE = pathPrefix1 + '/HPMS/National_{}_11_AdHocSQL.csv'.format(year) # Need to confirm - ALH

PATH_NPMRDS = pathPrefix3 + '/{}/{}/{}_Composite_Emissions.parquet'.format(state, county, state) # Need to confirm - ALH



if step == 1:
    NTD_01_NPMRDS.NPMRDS(SELECT_STATE, PATH_tmc_identification, PATH_tmc_shp, PATH_npmrds_raw_all, PATH_npmrds_raw_pass, PATH_npmrds_raw_truck, PATH_emission, PATH_TMAS_STATION, PATH_TMAS_CLASS_CLEAN, PATH_FIPS, PATH_NEI)
elif step == 2:
    NTD_02_MOVES_MOVES.MOVES(SELECT_STATE, PATH_TMAS_CLASS_CLEAN, PATH_HPMS, PATH_VM2, PATH_COUNTY_MILEAGE)
elif step == 3:
    NTD_03_SPEED.SPEED(SELECT_STATE, PATH_NPMRDS)  
elif step == 4:
    NTD_04_NOISE.NOISE(SELECT_STATE, SELECT_TMC, PATH_NPMRDS)

    
