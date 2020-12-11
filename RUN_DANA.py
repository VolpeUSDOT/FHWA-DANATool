# -*- coding: utf-8 -*-
"""
Created on Tue Oct 20 10:34:05 2020

@author: wchupp
"""

from lib import NTD_01_NPMRDS
from lib import NTD_02_MOVES
from lib import NTD_03_SPEED
from lib import NTD_04_NOISE

state  = 'MA'
step = 2
county = 'Middlesex'
SELECT_TMC = ['129+04374', '129P09003']
year = 2017


SELECT_STATE = state

PATH_TMAS_STATION = 'C:/DANAToolV3/Default Input Data/TMAS Data/TMAS {}/tmas_station_{}.csv'.format(year, year)
PATH_TMAS_CLASS_CLEAN = 'C:/DANAToolV3/Default Input Data/TMAS Data/TMAS {}/tmas_class_clean_{}.csv'.format(year, year)
PATH_FIPS = 'C:/DANAToolV3/Default Input Data/FIPS_County_Codes.csv'
PATH_NEI = 'C:/DANAToolV3/Default Input Data/NEI_Representative_Counties.csv'

PATH_npmrds_raw_all = 'C:/DANAToolV3/User Input Data/Example_MiddlesexCounty_Massachusetts/2018 NPMRDS Data/{}_{}_2018_ALL.csv'.format(state, county, state, county.upper())
PATH_npmrds_raw_pass = 'C:/DANAToolV3/User Input Data/Example_MiddlesexCounty_Massachusetts/2018 NPMRDS Data/{}_{}_2018_PASSENGER.csv'.format(state, county, state, county.upper())
PATH_npmrds_raw_truck = 'C:/DANAToolV3/User Input Data/Example_MiddlesexCounty_Massachusetts/2018 NPMRDS Data/{}_{}_2018_TRUCKS.csv'.format(state, county, state, county.upper())
PATH_tmc_identification = 'C:/DANAToolV3/User Input Data/Example_MiddlesexCounty_Massachusetts/2018 NPMRDS Data/TMC_Identification.csv'.format(state, county)
PATH_tmc_shp = 'C:/DANAToolV3/Default Input Data/National TMC Shapefile/NationalMerge.shp'
PATH_emission = 'C:/DANAToolV3/Default Input Data/NEI_National_Emissions_Rates_Basis.csv'

PATH_HPMS  = 'C:/DANAToolV2/Data Input/HPMS/{}_HPMS_2017.csv'.format(state)
PATH_VM2 = 'C:/DANAToolV2/Data Input/HPMS/VM2_2017.csv' 
PATH_COUNTY_MILEAGE = 'C:/DANAToolV2/Data Input/HPMS/National_2017_11_AdHocSQL.csv'

PATH_NPMRDS = 'C:/DANAToolV2/Step2_Outputs/{}/{}/{}_Composite_Emissions.parquet'.format(state, county, state)



if step == 1:
    NTD_01_NPMRDS.NPMRDS(SELECT_STATE, PATH_tmc_identification, PATH_tmc_shp, PATH_npmrds_raw_all, PATH_npmrds_raw_pass, PATH_npmrds_raw_truck, PATH_emission, PATH_TMAS_STATION, PATH_TMAS_CLASS_CLEAN, PATH_FIPS, PATH_NEI)
elif step==2:
    NTD_02_MOVES_MOVES.MOVES(SELECT_STATE, PATH_TMAS_CLASS_CLEAN, PATH_HPMS, PATH_VM2, PATH_COUNTY_MILEAGE)
elif step==3:
    NTD_03_SPEED.SPEED(SELECT_STATE, PATH_NPMRDS)  
elif step==4:
    NTD_04_NOISE.NOISE(SELECT_STATE, SELECT_TMC, PATH_NPMRDS)

    
