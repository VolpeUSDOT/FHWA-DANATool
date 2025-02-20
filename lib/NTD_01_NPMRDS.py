# -*- coding: utf-8 -*-
"""
Created by Cambridge Systematics
Modified By: Volpe National Transportation Systems Center

"""
import pandas as pd
import numpy as np
import datetime as dt
import pyarrow as pa
import pyarrow.parquet as pq
import time
import geopandas as gpd
import sys
from geopandas import GeoDataFrame
from shapely.geometry import Point
import pathlib
from .load_shapes import *
import pkg_resources
from tqdm.tk import tqdm
import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import psutil
import math
import io
from .TNMPyAide.TNMPyAide import TNMPyAide
from collections import namedtuple
import gc

PATH_tmc_shp = pkg_resources.resource_filename('lib', 'ShapeFilesCSV/')

def runTNMPyAide(group):
    link_grade = 0
    group['DATE'] = group['measurement_tstamp'].dt.date
        
    meta_data = namedtuple('meta_data', 'L1_name L1_tmc state county')
    meta = meta_data(group.iloc[0]['road'], group.iloc[0]['tmc'], group.iloc[0]['state'], group.iloc[0]['county'])
    
    if len(group['MAADT'].unique()) == 1 and np.isnan(group['MAADT'].unique()[0]):
        return 0
    
    else:
        
        linkResults = TNMPyAide(group, link_grade, meta, detailed_log=False)
        toReturn = [None, None, None, None, None, None, None, None]
        toReturn[0] = linkResults.worst_day.worst_hour_spl
        toReturn[1] = linkResults.worst_day.LAEQ_24_HR
        toReturn[2] = linkResults.worst_day.LDN
        toReturn[3] = linkResults.worst_day.LDEN
        toReturn[4] = linkResults.average_day.worst_hour_spl
        toReturn[5] = linkResults.average_day.LAEQ_24_HR
        toReturn[6] = linkResults.average_day.LDN
        toReturn[7] = linkResults.average_day.LDEN
        
        return toReturn

def NPMRDS_Helper(SELECT_STATE, PATH_tmc_identification, PATH_npmrds_raw_all, PATH_npmrds_raw_pass, PATH_npmrds_raw_truck, PATH_default_speeds,
           PATH_emission, PATH_TMAS_STATION, PATH_TMAS_CLASS_CLEAN, PATH_FIPS, PATH_NEI, /, 
           PATH_OUTPUT = 'Final Output', AUTO_DETECT_DATES=True, DATE_START=None, DATE_END=None):

    '''
    #SELECT_STATE='CO'
    SELECT_STATE='TX'
    PATH_tmc_identification='User Input Data Input/NPMRDS/TMC_Identification_CO.csv'
    PATH_tmc_shp='Data Input/NationalMerge/NationalMerge.shp'
    PATH_npmrds_raw_all='Data Input/NPMRDS/CO_2017_ALL.csv'
    PATH_npmrds_raw_pass='Data Input/NPMRDS/CO_2017_PASS.csv'
    PATH_npmrds_raw_truck='Data Input/NPMRDS/CO_2017_TRUCK.csv'
    PATH_emission='Data Input/Emission Rates/rates_2014v2nei_basis_20190206.csv'
    PATH_TMAS_CLASS_CLEAN=filepath+'tmas_class_clean.csv'
    PATH_TMAS_STATION_STATE=filepath+'TMAS_station_State.csv'
    PATH_FIPS='Data Input/default/fips codes.csv'
    PATH_NEI='Data Input/default/2014NEI_v2_Representative_Counties_Final.xlsx'
    '''
    '''
    SELECT_STATE='MA'
    PATH_tmc_identification='User Input Data/TMC_Identification.csv'
    PATH_tmc_shp='Data Input/NationalMerge/NationalMerge.shp'
    PATH_npmrds_raw_all='debug_2020/MA_MiddlesexCounty_2019_ALL.csv'
    PATH_npmrds_raw_pass='debug_2020/MA_MiddlesexCounty_2019_PASSENGER.csv'
    PATH_npmrds_raw_truck='debug_2020/MA_MiddlesexCounty_2019_TRUCKS.csv'
    PATH_emission='Data Input/Emission Rates/rates_2014v2nei_basis_20190206.csv'
    PATH_TMAS_CLASS_CLEAN=filepath+'tmas_class_clean.csv'
    PATH_TMAS_STATION_STATE=filepath+'TMAS_station_State.csv'
    PATH_FIPS='Data Input/default/fips codes.csv'
    PATH_NEI='Data Input/default/2014NEI_v2_Representative_Counties_Final.xlsx'
    '''
    #PATH_emission='Data Input/MOVES_Rates/nhs lpp rates_'+SELECT_STATE+'_wbt.csv'
    
    filepath = PATH_OUTPUT

    def lapTimer(text,now):
        print('%s%.3f' %(text,time.time()-now))
        return time.time()
    
    now=time.time()
    # State definition
    states = {
    'AK':['Alaska', 2],'AL':['Alabama',1],'AZ':['Arizona',4],'AR':['Arkansas',5],'CA':['California',6],'CO':['Colorado',8],
    'CT':['Connecticut',9],'DE':['Delaware',10],'DC':['District of Columbia',11],'FL':['Florida',12],'GA':['Georgia',13],'HI':['Hawaii',15],'ID':['Idaho',16],'IL':['Illinois',17],'IN':['Indiana',18],'IA':['Iowa',19],'KS':['Kansas',20],
    'KY':['Kentucky',21],'LA':['Louisiana',22],'ME':['Maine',23],'MD':['Maryland',24],'MA':['Massachusetts',25],
    'MI':['Michigan',26],'MN':['Minnesota',27],'MS':['Mississippi',28],'MO':['Missouri',29],'MT':['Montana',30],'NE':['Nebraska',31],
    'NV':['Nevada',32],'NH':['New Hampshire',33],'NJ':['New Jersey',34],'NM':['New Mexico',35],'NY':['New York',36],
    'NC':['North Carolina',37],'ND':['North Dakota',38],'OH':['Ohio',39],'OK':['Oklahoma',40],'OR':['Oregon',41],
    'PA':['Pennsylvania',42],'RI':['Rhode Island',44],'SC':['South Carolina', 45],'SD':['South Dakota',46],
    'TN':['Tennessee',47],
    'TX':['Texas', 48], 
    'UT':['Utah',49],'VT':['Vermont',50],'VA':['Virginia',51],
    'WA':['Washington',53],'WV':['West Virginia',54],'WI':['Wisconsin',55],'WY':['Wyoming',56]}
    
    ##########################################################################################
    
    # NPMRDS
    # FIPS/NEI have names and codes; TMC_Identification only has names; repcty only in code; Use code
    fips_header = ['STATE_NAME','STATE_CODE','COUNTY_CODE','COUNTY_NAME','FIPS_TYPE']
    fips = pd.read_csv(PATH_FIPS,header=None,names=fips_header)
    fips['COUNTY_ID'] = fips['STATE_CODE']*1000 + fips['COUNTY_CODE']
    repcty = pd.read_csv(PATH_NEI)
    state_county = pd.merge(fips, repcty, left_on=['STATE_CODE','COUNTY_ID'], right_on=['stateid','countyid'], how='left')
    state_county.drop(['stateid','countyid','County_Name'], inplace=True, axis=1)
    state_county.rename(columns={'State_Name':'STATE_FULL_NAME'}, inplace=True)
    state_county['COUNTY_NAME']=state_county['COUNTY_NAME'].str.replace(' County', '').str.lower()
    
    #a.	TMC Identification
    print ('Reading TMC Configuration Data')
    tmc = pd.read_csv(PATH_tmc_identification)
    #tmc=tmc.loc[~tmc['type'].str.contains('.', regex=False)]
    #a1. Clean raw TMC data (dir, AADT<100 = 100)
    tmc['route_sign'] = tmc['route_sign']-1
    tmc.loc[tmc['aadt']<=100, 'aadt'] = 100
    #a2. Clean the direction field; Drop addtional columns
    dir_dic = {'EASTBOUND':'EB', 'NORTHBOUND':'NB', 'WESTBOUND':'WB', 'SOUTHBOUND':'SB'}
    tmc['direction'].replace(dir_dic, inplace=True)                     ## 1st pass
    tmc['direction']=tmc['direction'].str.extract('(EB|NB|SB|WB)')      ## 2nd pass
    tmc['dir_num']=-1                                                    # if it is still -1, it means we don't have direction information
    tmc.loc[tmc['direction']=='NB','dir_num']=1
    tmc.loc[tmc['direction']=='EB','dir_num']=3
    tmc.loc[tmc['direction']=='SB','dir_num']=5
    tmc.loc[tmc['direction']=='WB','dir_num']=7
    tmc.drop(tmc[tmc['f_system'].isnull()].index, axis=0, inplace=True)
    # tmc.drop(tmc[tmc['direction'].isnull()].index, axis=0, inplace=True)
    tmc.drop(['intersection','zip','timezone_name','type','country','tmclinear','frc','border_set','structype','route_qual',
              'altrtename','nhs_pct','strhnt_typ','strhnt_pct','truck'], inplace=True, axis=1)
    
    #a3. Create data element for urban/rural designation to match the data element in the TMAS station data:
    print ('Creating data elements for Urban and Rural codes')
    tmc['urban_rural']=''
    tmc.loc[tmc['urban_code']<99999, 'urban_rural']='U'
    tmc.loc[tmc['urban_code']>=99999, 'urban_rural']='R'
    #a4. Add REPCTY
    tmc['state'] = tmc['state'].str.upper()
    tmc['county'] = tmc['county'].str.lower().str.replace(' county', '').str.replace(' and', '').str.replace('(', '').str.replace(')', '').str.replace('.', '').str.replace(' ', '').str.replace("'", '')
    state_county['COUNTY_NAME'] = state_county['COUNTY_NAME'].str.lower().str.replace(' county', '').str.replace('(', '').str.replace(')', '').str.replace('.', '').str.replace("'", '').str.replace(' municipality', '').str.replace(' borough', '').str.replace(' census area','').str.replace(' and', '').str.replace(' ', '')
    
    tmc_state_county = tmc.groupby(['state', 'county']).size().reset_index()[['state', 'county']]
    for index, row in tmc_state_county.iterrows():
        test_row = row.copy()
        fips_matches = state_county.loc[(state_county['STATE_NAME']==test_row['state'])&
                                        (state_county['COUNTY_NAME']==test_row['county'])]
        if len(fips_matches)==1:
            continue
        else: test_row['county'] = test_row['county']+'city'
        
        fips_matches = state_county.loc[(state_county['STATE_NAME']==test_row['state'])&
                                        (state_county['COUNTY_NAME']==test_row['county'])]
        
        if len(fips_matches)==1:
            tmc.loc[(tmc['state']==row['state'])&
                    (tmc['county']==row['county']), 'county'] = test_row['county']
            continue
        else: 
            test_row = row.copy()
            test_row['county'] = test_row['county']+'parish'
        
        fips_matches = state_county.loc[(state_county['STATE_NAME']==test_row['state'])&
                                        (state_county['COUNTY_NAME']==test_row['county'])]
        
        if len(fips_matches)==1:
            tmc.loc[(tmc['state']==row['state'])&
                    (tmc['county']==row['county']), 'county'] = test_row['county']
            continue
        else:
            er = RuntimeError("State and county combination in NPMRDS configuration file was not found in FIPS code definition. "
                              "Check the county name and try again. TMC County: {}".format(row['county']))
            raise er
            
    tmc = pd.merge(tmc, state_county, left_on=['state','county'], right_on=['STATE_NAME','COUNTY_NAME'], how='inner')
    tmc.drop(['state','county','STATE_NAME','COUNTY_NAME','FIPS_TYPE','STATE_FULL_NAME'], axis=1, inplace=True)
    tmc.rename(columns={'STATE_CODE':'state','COUNTY_CODE':'county'}, inplace=True)
    
    now=lapTimer('  took: ',now)
    
    #b. Create a template for all hours and all links

    print ('Reading NPMRDS speeds for all vehicles')
    
    if 'historical_average_speed' in pd.read_csv(PATH_npmrds_raw_all, nrows=1).columns.tolist():
        uc = ['measurement_tstamp', 'tmc_code', 'speed', 'travel_time_seconds', 'historical_average_speed', 'reference_speed']
    else: 
        uc = ['measurement_tstamp', 'tmc_code', 'speed', 'travel_time_seconds', 'average_speed', 'reference_speed']
    npmrds_raw = pd.read_csv(PATH_npmrds_raw_all, parse_dates=['measurement_tstamp'], 
                             usecols=uc,
                             dtype={'tmc_code':str, 'speed':float, 'travel_time_seconds':float})
    
    npmrds_raw.rename(columns={'historical_average_speed':'average_speed'}, inplace=True)

    print ('Creating NPMRDS Dataset Template')
    print ('  Autodetect date range: {}'.format(AUTO_DETECT_DATES))
    if AUTO_DETECT_DATES:
        startDate = npmrds_raw['measurement_tstamp'].min().date()
        endDate = npmrds_raw['measurement_tstamp'].max().date()+pd.Timedelta(days = 1)
    else:
        if DATE_START > DATE_END:
            raise ValueError("Start of date range is after end of date range.")
        if DATE_START < npmrds_raw['measurement_tstamp'].min() or \
           DATE_END > npmrds_raw['measurement_tstamp'].max():
            raise ValueError("Date range is outside minimum or maximum of raw NPMRDS input data.")
        
        startDate = DATE_START
        endDate= DATE_END+pd.Timedelta(days = 1)

    if startDate.year != (endDate-pd.Timedelta(days=1)).year:
        raise ValueError("This tool can only process data in one year. The start and end dates currently span multiple years.")
        
    date_template = pd.date_range(start=startDate, end=endDate, freq='H', inclusive='left')
    tmc_unique = tmc['tmc'].unique()
    print(len(tmc_unique))
    d = [{'tmc_code':i, 'measurement_tstamp': datetime} for i in tmc_unique for datetime in date_template]
    npmrds_template = pd.DataFrame(d)
    now=lapTimer('  took: ',now)
    
    print("Determine average and reference speed for all speed data")
    npmrds_raw['weekday'] = npmrds_raw['measurement_tstamp'].dt.dayofweek + 1
    npmrds_raw['hour'] = npmrds_raw['measurement_tstamp'].dt.hour
    npmrds_avg_all = npmrds_raw.groupby(['tmc_code', 'weekday', 'hour'])['average_speed'].mean().reset_index()
    npmrds_avg_all.rename(columns={'average_speed':'speed_all'}, inplace=True)
    npmrds_avg_all['npmrds_all_fill_method'] = 1
    npmrds_avg_all = npmrds_avg_all.loc[~npmrds_avg_all['speed_all'].isna()].reset_index()
    npmrds_ref_all = npmrds_raw.groupby(['tmc_code'])['reference_speed'].mean().reset_index()
    npmrds_ref_all.rename(columns={'reference_speed':'speed_all'}, inplace=True)
    npmrds_ref_all['npmrds_all_fill_method'] = 2
    
    
    print ('Cleaning NPMRDS data for all vehicles')
    # Only keep TMCs with available TMC definitions (inner join)
    tmc_for_cleaning = tmc.loc[:,['tmc','f_system','miles']]
    tmc_for_cleaning.rename(index=str, columns={"miles": "tmc_length"}, inplace=True)
    npmrds_raw = pd.merge(npmrds_raw, tmc_for_cleaning, left_on='tmc_code', right_on='tmc', how='inner')
    #Dropping speeds above 90 mph, OR speeds below 4 mph in freeways, OR speeds below 4 mph in arterials that are larger than 0.33 miles
    npmrds_raw.drop(npmrds_raw[npmrds_raw['speed']>90].index, axis=0, inplace=True)
    npmrds_raw.drop(npmrds_raw[(npmrds_raw['f_system']<=2) & (npmrds_raw['speed']<=4)].index, axis=0, inplace=True)
    npmrds_raw.drop(npmrds_raw[(npmrds_raw['f_system']>=3) & (npmrds_raw['tmc_length']>=0.33) & (npmrds_raw['speed']<=4)].index, axis=0, inplace=True)
    npmrds_raw.drop(['tmc','f_system'],axis=1,inplace=True)
    #c2. Aggregate 15-min NPMRDS speeds to hourly: mean speed & travel time
    print ('Calculating speeds for all vehicles')
    pd.to_datetime(npmrds_raw['measurement_tstamp'])
    npmrds_raw['travel_time_all']=npmrds_raw.loc[:,'travel_time_seconds']
    npmrds_all = npmrds_raw.groupby(['tmc_code',pd.Grouper(key='measurement_tstamp',freq='H')]).agg({'travel_time_seconds':'sum',
                                   'tmc_length':'sum', 'travel_time_all':'mean'}).reset_index()         # travel_time_all: arithmatic mean travel time
    npmrds_all['speed_all'] = npmrds_all['tmc_length']/(npmrds_all['travel_time_seconds']/3600)         # speed_all: space mean speed
    #Merge hourly speeds/travel times to npmrds template
    npmrds_all.drop(['tmc_length','travel_time_seconds'], inplace=True, axis=1)
    npmrds0 = pd.merge(npmrds_template, npmrds_all, on=['tmc_code','measurement_tstamp'], how='left')
    del npmrds_all, npmrds_raw
    now=lapTimer('  took: ',now)
    
    #Same process (c-c1-c2) for the Passenger NPMRDS dataset
    print ('Reading NPMRDS speeds for passenger vehicles')
    npmrds_raw = pd.read_csv(PATH_npmrds_raw_pass, parse_dates=['measurement_tstamp'], usecols=['measurement_tstamp', 'tmc_code', 'speed', 'travel_time_seconds'], dtype={'tmc_code':str, 'speed':float, 'travel_time_seconds':float})
    npmrds_raw = pd.merge(npmrds_raw, tmc_for_cleaning, left_on='tmc_code', right_on='tmc', how='inner')
    print ('Cleaning NPMRDS data for passenger vehicles')
    npmrds_raw.drop(npmrds_raw[npmrds_raw['speed']>90].index, axis=0, inplace=True)
    npmrds_raw.drop(npmrds_raw[(npmrds_raw['f_system']<=2) & (npmrds_raw['speed']<=4)].index, axis=0, inplace=True)
    npmrds_raw.drop(npmrds_raw[(npmrds_raw['f_system']>=3) & (npmrds_raw['tmc_length']>=0.33) & (npmrds_raw['speed']<=4)].index, axis=0, inplace=True)
    npmrds_raw.drop(['tmc','f_system'],axis=1,inplace=True)
    print ('Calculating speeds for passenger vehicles')
    pd.to_datetime(npmrds_raw['measurement_tstamp'])
    npmrds_raw['travel_time_pass']=npmrds_raw.loc[:,'travel_time_seconds']
    npmrds_pass = npmrds_raw.groupby(['tmc_code',pd.Grouper(key='measurement_tstamp',freq='H')]).agg({'travel_time_seconds':'sum','tmc_length':'sum','travel_time_pass':'mean'}).reset_index()
    npmrds_pass['speed_pass'] = npmrds_pass['tmc_length']/(npmrds_pass['travel_time_seconds']/3600)
    npmrds_pass.drop(['tmc_length','travel_time_seconds'], inplace=True, axis=1)
    print('Merging passenger speeds to all speeds')
    npmrds0 = pd.merge(npmrds0, npmrds_pass, on=['tmc_code','measurement_tstamp'], how='left')
    del npmrds_pass, npmrds_raw
    now=lapTimer('  took: ',now)
    
    #Same process (c-c1-c2) for the Truck NPMRDS dataset
    print ('Reading NPMRDS speeds for truck vehicles')
    npmrds_raw = pd.read_csv(PATH_npmrds_raw_truck, parse_dates=['measurement_tstamp'], usecols=['measurement_tstamp', 'tmc_code', 'speed', 'travel_time_seconds'], dtype={'tmc_code':str, 'speed':float, 'travel_time_seconds':float})
    npmrds_raw = pd.merge(npmrds_raw, tmc_for_cleaning, left_on='tmc_code', right_on='tmc', how='inner')
    print ('Cleaning NPMRDS data for truck vehicles')
    npmrds_raw.drop(npmrds_raw[npmrds_raw['speed']>90].index, axis=0, inplace=True)
    npmrds_raw.drop(npmrds_raw[(npmrds_raw['f_system']<=2) & (npmrds_raw['speed']<=4)].index, axis=0, inplace=True)
    npmrds_raw.drop(npmrds_raw[(npmrds_raw['f_system']>=3) & (npmrds_raw['tmc_length']>=0.33) & (npmrds_raw['speed']<=4)].index, axis=0, inplace=True)
    npmrds_raw.drop(['tmc','f_system'],axis=1,inplace=True)
    print ('Calculating speeds for truck vehicles')
    pd.to_datetime(npmrds_raw['measurement_tstamp'])
    npmrds_raw['travel_time_truck']=npmrds_raw.loc[:,'travel_time_seconds']
    npmrds_truck = npmrds_raw.groupby(['tmc_code',pd.Grouper(key='measurement_tstamp',freq='H')]).agg({'travel_time_seconds':'sum','tmc_length':'sum','travel_time_truck':'mean'}).reset_index()
    npmrds_truck['speed_truck'] = npmrds_truck['tmc_length']/(npmrds_truck['travel_time_seconds']/3600)
    npmrds_truck.drop(['tmc_length','travel_time_seconds'], inplace=True, axis=1)
    print('Merging truck speeds to all speeds')
    npmrds = pd.merge(npmrds0, npmrds_truck, on=['tmc_code','measurement_tstamp'], how='left')
    del npmrds_truck, npmrds_raw, npmrds0
    now=lapTimer('  took: ',now)
    
    #c3. Create data elements for year, month, day of week, hour
    print ('Creating data elements for year, month, day of week, hour')
    pd.to_datetime(npmrds['measurement_tstamp'])
    npmrds['year']=npmrds['measurement_tstamp'].dt.year
    npmrds['month']=npmrds['measurement_tstamp'].dt.month
    npmrds['day']=npmrds['measurement_tstamp'].dt.day
    npmrds['hour']=npmrds['measurement_tstamp'].dt.hour
    npmrds['weekday']=npmrds['measurement_tstamp'].dt.dayofweek +1
    #c4. Create data element for Weekday vs. Weekend
    print ('Creating data elements for Weekday vs. Weekend')
    npmrds['dow']=''
    npmrds.loc[npmrds['weekday']>5, 'dow']='WE'
    npmrds.loc[npmrds['weekday']<=5, 'dow']='WD'
    #c5 Determine peaking based on average annual speeds of all vehicles for hours 8 and 17
    
    if 'WD' in npmrds['dow'].unique():
        npmrds_peakAM = npmrds.loc[(npmrds['dow']=='WD') & (npmrds['hour'].isin([7,8]))].groupby(['tmc_code'], as_index=False)['speed_all'].mean()
        npmrds_peakPM = npmrds.loc[(npmrds['dow']=='WD') & (npmrds['hour'].isin([16,17]))].groupby(['tmc_code'], as_index=False)['speed_all'].mean()
    else:
        npmrds_peakAM = npmrds.loc[(npmrds['hour'].isin([7,8]))].groupby(['tmc_code'], as_index=False)['speed_all'].mean()
        npmrds_peakPM = npmrds.loc[(npmrds['hour'].isin([16,17]))].groupby(['tmc_code'], as_index=False)['speed_all'].mean()
    
    npmrds_peakAM.rename(columns={'speed_all': 'speed_all_AM'}, inplace=True)
    npmrds_peakPM.rename(columns={'speed_all': 'speed_all_PM'}, inplace=True)
    npmrds_peak=pd.merge(npmrds_peakAM, npmrds_peakPM, on=['tmc_code'], how='outer')
    npmrds_peak.loc[npmrds_peak['speed_all_AM']>npmrds_peak['speed_all_PM'],'peaking']='AM'
    npmrds_peak.loc[npmrds_peak['peaking']!='AM','peaking']='PM'
    npmrds_peak.drop(['speed_all_AM', 'speed_all_PM'], inplace=True, axis=1)
    npmrds=pd.merge(npmrds, npmrds_peak, on=['tmc_code'], how='left')
    # default is 'PM' in case no peaking assignment with WD & HOUR 8
    npmrds.loc[~npmrds['peaking'].isin(['AM','PM']), 'peaking']='PM' 
    #npmrds['peaking'].value_counts()
    now=lapTimer('  took: ',now)
    
    #d.	Merge NPMRDS data to TMCs
    print ('Merging speeds to TMC')
    npmrds_tmc = pd.merge(npmrds, tmc, left_on='tmc_code', right_on='tmc', how='left')
    #npmrds_tmc = npmrds_tmc.dropna(axis=0, subset=['route_sign'])
    npmrds_tmc.reset_index(inplace=True)
    del npmrds
    now=lapTimer('  took: ',now)
    
    print ('Filling missing NPMRDS speeds')
    npmrds_tmc.insert(8, 'npmrds_all_fill_method', np.nan)
    npmrds_tmc.insert(9, 'npmrds_pass_fill_method', np.nan)
    npmrds_tmc.insert(10, 'npmrds_truck_fill_method', np.nan)
    npmrds_tmc.loc[~npmrds_tmc['speed_all'].isna(), 'npmrds_all_fill_method'] = 0
    npmrds_tmc.loc[~npmrds_tmc['speed_pass'].isna(), 'npmrds_pass_fill_method'] = 0
    npmrds_tmc.loc[~npmrds_tmc['speed_truck'].isna(), 'npmrds_truck_fill_method'] = 0
    
    npmrds_tmc = npmrds_tmc.fillna(npmrds_tmc[['tmc_code', 'weekday', 'hour']].merge(npmrds_avg_all, on=['tmc_code', 'weekday', 'hour'], how='left'))
    npmrds_tmc = npmrds_tmc.fillna(npmrds_tmc[['tmc_code']].merge(npmrds_ref_all, on=['tmc_code'], how='left'))
    
    # This fills in missing speed information when there is no speed data at all
    # We can't calculate this until the tmc config is merged to the speed data, 
    # so this is done here instead of when the other averages are calculated
    npmrds_fclass_avg_speeds = npmrds_tmc.groupby(['urban_rural','f_system', 'weekday', 'hour'])['speed_all'].mean().reset_index()
    npmrds_fclass_avg_speeds['npmrds_all_fill_method'] = 3
    npmrds_tmc = npmrds_tmc.fillna(npmrds_tmc[['urban_rural', 'f_system', 'weekday', 'hour']].merge(npmrds_fclass_avg_speeds, on=['urban_rural', 'f_system', 'weekday', 'hour'], how='left'))    
    npmrds_fclass_avg_speeds.to_csv(filepath+'npmrds_average_speed_values.csv',index=False)
    
    #Read default speed file
    npmrds_default_speeds = pd.read_csv(PATH_default_speeds)
    npmrds_default_speeds['speed_all'] = npmrds_default_speeds['default_speed']
    npmrds_default_speeds['npmrds_all_fill_method'] = 4
    npmrds_tmc = npmrds_tmc.fillna(npmrds_tmc[['urban_rural', 'f_system']].merge(npmrds_default_speeds, on=['urban_rural', 'f_system'], how='left'))    
    
    if len(npmrds_tmc.loc[npmrds_tmc['speed_all'].isna()]) > 0:
           raise ValueError("The NPMRDS filling method did not work")
    
    npmrds_tmc.loc[npmrds_tmc['speed_pass'].isna(), 'npmrds_pass_fill_method'] = 1
    npmrds_tmc['speed_pass'] = npmrds_tmc['speed_pass'].fillna(npmrds_tmc['speed_all'])
    
    npmrds_tmc.loc[npmrds_tmc['speed_truck'].isna(), 'npmrds_truck_fill_method'] = 1
    npmrds_tmc['speed_truck'] = npmrds_tmc['speed_truck'].fillna(npmrds_tmc['speed_all'])
    
    npmrds_tmc['travel_time_all'] = npmrds_tmc['travel_time_all'].fillna(3600*npmrds_tmc['miles']/npmrds_tmc['speed_all'])
    npmrds_tmc['travel_time_pass'] = npmrds_tmc['travel_time_pass'].fillna(3600*npmrds_tmc['miles']/npmrds_tmc['speed_pass'])
    npmrds_tmc['travel_time_truck'] = npmrds_tmc['travel_time_truck'].fillna(3600*npmrds_tmc['miles']/npmrds_tmc['speed_truck'])
    
    npmrds_tmc.insert(9, 'npmrds_fill_method', None)

    npmrds_tmc['npmrds_fill_method'] = [[a, p, t] for a, p, t in zip(npmrds_tmc['npmrds_all_fill_method'],
                                                                     npmrds_tmc['npmrds_pass_fill_method'],
                                                                     npmrds_tmc['npmrds_truck_fill_method'])]

    npmrds_tmc.drop(['npmrds_all_fill_method', 'npmrds_pass_fill_method', 'npmrds_truck_fill_method'], inplace=True, axis=1)
    
    now=lapTimer('  took: ',now)
    
    
    tmc['tier'] = np.nan
    crs = {'init' :'epsg:4326'}
    #b1. Preparing TMC data for geoprocessing
    #Read the TMC shapefile
    print('Geoprocessing TMC data')
    shp = load_shape_csv(PATH_tmc_shp, crs)
    #b2. Merge the TMC shapefile with the tmc identification dataset
    tmc_new = pd.merge(tmc, shp, left_on='tmc', right_on='Tmc', how='inner')
    tmc_new.drop('Tmc', axis=1,inplace=True)
    #b3. Create a new feature as shapely LineString from the start and end points
    geo_tmc = GeoDataFrame(tmc_new, crs=crs, geometry='geometry')
    now=lapTimer('  took: ',now)
    
    '''
    print ('Exporting output')
    table = pa.Table.from_pandas(npmrds_tmc)
    pq.write_table(table, NPMRDS_parquet)
    '''
    ##########################################################################################
    # TIERS: TMC vs. TMAS station
    
    #a. Read TMAS Station and Classification
    print('Reading TMAS Data')    #This takes like 10 min
    tmas_class = dd.read_csv(PATH_TMAS_CLASS_CLEAN, dtype={'STATION_ID':str, 'ROUTE_NUMBER':str})
    now=lapTimer('  took: ',now)
    tmas_class_state = tmas_class[tmas_class['STATE_NAME']==SELECT_STATE]
    # with ProgressBar():
    tmas_class_state = tmas_class_state.compute()
    states_avlb=tmas_class_state['STATE_NAME'].drop_duplicates()
    if SELECT_STATE in states_avlb.values:
        
        #tmas_class_state['ROUTE_NUMBER'] = pd.to_numeric(tmas_class_state['ROUTE_NUMBER'], errors='coerce')
        tmas_class_state['ROUTE_NUMBER'].dropna(axis=0, inplace=True)
    
        #a1. Load national station file, 
        #    Select only desired State, 
        #    Select only data in the clean station array
        clean_stations = tmas_class_state['STATION_ID'].unique()
        tmas_station = pd.read_csv(PATH_TMAS_STATION, dtype={'STATION_ID':str})
        tmas_station_State = tmas_station[tmas_station['STATE_NAME']==SELECT_STATE]
        tmas_station_State.reset_index(inplace=True, drop=True)
        tmas_station_clean = tmas_station_State.loc[tmas_station_State['STATION_ID'].isin(clean_stations)]
        
        #a2. Preparing TMAS station data for geoprocessing
        #Create a new attribute as points from lat and long
        print('Geoprocessing TMAS station data')
        tmas_station_clean['geometry']=tmas_station_clean.apply(lambda row: Point(row["LONG"], row["LAT"]), axis=1)
        tmas_station_clean.reset_index(drop=True, inplace=True)    #Start the dataframe index from 0
        #Create a geodataframe to test geopandas capabilities
        geo_tmas = GeoDataFrame(tmas_station_clean.copy(), crs=crs, geometry='geometry')
        del tmas_station_clean
        now=lapTimer('  took: ',now)
        #b Read TMC Indentification data
        '''
        print('Reading TMC Identification data')
        tmc = pd.read_csv(tmc_identification)
        dir_dic = {'EASTBOUND':'EB', 'NORTHBOUND':'NB', 'WESTBOUND':'WB', 'SOUTHBOUND':'SB'}
        tmc['direction'].replace(dir_dic, inplace=True)
        tmc['direction']=tmc['direction'].str.extract('(EB|NB|SB|WB)')
        tmc['dir_num']=np.nan
        tmc['dir_num'].loc[tmc['direction']=='NB']=1
        tmc['dir_num'].loc[tmc['direction']=='EB']=3
        tmc['dir_num'].loc[tmc['direction']=='SB']=5
        tmc['dir_num'].loc[tmc['direction']=='WB']=7
        '''
        
        ##################################################
        #c. Tier 1: space join
        #c1. Merging TMC Link and TMAS Station data using a 0.2-mile buffer
        print('Merging TMAS and TMC data')
        geo_tmas['geometry'] = geo_tmas['geometry'].buffer(0.001)
        intersect = gpd.sjoin(geo_tmc, geo_tmas, op='intersects')
        #c2. Selecting only the data that matches on direction
        intersect_dir = intersect[intersect['dir_num']==intersect['DIR']]
        #c3. Assigning interected data as tier 1
        tier1 = intersect_dir.loc[:,['tmc','tier','STATION_ID', 'DIR']]
        tier1['tier']=1
        now=lapTimer('  took: ',now)
        
        if len(tier1) == 0:
            tier1_class = pd.DataFrame(columns=['STATION_ID','tmc','DIR','MONTH','DAY_TYPE','HOUR', 
                                                'HPMS_TYPE10','HPMS_TYPE25','HPMS_TYPE40','HPMS_TYPE50','HPMS_TYPE60', 
                                                'NOISE_AUTO','NOISE_MED_TRUCK','NOISE_HVY_TRUCK','NOISE_BUS','NOISE_MC'])
            tier1_volume = pd.DataFrame(columns=['STATION_ID', 'tmc','DIR',	'MONTH', 'DAY_TYPE', 'VOL', 'VOL_MEAN', 'VOLUME_MODIFIER'])
        else:
            #c4. Creating Tier 1 Classifications
            print('Defining Tier 1 Classifications')
            tmas_class_state_tier1 = pd.merge(tmas_class_state, tier1, on=['STATION_ID','DIR'])
            tmas_class_state_tier1_clean = tmas_class_state_tier1[tmas_class_state_tier1['tier']==1]
            # 'tmc' and 'STATION_ID'+'DIR' can be many-to-many
            
            if len(tmas_class_state_tier1_clean) == 0:
                tier1_class = pd.DataFrame(columns=['STATION_ID','tmc','DIR','MONTH','DAY_TYPE','HOUR', 
                                                    'HPMS_TYPE10','HPMS_TYPE25','HPMS_TYPE40','HPMS_TYPE50','HPMS_TYPE60', 
                                                    'NOISE_AUTO','NOISE_MED_TRUCK','NOISE_HVY_TRUCK','NOISE_BUS','NOISE_MC'])
                tier1_volume = pd.DataFrame(columns=['STATION_ID', 'tmc','DIR',	'MONTH', 'DAY_TYPE', 'VOL', 'VOL_MEAN', 'VOLUME_MODIFIER'])
            else:
                #daily_volume modifier
                daily_volume = tmas_class_state_tier1_clean.groupby(['STATION_ID','tmc','DIR','MONTH', 'DAY', 'DAY_TYPE']).filter(lambda x: x['VOL'].count()==24)
                daily_volume = daily_volume.groupby(['STATION_ID','tmc','DIR','MONTH', 'DAY', 'DAY_TYPE'])['VOL'].sum().reset_index()
                daily_volume_den = daily_volume.groupby(['STATION_ID','tmc','DIR'])['VOL'].mean().reset_index()
                daily_volume = daily_volume.groupby(['STATION_ID','tmc','DIR','MONTH','DAY_TYPE'])['VOL'].mean().reset_index()
                daily_volume = daily_volume.merge(daily_volume_den, on=['STATION_ID','tmc','DIR'], suffixes=['', '_MEAN'])
                daily_volume['VOLUME_MODIFIER'] = daily_volume['VOL']/daily_volume['VOL_MEAN']
                tier1_volume = daily_volume.groupby(['STATION_ID','tmc','DIR']).filter(lambda x: x['VOL'].count()>=6)
                
                hpms_numerator = tmas_class_state_tier1_clean.groupby(['STATION_ID','tmc','DIR','MONTH','DAY_TYPE','HOUR'], dropna=False)[['HPMS_TYPE10','HPMS_TYPE25','HPMS_TYPE40','HPMS_TYPE50','HPMS_TYPE60']].sum()
                noise_numerator = tmas_class_state_tier1_clean.groupby(['STATION_ID','tmc','DIR','MONTH','DAY_TYPE','HOUR'], dropna=False)[['NOISE_AUTO','NOISE_MED_TRUCK','NOISE_HVY_TRUCK','NOISE_BUS','NOISE_MC']].sum()
                tier1_hpms_classification = hpms_numerator.groupby(level=[0,1,2,3,4], as_index=False).apply(lambda x: x/x.sum().sum())      # aggregate hours to day first, then sum across typeXX, (without axis=1)
                tier1_noise_classification = noise_numerator.groupby(level=[0,1,2,3,4], as_index=False).apply(lambda x: x/x.sum().sum())
                tier1_hpms_classification.reset_index(inplace=True)
                tier1_noise_classification.reset_index(inplace=True, drop=True)
                tier1_class = pd.concat([tier1_hpms_classification,tier1_noise_classification], axis=1)
        
        tier1_volume.to_csv(filepath+'tier1_volume.csv',index=False)
        #tier1_class = pd.concat([tier1_hpms_classification,tier1_noise_classification], axis=1, sort=False)
        tier1_class.rename(index=str, columns={'HPMS_TYPE10':'PCT_TYPE10','HPMS_TYPE25':'PCT_TYPE25','HPMS_TYPE40':'PCT_TYPE40','HPMS_TYPE50':'PCT_TYPE50','HPMS_TYPE60':'PCT_TYPE60','NOISE_AUTO':'PCT_NOISE_AUTO','NOISE_MED_TRUCK':'PCT_NOISE_MED_TRUCK','NOISE_HVY_TRUCK':'PCT_NOISE_HVY_TRUCK','NOISE_BUS':'PCT_NOISE_BUS','NOISE_MC':'PCT_NOISE_MC', 'VOL':'DAILY_VOL'},inplace=True)
        tier1_class.to_csv(filepath+'tier1_class.csv',index=False)
        now=lapTimer('  took: ',now)
        
        # Defining annual average tier 1
        tier1_annual_average_class = tier1_class.groupby(['STATION_ID','tmc','DIR','DAY_TYPE','HOUR']).agg({'tmc':'count', 'PCT_TYPE10':'mean', 'PCT_TYPE25':'mean', 'PCT_TYPE40':'mean', 'PCT_TYPE50':'mean', 'PCT_TYPE60':'mean', 'PCT_NOISE_AUTO':'mean', 'PCT_NOISE_MED_TRUCK':'mean', 'PCT_NOISE_HVY_TRUCK':'mean', 'PCT_NOISE_BUS':'mean', 'PCT_NOISE_MC':'mean'})
        tier1_annual_average_class.rename(columns={'tmc':'count'}, inplace=True)
        tier1_annual_average_class.reset_index(inplace=True)
        tier1_annual_average_class = tier1_annual_average_class[tier1_annual_average_class['count']>=3].drop('count', axis=1)
        tier1_annual_average_class.to_csv(filepath+'tier1_annualaverage_class.csv',index=False)
        
        ##################################################
        #d. Creating Classification for Tier 2
        #Cross-classification variables are: STATE, COUNTY, ROUTE_SIGN, ROUTE_NUMBER, DOW, PEAKING, HOUR.
        print('Defining Tier 2 Classification')
        #daily_volume modifier
        daily_volume = tmas_class_state.groupby(['STATION_ID', 'COUNTY','ROUTE_SIGN','ROUTE_NUMBER','MONTH','DAY','DAY_TYPE','PEAKING']).filter(lambda x: x['VOL'].count()==24)
        daily_volume = daily_volume.groupby(['STATION_ID', 'COUNTY','ROUTE_SIGN','ROUTE_NUMBER','MONTH','DAY','DAY_TYPE','PEAKING'])['VOL'].sum().reset_index()
        daily_volume = daily_volume.groupby(['STATION_ID', 'COUNTY','ROUTE_SIGN','ROUTE_NUMBER','MONTH','DAY_TYPE','PEAKING'])['VOL'].mean().reset_index()
        daily_volume = daily_volume.groupby(['STATION_ID', 'COUNTY','ROUTE_SIGN','ROUTE_NUMBER','PEAKING']).filter(lambda x: x['VOL'].count()>=6) # filter stations so that outliers don't affect averages
        daily_volume = daily_volume.groupby(['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','MONTH','DAY_TYPE','PEAKING'])['VOL'].mean().reset_index() #average volume across all stations
        daily_volume_den = daily_volume.groupby(['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','PEAKING'])['VOL'].mean().reset_index()
        daily_volume = daily_volume.merge(daily_volume_den, on=['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','PEAKING'], suffixes=['', '_MEAN'])
        daily_volume['VOLUME_MODIFIER'] = daily_volume['VOL']/daily_volume['VOL_MEAN']
        tier2_volume = daily_volume.groupby(['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','PEAKING']).filter(lambda x: x['VOL'].count()>=6) #filter tier data so only data with at least 3 months is included
        tier2_volume.to_csv(filepath+'tier2_volume.csv',index=False)
        
        tier2_hpms = tmas_class_state.groupby(['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','MONTH','DAY_TYPE','PEAKING','HOUR'], dropna=False)[['HPMS_TYPE10','HPMS_TYPE25','HPMS_TYPE40','HPMS_TYPE50','HPMS_TYPE60']].sum()
        tier2_noise = tmas_class_state.groupby(['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','MONTH','DAY_TYPE','PEAKING','HOUR'], dropna=False)[['NOISE_AUTO','NOISE_MED_TRUCK','NOISE_HVY_TRUCK','NOISE_BUS','NOISE_MC']].sum()
        tier2_hpms_classification = tier2_hpms.groupby(level=[0,1,2,3,4,5], as_index=False).apply(lambda x: x/x.sum().sum())
        tier2_noise_classification = tier2_noise.groupby(level=[0,1,2,3,4,5], as_index=False).apply(lambda x: x/x.sum().sum())
        tier2_hpms_classification.reset_index(inplace=True)
        tier2_noise_classification.reset_index(inplace=True, drop=True)
        #tier2_class = pd.concat([tier2_hpms_classification,tier2_noise_classification], axis=1, sort=False)
        tier2_class = pd.concat([tier2_hpms_classification,tier2_noise_classification], axis=1)
        tier2_class.rename(index=str, columns={'HPMS_TYPE10':'PCT_TYPE10','HPMS_TYPE25':'PCT_TYPE25','HPMS_TYPE40':'PCT_TYPE40','HPMS_TYPE50':'PCT_TYPE50','HPMS_TYPE60':'PCT_TYPE60','NOISE_AUTO':'PCT_NOISE_AUTO','NOISE_MED_TRUCK':'PCT_NOISE_MED_TRUCK','NOISE_HVY_TRUCK':'PCT_NOISE_HVY_TRUCK','NOISE_BUS':'PCT_NOISE_BUS','NOISE_MC':'PCT_NOISE_MC', 'VOL':'DAILY_VOL'},inplace=True)
        tier2_class.to_csv(filepath+'tier2_class.csv',index=False)
        now=lapTimer('  took: ', now)
        
        # Defining annual average tier 2
        tier2_annual_average_class = tier2_class.groupby(['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','DAY_TYPE','PEAKING','HOUR']).agg({'COUNTY':'count', 'PCT_TYPE10':'mean', 'PCT_TYPE25':'mean', 'PCT_TYPE40':'mean', 'PCT_TYPE50':'mean', 'PCT_TYPE60':'mean', 'PCT_NOISE_AUTO':'mean', 'PCT_NOISE_MED_TRUCK':'mean', 'PCT_NOISE_HVY_TRUCK':'mean', 'PCT_NOISE_BUS':'mean', 'PCT_NOISE_MC':'mean'})
        tier2_annual_average_class.rename(columns={'COUNTY':'count'}, inplace=True)
        tier2_annual_average_class.reset_index(inplace=True)
        tier2_annual_average_class = tier2_annual_average_class[tier2_annual_average_class['count']>=3].drop('count', axis=1)
        tier2_annual_average_class.to_csv(filepath+'tier2_annualaverage_class.csv',index=False)

        
        ##################################################
        #e. Creating Classification for Tier 3
        #Cross-classification variables are: STATE, URB_RURAL, F_SYSTEM, DOW, PEAKING, HOUR.
        print('Defining Tier 3 Classification')
        
        #Daily Volume Modifier
        daily_volume = tmas_class_state.groupby(['STATION_ID', 'URB_RURAL','F_SYSTEM','MONTH','DAY','DAY_TYPE','PEAKING']).filter(lambda x: x['VOL'].count()==24)
        daily_volume = daily_volume.groupby(['STATION_ID','URB_RURAL','F_SYSTEM','MONTH','DAY','DAY_TYPE','PEAKING'])['VOL'].sum().reset_index()
        daily_volume = daily_volume.groupby(['STATION_ID','URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING'])['VOL'].mean().reset_index()
        daily_volume = daily_volume.groupby(['STATION_ID','URB_RURAL','F_SYSTEM','PEAKING']).filter(lambda x: x['VOL'].count()>=6) # filter stations so that outliers don't affect averages
        daily_volume = daily_volume.groupby(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING'])['VOL'].mean().reset_index() # average volume across all stations
        daily_volume_den = daily_volume.groupby(['URB_RURAL','F_SYSTEM','PEAKING'])['VOL'].mean().reset_index()
        daily_volume = daily_volume.merge(daily_volume_den, on=['URB_RURAL','F_SYSTEM','PEAKING'], suffixes=['', '_MEAN'])
        daily_volume['VOLUME_MODIFIER'] = daily_volume['VOL']/daily_volume['VOL_MEAN']
        tier3_volume = daily_volume.groupby(['URB_RURAL','F_SYSTEM','PEAKING']).filter(lambda x: x['VOL'].count()>=6) #filter to make sure there is at least 3 months of data to use the modifier
        tier3_volume.to_csv(filepath+'tier3_volume.csv',index=False)
        
        tier3_hpms = tmas_class_state.groupby(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR'], dropna=False)[['HPMS_TYPE10','HPMS_TYPE25','HPMS_TYPE40','HPMS_TYPE50','HPMS_TYPE60']].sum()
        tier3_noise = tmas_class_state.groupby(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR'], dropna=False)[['NOISE_AUTO','NOISE_MED_TRUCK','NOISE_HVY_TRUCK','NOISE_BUS','NOISE_MC']].sum()
        tier3_hpms_classification = tier3_hpms.groupby(level=[0,1,2,3,4], as_index=False).apply(lambda x: x/x.sum().sum())
        tier3_noise_classification = tier3_noise.groupby(level=[0,1,2,3,4], as_index=False).apply(lambda x: x/x.sum().sum())
        tier3_hpms_classification.reset_index(inplace=True)
        tier3_noise_classification.reset_index(inplace=True, drop=True)
        tier3_class = pd.concat([tier3_hpms_classification,tier3_noise_classification], axis=1)
        tier3_class.rename(index=str, columns={'HPMS_TYPE10':'PCT_TYPE10','HPMS_TYPE25':'PCT_TYPE25','HPMS_TYPE40':'PCT_TYPE40','HPMS_TYPE50':'PCT_TYPE50','HPMS_TYPE60':'PCT_TYPE60','NOISE_AUTO':'PCT_NOISE_AUTO','NOISE_MED_TRUCK':'PCT_NOISE_MED_TRUCK','NOISE_HVY_TRUCK':'PCT_NOISE_HVY_TRUCK','NOISE_BUS':'PCT_NOISE_BUS','NOISE_MC':'PCT_NOISE_MC', 'VOL':'DAILY_VOL'},inplace=True)
        tier3_class.to_csv(filepath+'tier3_class.csv',index=False)
        now=lapTimer('  took: ',now)
        
        # Defining annual average tier 3
        tier3_annual_average_class = tier3_class.groupby(['URB_RURAL','F_SYSTEM','DAY_TYPE','PEAKING','HOUR']).agg({'HOUR':'count', 'PCT_TYPE10':'mean', 'PCT_TYPE25':'mean', 'PCT_TYPE40':'mean', 'PCT_TYPE50':'mean', 'PCT_TYPE60':'mean', 'PCT_NOISE_AUTO':'mean', 'PCT_NOISE_MED_TRUCK':'mean', 'PCT_NOISE_HVY_TRUCK':'mean', 'PCT_NOISE_BUS':'mean', 'PCT_NOISE_MC':'mean'})
        tier3_annual_average_class.rename(columns={'HOUR':'count'}, inplace=True)
        tier3_annual_average_class.reset_index(inplace=True)
        tier3_annual_average_class = tier3_annual_average_class[tier3_annual_average_class['count']>=3].drop('count', axis=1)
        tier3_annual_average_class.to_csv(filepath+'tier3_annualaverage_class.csv',index=False)

        del tmas_class_state

    ##################################################
    #f. Creating Classification for Tier 4
    #Cross-classification variables are: URB_RURAL, F_SYSTEM, DOW, PEAKING, HOUR.  Note:  This distribution uses data from the entire country in the TMAS dataset.
    print('Defining Tier 4 Classification')
    
    #Daily Volume Modifier
    grouped = tmas_class.groupby(['STATION_ID', 'URB_RURAL','F_SYSTEM','MONTH','DAY','DAY_TYPE','PEAKING'])['VOL'].count().reset_index()
    grouped = grouped.rename(columns={'VOL': 'Count'})
    daily_volume = tmas_class.groupby(['STATION_ID','URB_RURAL','F_SYSTEM','MONTH','DAY','DAY_TYPE','PEAKING'])['VOL'].sum().reset_index()
    daily_volume = daily_volume.merge(grouped, on=['STATION_ID', 'URB_RURAL','F_SYSTEM','MONTH','DAY','DAY_TYPE','PEAKING'])
    daily_volume = daily_volume[daily_volume['Count']==24]
    daily_volume = daily_volume.groupby(['STATION_ID','URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING'])['VOL'].mean().reset_index()
    tier4_hpms = tmas_class.groupby(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR'])[['HPMS_TYPE10','HPMS_TYPE25','HPMS_TYPE40','HPMS_TYPE50','HPMS_TYPE60']].sum()
    tier4_noise = tmas_class.groupby(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR'])[['NOISE_AUTO','NOISE_MED_TRUCK','NOISE_HVY_TRUCK','NOISE_BUS','NOISE_MC']].sum()
    #with ProgressBar():
    
    daily_volume, tier4_hpms, tier4_noise = dask.compute(daily_volume, tier4_hpms, tier4_noise)
    
    daily_volume = daily_volume.groupby(['STATION_ID','URB_RURAL','F_SYSTEM','PEAKING']).filter(lambda x: x['VOL'].count()>=6)  # filter stations so that outliers don't affect averages
    daily_volume = daily_volume.groupby(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING'])['VOL'].mean().reset_index() # Average Volume across all stations
    daily_volume_den = daily_volume.groupby(['URB_RURAL','F_SYSTEM','PEAKING'])['VOL'].mean().reset_index()
    daily_volume = daily_volume.merge(daily_volume_den, on=['URB_RURAL','F_SYSTEM','PEAKING'], suffixes=['', '_MEAN'])
    daily_volume['VOLUME_MODIFIER'] = daily_volume['VOL']/daily_volume['VOL_MEAN']
    tier4_volume = daily_volume.groupby(['URB_RURAL','F_SYSTEM','PEAKING']).filter(lambda x: x['VOL'].count()>=6) #filter to make sure there is at least 3 months of data to use the modifier
    tier4_volume.to_csv(filepath+'tier4_volume.csv',index=False)
    
    tier4_hpms_classification = tier4_hpms.groupby(level=[0,1,2,3,4], as_index=False).apply(lambda x: x/x.sum().sum())
    tier4_noise_classification = tier4_noise.groupby(level=[0,1,2,3,4], as_index=False).apply(lambda x: x/x.sum().sum())
    tier4_hpms_classification.reset_index(inplace=True)
    tier4_noise_classification.reset_index(inplace=True, drop=True)
    tier4_class = pd.concat([tier4_hpms_classification,tier4_noise_classification], axis=1)
    #tier4_class = pd.concat([tier4_hpms_classification,tier4_noise_classification], axis=1, sort=False)
    tier4_class.rename(index=str, columns={'HPMS_TYPE10':'PCT_TYPE10','HPMS_TYPE25':'PCT_TYPE25','HPMS_TYPE40':'PCT_TYPE40','HPMS_TYPE50':'PCT_TYPE50','HPMS_TYPE60':'PCT_TYPE60','NOISE_AUTO':'PCT_NOISE_AUTO','NOISE_MED_TRUCK':'PCT_NOISE_MED_TRUCK','NOISE_HVY_TRUCK':'PCT_NOISE_HVY_TRUCK','NOISE_BUS':'PCT_NOISE_BUS','NOISE_MC':'PCT_NOISE_MC'},inplace=True)
    tier4_class.to_csv(filepath+'tier4_class.csv',index=False)
    del tmas_class, daily_volume, tier4_hpms, tier4_noise
    now=lapTimer('  took: ',now)
         
    ##########################################################################################
    # Merge Tiers (TMAS class vol % by tier) to NPMRDS (tmc)
    
    #a. Read NPMRDS data
    print ('Reading NPMRDS data')
    npmrds_tmc['dow'] = npmrds_tmc['dow'].astype('category')
    npmrds_tmc['peaking'] = npmrds_tmc['peaking'].astype('category')
    npmrds_tmc['direction'] = npmrds_tmc['direction'].astype('category')
    npmrds_tmc['state']= npmrds_tmc['state'].astype('category')
    npmrds_tmc['urban_rural'] = npmrds_tmc['urban_rural'].astype('category')
    npmrds_tmc['travel_time_all'] = npmrds_tmc['travel_time_all'].astype('float32')
    npmrds_tmc['speed_all'] = npmrds_tmc['speed_all'].astype('float32')
    npmrds_tmc['travel_time_pass'] = npmrds_tmc['travel_time_pass'].astype('float32')
    npmrds_tmc['speed_pass'] = npmrds_tmc['speed_pass'].astype('float32')
    npmrds_tmc['travel_time_truck'] = npmrds_tmc['travel_time_truck'].astype('float32')
    npmrds_tmc['speed_truck'] = npmrds_tmc['speed_truck'].astype('float32')
    npmrds_tmc['year'] = npmrds_tmc['year'].astype('int32')
    npmrds_tmc['month'] = npmrds_tmc['month'].astype('int32')
    npmrds_tmc['day'] = npmrds_tmc['day'].astype('int32')
    npmrds_tmc['hour'] = npmrds_tmc['hour'].astype('int32')
    npmrds_tmc['weekday'] = npmrds_tmc['weekday'].astype('int32')
    #npmrds_tmc['start_latitude'] = npmrds_tmc['start_latitude'].astype('float32')
    #npmrds_tmc['start_longitude'] = npmrds_tmc['start_longitude'].astype('float32')
    #npmrds_tmc['end_latitude'] = npmrds_tmc['end_latitude'].astype('float32')
    #npmrds_tmc['end_longitude'] = npmrds_tmc['end_longitude'].astype('float32')
    npmrds_tmc['miles'] = npmrds_tmc['miles'].astype('float32')
    #npmrds_tmc['road_order'] = npmrds_tmc['road_order'].astype('float32')
    npmrds_tmc['f_system'] = npmrds_tmc['f_system'].astype('int32')
    #npmrds_tmc['faciltype'] = npmrds_tmc['faciltype'].astype('category')
    #npmrds_tmc['thrulanes'] = npmrds_tmc['thrulanes'].astype('category')
    npmrds_tmc['route_numb'] = npmrds_tmc['route_numb'].astype(str)
    npmrds_tmc['route_sign'] = npmrds_tmc['route_sign'].astype('float32') + 1 # Adjust because this is off by one in the NPMRDS Data
    npmrds_tmc['aadt'] = npmrds_tmc['aadt'].astype('float32')
    npmrds_tmc['aadt_singl'] = npmrds_tmc['aadt_singl'].astype('float32')
    npmrds_tmc['aadt_combi'] = npmrds_tmc['aadt_combi'].astype('float32')
    #npmrds_tmc['nhs'] = npmrds_tmc['nhs'].astype('int32')
    npmrds_tmc['dir_num'] = npmrds_tmc['dir_num'].astype('int32')
    npmrds_tmc.drop(['tmc_code','urban_code'], inplace=True, axis=1)
    now=lapTimer('  took: ',now)
    
    #b. Merge Tier Data
    if SELECT_STATE in states_avlb.values:
        #b1. Tier 1
        #Read Tier 1 classifications
        print('Merging Tier 1 to NPMRDS')
        tier1_types = {
            'STATION_ID':str,
            'tmc':str,
            'DIR':'int32',
            'DAY_TYPE':str,
            'HOUR':'int32',
            'PCT_TYPE10':'float32',
            'PCT_TYPE25':'float32',
            'PCT_TYPE40':'float32',
            'PCT_TYPE50':'float32',
            'PCT_TYPE60':'float32',
            'PCT_NOISE_AUTO':'float32',
            'PCT_NOISE_MED_TRUCK':'float32',
            'PCT_NOISE_HVY_TRUCK':'float32',
            'PCT_NOISE_BUS':'float32',
            'PCT_NOISE_MC':'float32',
            'tier':'int32'}
        tier1_class = pd.read_csv(filepath+'tier1_class.csv', dtype=tier1_types).drop(columns=['level_0'], errors='ignore')
        tier1_class['tier']=1
        tier1_class.drop('STATION_ID',inplace=True,axis=1)
        #Merge with NPMRDS data
        npmrds_tier1 = pd.merge(npmrds_tmc, tier1_class, left_on=['tmc','dir_num', 'month', 'dow','hour'], right_on=['tmc','DIR','MONTH','DAY_TYPE','HOUR'], how='left')
        npmrds_for_tiers = npmrds_tier1[pd.isnull(npmrds_tier1['tier'])]
        npmrds_tier1 = npmrds_tier1[pd.notnull(npmrds_tier1['tier'])]
        now=lapTimer('  took: ',now)
        
        #b1. Tier 1 annual average
        #Read Tier 1 annual average classifications
        print('Merging Tier 1 annual average to NPMRDS')
        tier1_annualaverage_types = {
            'STATION_ID':str,
            'tmc':str,
            'DIR':'int32',
            'DAY_TYPE':str,
            'HOUR':'int32',
            'PCT_TYPE10':'float32',
            'PCT_TYPE25':'float32',
            'PCT_TYPE40':'float32',
            'PCT_TYPE50':'float32',
            'PCT_TYPE60':'float32',
            'PCT_NOISE_AUTO':'float32',
            'PCT_NOISE_MED_TRUCK':'float32',
            'PCT_NOISE_HVY_TRUCK':'float32',
            'PCT_NOISE_BUS':'float32',
            'PCT_NOISE_MC':'float32',
            'tier':'int32'}
        tier1_annualaverage_class = pd.read_csv(filepath+'tier1_annualaverage_class.csv', dtype=tier1_annualaverage_types).drop(columns=['level_0'], errors='ignore')
        tier1_annualaverage_class['tier']=1.5
        tier1_annualaverage_class.drop('STATION_ID',inplace=True,axis=1)
        #Merge with NPMRDS data
        npmrds_for_tiers.drop(['DIR','MONTH','DAY_TYPE','HOUR','PCT_TYPE10','PCT_TYPE25','PCT_TYPE40','PCT_TYPE50','PCT_TYPE60','PCT_NOISE_AUTO','PCT_NOISE_MED_TRUCK','PCT_NOISE_HVY_TRUCK','PCT_NOISE_BUS','PCT_NOISE_MC','tier'], inplace=True, axis=1)
        npmrds_tier1_annualaverage = pd.merge(npmrds_for_tiers, tier1_annualaverage_class, left_on=['tmc','dir_num', 'dow','hour'], right_on=['tmc','DIR','DAY_TYPE','HOUR'], how='left')
        npmrds_for_tiers = npmrds_tier1_annualaverage[pd.isnull(npmrds_tier1_annualaverage['tier'])]
        npmrds_tier1_annualaverage = npmrds_tier1_annualaverage[pd.notnull(npmrds_tier1_annualaverage['tier'])]
        now=lapTimer('  took: ',now)
        
        #b2. Tier 2
        #Read Tier 2 classifications
        print('Merging Tier 2 to NPMRDS')
        tier2_types = {
            #'COUNTY':str,
            'ROUTE_SIGN':'int32',
            'ROUTE_NUMBER':str,
            'DAY_TYPE':str,
            'PEAKING':str,
            'HOUR':'int32',
            'PCT_TYPE10':'float32',
            'PCT_TYPE25':'float32',
            'PCT_TYPE40':'float32',
            'PCT_TYPE50':'float32',
            'PCT_TYPE60':'float32',
            'PCT_NOISE_AUTO':'float32',
            'PCT_NOISE_MED_TRUCK':'float32',
            'PCT_NOISE_HVY_TRUCK':'float32',
            'PCT_NOISE_BUS':'float32',
            'PCT_NOISE_MC':'float32',
            'tier':'int32'}
        tier2_class = pd.read_csv(filepath+'tier2_class.csv',dtype=tier2_types).drop(columns=['level_0'], errors='ignore')
        tier2_class['tier']=2
        #tier2_class['ROUTE_NUMBER'] = pd.to_numeric(tier2_class['ROUTE_NUMBER'], errors='coerce')
        #tier2_class['COUNTY'] = tier2_class['COUNTY'].str.replace(' County','').str.upper()
        npmrds_for_tiers.drop(['DIR','DAY_TYPE','HOUR','PCT_TYPE10','PCT_TYPE25','PCT_TYPE40','PCT_TYPE50','PCT_TYPE60','PCT_NOISE_AUTO','PCT_NOISE_MED_TRUCK','PCT_NOISE_HVY_TRUCK','PCT_NOISE_BUS','PCT_NOISE_MC','tier'], inplace=True, axis=1)
        #Merge with NPMRDS data
        npmrds_tier2 = pd.merge(npmrds_for_tiers, tier2_class, left_on=['county','route_sign','route_numb','month','dow','peaking','hour'], 
                                right_on=['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','MONTH','DAY_TYPE','PEAKING','HOUR'], how='left')
        npmrds_for_tiers = npmrds_tier2[pd.isnull(npmrds_tier2['tier'])]
        npmrds_tier2 = npmrds_tier2[pd.notnull(npmrds_tier2['tier'])]
        now=lapTimer('  took: ',now)
        
        #b2. Tier 2 annual average
        #Read Tier 2 classifications
        print('Merging Tier 2 annual average to NPMRDS')
        tier2_annualaverage_types = {
            #'COUNTY':str,
            'ROUTE_SIGN':'int32',
            'ROUTE_NUMBER':str,
            'DAY_TYPE':str,
            'PEAKING':str,
            'HOUR':'int32',
            'PCT_TYPE10':'float32',
            'PCT_TYPE25':'float32',
            'PCT_TYPE40':'float32',
            'PCT_TYPE50':'float32',
            'PCT_TYPE60':'float32',
            'PCT_NOISE_AUTO':'float32',
            'PCT_NOISE_MED_TRUCK':'float32',
            'PCT_NOISE_HVY_TRUCK':'float32',
            'PCT_NOISE_BUS':'float32',
            'PCT_NOISE_MC':'float32',
            'tier':'int32'}
        tier2_annualaverage_class = pd.read_csv(filepath+'tier2_annualaverage_class.csv',dtype=tier2_annualaverage_types).drop(columns=['level_0'], errors='ignore')
        tier2_annualaverage_class['tier']=2.5
        npmrds_for_tiers.drop(['COUNTY', 'ROUTE_SIGN', 'ROUTE_NUMBER', 'PEAKING', 'MONTH', 'DAY_TYPE','HOUR','PCT_TYPE10','PCT_TYPE25','PCT_TYPE40','PCT_TYPE50','PCT_TYPE60','PCT_NOISE_AUTO','PCT_NOISE_MED_TRUCK','PCT_NOISE_HVY_TRUCK','PCT_NOISE_BUS','PCT_NOISE_MC','tier'], inplace=True, axis=1)
        #Merge with NPMRDS data
        npmrds_tier2_annualaverage = pd.merge(npmrds_for_tiers, tier2_annualaverage_class, left_on=['county','route_sign','route_numb','dow','peaking','hour'], 
                                right_on=['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','DAY_TYPE','PEAKING','HOUR'], how='left')
        npmrds_for_tiers = npmrds_tier2_annualaverage[pd.isnull(npmrds_tier2_annualaverage['tier'])]
        npmrds_tier2_annualaverage = npmrds_tier2_annualaverage[pd.notnull(npmrds_tier2_annualaverage['tier'])]
        now=lapTimer('  took: ',now)
        
        #b3. Tier 3
        #Read Tier 3 classifications
        print('Merging Tier 3 to NPMRDS')
        tier3_types = {
            'URB_RURAL':str,
            'F_SYSTEM':'int32',
            'DAY_TYPE':str,
            'PEAKING':str,
            'HOUR':'int32',
            'PCT_TYPE10':'float32',
            'PCT_TYPE25':'float32',
            'PCT_TYPE40':'float32',
            'PCT_TYPE50':'float32',
            'PCT_TYPE60':'float32',
            'PCT_NOISE_AUTO':'float32',
            'PCT_NOISE_MED_TRUCK':'float32',
            'PCT_NOISE_HVY_TRUCK':'float32',
            'PCT_NOISE_BUS':'float32',
            'PCT_NOISE_MC':'float32',
            'tier':'int32'}
        tier3_class = pd.read_csv(filepath+'tier3_class.csv', dtype=tier3_types).drop(columns=['level_0'], errors='ignore')
        tier3_class['tier']=3
        npmrds_for_tiers.drop(['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','DAY_TYPE','PEAKING','HOUR','PCT_TYPE10','PCT_TYPE25','PCT_TYPE40','PCT_TYPE50','PCT_TYPE60','PCT_NOISE_AUTO','PCT_NOISE_MED_TRUCK','PCT_NOISE_HVY_TRUCK','PCT_NOISE_BUS','PCT_NOISE_MC','tier'], inplace=True, axis=1)
        #Merging with Tier 3
        npmrds_tier3 = pd.merge(npmrds_for_tiers, tier3_class, left_on=['urban_rural','f_system','month','dow','peaking','hour'], right_on=['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR'], how='left')
        npmrds_for_tiers = npmrds_tier3[pd.isnull(npmrds_tier3['tier'])]
        npmrds_tier3 = npmrds_tier3[pd.notnull(npmrds_tier3['tier'])]
        now=lapTimer('  took: ',now)

        #b3. Tier 3
        #Read Tier 3 annual average classifications
        print('Merging Tier 3 annual average to NPMRDS')
        tier3_annualaverage_types = {
            'URB_RURAL':str,
            'F_SYSTEM':'int32',
            'DAY_TYPE':str,
            'PEAKING':str,
            'HOUR':'int32',
            'PCT_TYPE10':'float32',
            'PCT_TYPE25':'float32',
            'PCT_TYPE40':'float32',
            'PCT_TYPE50':'float32',
            'PCT_TYPE60':'float32',
            'PCT_NOISE_AUTO':'float32',
            'PCT_NOISE_MED_TRUCK':'float32',
            'PCT_NOISE_HVY_TRUCK':'float32',
            'PCT_NOISE_BUS':'float32',
            'PCT_NOISE_MC':'float32',
            'tier':'int32'}
        tier3_annualaverage_class = pd.read_csv(filepath+'tier3_annualaverage_class.csv', dtype=tier3_types).drop(columns=['level_0'], errors='ignore')
        tier3_annualaverage_class['tier']=3.5
        npmrds_for_tiers.drop(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR','MONTH','DAY_TYPE','PEAKING','HOUR','PCT_TYPE10','PCT_TYPE25','PCT_TYPE40','PCT_TYPE50','PCT_TYPE60','PCT_NOISE_AUTO','PCT_NOISE_MED_TRUCK','PCT_NOISE_HVY_TRUCK','PCT_NOISE_BUS','PCT_NOISE_MC','tier'], inplace=True, axis=1)
        #Merging with Tier 3
        npmrds_tier3_annualaverage = pd.merge(npmrds_for_tiers, tier3_annualaverage_class, left_on=['urban_rural','f_system','dow','peaking','hour'], right_on=['URB_RURAL','F_SYSTEM','DAY_TYPE','PEAKING','HOUR'], how='left')
        npmrds_for_tiers = npmrds_tier3_annualaverage[pd.isnull(npmrds_tier3_annualaverage['tier'])]
        npmrds_tier3_annualaverage = npmrds_tier3_annualaverage[pd.notnull(npmrds_tier3_annualaverage['tier'])]
        now=lapTimer('  took: ',now)
    
    #b4. Tier 4
    print('Merging Tier 4 to NPMRDS')
    tier4_types = {
        'URB_RURAL':str,
        'F_SYSTEM':'int32',
        'DAY_TYPE':str,
        'PEAKING':str,
        'HOUR':'int32',
        'PCT_TYPE10':'float32',
        'PCT_TYPE25':'float32',
        'PCT_TYPE40':'float32',
        'PCT_TYPE50':'float32',
        'PCT_TYPE60':'float32',
        'PCT_NOISE_AUTO':'float32',
        'PCT_NOISE_MED_TRUCK':'float32',
        'PCT_NOISE_HVY_TRUCK':'float32',
        'PCT_NOISE_BUS':'float32',
        'PCT_NOISE_MC':'float32',
        'tier':'int32'}
    tier4_class = pd.read_csv(filepath+'tier4_class.csv', dtype=tier4_types).drop(columns=['level_0']).drop(columns=['level_0'], errors='ignore')
    tier4_class['tier']=4
           
    if SELECT_STATE in states_avlb.values:
        npmrds_for_tiers.drop(['URB_RURAL','F_SYSTEM','DAY_TYPE','PEAKING','HOUR','PCT_TYPE10','PCT_TYPE25','PCT_TYPE40','PCT_TYPE50','PCT_TYPE60','PCT_NOISE_AUTO','PCT_NOISE_MED_TRUCK','PCT_NOISE_HVY_TRUCK','PCT_NOISE_BUS','PCT_NOISE_MC','tier'], inplace=True, axis=1)
        #Merging with Tier 4
        npmrds_tier4 = pd.merge(npmrds_for_tiers, tier4_class, left_on=['urban_rural','f_system','month','dow','peaking','hour'], right_on=['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR'], how='left')
        npmrds_for_tiers = npmrds_tier4[pd.isnull(npmrds_tier4['tier'])]
        npmrds_tier4 = npmrds_tier4[pd.notnull(npmrds_tier4['tier'])]
    else:
        npmrds_tier4 = pd.merge(npmrds_tmc, tier4_class, left_on=['urban_rural','f_system','month','dow','peaking','hour'], right_on=['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR'], how='left')
        npmrds_for_tiers = npmrds_tier4[pd.isnull(npmrds_tier4['tier'])]
        npmrds_tier4 = npmrds_tier4[pd.notnull(npmrds_tier4['tier'])]
    now=lapTimer('  took: ',now)
    
    #c. Merging dataset together
    print('Combining NPMRDS with Tiers')
    npmrds_for_tiers.drop(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR'], inplace=True, axis=1)
    if SELECT_STATE in states_avlb.values:
        npmrds_tier1.drop(['DIR','MONTH','DAY_TYPE','HOUR'], inplace=True, axis=1)
        npmrds_tier1_annualaverage.drop(['DIR','DAY_TYPE','HOUR'], inplace=True, axis=1)
        npmrds_tier2.drop(['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','MONTH','DAY_TYPE','PEAKING','HOUR'], inplace=True, axis=1)
        npmrds_tier2_annualaverage.drop(['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','DAY_TYPE','PEAKING','HOUR'], inplace=True, axis=1)
        npmrds_tier3.drop(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR'], inplace=True, axis=1)
        npmrds_tier3_annualaverage.drop(['URB_RURAL','F_SYSTEM','DAY_TYPE','PEAKING','HOUR'], inplace=True, axis=1)
        npmrds_tier4.drop(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR'], inplace=True, axis=1)
        tiers=[npmrds_tier1, npmrds_tier1_annualaverage, npmrds_tier2, npmrds_tier2_annualaverage, npmrds_tier3, npmrds_tier3_annualaverage, npmrds_tier4, npmrds_for_tiers]
    else:
        npmrds_tier4.drop(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','HOUR'], inplace=True, axis=1)
        tiers=[npmrds_tier4, npmrds_for_tiers]
    df = pd.concat(tiers)
    df.rename(index=str, columns={'miles':'tmc_length'}, inplace=True)
    #df.drop(['route_numb','route_sign','dir_num'],axis=1,inplace=True)
    now=lapTimer('  took: ',now)
    
    del npmrds_template
    if 'npmrds_tier4' in locals():
        del npmrds_tier4
    if 'npmrds_tier3' in locals():
        del npmrds_tier3
    if 'npmrds_tier2' in locals():
        del npmrds_tier2
    if 'npmrds_tier1' in locals():
        del npmrds_tier1
    del d
    
    # QC
    total_tmc = df['tmc'].nunique()
    tier1_tmc = df.loc[df['tier']==1,'tmc'].nunique()
    tier1_annualaverage_tmc = df.loc[df['tier']==1.5,'tmc'].nunique()
    tier2_tmc = df.loc[df['tier']==2,'tmc'].nunique()
    tier2_annualaverage_tmc = df.loc[df['tier']==2.5,'tmc'].nunique()
    tier3_tmc = df.loc[df['tier']==3,'tmc'].nunique()
    tier3_annualaverage_tmc = df.loc[df['tier']==3.5,'tmc'].nunique()
    tier4_tmc = df.loc[df['tier']==4,'tmc'].nunique()
    print('Total TMCs: %i' %total_tmc)
    print('Tier 1 TMCs: %i' %tier1_tmc)
    print('Tier 1 annual average TMCs: %i' %tier1_annualaverage_tmc)
    print('Tier 2 TMCs: %i' %tier2_tmc)
    print('Tier 2 annual average TMCs: %i' %tier2_annualaverage_tmc)
    print('Tier 3 TMCs: %i' %tier3_tmc)
    print('Tier 3 annual average TMCs: %i' %tier3_annualaverage_tmc)
    print('Tier 4 TMCs: %i' %tier4_tmc)
    
    '''
    #d. Exporting NPMRDS and classification data
    print('Exporting Tier data')
    #df.to_csv(outputpath+SELECT_STATE+'_Composite_Dataset.csv', index=False)
    npmrds_tmc = pa.Table.from_pandas(df)
    pq.write_table(npmrds_tmc, outputpath+SELECT_STATE+'_Composite_Dataset.parquet')
    now=lapTimer('  took: ',now)
    '''
    ##########################################################################################
    # Daily AADT Volume Modifier
    print("Combining NPMRDS with monthly average volume modifier")
    if SELECT_STATE in states_avlb.values:
        tier1_volume['tier']=1
        #tier1_volume.drop('STATION_ID',inplace=True,axis=1)
        #Merge with NPMRDS data
        npmrds_tier1_volume = pd.merge(npmrds_tmc, tier1_volume, left_on=['tmc','dir_num', 'month', 'dow'], 
                                       right_on=['tmc','DIR','MONTH','DAY_TYPE'], how='left')
        npmrds_for_tiers_volume = npmrds_tier1_volume[pd.isnull(npmrds_tier1_volume['tier'])]
        npmrds_tier1_volume = npmrds_tier1_volume[pd.notnull(npmrds_tier1_volume['tier'])]
        
        tier2_volume['tier']=2
        npmrds_for_tiers_volume.drop(['DIR','MONTH','DAY_TYPE','VOL', 'VOL_MEAN', 'VOLUME_MODIFIER', 'tier'], inplace=True, axis=1)
        #Merge with NPMRDS data
        npmrds_tier2_volume = pd.merge(npmrds_for_tiers_volume, tier2_volume, left_on=['county','route_sign','route_numb','month','dow','peaking'], 
                                right_on=['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','MONTH','DAY_TYPE','PEAKING'], how='left')
        npmrds_for_tiers_volume = npmrds_tier2_volume[pd.isnull(npmrds_tier2_volume['tier'])]
        npmrds_tier2_volume = npmrds_tier2_volume[pd.notnull(npmrds_tier2_volume['tier'])]
        
        tier3_volume['tier']=3
        npmrds_for_tiers_volume.drop(['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','MONTH','DAY_TYPE','PEAKING','VOL','VOL_MEAN','VOLUME_MODIFIER','tier'], inplace=True, axis=1)
        #Merging with Tier 3
        npmrds_tier3_volume = pd.merge(npmrds_for_tiers_volume, tier3_volume, left_on=['urban_rural','f_system','month','dow','peaking'], 
                                       right_on=['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING'], how='left')
        npmrds_for_tiers_volume = npmrds_tier3_volume[pd.isnull(npmrds_tier3_volume['tier'])]
        npmrds_tier3_volume = npmrds_tier3_volume[pd.notnull(npmrds_tier3_volume['tier'])]
        
    tier4_volume['tier']=4           
    if SELECT_STATE in states_avlb.values:
        npmrds_for_tiers_volume.drop(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING','VOL','VOL_MEAN','VOLUME_MODIFIER','tier'], inplace=True, axis=1)
        #Merging with Tier 4
        npmrds_tier4_volume = pd.merge(npmrds_for_tiers_volume, tier4_volume, left_on=['urban_rural','f_system','month','dow','peaking'], 
                                right_on=['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING'], how='left')
        npmrds_for_tiers_volume = npmrds_tier4_volume[pd.isnull(npmrds_tier4_volume['tier'])]
        npmrds_tier4_volume = npmrds_tier4_volume[pd.notnull(npmrds_tier4_volume['tier'])]
    else:
        npmrds_tier4_volume = pd.merge(npmrds_tmc, tier4_volume, left_on=['urban_rural','f_system','month','dow','peaking'], 
                                       right_on=['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING'], how='left')
        npmrds_for_tiers_volume = npmrds_tier4_volume[pd.isnull(npmrds_tier4_volume['tier'])]
        npmrds_tier4_volume = npmrds_tier4_volume[pd.notnull(npmrds_tier4_volume['tier'])]
    now=lapTimer('  took: ',now)
        
    print('Combining NPMRDS with Tiers')
    npmrds_for_tiers_volume.drop(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING'], inplace=True, axis=1)
    if SELECT_STATE in states_avlb.values:
        npmrds_tier1_volume.drop(['DIR','MONTH','DAY_TYPE'], inplace=True, axis=1)
        npmrds_tier2_volume.drop(['COUNTY','ROUTE_SIGN','ROUTE_NUMBER','MONTH','DAY_TYPE','PEAKING'], inplace=True, axis=1)
        npmrds_tier3_volume.drop(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING'], inplace=True, axis=1)
        npmrds_tier4_volume.drop(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING'], inplace=True, axis=1)
        tiers=[npmrds_tier1_volume, npmrds_tier2_volume, npmrds_tier3_volume, npmrds_tier4_volume, npmrds_for_tiers_volume]
    else:
        npmrds_tier4_volume.drop(['URB_RURAL','F_SYSTEM','MONTH','DAY_TYPE','PEAKING'], inplace=True, axis=1)
        tiers=[npmrds_tier4_volume, npmrds_for_tiers_volume]
    df_volume = pd.concat(tiers)
    df_volume = df_volume[['tmc', 'measurement_tstamp', 'VOL','VOL_MEAN','VOLUME_MODIFIER','tier']]
    df_volume.rename(index=str, columns={'miles':'tmc_length', 'tier':'tier_volume'}, inplace=True)
    now=lapTimer('  took: ',now)
    
    df = df.merge(df_volume, on=['tmc', 'measurement_tstamp'])
    
    del npmrds_tmc
    del df_volume
    
    ##########################################################################################
    # Emission Rates
    '''
    df = pq.read_table('Output/CO_Composite_Dataset.parquet')
    df = df.to_pandas()
    df_test = df[0:10]
    '''
    
    #c. Read the Tier data
    print ('Reading NPMRDS TMAS data')
    df['dow'] = df['dow'].astype('category')
    df['peaking'] = df['peaking'].astype('category')
    df['direction'] = df['direction'].astype('category')
    df['state']= df['state'].astype('category')
    df['urban_rural'] = df['urban_rural'].astype('category')
    #df['faciltype'] = df['faciltype'].astype('category')
    #df['thrulanes'] = df['thrulanes'].astype('category')
    df['tier'] = df['tier'].astype('category')
    
    #c1. Modify Tier data
    print ('Calulating Speed Bins')
    df['aadt']=df['aadt']/2
    df['dayid'] = np.nan
    df.loc[df['dow']=='WD','dayid'] = 5
    df.loc[df['dow']=='WE','dayid'] = 2
    df['dayid'] = df['dayid'].astype(int)
    
    df['monthid']=df['month']
    df['hourid']=df['hour']+1
    
    df['roadtypeid'] = np.nan
    df.loc[(df['urban_rural']=='U') & (df['f_system']<=2), 'roadtypeid'] = 4
    df.loc[(df['urban_rural']=='U') & (df['f_system']>2), 'roadtypeid'] = 5
    df.loc[(df['urban_rural']=='R') & (df['f_system']<=2), 'roadtypeid'] = 2
    df.loc[(df['urban_rural']=='R') & (df['f_system']>2), 'roadtypeid'] = 3
    
    df['avgspeedbinid']=np.nan
    df.loc[df['speed_all']<2.5, 'avgspeedbinid'] = 1
    df.loc[(df['speed_all']>=2.5)&(df['speed_all']<7.5), 'avgspeedbinid'] = 2
    df.loc[(df['speed_all']>=7.5)&(df['speed_all']<12.5), 'avgspeedbinid'] = 3
    df.loc[(df['speed_all']>=12.5)&(df['speed_all']<17.5), 'avgspeedbinid'] = 4
    df.loc[(df['speed_all']>=17.5)&(df['speed_all']<22.5), 'avgspeedbinid'] = 5
    df.loc[(df['speed_all']>=22.5)&(df['speed_all']<27.5), 'avgspeedbinid'] = 6
    df.loc[(df['speed_all']>=27.5)&(df['speed_all']<32.5), 'avgspeedbinid'] = 7
    df.loc[(df['speed_all']>=32.5)&(df['speed_all']<37.5), 'avgspeedbinid'] = 8
    df.loc[(df['speed_all']>=37.5)&(df['speed_all']<42.5), 'avgspeedbinid'] = 9
    df.loc[(df['speed_all']>=42.5)&(df['speed_all']<47.5), 'avgspeedbinid'] = 10
    df.loc[(df['speed_all']>=47.5)&(df['speed_all']<52.5), 'avgspeedbinid'] = 11
    df.loc[(df['speed_all']>=52.5)&(df['speed_all']<57.5), 'avgspeedbinid'] = 12
    df.loc[(df['speed_all']>=57.5)&(df['speed_all']<62.5), 'avgspeedbinid'] = 13
    df.loc[(df['speed_all']>=62.5)&(df['speed_all']<67.5), 'avgspeedbinid'] = 14
    df.loc[(df['speed_all']>=67.5)&(df['speed_all']<72.5), 'avgspeedbinid'] = 15
    df.loc[(df['speed_all']>=72.5), 'avgspeedbinid'] = 16
    
    df['avgspeedbinid_pass']=np.nan
    df.loc[df['speed_pass']<2.5, 'avgspeedbinid_pass'] = 1
    df.loc[(df['speed_pass']>=2.5)&(df['speed_pass']<7.5), 'avgspeedbinid_pass'] = 2
    df.loc[(df['speed_pass']>=7.5)&(df['speed_pass']<12.5), 'avgspeedbinid_pass'] = 3
    df.loc[(df['speed_pass']>=12.5)&(df['speed_pass']<17.5), 'avgspeedbinid_pass'] = 4
    df.loc[(df['speed_pass']>=17.5)&(df['speed_pass']<22.5), 'avgspeedbinid_pass'] = 5
    df.loc[(df['speed_pass']>=22.5)&(df['speed_pass']<27.5), 'avgspeedbinid_pass'] = 6
    df.loc[(df['speed_pass']>=27.5)&(df['speed_pass']<32.5), 'avgspeedbinid_pass'] = 7
    df.loc[(df['speed_pass']>=32.5)&(df['speed_pass']<37.5), 'avgspeedbinid_pass'] = 8
    df.loc[(df['speed_pass']>=37.5)&(df['speed_pass']<42.5), 'avgspeedbinid_pass'] = 9
    df.loc[(df['speed_pass']>=42.5)&(df['speed_pass']<47.5), 'avgspeedbinid_pass'] = 10
    df.loc[(df['speed_pass']>=47.5)&(df['speed_pass']<52.5), 'avgspeedbinid_pass'] = 11
    df.loc[(df['speed_pass']>=52.5)&(df['speed_pass']<57.5), 'avgspeedbinid_pass'] = 12
    df.loc[(df['speed_pass']>=57.5)&(df['speed_pass']<62.5), 'avgspeedbinid_pass'] = 13
    df.loc[(df['speed_pass']>=62.5)&(df['speed_pass']<67.5), 'avgspeedbinid_pass'] = 14
    df.loc[(df['speed_pass']>=67.5)&(df['speed_pass']<72.5), 'avgspeedbinid_pass'] = 15
    df.loc[(df['speed_pass']>=72.5), 'avgspeedbinid_pass'] = 16
    
    df['avgspeedbinid_truck']=np.nan
    df.loc[df['speed_truck']<2.5, 'avgspeedbinid_truck'] = 1
    df.loc[(df['speed_truck']>=2.5)&(df['speed_truck']<7.5), 'avgspeedbinid_truck'] = 2
    df.loc[(df['speed_truck']>=7.5)&(df['speed_truck']<12.5), 'avgspeedbinid_truck'] = 3
    df.loc[(df['speed_truck']>=12.5)&(df['speed_truck']<17.5), 'avgspeedbinid_truck'] = 4
    df.loc[(df['speed_truck']>=17.5)&(df['speed_truck']<22.5), 'avgspeedbinid_truck'] = 5
    df.loc[(df['speed_truck']>=22.5)&(df['speed_truck']<27.5), 'avgspeedbinid_truck'] = 6
    df.loc[(df['speed_truck']>=27.5)&(df['speed_truck']<32.5), 'avgspeedbinid_truck'] = 7
    df.loc[(df['speed_truck']>=32.5)&(df['speed_truck']<37.5), 'avgspeedbinid_truck'] = 8
    df.loc[(df['speed_truck']>=37.5)&(df['speed_truck']<42.5), 'avgspeedbinid_truck'] = 9
    df.loc[(df['speed_truck']>=42.5)&(df['speed_truck']<47.5), 'avgspeedbinid_truck'] = 10
    df.loc[(df['speed_truck']>=47.5)&(df['speed_truck']<52.5), 'avgspeedbinid_truck'] = 11
    df.loc[(df['speed_truck']>=52.5)&(df['speed_truck']<57.5), 'avgspeedbinid_truck'] = 12
    df.loc[(df['speed_truck']>=57.5)&(df['speed_truck']<62.5), 'avgspeedbinid_truck'] = 13
    df.loc[(df['speed_truck']>=62.5)&(df['speed_truck']<67.5), 'avgspeedbinid_truck'] = 14
    df.loc[(df['speed_truck']>=67.5)&(df['speed_truck']<72.5), 'avgspeedbinid_truck'] = 15
    df.loc[(df['speed_truck']>=72.5), 'avgspeedbinid_truck'] = 16
    
    df.loc[df['monthid']==1, 'monthid3'] = 120102
    df.loc[df['monthid']==2, 'monthid3'] = 120102
    df.loc[df['monthid']==3, 'monthid3'] = 30405
    df.loc[df['monthid']==4, 'monthid3'] = 30405
    df.loc[df['monthid']==5, 'monthid3'] = 30405
    df.loc[df['monthid']==6, 'monthid3'] = 60708
    df.loc[df['monthid']==7, 'monthid3'] = 60708
    df.loc[df['monthid']==8, 'monthid3'] = 60708
    df.loc[df['monthid']==9, 'monthid3'] = 91011
    df.loc[df['monthid']==10, 'monthid3'] = 91011
    df.loc[df['monthid']==11, 'monthid3'] = 91011
    df.loc[df['monthid']==12, 'monthid3'] = 120102
    
    df['repcty'] = df['repcty'] % 1000
    
    df['VMT'] = (df['PCT_TYPE10']+df['PCT_TYPE25']+df['PCT_TYPE40']+df['PCT_TYPE50']+df['PCT_TYPE60'])*df['aadt']*df['VOLUME_MODIFIER']*df['tmc_length']
    df['MAADT'] = df['aadt']*df['VOLUME_MODIFIER']
    df.drop(['VOL', 'VOL_MEAN'], inplace=True, axis=1)
    now=lapTimer('  took: ',now)
    
    df['avgspeedbinid_pass'].fillna(df['avgspeedbinid'], inplace=True)
    df['avgspeedbinid_truck'].fillna(df['avgspeedbinid'], inplace=True)
    
    print('RAM memory % used:', psutil.virtual_memory()[2])
    # Getting usage of virtual_memory in GB ( 4th field)
    print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
    
    
    #a. Read the MOVES emission rate files from ERG: nhs lpp rates_{state}_wbt.cs monthid dayid hourid roadtypeid hpmsvtypeid pollutantid avgspeedbinid grams_per_mile
    # updated rates table based on NEI region and 3-month
    print ('Reading and Processing Emission Rate Files')
    #emissions = pq.read_table(PATH_emission)
    #emissions = emissions.to_pandas()
    
    emissions = dd.read_parquet(PATH_emission)
    emissions = emissions.assign(state = emissions['repcty'] // 1000)
    emissions_state = emissions.loc[emissions['state']==states.get(SELECT_STATE)[1]]
    del emissions
    emissions_state = emissions_state.rename(columns={'season': 'monthid3'})
    emissions_state = emissions_state.assign(repcty_1 = emissions_state['repcty'] % 1000)
    emissions_state = emissions_state.astype({'hpmsvtypeid': str})
    emissions_state = emissions_state.astype({'pollutantid': str})

    emissions_state = emissions_state.drop(['repcty','state'], axis=1)
    emissions_state = emissions_state.rename(columns={'repcty_1':'repcty', 'monthid':'monthid3'})
        
    #Sort by: MONTHID HOURID ROADTYPEID HPMSTYPEID POLLUTANTID AVGSPEEDBINID 
    #emissions_state = emissions_state.sort_values(['repcty','monthid3','hourid','roadtypeid','hpmsvtypeid','pollutantid','avgspeedbinid'])
    emissions_pass = emissions_state.loc[emissions_state['hpmsvtypeid'].isin(['10', '25'])]
    emissions_truck = emissions_state.loc[emissions_state['hpmsvtypeid'].isin(['40', '50', '60'])]
    #b. Create grams per mile values for vehicletype/pollutant combinations. They will be in this order from the sort:
    
    del emissions_state

    now=lapTimer('  took: ',now)
    print('Waiting for Memory to Clear')
    gc.collect()
    time.sleep(5)
    print('RAM memory % used:', psutil.virtual_memory()[2])
    # Getting usage of virtual_memory in GB ( 4th field)
    print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
    
    print('Processing Passenger Emission Rates')
    emissions_pass = emissions_pass.compute()
    emissions_pass.sort_values(['repcty','monthid3','hourid','roadtypeid','hpmsvtypeid','pollutantid','avgspeedbinid'], inplace=True)
    emissions_pass=emissions_pass.pivot_table(index=['repcty','monthid3','hourid','roadtypeid','avgspeedbinid'], 
                                     columns=['hpmsvtypeid','pollutantid'], values='grams_per_mile')    
    emissions_pass.columns = emissions_pass.columns.map('_'.join)
    emissions_pass = emissions_pass.reset_index()
    emissions_pass = emissions_pass.rename(columns = {'avgspeedbinid': 'avgspeedbinid_pass'})
    
    #d. Merge the emission rates with the NPMRDS dataset
    print('Merging Passenger Emission Rates to NPMRDS data')
    df_emissions = df.merge(emissions_pass, how='left', on=['repcty','monthid3','hourid','roadtypeid','avgspeedbinid_pass'])
    del emissions_pass
    del df
    now=lapTimer('  took: ',now)
    
    print('Processing Truck Emission Rates')
    emissions_truck = emissions_truck.compute()
    emissions_truck.sort_values(['repcty','monthid3','hourid','roadtypeid','hpmsvtypeid','pollutantid','avgspeedbinid'], inplace=True)
    emissions_truck=emissions_truck.pivot_table(index=['repcty','monthid3','hourid','roadtypeid','avgspeedbinid'], 
                                     columns=['hpmsvtypeid','pollutantid'], values='grams_per_mile')
    emissions_truck.columns = emissions_truck.columns.map('_'.join)
    emissions_truck = emissions_truck.reset_index()
    emissions_truck = emissions_truck.rename(columns = {'avgspeedbinid': 'avgspeedbinid_truck'})
    
    #d. Merge the emission rates with the NPMRDS dataset
    print('Merging Truck Emission Rates to NPMRDS data')
    df_emissions = df_emissions.merge(emissions_truck, how='left', on=['repcty','monthid3','hourid','roadtypeid','avgspeedbinid_truck'])
    del emissions_truck
    now=lapTimer('  took: ',now)
    
    
    print('Exporting Final Dataset')
    #df_emissions.to_csv(outputpath+SELECT_STATE+'_Composite_Emission.csv', index=False)
    sample_df_nonulls = df_emissions.loc[df_emissions['travel_time_all'].notnull()].reset_index(drop=True).copy()
    df_emissions_sample = sample_df_nonulls[0:1000]
    df_emissions_sample = pd.concat([df_emissions_sample, sample_df_nonulls[-1000:-1]])
    df_emissions_sample.to_csv(filepath+SELECT_STATE+'_Composite_Emissions_SAMPLE.csv', index=False) 
    
    df_emissions_summary_cols = df_emissions[['tmc', 'road', 'tmc_length', 'speed_all', 'aadt']]
    pollutants = [2, 3, 5, 6, 87, 90, 98, 100, 110]
    vehtypes = [10, 25, 40, 50, 60]
    for pol in pollutants:
        df_emissions_summary_cols['TotEmissionsPerMile_{}'.format(pol)] = 0
        for veh in vehtypes:
            if '{}_{}'.format(veh, pol) in df_emissions.columns:
                df_emissions_summary_cols['TotEmissionsPerMile_{}'.format(pol)] = df_emissions_summary_cols['TotEmissionsPerMile_{}'.format(pol)] + \
                    df_emissions['PCT_TYPE{}'.format(veh)]*df_emissions['VMT']*df_emissions['{}_{}'.format(veh, pol)]
        
        df_emissions_summary_cols['TotEmissionsPerMile_{}'.format(pol)] = df_emissions_summary_cols['TotEmissionsPerMile_{}'.format(pol)]/df_emissions_summary_cols['tmc_length']                                          
    
    df_emissions_summary = df_emissions_summary_cols.groupby(['tmc', 'road'], dropna=False)
    noise_data = []
    df_emissions_groups = df_emissions.groupby('tmc')
    for t, group in df_emissions_groups:  
        res = runTNMPyAide(group)
        noise_s = {}
        noise_s['tmc'] = t
        if res == 0:
            noise_s['LAeq_WORST_HOUR'] = None
            noise_s['LAeq_24hrs_WORST_HOUR_DATE'] = None
            noise_s['Ldn_WORST_HOUR_DATE'] = None
            noise_s['Lden_WORST_HOUR_DATE'] = None
            noise_s['LAeq_WORST_HOUR_AVG'] = None
            noise_s['LAeq_24hrs_AVG_DAY'] = None
            noise_s['Ldn_AVG_DAY'] = None
            noise_s['Lden_AVG_DAY'] = None
        
        else: 
            
            noise_s['LAeq_WORST_HOUR'] = res[0]
            noise_s['LAeq_24hrs_WORST_HOUR_DATE'] = res[1]
            noise_s['Ldn_WORST_HOUR_DATE'] = res[2]
            noise_s['Lden_WORST_HOUR_DATE'] = res[3]
            noise_s['LAeq_WORST_HOUR_AVG'] = res[4]
            noise_s['LAeq_24hrs_AVG_DAY'] = res[5]
            noise_s['Ldn_AVG_DAY'] = res[6]
            noise_s['Lden_AVG_DAY'] = res[7]    
        
        noise_data.append(noise_s)

    noise_df = pd.DataFrame(data = noise_data)
    df_emissions_summary = df_emissions_summary.agg({'tmc_length':'mean', 'speed_all':'mean', 'aadt':'mean', 'TotEmissionsPerMile_2':'sum', 'TotEmissionsPerMile_3':'sum', 'TotEmissionsPerMile_5':'sum', 'TotEmissionsPerMile_6':'sum', 'TotEmissionsPerMile_87':'sum', 'TotEmissionsPerMile_90':'sum', 'TotEmissionsPerMile_98':'sum', 'TotEmissionsPerMile_100':'sum', 'TotEmissionsPerMile_110':'sum'}).reset_index()
    
    #df_emissions_summary = df_emissions_summary.loc[geo_df['TotEmissions110'] != 0]
    #df_emissions_summary.reset_index(inplace=True)
    df_emissions_summary = pd.merge(df_emissions_summary, noise_df, how='inner', on=('tmc'))
    df_emissions_summary = pd.merge(df_emissions_summary, geo_tmc[['tmc', 'geometry']], how='left', on=('tmc'))
    df_emissions_summary['geometry'] = df_emissions_summary['geometry'].astype('string')
    df_emissions_summary.rename(columns = {'speed_all': 'Average_Speed', 'aadt':'Average AADT'}, inplace=True)
    df_emissions_summary.to_csv(filepath+SELECT_STATE+'_Composite_Emissions_SUMMARY.csv', index=False)
    
    
    #npmrds_emissions = pa.Table.from_pandas(df_emissions)
    df_emissions.to_parquet(filepath+SELECT_STATE+'_Composite_Emissions.parquet', index=False)
    now=lapTimer('  took: ',now)
    '''
    if SELECT_TMC != []:
        print('Filtering to selected TMCs')
        df_emissions_select = df_emissions.loc[df_emissions['tmc'].isin(SELECT_TMC)]
        npmrds_emissions_select = pa.Table.from_pandas(df_emissions_select)
        pq.write_table(npmrds_emissions_select, outputpath+SELECT_STATE+'_Composite_Emissions_select.parquet')
        now=lapTimer('  took: ',now)
    ''' 
    print('Outputs saved in {}'.format(filepath))
    print('**********Process Completed**********')
    print('')

def NPMRDS(SELECT_STATE, PATH_tmc_identification, PATH_npmrds_raw_all, PATH_npmrds_raw_pass, PATH_npmrds_raw_truck, PATH_default_speeds,
           PATH_emission, PATH_TMAS_STATION, PATH_TMAS_CLASS_CLEAN, PATH_FIPS, PATH_NEI, /, 
           PATH_OUTPUT = 'Final Output', AUTO_DETECT_DATES=True, DATE_START=None, DATE_END=None):
    
    
    print('')
    print('********** Process Raw NPMRDS Data **********')

    pathlib.Path(PATH_OUTPUT).mkdir(exist_ok=True)
    #!!! INPUT Parameters
    filepath = PATH_OUTPUT + '/Process1_LinkLevelDataset/'
    pathlib.Path(filepath).mkdir(exist_ok=True)

    tmc = pd.read_csv(PATH_tmc_identification)

    n = len(tmc)
    maxTMCs = 600
    chunkSize = math.ceil(n/math.ceil(n/maxTMCs))

    print(f"{n} tmcs will be processed in {math.ceil(n/maxTMCs)} chunks")
    print("")

    Sample_df = pd.DataFrame()
    Summary_df = pd.DataFrame()

    for i, group in tmc.groupby(np.arange(len(tmc))//chunkSize):

        OUT_PATH = filepath + r'/OUTPUT-chunk{}/'.format(i)
        pathlib.Path(OUT_PATH).mkdir(exist_ok=True)

        newOutput = filepath
        pathlib.Path(newOutput).mkdir(exist_ok=True)

        print("------------------------------------------")
        print(f"Processing Chunk {i} to folder {OUT_PATH}")
        s_buf = io.StringIO()
        group.to_csv(s_buf)
        s_buf.seek(0)
        PATH_tmc_identification = s_buf
        print(len(group))

        NPMRDS_Helper(SELECT_STATE, PATH_tmc_identification, PATH_npmrds_raw_all, PATH_npmrds_raw_pass, 
                      PATH_npmrds_raw_truck, PATH_default_speeds, PATH_emission, PATH_TMAS_STATION, 
                      PATH_TMAS_CLASS_CLEAN, PATH_FIPS, PATH_NEI, PATH_OUTPUT = OUT_PATH)
        
        print("------------------------------------------")
        print(f"Appending chunk {i} to full outputs")
        Sample_chunk = pd.read_csv(OUT_PATH + r'\{}_Composite_Emissions_SAMPLE.csv'.format(SELECT_STATE))
        Summary_chunk = pd.read_csv(OUT_PATH + r'\{}_Composite_Emissions_SUMMARY.csv'.format(SELECT_STATE))
        LinkHourlyChunk = pq.read_table(OUT_PATH + r'\{}_Composite_Emissions.parquet'.format(SELECT_STATE))
        
        Sample_df = pd.concat([Sample_df, Sample_chunk])
        Summary_df = pd.concat([Summary_df, Summary_chunk])

        if i == 0:
            pqwriter = pq.ParquetWriter(newOutput+r'\{}_Composite_Emissions.parquet'.format(SELECT_STATE), LinkHourlyChunk.schema)
        pqwriter.write_table(LinkHourlyChunk)        
    
    Sample_df.to_csv(newOutput + r'{}_Composite_Emissions_SAMPLE.csv'.format(SELECT_STATE), index=False)
    Summary_df.to_csv(newOutput + r'{}_Composite_Emissions_SUMMARY.csv'.format(SELECT_STATE), index=False)    

