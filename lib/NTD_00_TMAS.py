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
from geopandas import GeoDataFrame
from shapely.geometry import Point
import pathlib
import pkg_resources
import requests
import json
import sys
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from tqdm.asyncio import tqdm
from multiprocessing import Pool, TimeoutError
from pandas.tseries.holiday import USFederalHolidayCalendar

from arcgis.features import FeatureLayer
from arcgis import geometry

def f2(chunk):
    return chunk

def f(tmas_station):
    
    tmas_station = tmas_station[1]
    
    states = {
    'AK':['Alaska', 2],'AL':['Alabama',1],'AZ':['Arizona',4],'AR':['Arkansas',5],'CA':['California',6],'CO':['Colorado',8],
    'CT':['Connecticut',9],'DE':['Delaware',10],'DC':['District of Columbia',11],'FL':['Florida',12],'GA':['Georgia',13],'HI':['Hawaii',15],'ID':['Idaho',16],'IL':['Illinois',17],'IN':['Indiana',18],'IA':['Iowa',19],'KS':['Kansas',20],
    'KY':['Kentucky',21],'LA':['Louisiana',22],'ME':['Maine',23],'MD':['Maryland',24],'MA':['Massachusetts',25],
    'MI':['Michigan',26],'MN':['Minnesota',27],'MS':['Mississippi',28],'MO':['Missouri',29],'MT':['Montana',30],'NE':['Nebraska',31],
    'NV':['Nevada',32],'NH':['New Hampshire',33],'NJ':['New Jersey',34],'NM':['New Mexico',35],'NY':['New York',36],
    'NC':['North Carolina',37],'ND':['North Dakota',38],'OH':['Ohio',39],'OK':['Oklahoma',40],'OR':['Oregon',41],
    'PA':['Pennsylvania',42],'RI':['Rhode Island',44],'SC':['South Carolina',46],'SD':['South Dakota',46],
    'TN':['Tennessee',47],
    'TX':['Texas', 48], 
    'UT':['Utah',49],'VT':['Vermont',50],'VA':['Virginia',51],
    'WA':['Washington',53],'WV':['West Virginia',54],'WI':['Wisconsin',55],'WY':['Wyoming',56]}
    
    
    
    state_code = tmas_station['FIPS']
    state = ''
    for key, value in states.items():
        if value[1] == state_code:
            if key == 'SD': state = 'SD'
            else: state = key
    if state == '':
        return tmas_station

    long = tmas_station['LONG']
    lat = tmas_station['LAT']

    url = "https://geo.dot.gov/server/rest/services/Hosted/HPMS_Full_{}_2022/FeatureServer/0".format(state)
    result = 0
    for i in range(5):
        try:
            Layer = FeatureLayer(url)
            result = Layer.query(geometry="{},{}".format(long, lat), units = 'esriSRUnit_Meter', distance = 20)
        except:
            pass
        if result != 0:
            break
    # if result == 0:
    #     url = "https://geo.dot.gov/server/rest/services/Hosted/HPMS_Full_{}_2019/FeatureServer/0".format(state)
    #     result = 0
    #     for i in range(10):
    #         try:
    #             Layer = FeatureLayer(url)
    #             result = Layer.query(geometry="{},{}".format(long, lat), units = 'esriSRUnit_Meter', distance = 20)
    #         except:
    #             pass
    #         if result != 0:
    #             break
    if result == 0:
        raise Exception('RuntimeError', 'URL {} did not work'.format(url))
    gjson_string = result.to_geojson
    gjson_dict = json.loads(gjson_string)
    links = GeoDataFrame.from_features(gjson_dict['features'])
    links.columns = links.columns.str.lower()
    if len(links) == 1:
        link = links.loc[0, :]
        tmas_station.loc['FIPS_COUNTY'] = link.loc['county_code']
        tmas_station.loc['URBAN_CODE'] = link.loc['urban_code']
        tmas_station.loc['F_SYSTEM'] = link.loc['f_system']
        tmas_station.loc['PR_SIGNING'] = link.loc['route_signing']
        tmas_station.loc['PR_NUMBER'] = link.loc['route_number']
    elif len(links) < 1:
        pass
    elif len(links) > 1:
        if 'route_name' in links.columns:
            links_test = links.loc[links['route_name'].str.isspace()==False].reset_index()
            if (len(links_test['route_name'].unique())) == 1:
                link = links_test.loc[0, :]
                tmas_station.loc['FIPS_COUNTY'] = link.loc['county_code']
                tmas_station.loc['URBAN_CODE'] = link.loc['urban_code']
                tmas_station.loc['F_SYSTEM'] = link.loc['f_system']
                tmas_station.loc['PR_SIGNING'] = link.loc['route_signing']
                tmas_station.loc['PR_NUMBER'] = link.loc['route_number']
                
                return tmas_station
        if 'route_id' in links.columns:
            links_test = links.loc[links['route_id'].str.isspace()==False].reset_index()
            if (len(links_test['route_id'].unique())) == 1:
                link = links_test.loc[0, :]
                tmas_station.loc['FIPS_COUNTY'] = link.loc['county_code']
                tmas_station.loc['URBAN_CODE'] = link.loc['urban_code']
                tmas_station.loc['F_SYSTEM'] = link.loc['f_system']
                tmas_station.loc['PR_SIGNING'] = link.loc['route_signing']
                tmas_station.loc['PR_NUMBER'] = link.loc['route_number']
                
                return tmas_station
        if 'route_number_t' in links.columns:
            links = links.loc[links['route_number_t'].str.isspace()==False].reset_index()
            if (len(links['route_number_t'].unique())) == 1:
                link = links.loc[0, :]
                tmas_station.loc['FIPS_COUNTY'] = link.loc['county_code']
                tmas_station.loc['URBAN_CODE'] = link.loc['urban_code']
                tmas_station.loc['F_SYSTEM'] = link.loc['f_system']
                tmas_station.loc['PR_SIGNING'] = link.loc['route_signing']
                tmas_station.loc['PR_NUMBER'] = link.loc['route_number']
                
                return tmas_station
        elif len(links) < 1:
            pass
        else:    
            route = pd.to_numeric(str(tmas_station['PR_NUMBER']).replace('I', ''), errors='coerce')
            for index, link in links.iterrows():
                if link['route_number']==route:
                    
                    tmas_station.loc['FIPS_COUNTY'] = link.loc['county_code']
                    tmas_station.loc['URBAN_CODE'] = link.loc['urban_code']
                    tmas_station.loc['F_SYSTEM'] = link.loc['f_system']
                    tmas_station.loc['PR_SIGNING'] = link.loc['route_signing']
                    tmas_station.loc['PR_NUMBER'] = link.loc['route_number']
                    
                    return tmas_station
    else:
        pass
    
    return tmas_station

def TMAS(SELECT_STATE, PATH_TMAS_STATION, PATH_TMAS_CLASS, PATH_FIPS, PATH_NEI, /, PATH_OUTPUT = 'Final Output', PREREADSTATION = False):
    pathlib.Path(PATH_OUTPUT).mkdir(exist_ok=True)
    filepath = PATH_OUTPUT + '/TMAS_Intermediate_Output/'
    pathlib.Path(filepath).mkdir(exist_ok=True) 
    
    def lapTimer(text,now):
        print('%s%.3f' %(text,time.time()-now))
        return time.time()
    
    now=time.time()
    
    if PREREADSTATION:
        print("Reading Pre-Processed TMAS Station Data")
        tmas_station = pd.read_csv(PATH_TMAS_STATION)
    else:    
        
        print('')
        print ('********** Process Raw TMAS Data **********')
        ##########################################################################################
        # TMAS STATION
        #a.	Read raw TMAS station data to file
        print ('Reading and Processing TMAS Station Data')
        station_width = [1,2,6,1,1,2,2,1,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1,12,6,8,9,4,6,2,2,3,1,12,1,1,8,1,8,50]
        station_name = ['TYPE','FIPS','STATION_ID','DIR','LANE','YEAR','FC_CODE','NO_LANES','SAMPLE_TYPE_VOL','LANES_MON_VOL','METHOD_VOL','SAMPLE_TYPE_CLASS','LANES_MON_CLASS','METHOD_CLASS','ALGO_CLASS','CLASS_SYST','SAMPLE_TYPE_TRCK','LANES_MON_TRCK','METHOD_TRCK','CALIB_TRCK','METHOD_DATA','SENSOR_TYPE','SENSOR_TYPE2','PURPOSE','LRS_ID','LRS_LOC','LAT','LONG','SHRP_ID','PREV_ID','YEAR_EST','YEAR_DISC','FIPS_COUNTY','HPMS_TYPE','HPMS_ID','NHS','PR_SIGNING','PR_NUMBER','CR_SIGNING','CR_NUMBER','LOCATION']
        tmas_station_raw = pd.read_fwf(PATH_TMAS_STATION,widths=station_width,header=None,names=station_name, engine='python')
     
        tmas_station_locs = tmas_station_raw[['FIPS', 'STATION_ID', 'LAT', 'LONG', 'DIR', 'PR_NUMBER']]
        tmas_station_locs.loc[:, 'LONG']=(tmas_station_locs['LONG']*-1)/1000000
        tmas_station_locs.loc[:, 'LAT']=tmas_station_locs['LAT']/1000000
        
        #t = f((472, tmas_station_locs.loc[2, :]))
    
        n = len(tmas_station_locs)
        with Pool(5) as p:
            tmas_station = pd.DataFrame()
            print("    starting processing")
            for sta in tqdm(p.imap(f, tmas_station_locs.iterrows(), chunksize=30), total=n):
                tmas_station = pd.concat([tmas_station, sta])   
        
        tmas_station.drop_duplicates(subset=['FIPS','STATION_ID','DIR'],inplace=True)
        
        fips_header = ['STATE_NAME','STATE_CODE','COUNTY_CODE','COUNTY_NAME','FIPS_TYPE']
        fips = pd.read_csv(PATH_FIPS,header=None,names=fips_header)
        tmas_station = pd.merge(tmas_station,fips,how='left',left_on=['FIPS','FIPS_COUNTY'],right_on=['STATE_CODE','COUNTY_CODE'])
        
        # NEI repcty
        repcty = pd.read_csv(PATH_NEI)
        repcty['countyid'] = repcty['countyid'] % 1000
        repcty['repcty'] = repcty['repcty'] % 1000
        tmas_station = pd.merge(tmas_station, repcty, left_on=['FIPS','FIPS_COUNTY'], right_on=['stateid','countyid'], how='left')
        
        
        tmas_station.loc[tmas_station['URBAN_CODE']<99999, 'URB_RURAL']='U'
        tmas_station.loc[tmas_station['URBAN_CODE']>=99999, 'URB_RURAL']='R'
        
        tmas_station=tmas_station[['FIPS','FIPS_COUNTY','STATION_ID','DIR','URB_RURAL','F_SYSTEM','PR_SIGNING','PR_NUMBER','LAT','LONG','STATE_NAME','COUNTY_NAME','repcty']]
        tmas_station.rename(columns={'FIPS': 'STATE', 'FIPS_COUNTY': 'COUNTY', 'PR_SIGNING':'ROUTE_SIGN', 'PR_NUMBER':'ROUTE_NUMBER', 'repcty':'REPCTY'}, inplace=True)
    
    
    tmas_station = tmas_station.loc[~tmas_station['COUNTY'].isnull()].reset_index()
    #c.	Select only desired State
    tmas_station_State = tmas_station[tmas_station['STATE_NAME']==SELECT_STATE]
    tmas_station_State.reset_index(inplace=True, drop=True)
    
    #d. Export as csv files in "Temp files"
    tmas_station.to_csv(filepath+'TMAS_station.csv',index=False)
    tmas_station_State.to_csv(filepath+'TMAS_station_State.csv',index=False)
    #tmas_station.to_csv(PATH_TMAS_STATION.replace('.dat','.csv'), index=False)
    #tmas_station_State.to_csv(PATH_TMAS_STATION.replace('.dat','_State.csv'), index=False)
    
    now=lapTimer('  took: ',now)
        
    ##########################################################################################
    # TMAS CLASS
    #a. Read in TMAS Class dataset

    print ('Reading TMAS Classification data')
    class_width = [1,2,6,1,1,2,2,2,2,5,5,5,5,5,5,5,5,5,5,5,5,5,5]
    class_header = ['TYPE','STATE','STATION_ID','DIR','LANE','YEAR','MONTH','DAY','HOUR',
                    'VOL','CLASS_1','CLASS_2','CLASS_3','CLASS_4','CLASS_5','CLASS_6','CLASS_7',
                    'CLASS_8','CLASS_9','CLASS_10','CLASS_11','CLASS_12','CLASS_13']
    tmas_types = {
        'TYPE':'category',
        'STATE': 'Int64',
        'FIPS':'float16',
        'STATION_ID':str,
        'DIR':'float16',
        'LANE':'float16',
        'YEAR':'float16',
        'MONTH':'float16',
        'DAY':'float16',
        'HOUR':'float16',
        'VOL':'float16',
        'CLASS_1':'float16',
        'CLASS_2':'float16',
        'CLASS_3':'float16',
        'CLASS_4':'float16',
        'CLASS_5':'float16',
        'CLASS_6':'float16',
        'CLASS_7':'float16',
        'CLASS_8':'float16',
        'CLASS_9':'float16',
        'CLASS_10':'float16',
        'CLASS_11':'float16',
        'CLASS_12':'float16',
        'CLASS_13':'float16',
        }
    tmas_class_raw = dd.read_fwf(PATH_TMAS_CLASS,widths=class_width,header=None,names=class_header,
                                      dtype=tmas_types) #.head(n=100000)# chunksize=100000)
    
    # tmas_class_raw = pd.DataFrame()
    # i = 0
    # for chunk in tqdm(tmas_class_raw_test):
    #     tmas_class_raw = tmas_class_raw.append(chunk, ignore_index=True)
    # #tmas_class_raw.to_csv(filepath+'TMAS_Class.csv', index=False)
    # now=lapTimer('  took: ',now)
    
    #b. Cleanup
    #b1. Aggregate lanes: Sum vehicle counts by state, station, date, hour, and direction
    #(b1a. to account for missing lane scenarios)
    print('Aggregating Classification data to link level')
    #tmas_class_raw.rename(columns={'FIPS': 'STATE'}, inplace=True)
    tmas_class_sum = tmas_class_raw.groupby(['STATE','STATION_ID','YEAR','MONTH','DAY','HOUR','DIR'])['VOL',
                                       'CLASS_1','CLASS_2','CLASS_3','CLASS_4','CLASS_5','CLASS_6','CLASS_7',
                                       'CLASS_8','CLASS_9','CLASS_10','CLASS_11','CLASS_12','CLASS_13'].sum()
    tmas_class_sum.reset_index(drop=True)
    ProgressBar().register()
    tmas_class_sum = tmas_class_sum.compute()
    now=lapTimer('  took: ',now)
    
    #b2. Clean data from days where stations recorded a total volume of 0.
    print('Cleaning data with a volume of 0 for entire days')
    tmas_day = tmas_class_sum.groupby(['STATE','STATION_ID','YEAR','MONTH','DAY','DIR'])
    clean_tmas = tmas_day.filter(lambda x: x['VOL'].sum()>0)    ## aggregate hourly volumes to daily volumes
    tmas_class_sum = tmas_class_sum.loc[clean_tmas.index]       ## maintain the original index
    tmas_class_sum.reset_index(inplace=True)
    del tmas_day
    del clean_tmas
    now=lapTimer('  took: ',now)
    
    #b3. Join file with TMAS station data
    print('Joining TMAS Station data')
    #tmas_station = pd.read_csv('Temp Files/TMAS_station.csv', dtype={'STATION_ID':str})
    tmas_class = pd.merge(tmas_class_sum, tmas_station, left_on=['STATE','STATION_ID','DIR'], 
                          right_on=['STATE','STATION_ID','DIR'], how='inner')
    del tmas_class_sum
    now=lapTimer('  took: ',now)
    
    #b4. Identify the peaking time period for the location and direction.
    # Create a new day of week variable 'DAY_TYPE' with two levels: Weekday & Weekend
    print('Creating the Day of Week field')
    tmas_class[['MONTH', 'DAY', 'YEAR']] = tmas_class[['MONTH', 'DAY', 'YEAR']].astype(str).replace('\.0', '', regex=True)
    tmas_class[['MONTH', 'DAY', 'YEAR']] = tmas_class[['MONTH', 'DAY', 'YEAR']].astype(str)
    tmas_class['DATE'] = tmas_class['MONTH']+'-'+tmas_class['DAY']+'-20'+tmas_class['YEAR']
    tmas_class['DATE'] = pd.to_datetime(tmas_class['DATE'], format='%m-%d-%Y')
    tmas_class['DOW'] = tmas_class['DATE'].dt.dayofweek +1
    tmas_class['DAY_TYPE'] = ''
    tmas_class.loc[tmas_class['DOW']<=5,'DAY_TYPE'] = 'WD'
    tmas_class.loc[tmas_class['DOW']>5,'DAY_TYPE'] = 'WE'
    now=lapTimer('  took: ',now)
    
    print ('Creating the Peaking data field')
    tmas_class_peak = tmas_class.loc[(tmas_class['DAY_TYPE']=='WD') & (tmas_class['HOUR']==8)].groupby(['STATE',
                               'STATION_ID','DIR'], as_index=False)['VOL'].mean()
    tmas_class_peak['VOL_MAX'] = tmas_class_peak.groupby(['STATE', 'STATION_ID'])['VOL'].transform('max')
    tmas_class_peak.loc[tmas_class_peak['VOL']==tmas_class_peak['VOL_MAX'], 'PEAKING']='AM'
    tmas_class_peak.loc[tmas_class_peak['PEAKING']!='AM', 'PEAKING']='PM'
    tmas_class_peak.drop(columns=['VOL', 'VOL_MAX'], inplace=True, axis=1)
    tmas_class = pd.merge(tmas_class, tmas_class_peak, left_on=['STATE','STATION_ID','DIR'], right_on=['STATE','STATION_ID','DIR'], how='left')
    # default is 'PM' in case no peaking assignment with WD & HOUR 8
    tmas_class.loc[~tmas_class['PEAKING'].isin(['AM','PM']), 'PEAKING']='PM'
    del tmas_class_peak
    now=lapTimer('  took: ',now) 
    
    # DOW for holidays
    print ('Other cleanup')
    
    cal = USFederalHolidayCalendar()
    holidaysIndex = cal.holidays()
    holidays = [x.strftime('%Y-%m-%d') for x in holidaysIndex.to_series()]

    tmas_class.loc[tmas_class['DATE'].isin(holidays), 'DOW'] = 8
    tmas_class.loc[tmas_class['DOW']==8,'DAY_TYPE'] = 'WE'

    now=lapTimer('  took: ',now) 
    
    #b6. Assign MOVES and Noise Analysis Vehicle Types
    #   i.	HPMS_TYPE10 = CLASS1
    #   ii.	HPMS_TYPE25 = CLASS2 + CLASS3
    #   iii.	 HPMS_TYPE40 = CLASS4
    #   iv.	 HPMS_TYPE50 = CLASS5 + CLASS6 + CLASS7
    #   v.	 HPMS_TYPE60 = CLASS8 + CLASS9 + CLASS10 + CLASS11 + CLASS12 + CLASS13
    #   vi.	 NOISE_AUTO  = CLASS2 + CLASS3
    #   vii.	 NOISE_MED_TRUCK = CLASS5
    #   viii.	 NOISE_HVY_TRUCK = CLASS6 + CLASS7 + CLASS8 + CLASS9 + CLASS10 + CLASS11 + CLASS12 + CLASS13
    #   ix.	 NOISE_BUS = CLASS4
    #   x.	 NOISE_MC = CLASS1
    
    print ('Creating the HPMS and Noise classifications')
    tmas_class['HPMS_TYPE10'] = tmas_class['CLASS_1']
    tmas_class['HPMS_TYPE25'] = tmas_class['CLASS_2'] + tmas_class['CLASS_3']
    tmas_class['HPMS_TYPE40'] = tmas_class['CLASS_4']
    tmas_class['HPMS_TYPE50'] = tmas_class['CLASS_5'] + tmas_class['CLASS_6'] + tmas_class['CLASS_7']
    tmas_class['HPMS_TYPE60'] = tmas_class['CLASS_8'] + tmas_class['CLASS_9'] + tmas_class['CLASS_10'] + tmas_class['CLASS_11'] + tmas_class['CLASS_12'] + tmas_class['CLASS_13']
    tmas_class['HPMS_ALL'] = tmas_class['HPMS_TYPE10']+tmas_class['HPMS_TYPE25']+tmas_class['HPMS_TYPE40']+tmas_class['HPMS_TYPE50']+tmas_class['HPMS_TYPE60']
    tmas_class['NOISE_AUTO'] = tmas_class['CLASS_2'] + tmas_class['CLASS_3']
    tmas_class['NOISE_MED_TRUCK'] = tmas_class['CLASS_5']
    tmas_class['NOISE_HVY_TRUCK'] = tmas_class['CLASS_6'] + tmas_class['CLASS_7'] + tmas_class['CLASS_8'] + tmas_class['CLASS_9'] + tmas_class['CLASS_10'] + tmas_class['CLASS_11'] + tmas_class['CLASS_12'] + tmas_class['CLASS_13']
    tmas_class['NOISE_BUS'] = tmas_class['CLASS_4']
    tmas_class['NOISE_MC'] = tmas_class['CLASS_1']
    tmas_class['NOISE_ALL'] = tmas_class['NOISE_AUTO']+tmas_class['NOISE_MED_TRUCK']+tmas_class['NOISE_HVY_TRUCK']+tmas_class['NOISE_BUS']+tmas_class['NOISE_MC']
    
    tmas_class_clean=tmas_class[['STATE','STATION_ID','DIR','DATE','YEAR','MONTH','DAY','HOUR','DAY_TYPE','PEAKING','VOL','F_SYSTEM','URB_RURAL',
                                 'COUNTY','REPCTY','ROUTE_SIGN','ROUTE_NUMBER','LAT','LONG','STATE_NAME','COUNTY_NAME',
                                 'HPMS_TYPE10','HPMS_TYPE25','HPMS_TYPE40','HPMS_TYPE50','HPMS_TYPE60','HPMS_ALL',
                                 'NOISE_AUTO','NOISE_MED_TRUCK','NOISE_HVY_TRUCK','NOISE_BUS','NOISE_MC','NOISE_ALL']]
    now=lapTimer('  took: ',now) 
    
    #c. Save the final dataset in "Temp Files" to merge it with TMC link later
    #This took 15 min
    print('Exporting classification data as a csv')
    tmas_class_clean.to_csv(filepath+'tmas_class_clean.csv',index=False, chunksize=10000)
    tmas_class_clean_sample = tmas_class_clean[0:10]
    tmas_class_clean_sample.to_csv(filepath+'tmas_class_clean_sample.csv',index=False)
    #tmas_class_clean.to_csv(PATH_TMAS_CLASS.replace('.dat','_clean.csv'),index=False)
    now=lapTimer('  took: ',now)
    print('Outputs saved in {}'.format(filepath))
    print('**********Process Completed**********')
    print('')