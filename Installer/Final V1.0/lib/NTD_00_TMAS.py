# -*- coding: utf-8 -*-
"""
Created by Cambridge Systematics
Modified By: Volpe National Transportation Systems Center

"""
import pandas as pd
import pathlib
import time

def TMAS(SELECT_STATE, PATH_TMAS_STATION, PATH_TMAS_CLASS, PATH_FIPS, PATH_NEI):
    
    filepath = 'TMAS_Intermediate_Output/'
    pathlib.Path(filepath).mkdir(exist_ok=True) 
    
    def lapTimer(text,now):
        print('%s%.3f' %(text,time.time()-now))
        return time.time()
    
    now=time.time()
    print('')
    print ('********** Process Raw TMAS Data **********')
    ##########################################################################################
    # TMAS STATION
    #a.	Read raw TMAS station data to file
    print ('Reading and Processing TMAS Station Data')
    station_width = [1,2,6,1,1,2,2,1,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1,12,6,8,9,4,6,2,2,3,1,12,1,1,8,1,8,50]
    station_name = ['TYPE','FIPS','STATION_ID','DIR','LANE','YEAR','FC_CODE','NO_LANES','SAMPLE_TYPE_VOL','LANES_MON_VOL','METHOD_VOL','SAMPLE_TYPE_CLASS','LANES_MON_CLASS','METHOD_CLASS','ALGO_CLASS','CLASS_SYST','SAMPLE_TYPE_TRCK','LANES_MON_TRCK','METHOD_TRCK','CALIB_TRCK','METHOD_DATA','SENSOR_TYPE','SENSOR_TYPE2','PURPOSE','LRS_ID','LRS_LOC','LAT','LONG','SHRP_ID','PREV_ID','YEAR_EST','YEAR_DISC','FIPS_COUNTY','HPMS_TYPE','HPMS_ID','NHS','PR_SIGNING','PR_NUMBER','CR_SIGNING','CR_NUMBER','LOCATION']
    tmas_station_raw = pd.read_fwf(PATH_TMAS_STATION,widths=station_width,header=None,names=station_name)
    tmas_station=tmas_station_raw[['FIPS','STATION_ID','DIR','LANE','YEAR','FC_CODE','NO_LANES','LRS_ID','LRS_LOC','LAT','LONG','FIPS_COUNTY','HPMS_TYPE','HPMS_ID','NHS','PR_SIGNING','PR_NUMBER','LOCATION']]
    tmas_station=tmas_station.loc[tmas_station['FIPS_COUNTY']!=0]
    #Cleanup TMAS station dataset
    tmas_station['LONG']=(tmas_station['LONG']*-1)/1000000
    tmas_station['LAT']=tmas_station['LAT']/1000000
    tmas_station['F_SYSTEM'] = tmas_station['FC_CODE'].str[0]
    tmas_station['URB_RURAL'] = tmas_station['FC_CODE'].str[1]
    tmas_station.drop_duplicates(subset=['FIPS','STATION_ID','DIR'],inplace=True)
    
    # b. Add composite of state and county names
    # FIPS
    fips_header = ['STATE_NAME','STATE_CODE','COUNTY_CODE','COUNTY_NAME','FIPS_TYPE']
    fips = pd.read_csv(PATH_FIPS,header=None,names=fips_header)
    tmas_station = pd.merge(tmas_station,fips,how='left',left_on=['FIPS','FIPS_COUNTY'],right_on=['STATE_CODE','COUNTY_CODE'])
    # NEI repcty
    repcty = pd.read_csv(PATH_NEI)
    repcty['countyid'] = repcty['countyid'] % 1000
    repcty['repcty'] = repcty['repcty'] % 1000
    tmas_station = pd.merge(tmas_station, repcty, left_on=['FIPS','FIPS_COUNTY'], right_on=['stateid','countyid'], how='inner')
    
    tmas_station=tmas_station[['FIPS','FIPS_COUNTY','STATION_ID','DIR','URB_RURAL','F_SYSTEM','PR_SIGNING','PR_NUMBER','LAT','LONG','LOCATION','STATE_NAME','COUNTY_NAME','repcty']]
    tmas_station.rename(columns={'FIPS': 'STATE', 'FIPS_COUNTY': 'COUNTY', 'PR_SIGNING':'ROUTE_SIGN', 'PR_NUMBER':'ROUTE_NUMBER', 'repcty':'REPCTY'}, inplace=True)
    
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
    class_header = ['TYPE','FIPS','STATION_ID','DIR','LANE','YEAR','MONTH','DAY','HOUR',
                    'VOL','CLASS_1','CLASS_2','CLASS_3','CLASS_4','CLASS_5','CLASS_6','CLASS_7',
                    'CLASS_8','CLASS_9','CLASS_10','CLASS_11','CLASS_12','CLASS_13']
    tmas_types = {
        'TYPE':'category',
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
    tmas_class_raw_test = pd.read_fwf(PATH_TMAS_CLASS,widths=class_width,header=None,names=class_header,
                                      dtype=tmas_types, chunksize=100000)
    tmas_class_raw=pd.concat(tmas_class_raw_test, ignore_index=True)
    #tmas_class_raw.to_csv(filepath+'TMAS_Class.csv', index=False)
    now=lapTimer('  took: ',now)
    
    #b. Cleanup
    #b1. Aggregate lanes: Sum vehicle counts by state, station, date, hour, and direction
    #(b1a. to account for missing lane scenarios)
    print('Aggregating Classification data to link level')
    tmas_class_raw.rename(columns={'FIPS': 'STATE'}, inplace=True)
    tmas_class_sum = tmas_class_raw.groupby(['STATE','STATION_ID','YEAR','MONTH','DAY','HOUR','DIR'])['VOL',
                                       'CLASS_1','CLASS_2','CLASS_3','CLASS_4','CLASS_5','CLASS_6','CLASS_7',
                                       'CLASS_8','CLASS_9','CLASS_10','CLASS_11','CLASS_12','CLASS_13'].sum()
    tmas_class_sum.reset_index(inplace=True)
    now=lapTimer('  took: ',now)
    
    #b2. Clean data from days where stations recorded a total volume of 0.
    print('Cleaning data with a volume of 0 for entire days')
    tmas_day = tmas_class_sum.groupby(['STATE','STATION_ID','YEAR','MONTH','DAY','DIR'])
    clean_tmas = tmas_day.filter(lambda x: x['VOL'].sum()>0)    ## aggregate hourly volumes to daily volumes
    tmas_class_sum = tmas_class_sum.loc[clean_tmas.index]       ## maintain the original index
    now=lapTimer('  took: ',now)
    
    #b3. Join file with TMAS station data
    print('Joining TMAS Station data')
    #tmas_station = pd.read_csv('Temp Files/TMAS_station.csv', dtype={'STATION_ID':str})
    tmas_class = pd.merge(tmas_class_sum, tmas_station, left_on=['STATE','STATION_ID','DIR'], 
                          right_on=['STATE','STATION_ID','DIR'], how='inner')
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
    now=lapTimer('  took: ',now) 
    
    # DOW for holidays
    print ('Other cleanup')
    holidays = ['2015-01-01',	'2015-01-19', '2015-02-16', '2015-05-25', '2015-07-03', '2015-09-07', '2015-11-26', '2015-12-25',
                '2016-01-01',	'2016-01-18', '2016-02-15', '2016-05-30', '2016-07-04', '2016-09-05', '2016-11-24', '2016-12-26',
                '2017-01-01',	'2017-01-16', '2017-02-20', '2017-05-29', '2017-07-04', '2017-09-04', '2017-11-23', '2017-12-25',
                '2018-01-01',	'2018-01-15', '2018-02-19', '2018-05-28', '2018-07-04', '2018-09-03', '2018-11-22', '2018-12-25',
                '2019-01-01',	'2019-01-21', '2019-02-18', '2019-05-27', '2019-07-04', '2019-09-02', '2019-11-28', '2019-12-25',
                '2020-01-01',	'2020-01-20', '2020-02-17', '2020-05-25', '2020-07-03', '2020-09-07', '2020-11-26', '2020-12-25',
                '2021-01-01',	'2021-01-18', '2021-02-15', '2021-05-31', '2021-07-05', '2021-09-06', '2021-11-25', '2021-12-24']

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
                                 'COUNTY','REPCTY','ROUTE_SIGN','ROUTE_NUMBER','LAT','LONG','STATE_NAME','COUNTY_NAME','LOCATION',
                                 'HPMS_TYPE10','HPMS_TYPE25','HPMS_TYPE40','HPMS_TYPE50','HPMS_TYPE60','HPMS_ALL',
                                 'NOISE_AUTO','NOISE_MED_TRUCK','NOISE_HVY_TRUCK','NOISE_BUS','NOISE_MC','NOISE_ALL']]
    now=lapTimer('  took: ',now) 
    
    #c. Save the final dataset in "Temp Files" to merge it with TMC link later
    #This took 15 min
    print('Exporting classification data as a csv')
    tmas_class_clean.to_csv(filepath+'tmas_class_clean.csv',index=False)
    tmas_class_clean_sample = tmas_class_clean[0:10]
    tmas_class_clean_sample.to_csv(filepath+'tmas_class_clean_sample.csv',index=False)
    #tmas_class_clean.to_csv(PATH_TMAS_CLASS.replace('.dat','_clean.csv'),index=False)
    now=lapTimer('  took: ',now)
    print('Outputs saved in {}'.format(filepath))
    print('**********Process Completed**********')
    print('')