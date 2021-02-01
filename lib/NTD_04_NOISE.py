# -*- coding: utf-8 -*-
"""
Created on Mon Nov 25 09:33:37 2019

@author: rge
"""
import pandas as pd
import numpy as np
import datetime as dt
import pyarrow as pa
import pyarrow.parquet as pq
import time
import pathlib

def NOISE(SELECT_STATE, SELECT_TMC, PATH_NPMRDS):
    #!!! INPUT Parameters
    #pathlib.Path(filepath).mkdir(exist_ok=True) 
    outputpath = 'Final Output/Process4_TNM_AIDE_Inputs/'
    pathlib.Path(outputpath).mkdir(exist_ok=True) 
    
    #SELECT_STATE='CO'
    #PATH_NPMRDS='Output/CO_Composite_Emissions.parquet'
    #SELECT_TMC=['116+08517']
    
    #PATH_emission='Data Input/MOVES_Rates/nhs lpp rates_'+SELECT_STATE+'_wbt.csv'
    
    def lapTimer(text,now):
        print('%s%.3f' %(text,time.time()-now))
        return time.time()
    
    now=time.time()
    
    print('')
    print ('********** Produce Noise Model Inputs **********')
    ##########################################################################################
    if SELECT_TMC != []:
        print ('Reading Composite Dataset')
        df_emissions = pq.read_table(PATH_NPMRDS)
        df_emissions = df_emissions.to_pandas()
        #df=pd.read_csv(PATH_NPMRDS)
        now=lapTimer('  took: ',now)
    
        print('Filtering to selected TMCs')
        df_emissions_select = df_emissions.loc[df_emissions['tmc'].isin(SELECT_TMC)]
        df_emissions_select.loc[:,'date'] = pd.to_datetime(df_emissions_select[['year', 'month', 'day']])
        #df_emissions_select['date'] = str(df_emissions_select['year'])+'/'+str(df_emissions_select['month'])+'/'+str(df_emissions_select['day'])
        df_emissions_select = df_emissions_select[['tmc', 'date', 'hour', 'road', 'direction', 'state', 'county', 'start_latitude',
                                                   'start_longitude', 'end_latitude', 'end_longitude', 'tmc_length', 'road_order',
                                                   'f_system', 'thrulanes', 'aadt', 'aadt_singl', 'aadt_combi', 'travel_time_all',
                                                   'travel_time_pass', 'travel_time_truck', 'speed_all', 'speed_pass', 'speed_truck',
                                                   'PCT_NOISE_AUTO', 'PCT_NOISE_MED_TRUCK', 'PCT_NOISE_HVY_TRUCK', 'PCT_NOISE_BUS',
                                                   'PCT_NOISE_MC']]
        
        df_emissions_select.to_csv(outputpath+SELECT_STATE+'_Composite_Emissions_select.csv', index=False)
        now=lapTimer('  took: ',now)
        print('Outputs saved in {}\\'.format(outputpath))
    else:
        print('No TMC input provided.')
        
    print('**********Process Completed**********')
    print('')