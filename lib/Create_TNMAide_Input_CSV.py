# -*- coding: utf-8 -*-
"""
Created on Thu Jun  9 11:53:37 2022

@author: william.chupp
"""

# -*- coding: utf-8 -*-
"""
Created on Wed Jun  8 11:11:45 2022

@author: william.chupp
"""

# -*- coding: utf-8 -*-
"""
Created By: Volpe National Transportation Systems Center

"""
import pandas as pd
import pyarrow.parquet as pq

Summary_df = pd.DataFrame()
    
Summary_df = pd.read_csv(r'D:\Project\DANA\Data\TNMAide Input Samples\GA_Composite_Emissions_SUMMARY_NOISE_2019.csv')
    
link_file = pq.ParquetFile(r'D:\Project\DANA\Data\TNMAide Input Samples\GA_Composite_Emissions.parquet')

linkLevel_df = pd.DataFrame()

readColumns = ['tmc', 'year', 'month', 'day', 'hour', 'road', 'direction', 'state', 'county', 'start_latitude',
                'start_longitude', 'end_latitude', 'end_longitude', 'tmc_length', 'road_order',
                'f_system', 'thrulanes', 'aadt', 'aadt_singl', 'aadt_combi', 'travel_time_all',
                'travel_time_pass', 'travel_time_truck', 'speed_all', 'speed_pass', 'speed_truck',
                'PCT_NOISE_AUTO', 'PCT_NOISE_MED_TRUCK', 'PCT_NOISE_HVY_TRUCK', 'PCT_NOISE_BUS',
                'PCT_NOISE_MC']

print("reading in link level data")
f = link_file.read(columns = readColumns)
linkLevel_df = f.to_pandas(categories = ['tmc'])
del f

tmc = '101+07005' # No noise data - including worst hour
tmc = '101+05209' # No average worst hour
group = linkLevel_df[linkLevel_df.tmc==tmc]
group['tmc'] = group['tmc'].astype(str)
group.reset_index(inplace=True)
group.loc[:,'date'] = pd.to_datetime(group[['year', 'month', 'day']])
group_tmc2 = group.copy()
group_tmc2['tmc'] = group['tmc'] + '_'
group_tmc2['aadt'] = 1
group = group.append(group_tmc2)
group.reset_index(inplace=True)
group['thrulanes'].max()
group.rename(columns = {'tmc' : 'TMC', 'road' : 'ROAD', 'direction' : 'DIRECTION',                     
                        'county' : 'COUNTY', 'state' : 'STATE', 'hour' : 'HOUR',                 
                        'date' : 'DATE', 'PCT_NOISE_MED_TRUCK' : 'PCT_NOISE_MEDIUM',                     
                        'PCT_NOISE_HVY_TRUCK' : 'PCT_NOISE_HEAVY',
                        'speed_pass' : 'SPEED_PASS', 'speed_truck' : 'SPEED_TRUCK',                  
                        'speed_all' : 'SPEED_ALL_vehs'}, inplace=True)

utmc = pd.unique(linkLevel_df.tmc).categories 

group.to_csv(r'D:\Project\DANA\Data\TNMAide Input Samples\\' + tmc + '.csv')       