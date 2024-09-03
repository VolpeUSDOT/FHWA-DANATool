

import pandas as pd
import pyarrow.parquet as pq
import dask
import dask.dataframe as dd
from dask.diagnostics import progress
from dask.diagnostics import profile
from collections import namedtuple
import numpy as np
import time

from .TNMPyAide.TNMPyAide import TNMPyAide

def get_TNMPyAide_inputs(PATH_NPMRDS, TMC1, TMC2=None):

    def lapTimer(text,now):
        print('%s%.3f' %(text,time.time()-now))
        return time.time()
    
    now=time.time()

    print("Reading NPMRDS Clean Link Data")
    #fullLinkLevel = pq.read_table(PATH_NPMRDS, memory_map=True)
    fullLinkLevel = dd.read_parquet(PATH_NPMRDS,)

    print("Processing Data. This may take a while.")
    #fullLinkLevel = fullLinkLevel.to_pandas()
    TMC1group = fullLinkLevel.loc[fullLinkLevel['tmc']==TMC1]
    TMC2group = fullLinkLevel.loc[fullLinkLevel['tmc']==TMC2]
    TMC1group, TMC2group = dask.compute(TMC1group, TMC2group, scheduler='threads')
    now=lapTimer('  took: ',now)
    group = pd.concat([TMC1group, TMC2group], axis=0, ignore_index=True).reset_index(drop=True)
    return group

def call_TNMAide(group, link_grade, median_width, number_of_lanes):
    print("setting up TNMAide Inputs")
    group.columns = group.columns.str.upper()

    TMC1 = group['TMC'].unique()[0]
    if len(group['TMC'].unique()) > 1:
        TMC2 = group['TMC'].unique()[1]
    
        road1 = group['ROAD'].unique()[0]
        if len(group['ROAD'].unique()) > 1:
            road2 = group['ROAD'].unique()[1]
        else: road2 = road1
        state = group['STATE'].unique()[0]
        county = group['COUNTY'].unique()[0]

        group['DATE'] = group['MEASUREMENT_TSTAMP'].dt.date
            
        meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
        meta = meta_data(road1, road2, TMC1, TMC2, state, county)
    
    else:
        meta_data = namedtuple('meta_data', 'L1_name L1_tmc state county')
        meta = meta_data(group.iloc[0]['ROAD'], TMC1, group['STATE'].unique()[0], group['COUNTY'].unique()[0])
    
    group['DATE'] = group['MEASUREMENT_TSTAMP'].dt.date
    print("Calculating TNMAide")
    if len(group['MAADT'].unique()) == 1 and np.isnan(group['MAADT'].unique()[0]):
        raise(ValueError("MAADT values are NaN in the link level dataset. Please check the input data and try again."))
    else:
        
        result =  TNMPyAide(group, link_grade, meta, median_width, number_of_lanes, detailed_log = False)   
        return result