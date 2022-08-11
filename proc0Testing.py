# -*- coding: utf-8 -*-
"""
Created on Thu Aug 11 09:05:39 2022

@author: william.chupp
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
ProgressBar().register()
from tqdm.asyncio import tqdm
from multiprocessing import Pool, TimeoutError
from pandas.tseries.holiday import USFederalHolidayCalendar

from arcgis.features import FeatureLayer
from arcgis import geometry

from lib import NTD_00_TMAS

PATH_TMAS_STATION = r'H:\DANATool\TMAS 2021\TMAS_Station_2021_1.dat'

station_width = [1,2,6,1,1,2,2,1,1,1,1,1,1,1,1,2,1,1,1,1,1,1,1,1,12,6,8,9,4,6,2,2,3,1,12,1,1,8,1,8,50]
station_name = ['TYPE','FIPS','STATION_ID','DIR','LANE','YEAR','FC_CODE','NO_LANES','SAMPLE_TYPE_VOL','LANES_MON_VOL','METHOD_VOL','SAMPLE_TYPE_CLASS','LANES_MON_CLASS','METHOD_CLASS','ALGO_CLASS','CLASS_SYST','SAMPLE_TYPE_TRCK','LANES_MON_TRCK','METHOD_TRCK','CALIB_TRCK','METHOD_DATA','SENSOR_TYPE','SENSOR_TYPE2','PURPOSE','LRS_ID','LRS_LOC','LAT','LONG','SHRP_ID','PREV_ID','YEAR_EST','YEAR_DISC','FIPS_COUNTY','HPMS_TYPE','HPMS_ID','NHS','PR_SIGNING','PR_NUMBER','CR_SIGNING','CR_NUMBER','LOCATION']

tmas_station_raw = pd.read_fwf(PATH_TMAS_STATION,widths=station_width,header=None,names=station_name, engine='python')

tmas_station_locs = tmas_station_raw[['FIPS', 'STATION_ID', 'LAT', 'LONG', 'DIR', 'PR_NUMBER']]
tmas_station_locs.loc[:, 'LONG']=(tmas_station_locs['LONG']*-1)/1000000
tmas_station_locs.loc[:, 'LAT']=tmas_station_locs['LAT']/1000000

tmas_station_locs

sta = tmas_station_locs.loc[3412, :]

print(NTD_00_TMAS.f((1, sta)))