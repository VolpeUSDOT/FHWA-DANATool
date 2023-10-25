"""
Created By: Volpe National Transportation Systems Center

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
from shapely import wkt
import pathlib

import os

def load_shape(folder):
    
    national_shape = GeoDataFrame()
    for subdir, dirs, files in os.walk(folder):
        for filename in files:
            filepath = subdir + os.sep + filename
    
            if filepath.endswith(".shp"):
                state_shape = gpd.read_file(filepath)
                national_shape = national_shape.append(state_shape)
    
    return national_shape

def load_shape_csv(folder, crs):
    
    national_shape = pd.DataFrame()
    for subdir, dirs, files in os.walk(folder):
        for filename in files:
            filepath = subdir + os.sep + filename
            if filepath.endswith(".csv"):
                state_shape = pd.read_csv(filepath)
                national_shape = pd.concat([national_shape, state_shape])
    
    national_shape['geometry'] = national_shape['geometry'].apply(wkt.loads)
    national_shape = gpd.GeoDataFrame(national_shape, geometry=national_shape['geometry'], crs=crs)
    return national_shape