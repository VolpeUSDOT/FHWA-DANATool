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
import pathlib
from tqdm import tqdm

import os

def load_shape(folder):
    
    national_shape = GeoDataFrame()
    for subdir, dirs, files in os.walk(folder):
        for filename in files:
            filepath = subdir + os.sep + filename
    
            if filepath.endswith(".shp"):
                state_shape = gpd.read_file(filepath)
                national_shape = pd.concat([national_shape, state_shape])
    
    return national_shape