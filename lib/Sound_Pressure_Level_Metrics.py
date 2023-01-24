# -*- coding: utf-8 -*-
"""
Created By: Volpe National Transportation Systems Center
Created on Tue Nov 8 17:49:55 2022
Last Revision on Nov 9, 2022
VERSION 2.1

@author: aaron.hastings
"""

import pandas as pd
import numpy as np

class Sound_Pressure_Level:
    
    
    # These are static methods that can be called with various objects that 
    # have 24, hourly spl entries with the following conditions:
    # dataframe: must pass spl_col_name corresponding to the column of spl data
    #            dataframe will be converted to numpy array of type float
    # other objects: will be converted to numpy array of type float if possible
    @staticmethod
    def Compute_LOG_AVG(float_array):
        AVG = 10*np.log10(sum(10**(float_array/10))) - 10*np.log10(len(float_array))
        return AVG
    
    @staticmethod
    def Validate_24_Hour_Data(data_24_hrs, spl_col_name = None):
        # Determine object type passed, convert to numpy array of type float
        # The array will be a new object and not a reference to the input data
        if isinstance(data_24_hrs, pd.DataFrame):
            if spl_col_name != None:
                data_24_hrs = data_24_hrs[spl_col_name].to_numpy(dtype=float, copy=True)
            else:
                 print('-----SoundPressureLevel.LAeq_24_Hour validation error.')
                 print('-----Dataframes must be passed with spl_col_name.')   
                 # TBD add GUI error message
                 return None
        else: # Too many options to handle individually, so use try
            try:
                data_24_hrs = np.array(data_24_hrs, dtype=float, copy=True)
            except:
                print('-----SoundPressureLevel.LAeq_24_Hour validation error.')
                print('-----Data_24_hrs must be a dataframe, list, series, or array with numeric data.')
                # TBD add GUI error message
                return None
                    
        # data_24_hrs is now an np array of floats
        # Need to make sure that there are 24 finite values
        if data_24_hrs.shape[0] != 24:
            print('-----SoundPressureLevel.LAeq_24_Hour validation error.')
            print('-----Dataframe must have exactly 24 rows.')
            # TBD add GUI error message
            return None
        if not all(np.isfinite(data_24_hrs)):
            print('-----SoundPressureLevel.LAeq_24_Hour validation error.')
            print('-----All hours must have finite SPL values.')
            # TBD add GUI error message
            return None
        
        # Have a 24 element np array of finite floats
        return data_24_hrs
    
    @staticmethod
    def LEQ_24_HR(data_24_hrs, spl_col_name = None):
        # Note, if A-weighted metric desired, must pass A-weighted data      
        
        data_24_hrs = Sound_Pressure_Level.Validate_24_Hour_Data(data_24_hrs, spl_col_name)
        if data_24_hrs is None:
            print('-----Error in SoundPressureLevel.LEQ_24_HR.')
            return None
                
        LAeq_24hrs = Sound_Pressure_Level.Compute_LOG_AVG(data_24_hrs)
        return LAeq_24hrs
    
    @staticmethod
    def LDN(data_24_hrs, spl_col_name = None):
        # Must pass A-weighted data for proper computation
        # Adjustments by Hour
        # LDEN	LDN	HOUR
        # 10	10	0
        # ...........
        # 10	10	6
        # 5		    19
        # 5		    20
        # 5		    21
        # 10	10	22
        # 10	10	23
        
        # Convert to valid numpy array
        data_24_hrs = Sound_Pressure_Level.Validate_24_Hour_Data(data_24_hrs, spl_col_name)
        if data_24_hrs is None:
            print('-----Error in SoundPressureLevel.LDN.')
            return None
            
        # Add DEN Penalties
        data_24_hrs[0:7] = data_24_hrs[0:7] + 10
        data_24_hrs[22:] = data_24_hrs[22:] + 10

        # Compute Average
        Ldn = Sound_Pressure_Level.Compute_LOG_AVG(data_24_hrs)
        return Ldn
 
    @staticmethod
    def LDEN(data_24_hrs, spl_col_name = None):
        # Must pass A-weighted data for proper computation
        # Adjustments by Hour
        # LDEN	LDN	HOUR
        # 10	10	0
        # ...........
        # 10	10	6
        # 5		    19
        # 5		    20
        # 5		    21
        # 10	10	22
        # 10	10	23
        
        # Convert to valid numpy array
        data_24_hrs = Sound_Pressure_Level.Validate_24_Hour_Data(data_24_hrs, spl_col_name)
        if data_24_hrs is None:
            print('-----Error in SoundPressureLevel.LDEN.')
            return None
            
        # Add DEN Penalties
        data_24_hrs[0:7] = data_24_hrs[0:7] + 10
        data_24_hrs[19:22] = data_24_hrs[19:22] + 5
        data_24_hrs[22:] = data_24_hrs[22:] + 10

        # Compute Average
        Lden = Sound_Pressure_Level.Compute_LOG_AVG(data_24_hrs)
        return Lden