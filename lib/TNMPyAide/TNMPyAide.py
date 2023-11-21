# -*- coding: utf-8 -*-
"""
Created By: Volpe National Transportation Systems Center
Created on Thu Nov 10 08:05:52 2022
Based On: TNMAide.xlsm created by Cambridge Systematics 

Last Revision on June 9, 2022
VERSION 2.0

@author: aaron.hastings
"""

import pandas as pd
import numpy as np
from collections import namedtuple
from .DANA_Noise_Data import DANA_Noise_Data as DND
from .Sound_Pressure_Level_Metrics import Sound_Pressure_Level_Metrics as SPL_Metrics
from inspect import currentframe, getframeinfo
from .DANAPlot import DANAPlot as dp



class TNMPyAide:
    # This script computes sound levels at a 50 ft reference location using:
    # Traffic Data fron DANA: Vol and Speed by Vehicle Type
    #                         For each hour of each day over year (std and leap)
    #                         For up to two links
    # TNM REMELs Equations
    #
    # ASSUMPTIONS:
    # 0) One or two links will be analyzed - if two, then links will be combined for worst hour analysis
    # 1) Links are "infintely" long
    # 2) All traffic on link is at center of lane nearest reference location (see ascii sketch)
    #         TBD: This could be improved by distributing traffic and accounting for additional distance
    # 3) Free-field divergence to account for far link (far lane) uses basic following model 
    #         far lane correction = 15*log10(dist(near lane middle) / dist(far lane middle)) 
    #
    #         Far Link Lanes       Median          Near Link Lanes              Ref Mic        
    #      |        v         | x x x x x x x x |         ^        | 
    #      |        v         | x x x x x x x x |         ^        | 
    #      |        v         | x x x x x x x x |         ^        | 
    #      |        v         | x x x x x x x x |         ^        | 
    #      |        v         | x x x x x x x x |         ^     |< |    50ft      >o
    #      |        v         | x x x x x x x x |         ^        | 
    #      |        v         | x x x x x x x x |         ^        | 
    #      |        v         | x x x x x x x x |         ^        | 
    #      |        v         | x x x x x x x x |         ^        | 
    #
    # INPUTS:
    # link_grade = tuple of link grades between -8 and 8 for links L1 and L2
    # df_DANA = dataframe with hourly traffic volumes and speeds for each vehicle type for L1 and L2
    # median_width = median width between L1 and L2
    #      If L2 does not exist, median_width is not needed
    # number_of_lanes = tuple with the number of lanes in L1 and L2
    # meta = meta data for documentation
    #      L1 roadway name
    #      L2 roadway name    
    #      L1 TMC
    #      L2 TMC
    #      State
    #      County
    
    # INTERMEDIATE OBJS:
    # DND
    # dataframe with hourly levels for L1 and L2 and Ltotal (Levels only)
    #      L1 = near lane
    #      L2 = far lane
    #      Ltotal = combined (Levels only)
    # Average_Day = dataframe with average hourly levels - same as previous dataframe, but just one "avg" day
    
    # ChangeLog 
    # V2.1
    # Made grade for link 2 explicit, rather than derived from link 1
    # Worst hour is only determined from average day
    # Removed worst hour of all 365*24 hours - as this value could vary widely from year to year
    #                                          still computes summary data for both average day and day with worst hour
    # Refactored code 
    #    Ordering and level computations are not done using the Link_Sound_Data class
    #    SPL computations have been moved into static methods of Sound_Pressure_Level class
    #    REMELs calculations computed using static method in Compute_REMELs class
    # Average day now uses energy average instead of linear average of days - consistent with averaging for exposure
    
    def __init__(self, df_DANA, link_grade, meta = 0, median_width = 0, number_of_lanes = 2, robust_speeds = False, detailed_log = True):
        # df_DANA and link_grade are only inputs needed for single TMC
        # two TMCs require additional inputs
               
        # BASIC INPUPT CHECKS
        df_DANA.columns = df_DANA.columns.str.upper()
        
        # Handle TMCs
        if any(df_DANA.columns == 'TMC'):
            tmcs_in_df_DANA = list(df_DANA.TMC.unique())
            if len(tmcs_in_df_DANA) > 2:
                print('TNMPyAide found ' + str(len(tmcs_in_df_DANA)) + ' links in the data file.')
                print('Links found = ' + str(tmcs_in_df_DANA))
                print('TNMPyAide can handle at most 2 TMC links.')
                
                frameinfo = getframeinfo(currentframe())
                print(frameinfo.filename, frameinfo.lineno)
                print('')

                return
            if len(tmcs_in_df_DANA) < 1:
                print('TNMPyAide requires at least one TMC link.')
                return
        else:
            print('Data frame must include a column TMC with the tmc ids')
            return
        self.number_of_links = len(tmcs_in_df_DANA)
        self.tmcs = tmcs_in_df_DANA

        # Define meta_data template
        if self.number_of_links == 1:
            meta_data = namedtuple('meta_data', 'L1_name L1_tmc state county')
        else:
            meta_data = namedtuple('meta_data', 'L1_name L2_name L1_tmc L2_tmc state county')
        
        # Handle State
        if any(df_DANA.columns == 'STATE'):
            state_name = list(df_DANA.STATE.unique())
            if len(state_name) > 1:
                print('Warning, TNMPyAide only handles one state name. Using the first state name encountered')
            state_name = state_name[0]
        else:
            print('Warning no STATE column found in data frame. Using NA for state name.')
            state_name = 'NA'
        self.state_name = state_name
            
        # Handle County
        if any(df_DANA.columns == 'COUNTY'):
            county_name = list(df_DANA.COUNTY.unique())
            if len(county_name) > 1:
                print('Warning, TNMPyAide only handles one county name. Using the first county name encountered')
                print('This is only used for labeling, not calculations.')
            county_name = county_name[0]
        else:
            print('Warning no COUNTY column found in data frame. Using NA for county name.')
            print('This is only used for labeling, not calculations.')
            county_name = 'NA'
        self.county_name = county_name
        
        # Handle meta
        if self.number_of_links != 1 and meta == 0:
            # meta must be explicityly defined by user for two link cases
            print('Meta must be explicityly defined by user for two link cases.')
            return
        # Setup default meta data if needed for single link case
        if meta == 0:
            # We can only get to here if there is only one tmc
            TMC_ID_1 = tmcs_in_df_DANA[0]
            meta = meta_data('Link One', TMC_ID_1, state_name, county_name)
        self.meta = meta
        
        # If a single link, then default values for these are fine
        # Otherwise not using correct values will affect attenuation for far link 
        self.median_width = median_width
        self.number_of_lanes = number_of_lanes
        
        # Remove any links not included in the meta data
        df_selected_tmcs = pd.DataFrame() 
        df_grouped  = df_DANA.groupby('TMC', sort=False)
        for tmc, group in df_grouped:
            if tmc == meta.L1_tmc:
                df_selected_tmcs = pd.concat([df_selected_tmcs, group], ignore_index=True) # Should consider concat instead of append
        if self.number_of_links == 2:
            for tmc, group in df_grouped:
                if tmc == meta.L2_tmc:
                    df_selected_tmcs = pd.concat([df_selected_tmcs, group], ignore_index=True)

        # Should now have only the correct TMCs and they should be in order        
        df_DANA = df_selected_tmcs
        # Compute LAeq at Ref Distance and Reorganize df_DANA for easier analysis
        dnd_obj = DND(df_DANA, link_grade)   
        
        # Make sure that links are in order, i.e. first link/TMC should be near lane
        if meta.L1_tmc != dnd_obj.df_Traffic_Noise.TMC_L1[0]:
            #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            print('Error - unhandled exception TMCs are in incorrect order after reorganizing in DND')
            print('This error should be handled in future code update')
            return
            #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        
        # CLEAN UP DATAFRAME - CONVERT PCTs to VOLs
        #######################################################################
        # Combine ADT and PCT to get hourly traffic, save ADT as separate obj
        
        dnd_obj.df_Traffic_Noise['PCT_AT_L1'] = dnd_obj.df_Traffic_Noise['PCT_AT_L1'] * dnd_obj.df_Traffic_Noise['MAADT_L1']  
        dnd_obj.df_Traffic_Noise['PCT_MT_L1'] = dnd_obj.df_Traffic_Noise['PCT_MT_L1'] * dnd_obj.df_Traffic_Noise['MAADT_L1']  
        dnd_obj.df_Traffic_Noise['PCT_HT_L1'] = dnd_obj.df_Traffic_Noise['PCT_HT_L1'] * dnd_obj.df_Traffic_Noise['MAADT_L1']  
        dnd_obj.df_Traffic_Noise['PCT_BUS_L1'] = dnd_obj.df_Traffic_Noise['PCT_BUS_L1'] * dnd_obj.df_Traffic_Noise['MAADT_L1']  
        dnd_obj.df_Traffic_Noise['PCT_MC_L1'] = dnd_obj.df_Traffic_Noise['PCT_MC_L1'] * dnd_obj.df_Traffic_Noise['MAADT_L1']  

        rename_dict = {'PCT_AT_L1': 'VOL_AT_L1', 
                          'PCT_MT_L1': 'VOL_MT_L1', 
                          'PCT_HT_L1': 'VOL_HT_L1', 
                          'PCT_BUS_L1': 'VOL_BUS_L1', 
                          'PCT_MC_L1': 'VOL_MC_L1' }
        dnd_obj.df_Traffic_Noise.rename(columns=rename_dict, inplace=True)
        
        if self.number_of_links == 2:
            dnd_obj.df_Traffic_Noise['PCT_AT_L2'] = dnd_obj.df_Traffic_Noise['PCT_AT_L2'] * dnd_obj.df_Traffic_Noise['MAADT_L2']  
            dnd_obj.df_Traffic_Noise['PCT_MT_L2'] = dnd_obj.df_Traffic_Noise['PCT_MT_L2'] * dnd_obj.df_Traffic_Noise['MAADT_L2']  
            dnd_obj.df_Traffic_Noise['PCT_HT_L2'] = dnd_obj.df_Traffic_Noise['PCT_HT_L2'] * dnd_obj.df_Traffic_Noise['MAADT_L2']  
            dnd_obj.df_Traffic_Noise['PCT_BUS_L2'] = dnd_obj.df_Traffic_Noise['PCT_BUS_L2'] * dnd_obj.df_Traffic_Noise['MAADT_L2']  
            dnd_obj.df_Traffic_Noise['PCT_MC_L2'] = dnd_obj.df_Traffic_Noise['PCT_MC_L2'] * dnd_obj.df_Traffic_Noise['MAADT_L2']  

            rename_dict = {'PCT_AT_L2': 'VOL_AT_L2', 
                              'PCT_MT_L2': 'VOL_MT_L2', 
                              'PCT_HT_L2': 'VOL_HT_L2', 
                              'PCT_BUS_L2': 'VOL_BUS_L2', 
                              'PCT_MC_L2': 'VOL_MC_L2' }
            dnd_obj.df_Traffic_Noise.rename(columns=rename_dict, inplace=True)
            
        # REMOVE ANY HOURS WITH CLEARLY BAD DATA, E.G. SPEEDS should be (0, 150) mph
        # isna data checked by DANA_Noise_Data class method
        self.original_number_of_hours = len(dnd_obj.df_Traffic_Noise)
        if self.number_of_links == 1:
            dnd_obj.df_Traffic_Noise = dnd_obj.df_Traffic_Noise[(dnd_obj.df_Traffic_Noise['SPD_ALL_L1'] < 150) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_AT_L1']  < 150) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_HT_L1']  < 150) ]
        else:
            dnd_obj.df_Traffic_Noise = dnd_obj.df_Traffic_Noise[(dnd_obj.df_Traffic_Noise['SPD_ALL_L1']  < 150) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_AT_L1']  < 150) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_HT_L1']  < 150) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_ALL_L2']  < 150) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_AT_L2']  < 150) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_HT_L2']  < 150) ]

        if self.number_of_links == 1:
            dnd_obj.df_Traffic_Noise = dnd_obj.df_Traffic_Noise[(dnd_obj.df_Traffic_Noise['SPD_ALL_L1'] > 0) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_AT_L1']  > 0) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_HT_L1']  > 0) ]
        else:
            dnd_obj.df_Traffic_Noise = dnd_obj.df_Traffic_Noise[(dnd_obj.df_Traffic_Noise['SPD_ALL_L1']  > 0) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_AT_L1']  > 0) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_HT_L1']  > 0) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_ALL_L2']  > 0) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_AT_L2']  > 0) & \
                                                            (dnd_obj.df_Traffic_Noise['SPD_HT_L2']  > 0) ]
                    
        self.final_number_of_hours = len(dnd_obj.df_Traffic_Noise)
        self.number_of_dropped_hours = self.original_number_of_hours - self.final_number_of_hours
        if detailed_log:
            print('Number of dropped hours: ' + str(self.number_of_dropped_hours))
        
        # Determine Total SPL for both links (or use the single link if only one is given)
        dnd_obj.df_Traffic_Noise['SPL_Total'] = dnd_obj.df_Traffic_Noise['SPL_Total_L1']
        if self.number_of_links == 2:
             distance_to_center_L1 = 50 + -12/2 + self.number_of_lanes[0] * 12/2
             distance_to_center_L2 = distance_to_center_L1 + self.number_of_lanes[0] * 12/2 + self.median_width + self.number_of_lanes[1] * 12/2
             rel_attenuation_L2 = 15 * np.log10(distance_to_center_L1 / distance_to_center_L2)
             #dnd_obj.df_Traffic_Noise['SPL_Total'].loc['SPL_Total'] = \
             #SPL_Metrics.Compute_LOG_SUM(np.asarray([dnd_obj.df_Traffic_Noise['SPL_Total'], dnd_obj.df_Traffic_Noise['SPL_Total_L2']+rel_attenuation_L2]))
             array_whos_columns_are_to_be_summed = np.asarray([dnd_obj.df_Traffic_Noise['SPL_Total'], dnd_obj.df_Traffic_Noise['SPL_Total_L2']+rel_attenuation_L2])
             temp = SPL_Metrics.Compute_LOG_SUM(array_whos_columns_are_to_be_summed)  
             dnd_obj.df_Traffic_Noise['SPL_Total'] = temp
        
        # Ready to compute average day
        if self.number_of_links == 1:
            df_avg = pd.DataFrame(columns = ['HOUR', 'SPD_ALL_L1', 'SPD_AT_L1', 'SPD_HT_L1', \
                                             'VOL_Total_L1', 'VOL_AT_L1', 'VOL_MT_L1', 'VOL_HT_L1', 'VOL_BUS_L1', 'VOL_MC_L1', \
                                             'SPL_AT_L1', 'SPL_MT_L1', 'SPL_HT_L1', 'SPL_BUS_L1', 'SPL_MC_L1', \
                                             'SPL_Total_L1', 'SPL_Total'])
                
        else:
            df_avg = pd.DataFrame(columns = ['HOUR', 'SPD_ALL_L1', 'SPD_AT_L1', 'SPD_HT_L1', \
                                             'VOL_Total_L1', 'VOL_AT_L1', 'VOL_MT_L1', 'VOL_HT_L1', 'VOL_BUS_L1', 'VOL_MC_L1', \
                                             'SPL_AT_L1', 'SPL_MT_L1', 'SPL_HT_L1', 'SPL_BUS_L1', 'SPL_MC_L1', \
                                             'SPL_Total_L1', \
                                             'SPD_ALL_L2', 'SPD_AT_L2', 'SPD_HT_L2', \
                                             'VOL_Total_L2', 'VOL_AT_L2', 'VOL_MT_L2', 'VOL_HT_L2', 'VOL_BUS_L2', 'VOL_MC_L2', \
                                             'SPL_AT_L2', 'SPL_MT_L2', 'SPL_HT_L2', 'SPL_BUS_L2', 'SPL_MC_L2', \
                                             'SPL_Total_L2', 'SPL_Total'])

        df_grouped = dnd_obj.df_Traffic_Noise.groupby(['HOUR'])
        
        if self.number_of_links == 1:
            for hour, group in df_grouped:
                group.drop(['DATE','TMC_L1'], axis=1, inplace = True)
                
                
                # Do linear average for most data
                df_avg.loc[hour] = group.mean()
                df_avg.loc[hour,'VOL_Total_L1'] = df_avg.loc[hour,'VOL_AT_L1'] + \
                                            df_avg.loc[hour,'VOL_MT_L1'] + \
                                            df_avg.loc[hour,'VOL_HT_L1'] + \
                                            df_avg.loc[hour,'VOL_BUS_L1'] + \
                                            df_avg.loc[hour,'VOL_MC_L1']
                
                # Do log average for SPL data - if not can end up with -inf from original linear averge fron group.mean()
                SPL_Log_Avgs = 10*np.log10(np.mean(np.power(10,group.loc[:,['SPL_AT_L1', 'SPL_MT_L1', 'SPL_HT_L1', 'SPL_BUS_L1', 'SPL_MC_L1','SPL_Total_L1','SPL_Total']]/10)))                                    
                df_avg.loc[hour,['SPL_AT_L1', 'SPL_MT_L1', 'SPL_HT_L1', 'SPL_BUS_L1', 'SPL_MC_L1','SPL_Total_L1','SPL_Total']] = SPL_Log_Avgs



        else:
            for hour, group in df_grouped:
                group.drop(['DATE','TMC_L1','TMC_L2'], axis=1, inplace = True)
                df_avg.loc[hour] = group.mean()
                df_avg.loc[hour,'VOL_Total_L1'] = df_avg.loc[hour,'VOL_AT_L1'] + \
                                            df_avg.loc[hour,'VOL_MT_L1'] + \
                                            df_avg.loc[hour,'VOL_HT_L1'] + \
                                            df_avg.loc[hour,'VOL_BUS_L1'] + \
                                            df_avg.loc[hour,'VOL_MC_L1']
                df_avg.loc[hour,'VOL_Total_L2'] = df_avg.loc[hour,'VOL_AT_L2'] + \
                                            df_avg.loc[hour,'VOL_MT_L2'] + \
                                            df_avg.loc[hour,'VOL_HT_L2'] + \
                                            df_avg.loc[hour,'VOL_BUS_L2'] + \
                                            df_avg.loc[hour,'VOL_MC_L2']

                # Do log average for SPL data
                SPL_Log_Avgs = 10*np.log10(np.mean(np.power(10,group.loc[:,['SPL_AT_L1', 'SPL_MT_L1', 'SPL_HT_L1', 'SPL_BUS_L1', 'SPL_MC_L1','SPL_Total_L1']]/10)))                                    
                df_avg.loc[hour,['SPL_AT_L1', 'SPL_MT_L1', 'SPL_HT_L1', 'SPL_BUS_L1', 'SPL_MC_L1','SPL_Total_L1']] = SPL_Log_Avgs

                SPL_Log_Avgs = 10*np.log10(np.mean(np.power(10,group.loc[:,['SPL_AT_L2', 'SPL_MT_L2', 'SPL_HT_L2', 'SPL_BUS_L2', 'SPL_MC_L2','SPL_Total_L2','SPL_Total']]/10)))                                    
                df_avg.loc[hour,['SPL_AT_L2', 'SPL_MT_L2', 'SPL_HT_L2', 'SPL_BUS_L2', 'SPL_MC_L2','SPL_Total_L2','SPL_Total']] = SPL_Log_Avgs

        for col in df_avg.columns:
            df_avg[col] = pd.to_numeric(df_avg[col])
        
        self.df_avg_day = df_avg # Save data for "average day"

        # Summary Data
        self.df_Traffic_Noise = dnd_obj.df_Traffic_Noise
        day_summary = namedtuple('day_summary', 'day df_hourly_spl worst_hour_idx worst_hour worst_hour_spl LAEQ_24_HR LDN LDEN')
                
        # Average Day
        worst_hour_idx = df_avg['SPL_Total'].idxmax()
        worst_hour = df_avg.loc[worst_hour_idx, 'HOUR']
        worst_hour_spl = df_avg['SPL_Total'].max()
        day = 'average'
        metrics = self.Compute_Daily_Metrics(df_avg)
        self.average_day = day_summary(day, df_avg, worst_hour_idx, worst_hour, worst_hour_spl, metrics[0], metrics[1], metrics[2])
        if detailed_log:
            print('\nResults for Average Day')
            print('-----------------------')
            print('Date: ' + str(day))
            print('Worst Hour IDX: ' + str(worst_hour_idx))
            print('Worst Hour: ' + str(worst_hour))
            print('Worst Hour SPL: ' + str(worst_hour_spl))
            print('LAEQ_24_HR: ' + str(metrics[0]))
            print('LDN: ' + str(metrics[1]))
            print('LDEN: ' + str(metrics[2]))
            print('')
            print(self.average_day.df_hourly_spl['SPL_Total'])
        
        # Day with highest single hour (max of all hourly SPLs)
        worst_hour_idx = self.df_Traffic_Noise['SPL_Total'].idxmax()
        worst_hour = self.df_Traffic_Noise.loc[worst_hour_idx, 'HOUR']
        worst_hour_spl = self.df_Traffic_Noise['SPL_Total'].max()
        day = self.df_Traffic_Noise.loc[worst_hour_idx, 'DATE']                
        df_hourly_spl = self.df_Traffic_Noise[self.df_Traffic_Noise['DATE']==day]
        
        self.df_worst_day = df_hourly_spl
        
        metrics = self.Compute_Daily_Metrics(df_hourly_spl)
        self.worst_day = day_summary(day, df_hourly_spl, worst_hour_idx, worst_hour, worst_hour_spl, metrics[0], metrics[1], metrics[2])
        if detailed_log:
            print('\n\nResults for Worst Day')
            print('-----------------------')
            print('Date: ' + str(day))
            print('Worst Hour IDX: ' + str(worst_hour_idx))
            print('Worst Hour: ' + str(worst_hour))
            print('Worst Hour SPL: ' + str(worst_hour_spl))
            print('LAEQ_24_HR: ' + str(metrics[0]))
            print('LDN: ' + str(metrics[1]))
            print('LDEN: ' + str(metrics[2]))
            print('')
            print(self.worst_day.df_hourly_spl['SPL_Total'])
        
    def Compute_Daily_Metrics(self, df_day):
        LAEQ_24_HR = SPL_Metrics.LEQ_24_HR(df_day['SPL_Total'])
        LDN = SPL_Metrics.LDN(df_day['SPL_Total'])
        LDEN = SPL_Metrics.LDEN(df_day['SPL_Total'])
        return [LAEQ_24_HR, LDN, LDEN]
       
    def Plot_Avg_Day_Hourly_SPL(self):
        figs = []
        axs = []
        
        # ALL DATA LINK 1
        x = np.array([self.df_avg_day.HOUR, self.df_avg_day.HOUR, self.df_avg_day.HOUR, 
                      self.df_avg_day.HOUR, self.df_avg_day.HOUR, self.df_avg_day.HOUR])
        y = np.array([self.df_avg_day.SPL_AT_L1, self.df_avg_day.SPL_MT_L1, 
                      self.df_avg_day.SPL_HT_L1, self.df_avg_day.SPL_BUS_L1, 
                      self.df_avg_day.SPL_MC_L1, self.df_avg_day.SPL_Total_L1]) 
        fig, ax = dp.Line_Plot(x, y, 'Hour', 'Hourly LAeq, dB(A)', 'Average Day SPL, Link 1')
        labels = ['AT(L1)','MT(L1)','HT(L1)','Bus(L1)','MC(L1)','Total(L1)']
        dp.Add_Legend(fig, ax, labels)
        figs.append(fig)
        axs.append(ax)
    
        # ALL DATA LINK 2
        y = np.array([self.df_avg_day.SPL_AT_L2, self.df_avg_day.SPL_MT_L2, 
                      self.df_avg_day.SPL_HT_L2, self.df_avg_day.SPL_BUS_L2, 
                      self.df_avg_day.SPL_MC_L2, self.df_avg_day.SPL_Total_L2]) 
        fig, ax = dp.Line_Plot(x, y, 'Hour', 'Hourly LAeq, dB(A)', 'Average Day SPL, Link 2')
        labels = ['AT(L2)','MT(L2)','HT(L2)','Bus(L2)','MC(L2)','Total(L2)']
        dp.Add_Legend(fig, ax, labels)
        figs.append(fig)
        axs.append(ax)

        # JUST TOTAL SPL
        x = np.array([self.df_avg_day.HOUR])
        y = np.array([self.df_avg_day.SPL_Total]) 
        fig, ax = dp.Line_Plot(x, y, 'Hour', 'Hourly LAeq, dB(A)', 'Average Day Total SPL')
        figs.append(fig)
        axs.append(ax)
        return figs, axs

    def Plot_Avg_Day_Hourly_Speed(self):
        figs = []
        axs = []
        
        x = np.array([self.df_avg_day.HOUR, self.df_avg_day.HOUR, self.df_avg_day.HOUR, 
                      self.df_avg_day.HOUR, self.df_avg_day.HOUR, self.df_avg_day.HOUR])
        y = np.array([self.df_avg_day.SPD_ALL_L1, self.df_avg_day.SPD_AT_L1, 
                      self.df_avg_day.SPD_HT_L1, self.df_avg_day.SPD_ALL_L2, 
                      self.df_avg_day.SPD_AT_L2, self.df_avg_day.SPD_HT_L2]) 
        fig, ax = dp.Line_Plot(x, y, 'Hour', 'Speed, MPH', 'Average Hourly Speed')
        labels = ['All(L1)','AT(L1)','HT(L1)','All(L2)','AT(L2)','HT(L2)']
        dp.Add_Legend(fig, ax, labels)
        figs.append(fig)
        axs.append(ax)
        return figs, axs
    
    def Plot_Hourly_Speed_Histograms(self):
        figs = []
        axs = []

        bin_centers = np.array(range(0,105,5))
        
        data = np.array([self.df_Traffic_Noise.SPD_ALL_L1])
        fig, ax = dp.Histogram(data, bin_centers, xlabel = 'Speed, MPH', title = 'Speed Distribution (All, L1)')
        figs.append(fig)
        axs.append(ax)

        data = np.array([self.df_Traffic_Noise.SPD_AT_L1])
        fig, ax = dp.Histogram(data, bin_centers, xlabel = 'Speed, MPH', title = 'Speed Distribution (AT, L1)')
        figs.append(fig)
        axs.append(ax)

        data = np.array([self.df_Traffic_Noise.SPD_HT_L1])
        fig, ax = dp.Histogram(data, bin_centers, xlabel = 'Speed, MPH', title = 'Speed Distribution (HT, L1)')
        figs.append(fig)
        axs.append(ax)

        data = np.array([self.df_Traffic_Noise.SPD_ALL_L1])
        fig, ax = dp.Histogram(data, bin_centers, xlabel = 'Speed, MPH', title = 'Speed Distribution (All, L2)')
        figs.append(fig)
        axs.append(ax)

        data = np.array([self.df_Traffic_Noise.SPD_AT_L2])
        fig, ax = dp.Histogram(data, bin_centers, xlabel = 'Speed, MPH', title = 'Speed Distribution (AT, L2)')
        figs.append(fig)
        axs.append(ax)

        data = np.array([self.df_Traffic_Noise.SPD_HT_L2])
        fig, ax = dp.Histogram(data, bin_centers, xlabel = 'Speed, MPH', title = 'Speed Distribution (HT, L2)')
        figs.append(fig)
        axs.append(ax)

        return [figs, axs]
    
    def Plot_Hourly_SPL_Histograms(self):
        bin_centers = np.array(range(65,95,1))
        
        data = np.array([self.df_Traffic_Noise.SPL_Total])
        fig, ax = dp.Histogram(data, bin_centers, xlabel = 'Hourly LAeq, dB(A)', title = 'Total SPL', xtickstep=2, width = 1)

        return [[fig], ax]
   
 