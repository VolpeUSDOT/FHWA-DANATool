# -*- coding: utf-8 -*-
"""
Created By: Volpe National Transportation Systems Center
Based On: TNMAide.xlsm created by Cambridge Systematics 
Created on Tue May 17, 07:36:17, 2022
Last Revision on June 9, 2022
VERSION 2.0

@author: Aaron.Hastings
"""

import pandas as pd
import numpy as np
import math

class TNMAide: 
    # SCOPE:
    #--------------------------------------------------------------------------
    # This script implements all the functionality of the TNMAide Spreadsheet
    # The current version only outputs numeric data (no graphs)
    # Future versions may include graphs
    # Because this is intneded to fit within the DANA ecosystem, the
    #    implementation is constrained to a single script, with relevant
    #    limitations (e.g. three classes, one for methods, one for present and
    #                      one for future conditions could have been better.)
    #    This follows python convention of one module one file; however, in 
    #        the future this may be restucutred, TBD.
    
    # NOTES:
    # WORST_HOUR_DATE   refers to the date that has the highest 1-hr SPL  
    #                   totalled over both the near and far lanes     
    # WORST_HOUR       refers to the hour within the worst date that had that 
    #                  highest SPL
    # AVG_DAY          refers to metrics that have been average over all 365* 
    #                  or 366* days all hours inclusive
    # AVG_HOUR         refers to metrics that have been averaged over the same 
    #                  hour for the entire calendar year*
    # WORST_HOUR_AVG   refers to the worst hour within an AVG_DAY
    # * assuming a full year has been provided
    # NOTE - future levels are predicted based on Day, Evening, Night percents
    # and existing traffic volumes. However, the DEN percents do not indicate
    # how traffic changes between links. Therefore delta is assumed to apply
    # equally to both links.
    

    # TESTS:
    #--------------------------------------------------------------------------
    # 1) 365 Day Year, Full Dataset, No Grade, No Median, 2 Lanes
    # 2) 365 Day Year, Full Dataset, No Grade, No Median, 6 Lanes
    # 3) 365 Day Year, Full Dataset, No Grade, 50 ft Median, 6 Lanes
    # 4) 365 Day Year, Full Dataset, 2% Grade, 50 ft Median, 6 Lanes           
    # 5) 365 Day Year, Full Dataset, -2% Grade, 50 ft Median, 6 Lanes          
    # 6) 366 Day Year, Full Dataset, 2% Grade, 50 ft Median, 6 Lanes           
    #    -- Consitency will fail due to grade bug for far lane in spreadsheet
    #    -- See Leq Worst Hour Calculations tab, Column N, Rows 8769 to 8793
    #    -- Consitency will fail due to range to search for worst hour
    #    -- See Leq Worst Hour Calculations tab, Column Z, Row 17587
    # 6) 366 Day Year, Full Dataset, 0% Grade, 50 ft Median, 6 Lanes   
    #    -- Consitency will fail due to range to search for worst hour
    #    -- See Leq Worst Hour Calculations tab, Column Z, Row 17587
    # 7) One Month of Data, 0% Grade, 50 ft Median, 6 Lanes
    #    -- Python works
    #    -- Spreadsheet will fail, can only handle 365 or 366 days
    # 8) 365 Day Year, One Direction Only, No Grade, 50 ft Median, 6 Lanes     
    #    -- Neither Python nor Spreadsheet work, they both need two links



    # CONSISTENCY (Based on Spreadsheet Evaluation 5/26/22 - 6/1/22):
    #--------------------------------------------------------------------------
    # The following items diverge from the spreadsheet version:
    #     In Compute_REMELs_Energy():
    #        The "c" coefficient for motorcycles is set to 56.086099 in the 
    #        python script. This is consistent with TNM 3.1 code 
    #        (VehicleFlow.cs) and the TNM 3.1 Tech Manual. The "c" coefficient 
    #        for motorcylces is set to 56.0 in the spreadsheet. This is 
    #        consistent with the TNM Technical Manual V1.0. We don't have the 
    #        source code pre-TNM 2.0 to verify earlier versions, but back as 
    #        far as TNM 2.0, "c" was set to 56.086099 (see emittbls.cpp).  
    #        Therefore, this value should be used for any version of TNMAide. 
    #        Using the two different values for "c" can result in differences 
    #        on the order of 0.05 dB between the two methods.
    #        FIXED in 2022-06 Final TNMAide
    #     In Compute_REMELs_Energy():
    #        In the spreadsheet version, during energy calculation, the speed 
    #        is converted from english to metric and then back to english units 
    #        using the two coefficients, 0.6214 and 1.6093. These just add 
    #        rounding errors to the math since they should reflect a total 
    #        value of 1.0 instead of 1.00001902 as is the case. This redundent 
    #        conversion has been omitted from the python version. This 
    #        difference can produce errors on the order of 0.005 dB.
    #        FIXED in 2022-06 Final TNMAide
    #     In Create_Worst_Hour_Calculations_Dataframe():
    #        Variables X through AB should use 1.60934 to convert from mph to     
    #        kph. Columns X through AB of "Leq Worst Hour Calculations" tab in  
    #        the spreadsheet version uses 1.603 to convert from mph to kph,  
    #        which is not correct. The spreadsheet uses a correct conversion  
    #        factor elsewhere (1.6093) so this is likely a typo.
    #        FIXED in 2022-06 Final TNMAide
    #     In Create_Worst_Hour_Calculations_Dataframe():
    #        The volumes for all vehicle classes are computed by multiplying 
    #        the PCT_NOISE_# by the aadt, where # signifies the particular 
    #        vehicle class.
    #        Column G (BUS Volume) of "Leq Worst Hour Calculations" tab in the 
    #        spreadsheet version multiplies PCT_NOISE_# by "AADT Single Unit",
    #        while all other vehicle classes use the same same method as in the 
    #        python function. Because the combined PCT_NOISE_# over all vehicle
    #        types and all hours of the day sum to 1, in order to obtain the 
    #        total aadt, all PCT_NOISE_# values must multiply aadt. Having the 
    #        BUS multiply by "AADT Single Unit" means that the full aadt will 
    #        not be realized.
    #        FIXED in 2022-06 Final TNMAide
    #     In Create_Worst_Hour_Calculations_Dataframe():
    #        Variable Z (LAeq,1hr for HT) is computed by referencing the HT 
    #        volume according to variable F. In the spreadsheet, this column 
    #        erroneously assigns the LAeq,1hr for Buses (Column AA) for the 
    #        HT volume.
    #        FIXED in 2022-06 Final TNMAide
    #     In "Leq Worst Hour Calculations" tab in Column AD, in the function:
    #        =IF($B$17529=0,(IF(AC8769=" "," ",IF(AC9=" "," " ...
    #        The value $B$17529 needs to be set to $B$17530 in order to work 
    #        correctly, because $B$17529 will always equal zero as it points to
    #        the first hour (0) of the 366th day. $B$17530 points to the second
    #        hour (1) will only be non-zero if a leap day exists. I.E. the 
    #        original code will ignore the shift do to the extra day in Feb
    #        for a leap year. This will shift MOST of the year off by 1 day.
    #        An unmodified spreadsheet should work for non-leap years but will
    #        not match the python script for leap years.
    #        FIXED in 2022-06 Final TNMAide
    #     In "Leq Worst Hour Calculations" tab spreadsheet does not flip grade 
    #        for far lanes for rows that could EITHER be link 1 or link 2. 
    #        See Leq Worst Hour Calculations tab, Column N, Rows 8769 to 8793
    #        Error should only happen during leap year with grade >= 1.5%.
    #        It will affect combined near and far lane level for these rows.    
    #        NOT FIXED in 2022-06 Final TNMAide
    #     In "Leq Worst Hour Calculations" tab spreadsheet row 17584 cols E - I
    #        Do not account for the last day of a leap year when computing AADT
    #        NOT FIXED in 2022-06 Final TNMAide
    #     Ldn and Lden calculations future case only account for Autos, MT, and
    #        HTs in the spreadsheet. This is inconsistent with present results.
    #        Python script includes all traffic types for consistency with 
    #        present results.
    #        NOT FIXED in 2022-06 Final TNMAide
    #     Ldn and Lden calculations in spreadsheet compute future levels, by
    #        subtracting 10*LOG(New Vol(Hr)/Original Worst Hour Date Vol(Hr))
    #        from the total SPL(Hr). This does not account for a different
    #        traffic ratio in the far links. See e.g. Column AX and AS of tab
    #        Leq Worst Hour Calculations
    #        Python implementation uses combined traffic volumes for both links
    #        NOT FIXED in 2022-06 Final TNMAide    
    #     Ldn and Lden calculations are oversimplified in the spreadsheet. 
    #        These produce higher LAeq and Lden values when using the exact
    #        same traffic distributions for the future case
    #        NOT FIXED in 2022-06 Final TNMAide     
    #     Ldn spreadsheet calculations have a typo for somem data in 
    #        Calculations (3) tab
    #        NOT FIXED in 2022-06 Final TNMAide     
    
    # INPUTS: 
    #--------------------------------------------------------------------------
    # number_of_lanes         = Total Number of Lanes inboth directions, 
    #                           e.g. for 2 NB & 2 SB lanes, number_of_lanes = 4
    # median_width            = Median Width
    # near_lane_roadway_grade = Roadway Grade (in Direction of Near Lanes)
    #                         = 100 * rise / run
    #
    # df = Dataframe containing all input data except the three values above:
    #      Columns of df are generated by the DANA tool and include:
    #           TMC, DATE, HOUR, ROAD, DIRECTION, STATE, COUNTY,
    #           start_latitude, start_longitude, end_latitude, end_longitude
    #           TMC_LENGTH, road_order, f_system, thrulanes, aadt, 
    #           AADT_Single_Unit, aadt_combinations, TT_ALL_vehs, TT_PASS,
    #           TT_TRUCK, SPEED_ALL_vehs, SPEED_PASS, SPEED_TRUCK,
    #           PCT_NOISE_AUTO, PCT_NOISE_MEDIUM, PCT_NOISE_HEAVY,
    #           PCT_NOISE_BUS, PCT_NOISE_MC
    #           
    #      For a description of each of these columns, see Appendix C, 
    #           Table 4.4 on page 80 of the DANA User's Guide.
    #           https://www.fhwa.dot.gov/environment/air_quality/methodologies/
    #           dana/dana_userguide_r2.pdf
    #           - OR -
    #           https://www.fhwa.dot.gov/environment/noise/traffic_noise_model/
    #           tnm_aide_userguide/


    # ATTRIBUTES:
    #--------------------------------------------------------------------------
    
    # Input Attributes ...
    #--------------------------------------------------------------------------
    # number_of_lanes             (Same Value as Input Parameter)
    # median_width                (Same Value as Input Parameter)
    # near_lane_roadway_grade     (Same Value as Input Parameter)
    # df_input                    (Same Value as df Input Parameter)
    #
    
    # Intermediate Attributes (Used for Documentation) ...
    #--------------------------------------------------------------------------
    # TMCS                  = list of two TMCs, near lane / link assumed first
    # computed_futures      = (bool) flag that indicates if future metrics have  
    #                         been calculated
    # roadway               = (str) name of roadway, e.g. 'I-75' 
    #                         --> df.loc[0,'ROAD']
    # direction_near_lanes  = (str) direction of the near lanes, e.g. 'SB' 
    #                         --> df.loc[0,'DIRECTION']
    # direction_far_lanes   = (str) direction of far lanes, e.g. 'northbound' 
    #                         --> df.loc[17519,'DIRECTION']
    # county                = (str) name of county that the link is in
    # state                 = (str) name of the state that the link is in
    # df_Leq_Worst_Hour_Calculations   = (df) represents most of the "Leq Worst Hour 
    #                                    Calculations" tab of the spreadsheet

    # "Results" Attributes ...
    #--------------------------------------------------------------------------
    # AADT                    = (float) Average Annual Daily Traffic for 
    #                           present condition [df.aadt]
    # Auto_Overall_Fraction   = Auto Fraction of AADT (note Daily, not Hourly)
    # MT_Overall_Fraction     = MT Fraction of AADT (note Daily, not Hourly)
    # HT_Overall_Fraction     = HT Fraction of AADT (note Daily, not Hourly)
    # BUS_Overall_Fraction    = BUS Fraction of AADT (note Daily, not Hourly)
    # MC_Overall_Fraction     = MC Fraction of AADT (note Daily, not Hourly)
    # WORST_HOUR_AVG          = (int) the hour of the day that, on average, 
    #                           produces the highest estimated LAeq,1-Hr.
    # WORST_HOUR              = (int) the single hour of the entire year that 
    #                           produces the highest estimated LAeq,1-Hr.
    #                           There are 365 * 24 possible options for this hr
    #                           however the hour is represented as a number 
    #                           between 1 and 24 for the given day.
    # WORST_HOUR_DATE         = (string) The date that the WORST_HOUR occurs 
    #                           for present condition 
    # LAeq_WORST_HOUR         = (float) The 1-Hour LAeq associated with the  
    #                           WORST_HOUR for present condition
    # LAeq_WORST_HOUR_AVG     = (float) The 1-Hour LAeq associated with the  
    #                           WORST_HOUR_AVG for present condition
    # LAeq_24hrs_AVG_DAY      = (float) The 24-hour LAeq for an average day  
    #                           (AVG_DAY). Similar to Ldn and Lden, except no   
    #                           penalties for time of day
    # Ldn_AVG_DAY             = (float) AVG Annual LDN for present condition
    # Lden_AVG_DAY            = (float) AVG Annual LDEN for present condition
    # LAeq_24hrs_WORST_HOUR_DATE  = (float) The 24-hour LAeq for the   
    #                           WORST_HOUR_DATE. Similar to Ldn and Lden,    
    #                           except no penalties fo time of day
    # Ldn_WORST_HOUR_DATE     = (float) LDN for WORST_HOUR_DATE for
    #                           present condition
    # Lden_WORST_HOUR_DATE    = (float) LDEN for WORST_HOUR_DATE for 
    #                           present condition
    # df_day_WORST_HOUR_DATE  = (dataframe) Summary dataframe of important data 
    #                           related to PRESENT results, WORST_HOUR_DATE 
    # df_day_AVG_DAY          = (dataframe) Summary dataframe of important data 
    #                           related to PRESENT results, DAY AVG
    # df_day_WORST_HOUR_DATE_future = (dataframe) Summary dataframe of  
    #                                 important data related to FUTURE results, 
    #                                 DAY OF WORST HOUR
    # df_da_AVG_DAY_future    = (dataframe) Summary dataframe of important data 
    #                           related to FUTURE results, DAY AVG
    
    
    # Methods
    #--------------------------------------------------------------------------

    #%% Initialization   
    def __init__(self, df, number_of_lanes = 2, median_width = 10.0, 
                 near_lane_roadway_grade = 0.0, do_two_lanes = True, 
                 robust_speeds = False):
        # Evaluate inputs
        df_rows_columns = df.shape
        if df_rows_columns[0] < 17520:
            print('Warning: Insufficient number of hours in the data table for a full year. '
                  + 'TNMAide requires 17520 hours for a standard year and 17544 hours ' 
                  + 'for a leap year. However, analysis of smaller data sets can be done.')
            print(" ")
                

        if df_rows_columns[1] < 29:
            print('Warning: There may be insufficient columns in the data table.'
                  + 'Standard input consists of 29 columns in the data table.')
            print(" ")
                
        # These are flags
        self.do_two_lanes = do_two_lanes
        self.robust_speeds = robust_speeds
        
        # These Replicate the User Inputs
        self.df_input = df.replace(999999.0, np.nan)
        self.number_of_lanes = number_of_lanes
        self.median_width = median_width 
        self.near_lane_roadway_grade = near_lane_roadway_grade
        
        # Intermediate Attributes
        self.TMCS = self.df_input.TMC.unique() # First TMC is assumed to be the near lanes
        if len(self.TMCS) != 2.0:
            print('Error: TNMAide requires two and only two TMCs. Exiting Instantiation.')
            print(" ")
            return
        self.computed_futures = False
        self.roadway = df.loc[0,'ROAD']
        self.direction_near_lanes = df.loc[0,'DIRECTION']       
        self.direction_far_lanes = df.loc[df.shape[0]-1,'DIRECTION']
        self.county = df.loc[0,'COUNTY']
        self.state = df.loc[0,'STATE']
        
        # "Results" Attributes
        # These two dataframes hold all results
        #self.df_results_present = self.Get_Empty_Results_DataFrame()
        #self.df_results_future = self.Get_Empty_Results_DataFrame()
        
        # Compute Present Condition as Part of the Instantiation
        self.Compute_Present_Condition()
 
    
    def Print_Input_Dataframe(self):
        print(self.df_input)
        return 0
   
    
    def Compute_Present_Condition(self):
        self.Create_Leq_Worst_Hour_Calculations_Dataframe()
        save_csv = False
        if (save_csv):
            self.df_Leq_Worst_Hour_Calculations.to_csv('temp.csv')
        self.Compute_Summary_Data_Worst_Hour()
        self.Compute_Summary_Data_AVG_Day()
        return 0

   
    def Create_Leq_Worst_Hour_Calculations_Dataframe(self):      
        # Note: Variable names correspond to columns in TNMAide Leq Worst Hour Calculations tab
        B = self.df_input.loc[:,'HOUR']
        C = self.df_input.loc[:,'DATE']
        D = self.df_input.loc[:,'aadt'] * self.df_input.loc[:,'PCT_NOISE_AUTO']
        E = self.df_input.loc[:,'aadt'] * self.df_input.loc[:,'PCT_NOISE_MEDIUM']
        F = self.df_input.loc[:,'aadt'] * self.df_input.loc[:,'PCT_NOISE_HEAVY']
        G = self.df_input.loc[:,'aadt'] * self.df_input.loc[:,'PCT_NOISE_BUS']
        H = self.df_input.loc[:,'aadt'] * self.df_input.loc[:,'PCT_NOISE_MC']
        I = self.df_input.loc[:,'SPEED_PASS']
        J = self.df_input.loc[:,'SPEED_TRUCK']
        
        K = self.df_input.loc[:,'SPEED_ALL_vehs'] # Note in the spreadsheet this is a filler col, but we need this for future computations, so adding it here
        
        L = self.Compute_REMELs_Energy('AUTO')
        M = self.Compute_REMELs_Energy('MT')
        N = self.Compute_REMELs_Energy('HT')
        O = self.Compute_REMELs_Energy('BUS')
        P = self.Compute_REMELs_Energy('MC')
        
        Q = np.nan * I # This is a filler column so that this df aligns with table in spreadsheet
        
        R = 10*np.log10(L) # Auto
        S = 10*np.log10(M) # MT
        T = 10*np.log10(N) # HT
        U = 10*np.log10(O) # BUS
        V = 10*np.log10(P) # MC
        
        W = np.nan * I # This is a filler column so that this df aligns with table in spreadsheet
        
        X = 10*np.log10(D/(1.60934 * self.df_input.loc[:,'SPEED_PASS'])) + R - 13.2        # Auto
        Y = 10*np.log10(E/(1.60934 * self.df_input.loc[:,'SPEED_TRUCK'])) + S - 13.2       # MT
        Z = 10*np.log10(F/(1.60934 * self.df_input.loc[:,'SPEED_TRUCK'])) + T - 13.2       # HT
        AA = 10*np.log10(G/(1.60934 * self.df_input.loc[:,'SPEED_TRUCK'])) + U - 13.2      # BUS
        AB = 10*np.log10(H/(1.60934 * self.df_input.loc[:,'SPEED_ALL_vehs'])) + V - 13.2   # MC 
        
        AC = 10*np.log10(10**(X/10)+10**(Y/10)+10**(Z/10)+10**(AA/10)+10**(AB/10))
        
        AD = np.nan * I # This is a filler column so that this df aligns with table in spreadsheet
               
        # Create the Results Dataframe
        d = { 'B' : B, 'C' : C, 'D' : D, 'E' : E, 'F' : F, 'G' : G, 'H' : H,
              'I' : I, 'J' : J, 'K' : K, 'L' : L, 'M' : M, 'N' : N, 'O' : O,
              'P' : P, 'Q' : Q, 'R' : R, 'S' : S, 'T' : T, 'U' : U, 'V' : V,
              'W' : W, 'X' : X, 'Y' : Y, 'Z' : Z, 'AA' : AA, 'AB' : AB, 
              'AC' : AC, 'AD' : AD }
        self.df_Leq_Worst_Hour_Calculations = pd.DataFrame(data=d)
        
        # Now Fill In Column AD
        # Do this after creating the dataframe to keep track of dates and hours
        
        # Distance Correction From Reference Position for Far Lane Noise
        distance_to_middle_of_near_lanes = 50.0 + 12*self.number_of_lanes/4.0
        distance_to_middle_of_far_lanes = distance_to_middle_of_near_lanes \
            + 12*self.number_of_lanes/4.0 + self.median_width + 12*self.number_of_lanes/4.0 
        far_lane_distance_correction = 15*np.log10(distance_to_middle_of_near_lanes \
                                                   / distance_to_middle_of_far_lanes)
        
        # Could do this faster with a vectorized function, but this should be more robust
        # e.g. should be able to handle non-standard date ranges
        all_dates = self.df_Leq_Worst_Hour_Calculations.C.unique()
        hours = list(range(0,24))
        if (self.do_two_lanes):
            for this_date in all_dates:
                for this_hour in hours:
                    mask = (self.df_Leq_Worst_Hour_Calculations.C == this_date) & (self.df_Leq_Worst_Hour_Calculations.B == this_hour)
                    two_links_same_time = self.df_Leq_Worst_Hour_Calculations.AC[mask]
                    idx_near_lane = two_links_same_time.index[0]
                    two_spls = list(two_links_same_time)
                    energy_1 = 10**(two_spls[0]/10)
                    energy_2 = 10**( (two_spls[1] + far_lane_distance_correction) / 10)
                    SPL = 10 * np.log10(energy_1 + energy_2)
                    
                    self.df_Leq_Worst_Hour_Calculations.loc[idx_near_lane, 'AD'] = SPL
        else:
            last_row = int(math.floor(self.df_Leq_Worst_Hour_Calculations.shape[0]/2))
            self.df_Leq_Worst_Hour_Calculations.loc[0:last_row, 'AD'] = self.df_Leq_Worst_Hour_Calculations.AC[0:last_row].copy()
            
        # Determine Percent Missing Speed Data
        speed_auto = self.df_input.loc[:,'SPEED_PASS']
        speed_truck = self.df_input.loc[:,'SPEED_TRUCK']
        speed_all = self.df_input.loc[:,'SPEED_ALL_vehs']
        
        self.percent_missing_auto_speeds = round(100*(1-len(speed_auto[~np.isnan(speed_auto)])/len(speed_auto)),1)
        self.percent_missing_mt_speeds = round(100*(1-len(speed_truck[~np.isnan(speed_truck)])/len(speed_truck)),1)
        self.percent_missing_ht_speeds = round(100*(1-len(speed_truck[~np.isnan(speed_truck)])/len(speed_truck)),1)
        self.percent_missing_bus_speeds = round(100*(1-len(speed_truck[~np.isnan(speed_truck)])/len(speed_truck)),1)
        self.percent_missing_mc_speeds = round(100*(1-len(speed_all[~np.isnan(speed_all)])/len(speed_all)),1)
                 
        return 0
    
    
    def Compute_REMELs_Energy(self, vehicle_type):
    # REMELS COEFFICIENTS
        if vehicle_type == 'AUTO':
            a = 41.740807
            b = 1.148546
            c = 50.128316
            speed_mph = self.df_input.loc[:,'SPEED_PASS']
        elif vehicle_type == 'MT':
            a = 33.918713
            b = 20.591046
            c = 68.002978
            speed_mph = self.df_input.loc[:,'SPEED_TRUCK']
        elif vehicle_type == 'HT':
            a = 35.87985
            b = 21.019665
            c = 74.298135 
            speed_mph = self.df_input.loc[:,'SPEED_TRUCK']
        elif vehicle_type == 'BUS':
            a = 23.47953
            b = 38.006238
            c = 68.002978
            speed_mph = self.df_input.loc[:,'SPEED_TRUCK']
        elif vehicle_type == 'MC':
            a = 41.022542
            b = 10.013879
            c = 56.086099 # CONSISTENCY CHECK: Spreadsheet version 1.0 uses 56.
            speed_mph = self.df_input.loc[:,'SPEED_ALL_vehs']
            
        if self.robust_speeds:
            ersatz_speed = self.df_input.loc[:,'SPEED_ALL_vehs']
            speed_mph[np.isnan(speed_mph)] = ersatz_speed[np.isnan(speed_mph)]
                      
        energy = (speed_mph**(a/10)) * (10**(b/10)) + 10**(c/10)
        
        # Adjust for HT full-throttle on inclines if needed (CORRECT METHOD)
        if vehicle_type == 'HT' and abs(self.near_lane_roadway_grade) >= 1.5:
            c = 80 # HT Full Throttle
            # Get beginning of far lanes
            pivot_idx = len(self.df_input.index[self.df_input.TMC == self.TMCS[0]] )
            if self.near_lane_roadway_grade >= 1.5: 
                # Near lanes are inclined
                # !!!! Indexing confirmed to be correct 5/26/22 4:13 PM !!!!
                speed_mph = self.df_input.loc[0:pivot_idx-1,'SPEED_TRUCK']
                energy[0:pivot_idx] = (speed_mph**(a/10)) * (10**(b/10)) + 10**(c/10)
            else: 
                # Far lanes are inclined
                # !!!! Indexing confirmed to be correct 5/26/22 4:13 PM !!!!
                speed_mph = self.df_input.loc[pivot_idx:,'SPEED_TRUCK']
                energy[pivot_idx:] = (speed_mph**(a/10)) * (10**(b/10)) + 10**(c/10)

        return energy

    
    def Compute_Summary_Data_Worst_Hour(self):
        # COMBINE RESULTS FOR BOTH LINKS
        # Create dataframe for DAY WORST OF 365 HOURS, based on single worst hour of any day, 
        # based on total levels of both near and far lanes.
        # df includes total volumes, average speeds, and total hourly levels
        # Traffic_Hour      Auto_Vol      MT_Vol     ...  Auto_Speed   Truck_Speed  Total_SPL
        # 0                 2471.973755   52.401358  ...  63.579188    60.135268    81.151459
        # 1                 1509.602227   44.419772  ...  65.044710    58.863831    80.312725
        # 2                 1198.049828   46.968836  ...  64.057102    57.537148    79.836404
        # 3                 1358.733293   63.569662  ...  58.730976    59.466569    80.460046

        # Local variables
        number_of_total_spl_rows = math.floor(self.df_Leq_Worst_Hour_Calculations.shape[0]/2)
        idx_last_total_spl_row = number_of_total_spl_rows - 1
        number_of_nans_in_total_spl = np.isnan(self.df_Leq_Worst_Hour_Calculations.loc[0:idx_last_total_spl_row, 'AD']).sum()
        
        # Directly Accessable Attributes
        if number_of_nans_in_total_spl >= number_of_total_spl_rows:
            self.df_day_WORST_HOUR_DATE = np.nan
            
            self.LAeq_24hrs_WORST_HOUR_DATE = np.nan
            self.Ldn_WORST_HOUR_DATE = np.nan
            self.Lden_WORST_HOUR_DATE = np.nan
            self.WORST_HOUR = np.nan
            self.WORST_HOUR_DATE = np.nan
            self.LAeq_WORST_HOUR = np.nan
            
            return 
        
        self.WORST_HOUR = self.df_Leq_Worst_Hour_Calculations.B[self.df_Leq_Worst_Hour_Calculations.AD.idxmax()]
        self.WORST_HOUR_DATE = self.df_Leq_Worst_Hour_Calculations.C[self.df_Leq_Worst_Hour_Calculations.AD.idxmax()]
        self.LAeq_WORST_HOUR = self.df_Leq_Worst_Hour_Calculations.AD.max()
        
        # Local variables
        df_all_days = self.df_Leq_Worst_Hour_Calculations.copy()
        idx_hour_WORST_HOUR = df_all_days.AD.idxmax()
        date_WORST_DAY = df_all_days.C[idx_hour_WORST_HOUR]
        
        # Requires full day for each link for WORST DAY
        first_link_idx_hour_0 = df_all_days.C[df_all_days.C == date_WORST_DAY].index[0]
        second_link_idx_hour_0 = df_all_days.C[df_all_days.C == date_WORST_DAY].index[24]
               
        # Inputs to this dataframe
        Traffic_Hour = df_all_days.B[first_link_idx_hour_0:first_link_idx_hour_0+24].copy()
        
        # Total Volumes by Hour for DAY WORST OF 365 HOURS
        # Note, confirmed .to_numpy() does return reference, which carries 
        # through even to creation of the new df, so want to include .copy()
        
        # Volumes - Total of Both Links (Near and Far Lanes) for Each Hour
        Auto_Total_Vol = df_all_days.D[first_link_idx_hour_0:first_link_idx_hour_0+24].copy().to_numpy() \
            + df_all_days.D[second_link_idx_hour_0:second_link_idx_hour_0+24].copy().to_numpy()
        
        MT_Total_Vol = df_all_days.E[first_link_idx_hour_0:first_link_idx_hour_0+24].copy().to_numpy() \
            + df_all_days.E[second_link_idx_hour_0:second_link_idx_hour_0+24].copy().to_numpy()
        
        HT_Total_Vol = df_all_days.F[first_link_idx_hour_0:first_link_idx_hour_0+24].copy().to_numpy() \
            + df_all_days.F[second_link_idx_hour_0:second_link_idx_hour_0+24].copy().to_numpy()
        
        BUS_Total_Vol = df_all_days.G[first_link_idx_hour_0:first_link_idx_hour_0+24].copy().to_numpy() \
            + df_all_days.G[second_link_idx_hour_0:second_link_idx_hour_0+24].copy().to_numpy()
        
        MC_Total_Vol = df_all_days.H[first_link_idx_hour_0:first_link_idx_hour_0+24].copy().to_numpy() \
            + df_all_days.H[second_link_idx_hour_0:second_link_idx_hour_0+24].copy().to_numpy()
        
        # Average Speeds - Averaged over Both Links (Near and Far Lanes) for Each Hour
        Auto_Speed = df_all_days.I[first_link_idx_hour_0:first_link_idx_hour_0+24].copy().to_numpy() \
            + df_all_days.I[second_link_idx_hour_0:second_link_idx_hour_0+24].copy().to_numpy()
        Auto_Speed = 0.5*Auto_Speed
        
        Truck_Speed = df_all_days.J[first_link_idx_hour_0:first_link_idx_hour_0+24].copy().to_numpy() \
            + df_all_days.J[second_link_idx_hour_0:second_link_idx_hour_0+24].copy().to_numpy()
        Truck_Speed = 0.5*Truck_Speed
        
        MC_Speed = df_all_days.K[first_link_idx_hour_0:first_link_idx_hour_0+24].copy().to_numpy() \
            + df_all_days.K[second_link_idx_hour_0:second_link_idx_hour_0+24].copy().to_numpy()
        MC_Speed = 0.5*MC_Speed

        # Total SPL -  Comes straight from Column AD of the WORST_HOUR_DATE       
        Total_SPL = df_all_days.AD[first_link_idx_hour_0:first_link_idx_hour_0+24].copy().to_numpy()

        d = { 'Traffic_Hour' : Traffic_Hour, 'Auto_Total_Vol' : Auto_Total_Vol,
              'MT_Total_Vol' : MT_Total_Vol, 'HT_Total_Vol' : HT_Total_Vol, 
              'BUS_Total_Vol' : BUS_Total_Vol, 'MC_Total_Vol' : MC_Total_Vol,
              'Auto_Speed' : Auto_Speed, 'Truck_Speed' : Truck_Speed, 
              'MC_Speed' : MC_Speed, 'Total_SPL' : Total_SPL }
              
        self.df_day_WORST_HOUR_DATE = pd.DataFrame(data=d).reset_index(drop=True)
        
        # More Directly Accessable Attributes
        self.LAeq_24hrs_WORST_HOUR_DATE = self.Compute_24_Hour_LAeq(self.df_day_WORST_HOUR_DATE)
        self.Ldn_WORST_HOUR_DATE = self.Compute_LDN(self.df_day_WORST_HOUR_DATE)
        self.Lden_WORST_HOUR_DATE = self.Compute_LDEN(self.df_day_WORST_HOUR_DATE)

        return 0
        
    
    def Compute_Summary_Data_AVG_Day(self):
        # Create dataframe for AVG DAY, including WORST HOUR of AVG DAY
        df_all_days = self.df_Leq_Worst_Hour_Calculations.copy()
        
        Traffic_Hour = np.zeros((24), dtype=int)
        
        Auto_Total_Vol = np.zeros((24), dtype=float)
        MT_Total_Vol = np.zeros((24), dtype=float)
        HT_Total_Vol = np.zeros((24), dtype=float)
        BUS_Total_Vol = np.zeros((24), dtype=float)
        MC_Total_Vol = np.zeros((24), dtype=float)
        
        Auto_Speed = np.zeros((24), dtype=float)
        Truck_Speed = np.zeros((24), dtype=float)
        MC_Speed = np.zeros((24), dtype=float)
                
        Total_SPL = np.zeros((24), dtype=float)
        
        idx = 0
        for hour in range(0,24):
            Traffic_Hour[idx] = hour

            # Get df of All Hours of Current Hour Throughout the Year 
            # Include both links (near and far lanes)
            df_hr = df_all_days[df_all_days.B == hour]
            
            # Volumes are determined by summing all available hourly volumes for
            # both links (near and far lanes), since it takes one sample from each
            # link to estimate the total, the average is determined by dividing 
            # the total by half of number of samples
            Auto_Total_Vol[idx] = df_hr.D.sum()/( 0.5 * df_hr.shape[0] )
            MT_Total_Vol[idx] = df_hr.E.sum()/( 0.5 * df_hr.shape[0] )
            HT_Total_Vol[idx] = df_hr.F.sum()/( 0.5 * df_hr.shape[0] )
            BUS_Total_Vol[idx] = df_hr.G.sum()/( 0.5 * df_hr.shape[0] )
            MC_Total_Vol[idx] = df_hr.H.sum()/( 0.5 * df_hr.shape[0] )
            
            # Since do not need to sum both lanes, average is over all samples
            Auto_Speed[idx] = df_hr.I.sum()/df_hr.shape[0]
            Truck_Speed[idx] = df_hr.J.sum()/df_hr.shape[0]
            MC_Speed[idx] = df_hr.K.sum()/df_hr.shape[0]
            
            # Average levels all rows, since levels are already totaled across links 
            # Since we are interedsted in the average exposure, should use log
            # averaging
            spl = df_hr.AD.to_numpy()
            spl = spl[~np.isnan(spl)]
            Total_SPL[idx] = 10*np.log10(sum(10**(spl/10))) - 10*np.log10(len(spl))
            idx = idx + 1

        d = { 'Traffic_Hour' : Traffic_Hour, 'Auto_Total_Vol' : Auto_Total_Vol,
              'MT_Total_Vol' : MT_Total_Vol, 'HT_Total_Vol' : HT_Total_Vol, 
              'BUS_Total_Vol' : BUS_Total_Vol, 'MC_Total_Vol' : MC_Total_Vol,
              'Auto_Speed' : Auto_Speed, 'Truck_Speed' : Truck_Speed, 
              'MC_Speed' : MC_Speed, 'Total_SPL' : Total_SPL }
        
        self.df_day_AVG_DAY = pd.DataFrame(data=d)
        
        
        self.LAeq_WORST_HOUR_AVG = self.df_day_AVG_DAY.Total_SPL.max()    
        
        tmp = self.df_day_AVG_DAY.loc[self.df_day_AVG_DAY['Total_SPL'] == \
                                                      self.LAeq_WORST_HOUR_AVG,'Traffic_Hour']
        if len(tmp) > 0:
            self.WORST_HOUR_AVG = tmp.values[0]
        else:
            self.WORST_HOUR_AVG = np.nan
                
        self.LAeq_24hrs_AVG_DAY = self.Compute_24_Hour_LAeq(self.df_day_AVG_DAY)
        self.Ldn_AVG_DAY = self.Compute_LDN(self.df_day_AVG_DAY)
        self.Lden_AVG_DAY = self.Compute_LDEN(self.df_day_AVG_DAY)

        # This is designed to match the spreadsheet in the OUTPUT tab
        # Note there is a minor bug in the spreadsheet where it does not account
        # for the last day of a leap year. So in that case there may be a slight
        # descrepency
                
        # Note, AADT is computed from user inputs for each link
        # Note, Fractions by Vehicle are Computed from Actual Data 
        # (and the Sum of those actual data)
        # The sum of the actual data and the AADT will not be an exact match
        # due to sporadic missing hourly data. But fractions should be good 
        # estimates.
        self.AADT = self.df_input.aadt.unique().sum()
        
        cols_to_sum = ('Auto_Total_Vol','MT_Total_Vol','HT_Total_Vol','BUS_Total_Vol','MC_Total_Vol')
        summed_cols = self.df_day_AVG_DAY.loc[:,cols_to_sum].sum()
        total_actual_sum = summed_cols.sum()

        self.Auto_Overall_Fraction = 100 * summed_cols['Auto_Total_Vol'] / total_actual_sum
        self.MT_Overall_Fraction = 100 * summed_cols['MT_Total_Vol'] / total_actual_sum
        self.HT_Overall_Fraction = 100 * summed_cols['HT_Total_Vol'] / total_actual_sum
        self.BUS_Overall_Fraction = 100 * summed_cols['BUS_Total_Vol'] / total_actual_sum
        self.MC_Overall_Fraction = 100 * summed_cols['MC_Total_Vol'] / total_actual_sum
        
        return 0
    
    

    
    
    def Compute_Day_Eve_Night_Fractions(self, df, Vehicle_Type, bool_compute_ldn):
        # Compute Day, Evening and Night Percents for Each Vehicle Type
        # Based on a 24-Hour Dataframe
        # NOTE, Day for Ldn = Sum of THIS Day and Evening, i.e.
        # Day = Day + Evening for Ldn

        if (bool_compute_ldn):
            Fractions = [0, 0]
        else:
            Fractions = [0, 0, 0]
        
        Total_Vol = df.Auto_Total_Vol.sum() + df.MT_Total_Vol.sum() \
            + df.HT_Total_Vol.sum() + df.BUS_Total_Vol.sum() \
            + df.MC_Total_Vol.sum()
        
        col = Vehicle_Type + "_Total_Vol"
        
        if (bool_compute_ldn):
            Fractions[0] = df.loc[7:21,col].sum() / Total_Vol # Day (including Evening)
            Fractions[1] = ( df.loc[0:6,col].sum() + df.loc[22:23,col].sum())/ Total_Vol # Night
        else:
            Fractions[0] = df.loc[7:18,col].sum() / Total_Vol # Day (excluding Evening)
            Fractions[1] = df.loc[19:21,col].sum() / Total_Vol # Evening
            Fractions[2] = ( df.loc[0:6,col].sum() + df.loc[22:23,col].sum())/ Total_Vol # Night
        
        return Fractions
        
    #%% Future Metrics
    def Compute_Future_Metrics(self, future_aadt = np.nan,
                                     Auto_Fractions = [np.nan, np.nan],
                                     MT_Fractions = [np.nan, np.nan],
                                     HT_Fractions = [np.nan, np.nan],
                                     BUS_Fractions = [np.nan, np.nan],
                                     MC_Fractions = [np.nan, np.nan]):     
        # If future_aadt is left as default, then present AADT will be used
        if np.isnan(future_aadt):
            self.future_aadt = self.AADT
        else:
            self.future_aadt = future_aadt
            
        # Make sure all Vehicle fractions have same length (should be two or three elements)
        if (not(len(Auto_Fractions) == len(MT_Fractions) and \
                len(MT_Fractions) == len(HT_Fractions) and \
                len(HT_Fractions) == len(BUS_Fractions) and \
                len(BUS_Fractions) == len(MC_Fractions))):
            print(" ")
            print("Error user input traffic data are not the same length.")
            print(" ")
            return
            
        # Determine if Ldn or Lden should be computed. This is determined based
        # on the length of the volume fractions
        if (len(Auto_Fractions) == 2):
            Bool_Compute_LDN = True
        elif (len(Auto_Fractions) == 3):
            Bool_Compute_LDN = False
        else:
            print(" ")
            print("Error user input traffic data are not of length 2 or 3.")
            print(" ")
            return
            
        # In order to do volume fraction corrections. Present day Day, Evening and
        #    Night fractions are needed
        
        # Present Day DEN Fractions Worst Date
        self.Present_AUTO_DEN_Fractions_Worst_Day = \
            self.Compute_Day_Eve_Night_Fractions(self.df_day_WORST_HOUR_DATE, 'Auto', Bool_Compute_LDN)
            
        self.Present_MT_DEN_Fractions_Worst_Day = \
            self.Compute_Day_Eve_Night_Fractions(self.df_day_WORST_HOUR_DATE, 'MT' ,Bool_Compute_LDN)    
            
        self.Present_HT_DEN_Fractions_Worst_Day = \
            self.Compute_Day_Eve_Night_Fractions(self.df_day_WORST_HOUR_DATE, 'HT', Bool_Compute_LDN)    

        self.Present_BUS_DEN_Fractions_Worst_Day = \
            self.Compute_Day_Eve_Night_Fractions(self.df_day_WORST_HOUR_DATE, 'BUS', Bool_Compute_LDN)    

        self.Present_MC_DEN_Fractions_Worst_Day = \
            self.Compute_Day_Eve_Night_Fractions(self.df_day_WORST_HOUR_DATE, 'MC', Bool_Compute_LDN)    

        sum_perc = sum(self.Present_AUTO_DEN_Fractions_Worst_Day) \
            + sum(self.Present_MT_DEN_Fractions_Worst_Day) \
            + sum(self.Present_HT_DEN_Fractions_Worst_Day) \
            + sum(self.Present_BUS_DEN_Fractions_Worst_Day) \
            + sum(self.Present_MC_DEN_Fractions_Worst_Day)
        if (round(sum_perc,1) != 1.0):
            print(" ")
            print("Warning sum present vehicle fractions for worst hour/day = " + str(round(sum_perc,1)) + ", not 1.0")
            print(" ")
            
        # Present Day DEN Fractions Average Day
        self.Present_AUTO_DEN_Fractions_AVG_Day = \
            self.Compute_Day_Eve_Night_Fractions(self.df_day_AVG_DAY, 'Auto', Bool_Compute_LDN)
            
        self.Present_MT_DEN_Fractions_AVG_Day = \
            self.Compute_Day_Eve_Night_Fractions(self.df_day_AVG_DAY, 'MT', Bool_Compute_LDN)    
            
        self.Present_HT_DEN_Fractions_AVG_Day = \
            self.Compute_Day_Eve_Night_Fractions(self.df_day_AVG_DAY, 'HT', Bool_Compute_LDN)    

        self.Present_BUS_DEN_Fractions_AVG_Day = \
            self.Compute_Day_Eve_Night_Fractions(self.df_day_AVG_DAY, 'BUS', Bool_Compute_LDN)    

        self.Present_MC_DEN_Fractions_AVG_Day = \
            self.Compute_Day_Eve_Night_Fractions(self.df_day_AVG_DAY, 'MC', Bool_Compute_LDN)    

        sum_perc = sum(self.Present_AUTO_DEN_Fractions_AVG_Day) \
            + sum(self.Present_MT_DEN_Fractions_AVG_Day) \
            + sum(self.Present_HT_DEN_Fractions_AVG_Day) \
            + sum(self.Present_BUS_DEN_Fractions_AVG_Day) \
            + sum(self.Present_MC_DEN_Fractions_AVG_Day)
        if (round(sum_perc,1) != 1.0):
            print(" ")
            print("Warning sum present vehicle fractions for avg hour/day = " + str(round(sum_perc,1)) + ", not 1.0")
            print(" ")


        # Determine if we have enough user input for DN / DEN, otherwise will fall back to present fractions
        Day_or_Night_are_NAN = np.isnan([ Auto_Fractions, MT_Fractions, HT_Fractions, \
                                          BUS_Fractions, MC_Fractions])                                           
        
        # SET FLAG TO KEEP TRACK OF WHERE DEN DATA COME FROM
        Using_User_Data = False
        if (Day_or_Night_are_NAN.any()):
            # If any vehicle data are left as default (nan), then will use 
            # present % for all vehicles, otherwise no good way to guess the nans
            
            # For Worst Date
            self.Future_AUTO_DEN_Fractions_Worst_Day = self.Present_AUTO_DEN_Fractions_Worst_Day
            self.Future_MT_DEN_Fractions_Worst_Day = self.Present_MT_DEN_Fractions_Worst_Day
            self.Future_HT_DEN_Fractions_Worst_Day = self.Present_HT_DEN_Fractions_Worst_Day
            self.Future_BUS_DEN_Fractions_Worst_Day = self.Present_BUS_DEN_Fractions_Worst_Day
            self.Future_MC_DEN_Fractions_Worst_Day = self.Present_MC_DEN_Fractions_Worst_Day
    
            sum_perc = sum(self.Future_AUTO_DEN_Fractions_Worst_Day) \
                + sum(self.Future_MT_DEN_Fractions_Worst_Day) \
                + sum(self.Future_HT_DEN_Fractions_Worst_Day) \
                + sum(self.Future_BUS_DEN_Fractions_Worst_Day) \
                + sum(self.Future_MC_DEN_Fractions_Worst_Day)

            if (round(sum_perc,1) != 1.0):
                print(" ")
                print("Warning sum future vehicle fractions for worst hour/day = " + str(round(sum_perc,1)) + ", not 1.0")
                print(" ")
            
            # For Average
            self.Future_AUTO_DEN_Fractions_AVG_Day = self.Present_AUTO_DEN_Fractions_AVG_Day
            self.Future_MT_DEN_Fractions_AVG_Day = self.Present_MT_DEN_Fractions_AVG_Day
            self.Future_HT_DEN_Fractions_AVG_Day = self.Present_HT_DEN_Fractions_AVG_Day
            self.Future_BUS_DEN_Fractions_AVG_Day = self.Present_BUS_DEN_Fractions_AVG_Day
            self.Future_MC_DEN_Fractions_AVG_Day = self.Present_MC_DEN_Fractions_AVG_Day
    
            sum_perc = sum(self.Future_AUTO_DEN_Fractions_AVG_Day) \
                + sum(self.Future_MT_DEN_Fractions_AVG_Day) \
                + sum(self.Future_HT_DEN_Fractions_AVG_Day) \
                + sum(self.Future_BUS_DEN_Fractions_AVG_Day) \
                + sum(self.Future_MC_DEN_Fractions_AVG_Day)

            if (round(sum_perc,1) != 1.0):
                print(" ")
                print("Warning sum future vehicle fractions for worst hour/day = " + str(round(sum_perc,1)) + ", not 1.0")
                print(" ")
                
            # Invalideate User Data
            self.Future_AUTO_DEN_Fractions_User_Data = np.nan
            self.Future_MT_DEN_Fractions_User_Data = np.nan
            self.Future_HT_DEN_Fractions_User_Data = np.nan
            self.Future_BUS_DEN_Fractions_User_Data = np.nan
            self.Future_MC_DEN_Fractions_User_Data = np.nan
                
        else:
            # User has provided sufficient future percentages to at least compute Ldn
            Using_User_Data= True
            
            # For User DEN Data
            self.Future_AUTO_DEN_Fractions_User_Data = Auto_Fractions
            self.Future_MT_DEN_Fractions_User_Data = MT_Fractions
            self.Future_HT_DEN_Fractions_User_Data = HT_Fractions
            self.Future_BUS_DEN_Fractions_User_Data = BUS_Fractions
            self.Future_MC_DEN_Fractions_User_Data = MC_Fractions
            
            sum_perc = sum(self.Future_AUTO_DEN_Fractions_User_Data) \
                + sum(self.Future_MT_DEN_Fractions_User_Data) \
                + sum(self.Future_HT_DEN_Fractions_User_Data) \
                + sum(self.Future_BUS_DEN_Fractions_User_Data) \
                + sum(self.Future_MC_DEN_Fractions_User_Data)
                
            if (round(sum_perc,1) != 1.0):
                print(" ")
                print("Warning sum future vehicle fractions for user input = " + str(round(sum_perc,1)) + ", not 1.0")
                print(" ")
                
            # Invalidate Worst and AVG DEN Data
            self.Future_AUTO_DEN_Fractions_Worst_Day = np.nan
            self.Future_MT_DEN_Fractions_Worst_Day = np.nan
            self.Future_HT_DEN_Fractions_Worst_Day = np.nan
            self.Future_HT_DEN_Fractions_Worst_Day = np.nan
            self.Future_MC_DEN_Fractions_Worst_Day = np.nan
            
            self.Future_AUTO_DEN_Fractions_AVG_Day = np.nan
            self.Future_MT_DEN_Fractions_AVG_Day = np.nan
            self.Future_HT_DEN_Fractions_AVG_Day = np.nan
            self.Future_HT_DEN_Fractions_AVG_Day = np.nan
            self.Future_MC_DEN_Fractions_AVG_Day = np.nan
        
        return
    
    