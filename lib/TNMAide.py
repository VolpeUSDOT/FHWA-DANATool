# -*- coding: utf-8 -*-
"""
Created By: Volpe National Transportation Systems Center
Based On: TNMAide.xlsm created by Cambridge Systematics 
Created on Tue May 17 07:36:17 2022

@author: Aaron.Hastings
"""

import pandas as pd
import numpy as np

class TNMAide:  
    # SCOPE:
    #--------------------------------------------------------------------------
    # TBD
    
    # NOTES:
    # WORST_HOUR_DATE   refers to the date that has the highest 1-hr SPL  
    #                   totalled over both the near and far lanes     
    # WORST_HOUR       refers to the hour within the worst date that had that 
    #                  highest SPL
    # AVG_DAY          refers to metrics that have been average over all 365* 
    #                  or 366* days all hours inclusive
    # AVG_HOUR         refers to metrics that have been averaged over the same 
    #                  hour for the entire calendar year*
    # WORST_HOUR_AVG   refers to the worst hour within an AVG DAY
    # * assuming a full year has been provided
    

    # TESTS:
    #--------------------------------------------------------------------------
    # 1) 365 Day Year, Full Dataset, No Grade, No Median, 2 Lanes
    # 2) 365 Day Year, Full Dataset, No Grade, No Median, 6 Lanes
    # 3) 365 Day Year, Full Dataset, No Grade, 50 ft Median, 6 Lanes
    # 4) 365 Day Year, Full Dataset, 2% Grade, 50 ft Median, 6 Lanes           
    # 5) 365 Day Year, Full Dataset, -2% Grade, 50 ft Median, 6 Lanes          
    # 6) 366 Day Year, Full Dataset, 2% Grade, 50 ft Median, 6 Lanes           
    #    -- Will fail due to grade bug for far lane in spreadsheet
    #    -- See Leq Worst Hour Calculations tab, Column N, Rows 8769 to 8793
    # 6) 366 Day Year, Full Dataset, 0% Grade, 50 ft Median, 6 Lanes           
    # 7) One Month of Data, 0% Grade, 50 ft Median, 6 Lanes
    # 8) 365 Day Year, One Direction Only, No Grade, 50 ft Median, 6 Lanes     
    #    -- Will fail, spreadsheet can only handle 365 or 366 days


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


    # PROPERTIES:
    #--------------------------------------------------------------------------
    
    # Input Properties ...
    #--------------------------------------------------------------------------
    # number_of_lanes             (Same Value as Input Parameter)
    # median_width                (Same Value as Input Parameter)
    # near_lane_roadway_grade     (Same Value as Input Parameter)
    # df_input                    (Same Value as df Input Parameter)
    #
    
    # Intermediate Properties (Used for Documentation) ...
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

    # "Results" Properties ...
    #--------------------------------------------------------------------------
    # AADT                    = (float) Average Annual Daily Traffic for 
    #                           present condition [df.aadt]
    # Auto_Fraction           = Auto Fraction of AADT (note Daily, not Hourly)
    # MT_Fraction             = MT Fraction of AADT (note Daily, not Hourly)
    # HT_Fraction             = HT Fraction of AADT (note Daily, not Hourly)
    # BUS_Fraction            = BUS Fraction of AADT (note Daily, not Hourly)
    # MC_Fraction             = MC Fraction of AADT (note Daily, not Hourly)
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
    #                           except no penalties for time of day
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

    def __init__(self, df, number_of_lanes = 2, median_width = 10.0, near_lane_roadway_grade = 0.0):
        # Evaluate inputs
        df_rows_columns = df.shape
        if df_rows_columns[0] < 17520:
            print('Warning: Insufficient number of hours in the data table for a full year. '
                  + 'TNMAide requires 17520 hours for a standard year and 17544 hours ' 
                  + 'for a leap year. However, analysis of smaller data sets can be done.')

        if df_rows_columns[1] < 29:
            print('Warning: There may be insufficient columns in the data table.'
                  + 'Standard input consists of 29 columns in the data table.')
        
        # These Replicate the User Inputs
        self.df_input = df.replace(999999.0, np.nan)
        self.number_of_lanes = number_of_lanes
        self.median_width = median_width 
        self.near_lane_roadway_grade = near_lane_roadway_grade
        
        # Intermediate Properties
        self.TMCS = self.df_input.TMC.unique() # First TMC is assumed to be the near lanes
        if len(self.TMCS) > 2:
            print('Error: TNMAide can handle at most two (2) TMCs.')
            return
        self.computed_futures = False
        self.roadway = df.loc[0,'ROAD']
        self.direction_near_lanes = df.loc[0,'DIRECTION']
        self.direction_far_lanes = df.loc[17519,'DIRECTION']
        self.county = df.loc[0,'COUNTY']
        self.state = df.loc[0,'STATE']
        
        # "Results" Properties
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
        
        K = np.nan * I # This is a filler column so that this df aligns with table in spreadsheet
        
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
                        
        return 0
    
    
    def Compute_Future_Metrics_Current_Distribution(self, future_aadt, future_metric = 'lden'):     
        self.future_aadt = future_aadt
        self.percents_will_change = False
        
        if future_metric == 'lden' or future_metric == 'ldn':
            self.future_metric = future_metric 
        else:
            print('Invalied metric selection, ' + future_metric + ' for computing future metrics.' )
        # The sum of all the future_#_percent_* should equal 100
        self.future_daytime_percent_autos = 100
        self.future_evening_percent_autos = 0
        self.future_nighttime_percent_autos = 0
        self.future_daytime_percent_mts = 0
        self.future_evening_percent_mts = 0
        self.future_nighttime_percent_mts = 0
        self.future_daytime_percent_hts = 0
        self.future_evening_percent_hts = 0
        self.future_nighttime_percent_hts = 0         
        self.computed_futures = True
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
            c = 56.086099 # CONSISTENCY CHECK: Spreadsheet version uses 56.
            speed_mph = self.df_input.loc[:,'SPEED_ALL_vehs']
                      
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
        # Create dataframe for DAY WORST OF 365 HOURS, based on single worst hour of any day, 
        # based on total levels of both near and far lanes.
        # df includes total volumes, average speeds, and total hourly levels
        # Traffic_Hour      Auto_Vol      MT_Vol     ...  Auto_Speed   Truck_Speed  Total_SPL
        # 0                 2471.973755   52.401358  ...  63.579188    60.135268    81.151459
        # 1                 1509.602227   44.419772  ...  65.044710    58.863831    80.312725
        # 2                 1198.049828   46.968836  ...  64.057102    57.537148    79.836404
        # 3                 1358.733293   63.569662  ...  58.730976    59.466569    80.460046

        # Directly Accessable Properties
        self.WORST_HOUR = self.df_Leq_Worst_Hour_Calculations.B[self.df_Leq_Worst_Hour_Calculations.AD.idxmax()]
        self.WORST_HOUR_DATE = self.df_Leq_Worst_Hour_Calculations.C[self.df_Leq_Worst_Hour_Calculations.AD.idxmax()]
        self.LAeq_WORST_HOUR = self.df_Leq_Worst_Hour_Calculations.AD.max()
        
        # Intermediate values
        df_all_days = self.df_Leq_Worst_Hour_Calculations.copy()
        idx_hour_WORST_HOUR = df_all_days.AD.idxmax()
        date_WORST_DAY = df_all_days.C[idx_hour_WORST_HOUR]
        
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
        
        # Total SPL -  Comes straight from Column AD of the WORST_HOUR_DATE       
        Total_SPL = df_all_days.AD[first_link_idx_hour_0:first_link_idx_hour_0+24].copy().to_numpy()

        d = { 'Traffic_Hour' : Traffic_Hour, 'Auto_Total_Vol' : Auto_Total_Vol,
              'MT_Total_Vol' : MT_Total_Vol, 'HT_Total_Vol' : HT_Total_Vol, 
              'BUS_Total_Vol' : BUS_Total_Vol, 'MC_Total_Vol' : MC_Total_Vol,
              'Auto_Speed' : Auto_Speed, 'Truck_Speed' : Truck_Speed, 'Total_SPL' : Total_SPL }
              
        self.df_day_WORST_HOUR_DATE = pd.DataFrame(data=d).reset_index(drop=True)
        
        # More Directly Accessable Properties
        # TBD
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
        
        Total_SPL = np.zeros((24), dtype=float)
        
        idx = 0
        for hour in range(0,24):
            Traffic_Hour[idx] = hour

            # Get df of All Hours of Current Hour Throughout the Year 
            # Include both links (near and far lanes)
            df_hr = df_all_days[df_all_days.B == hour]
            
            # Volumes are determined by summing all available hourly volumes for
            # both links (near and far lanes), since it takes on sample from each
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
              'Auto_Speed' : Auto_Speed, 'Truck_Speed' : Truck_Speed, 'Total_SPL' : Total_SPL }
        self.df_day_AVG_DAY = pd.DataFrame(data=d)

        self.WORST_HOUR_AVG = Total_SPL.argmax()
        self.LAeq_WORST_HOUR_AVG = Total_SPL.max()
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
        # estimated.
        self.AADT = self.df_input.aadt.unique().sum()
        
        cols_to_sum = ('Auto_Total_Vol','MT_Total_Vol','HT_Total_Vol','BUS_Total_Vol','MC_Total_Vol')
        summed_cols = self.df_day_AVG_DAY.loc[:,cols_to_sum].sum()
        total_actual_sum = summed_cols.sum()

        self.Auto_Fraction = 100 * summed_cols['Auto_Total_Vol'] / total_actual_sum
        self.MT_Fraction = 100 * summed_cols['MT_Total_Vol'] / total_actual_sum
        self.HT_Fraction = 100 * summed_cols['HT_Total_Vol'] / total_actual_sum
        self.BUS_Fraction = 100 * summed_cols['BUS_Total_Vol'] / total_actual_sum
        self.MC_Fraction = 100 * summed_cols['MC_Total_Vol'] / total_actual_sum
        
        return 0
    
    def Compute_24_Hour_LAeq(self, df_summary_day):
        LAeq_24hrs = 10*np.log10(sum(10**(df_summary_day.Total_SPL/10))) - 10*np.log10(24)
        return LAeq_24hrs
    
    def Compute_LDN(self, df_summary_day):
        # Adjustments by Hour
        # LDEN	LDN	HOUR
        # 10	10	0
        # 10	10	1
        # 10	10	2
        # 10	10	3
        # 10	10	4
        # 10	10	5
        # 10	10	6
        # 5		    19
        # 5		    20
        # 5		    21
        # 10	10	22
        # 10	10	23
        df = df_summary_day.copy()
        df.loc[0:6, 'Total_SPL'] = df.loc[0:6, 'Total_SPL'] + 10
        df.loc[22:, 'Total_SPL'] = df.loc[22:, 'Total_SPL'] + 10
        Ldn = 10*np.log10(sum(10**(df.Total_SPL/10))) - 10*np.log10(24)
        return Ldn
        
        return 0
 
    def Compute_LDEN(self, df_summary_day):
        # Adjustments by Hour
        # LDEN	LDN	HOUR
        # 10	10	0
        # 10	10	1
        # 10	10	2
        # 10	10	3
        # 10	10	4
        # 10	10	5
        # 10	10	6
        # 5		    19
        # 5		    20
        # 5		    21
        # 10	10	22
        # 10	10	23
        df = df_summary_day.copy()
        df.loc[0:6, 'Total_SPL'] = df.loc[0:6, 'Total_SPL'] + 10
        df.loc[19:21, 'Total_SPL'] = df.loc[19:21, 'Total_SPL'] + 5
        df.loc[22:, 'Total_SPL'] = df.loc[22:, 'Total_SPL'] + 10
        Lden = 10*np.log10(sum(10**(df.Total_SPL/10))) - 10*np.log10(24)
        return Lden
