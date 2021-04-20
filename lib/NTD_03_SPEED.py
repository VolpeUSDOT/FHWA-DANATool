# -*- coding: utf-8 -*-
"""
Created by Cambridge Systematics
Modified By: Volpe National Transportation Systems Center

"""
import pandas as pd
import numpy as np
import pathlib
import time
import pyarrow as pa
import pyarrow.parquet as pq

def SPEED(SELECT_STATE, PATH_NPMRDS):
    
    def lapTimer(text,now):
        print('%s%.3f' %(text,time.time()-now))
        return time.time()
    
    now=time.time()
    
    outputpath = 'Final Output/Process3_MOVES_Speed_Distributions/'
    pathlib.Path(outputpath).mkdir(exist_ok=True)
    
    print ('')
    print ('********** Produce Speed Distributions **********')
    
    print ('Reading Composite Dataset')
    df = pq.read_table(PATH_NPMRDS)
    df = df.to_pandas()
    
    now=lapTimer('  took: ',now)
    
    #Developing Average Speed Distribution Summaries
    print ('Developing Average Speed Distribution Summaries')
    #Creating Speed data from passenger and truck speeds
    df['TT_PASS'] = df['travel_time_pass']
    df.loc[df['TT_PASS'].isnull(), 'TT_PASS'] = df.loc[df['TT_PASS'].isnull(),'travel_time_all']
    df['TT_TRUCK'] = df['travel_time_truck']
    df.loc[df['TT_TRUCK'].isnull(), 'TT_TRUCK'] = df.loc[df['TT_TRUCK'].isnull(), 'travel_time_all']
    df['VEH_HR10'] = df['PCT_TYPE10']*df['aadt']*df['TT_PASS']/3600
    df['VEH_HR25'] = df['PCT_TYPE25']*df['aadt']*df['TT_PASS']/3600
    df['VEH_HR40'] = df['PCT_TYPE40']*df['aadt']*df['TT_TRUCK']/3600
    df['VEH_HR50'] = df['PCT_TYPE50']*df['aadt']*df['TT_TRUCK']/3600
    df['VEH_HR60'] = df['PCT_TYPE60']*df['aadt']*df['TT_TRUCK']/3600
    df['VMT10'] = df['PCT_TYPE10']*df['aadt']*df['tmc_length']
    df['VMT25'] = df['PCT_TYPE25']*df['aadt']*df['tmc_length']
    df['VMT40'] = df['PCT_TYPE40']*df['aadt']*df['tmc_length']
    df['VMT50'] = df['PCT_TYPE50']*df['aadt']*df['tmc_length']
    df['VMT60'] = df['PCT_TYPE60']*df['aadt']*df['tmc_length']
    df['TSPD'] = (df['VMT10']+df['VMT25']+df['VMT40']+df['VMT50']+df['VMT60'])/(df['VEH_HR10']+df['VEH_HR25']+df['VEH_HR40']+df['VEH_HR50']+df['VEH_HR60'])
    #Creating Speed bins
    df['avgSpeedBinID']=np.nan
    
    df.loc[df['TSPD']<2.5, 'avgSpeedBinID'] = 1
    df.loc[(df['TSPD']>=2.5)&(df['TSPD']<7.5), 'avgSpeedBinID'] = 2
    df.loc[(df['TSPD']>=7.5)&(df['TSPD']<12.5), 'avgSpeedBinID'] = 3
    df.loc[(df['TSPD']>=12.5)&(df['TSPD']<17.5), 'avgSpeedBinID'] = 4
    df.loc[(df['TSPD']>=17.5)&(df['TSPD']<22.5), 'avgSpeedBinID'] = 5
    df.loc[(df['TSPD']>=22.5)&(df['TSPD']<27.5), 'avgSpeedBinID'] = 6
    df.loc[(df['TSPD']>=27.5)&(df['TSPD']<32.5), 'avgSpeedBinID'] = 7
    df.loc[(df['TSPD']>=32.5)&(df['TSPD']<37.5), 'avgSpeedBinID'] = 8
    df.loc[(df['TSPD']>=37.5)&(df['TSPD']<42.5), 'avgSpeedBinID'] = 9
    df.loc[(df['TSPD']>=42.5)&(df['TSPD']<47.5), 'avgSpeedBinID'] = 10
    df.loc[(df['TSPD']>=47.5)&(df['TSPD']<52.5), 'avgSpeedBinID'] = 11
    df.loc[(df['TSPD']>=52.5)&(df['TSPD']<57.5), 'avgSpeedBinID'] = 12
    df.loc[(df['TSPD']>=57.5)&(df['TSPD']<62.5), 'avgSpeedBinID'] = 13
    df.loc[(df['TSPD']>=62.5)&(df['TSPD']<67.5), 'avgSpeedBinID'] = 14
    df.loc[(df['TSPD']>=67.5)&(df['TSPD']<72.5), 'avgSpeedBinID'] = 15
    df.loc[(df['TSPD']>=72.5), 'avgSpeedBinID'] = 16
    #Creating speed summary attribute
    df['hourdayID'] = df['hourid'].astype(str)+df['dayid'].astype(str)
    #Grouping speeds by bin
    df_speed = df.groupby(['county','roadtypeid','hourdayID','avgSpeedBinID']).agg({'VEH_HR10':'sum','VEH_HR25':'sum','VEH_HR40':'sum','VEH_HR50':'sum','VEH_HR60':'sum'})
    df_speed_den = df.groupby(['county','roadtypeid','hourdayID']).agg({'VEH_HR10':'sum','VEH_HR25':'sum','VEH_HR40':'sum','VEH_HR50':'sum','VEH_HR60':'sum'})
    df_speed_class = df_speed.div(df_speed.groupby(['county','roadtypeid','hourdayID']).transform('sum'))
    df_speed_class.reset_index(inplace=True)
    #Creating Speed Summaries
    df_11 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR10']]
    df_11['sourceTypeID']=11
    df_11.rename(index=str, columns={'VEH_HR10':'avgSpeedFraction'}, inplace=True)
    df_21 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR25']]
    df_31 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR25']]
    df_32 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR25']]
    df_21['sourceTypeID']=21
    df_31['sourceTypeID']=31
    df_32['sourceTypeID']=32
    df_21.rename(index=str, columns={'VEH_HR25':'avgSpeedFraction'}, inplace=True)
    df_31.rename(index=str, columns={'VEH_HR25':'avgSpeedFraction'}, inplace=True)
    df_32.rename(index=str, columns={'VEH_HR25':'avgSpeedFraction'}, inplace=True)
    df_41 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR40']]
    df_42 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR40']]
    df_43 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR40']]
    df_41['sourceTypeID']=41
    df_42['sourceTypeID']=42
    df_43['sourceTypeID']=43
    df_41.rename(index=str, columns={'VEH_HR40':'avgSpeedFraction'}, inplace=True)
    df_42.rename(index=str, columns={'VEH_HR40':'avgSpeedFraction'}, inplace=True)
    df_43.rename(index=str, columns={'VEH_HR40':'avgSpeedFraction'}, inplace=True)
    df_51 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR50']]
    df_52 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR50']]
    df_53 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR50']]
    df_54 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR50']]
    df_51['sourceTypeID']=51
    df_52['sourceTypeID']=52
    df_53['sourceTypeID']=53
    df_54['sourceTypeID']=54
    df_51.rename(index=str, columns={'VEH_HR50':'avgSpeedFraction'}, inplace=True)
    df_52.rename(index=str, columns={'VEH_HR50':'avgSpeedFraction'}, inplace=True)
    df_53.rename(index=str, columns={'VEH_HR50':'avgSpeedFraction'}, inplace=True)
    df_54.rename(index=str, columns={'VEH_HR50':'avgSpeedFraction'}, inplace=True)
    df_61 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR60']]
    df_62 = df_speed_class.loc[:,['county','roadtypeid','hourdayID','avgSpeedBinID','VEH_HR60']]
    df_61['sourceTypeID']=61
    df_62['sourceTypeID']=62
    df_61.rename(index=str, columns={'VEH_HR60':'avgSpeedFraction'}, inplace=True)
    df_62.rename(index=str, columns={'VEH_HR60':'avgSpeedFraction'}, inplace=True)
    #Merging speed summaries together
    dfs = [df_11,df_21,df_31,df_32,df_41,df_42,df_43,df_51,df_52,df_53,df_54,df_61,df_62]
    speedFraction = pd.concat(dfs, ignore_index=True)
    now=lapTimer('  took: ',now)
    
    #Creating final dataset template
    print ('Creating Output Template')
    all_county = []
    all_source = []
    all_road = []
    all_hour = []
    all_speeds = []
    speeds = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16]
    for county in speedFraction['county'].unique():
        for source in speedFraction['sourceTypeID'].unique():
            for road in speedFraction['roadtypeid'].unique():
                for hour in speedFraction['hourdayID'].unique():
                    for i in speeds:
                        all_county.append(county)
                        all_source.append(source)
                        all_road.append(road)
                        all_hour.append(hour)
                        all_speeds.append(i)
    d = {'county':all_county, 'sourceTypeID':all_source, 'roadtypeid':all_road, 'hourdayID':all_hour, 'avgSpeedBinID':all_speeds}
    speed_fraction2 = pd.DataFrame(d)
    now=lapTimer('  took: ',now)
    
    #Merging speed data to template
    print ('Merging Speed Data to Output Template')
    speedFraction_final = pd.merge(speed_fraction2, speedFraction, on=['county','sourceTypeID','roadtypeid','hourdayID','avgSpeedBinID'], how='left')
    #Filling missing values with 0
    speedFraction_final['avgSpeedFraction'].fillna(0, inplace=True)
    speedFraction_final_cols=speedFraction_final.columns.tolist()
    now=lapTimer('  took: ',now)
    
    #Exporting data
    print ('Exporting Data')
    pathlib.Path(outputpath).mkdir(exist_ok=True)
    speedFraction_final.to_csv(outputpath+SELECT_STATE+'_SPEED_DISTRIBUTION.csv', index=False)
    df3_county = speedFraction_final.groupby('county')
    df3_filepath = outputpath+SELECT_STATE+'_SPEED_DISTRIBUTION/'
    pathlib.Path(df3_filepath).mkdir(exist_ok=True) 
    for name, group in df3_county:
        group.to_csv(df3_filepath+SELECT_STATE+'_SPEED_DISTRIBUTION'+'_'+str(name)+'.csv', index=False, columns=['sourceTypeID','roadtypeid','hourdayID','avgSpeedBinID','avgSpeedFraction'])
    now=lapTimer('  took: ', now)
    print('Outputs saved in Final Output\\')
    print('**********Process Completed**********')
    print('')