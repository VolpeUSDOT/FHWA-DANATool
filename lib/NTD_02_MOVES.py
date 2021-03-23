# -*- coding: utf-8 -*-
"""
Created by Cambridge Systematics
Modified By: Volpe National Transportation Systems Center

"""
import pandas as pd
import numpy as np
import pathlib
#import geopandas as gpd
import pyarrow as pa
import pyarrow.parquet as pq
import time

def MOVES(SELECT_STATE, PATH_NPMRDS, PATH_HPMS, PATH_VM2, PATH_COUNTY_MILEAGE): 
    #!!! INPUT Parameters
    
    filepath = '/'
    #pathlib.Path(filepath).mkdir(exist_ok=True) 
    outputpath = 'Final Output/Process2_MOVES_VMT_Distributions/'
    pathlib.Path(outputpath).mkdir(exist_ok=True)
    
    def lapTimer(text,now):
        print('%s%.3f' %(text,time.time()-now))
        return time.time()
    
    now=time.time()
    print ('')
    print ('********** Produce MOVES VMT Inputs **********')
    ##########################################################################################
    #a. State definition
    states = {
    'AL':['Alabama',1],'AK':['Alaska',2],'AZ':['Arizona',4],'AR':['Arkansas',5],'CA':['California',6],'CO':['Colorado',8],
    'CT':['Connecticut',9],'DE':['Delaware',10],'DC':['District of Columbia',11],'FL':['Florida',12],'GA':['Georgia',13],
    'HI':['Hawaii',15],'ID':['Idaho',16],'IL':['Illinois',17],'IN':['Indiana',18],'IA':['Iowa',19],'KS':['Kansas',20],
    'KY':['Kentucky',21],'LA':['Louisiana',22],'ME':['Maine',23],'MD':['Maryland',24],'MA':['Massachusetts',25],
    'MI':['Michigan',26],'MN':['Minnesota',27],'MS':['Mississippi',28],'MO':['Missouri',29],'MT':['Montana',30],'NE':['Nebraska',31],
    'NV':['Nevada',32],'NH':['New Hampshire',33],'NJ':['New Jersey',34],'NM':['New Mexico',35],'NY':['New York',36],
    'NC':['North Carolina',37],'ND':['North Dakota',38],'OH':['Ohio',39],'OK':['Oklahoma',40],'OR':['Oregon',41],
    'PA':['Pennsylvania',42],'PR':['Puerto Rico',72],'RI':['Rhode Island',44],'SC':['South Carolina',46],'SD':['South Dakota',46],
    'TN':['Tennessee',47],'TX':['Texas',48],'UT':['Utah',49],'VT':['Vermont',50],'VA':['Virginia',51],
    'WA':['Washington',53],'WV':['West Virginia',54],'WI':['Wisconsin',55],'WY':['Wyoming',56]}
    
    print ('Processing State VMT data')
    #b. VMT data 
    #Caluclate VMT by County & Functional Classification & Urban/Rural
    '''
    data:
        HPMS: HPMS section VMT for FS 1-5; can be grouped into county level
        vm2:  VMT total, by FS (FS 1-7)
        County_summary: mileage by county (FS 6-7)
    final result (State_VMT) used for Regional VMT/roadType VMT calculation
    '''
    #Read HPMS VMT data
    print ('Reading in State HPMS')
    HPMS = pd.read_csv(PATH_HPMS)
    HPMS.columns=[x.lower() for x in HPMS.columns]
    HPMS.rename(columns={'f_system':'F_System', 'urban_code': 'Urban_Code', 'aadt': 'AADT', 
                         'begin_point': 'Begin_Point', 'begin_poin': 'Begin_Point', 'end_point': 'End_Point', 
                         'county_cod': 'County_Code', 'County_Cod': 'County_Code', 'county_code': 'County_Code'}, inplace=True)
    #HPMS.drop(HPMS[HPMS['F_System']==6].index, inplace=True)
    #HPMS.drop(HPMS[HPMS['F_System']==7].index, inplace=True)
    HPMS['Urban_rural']=''
    HPMS.loc[HPMS['Urban_Code']<99999, 'Urban_rural']='U'
    HPMS.loc[HPMS['Urban_Code']>=99999, 'Urban_rural']='R'
    HPMS['Length'] = HPMS['End_Point'] - HPMS['Begin_Point']
    HPMS['VMT'] = HPMS['AADT']*HPMS['Length']*365
    HPMScounty_list=HPMS['County_Code'].drop_duplicates().tolist()
    
    #Reading VM2 State VMT
    print ('Reading in Highway Statistics VM2')
    vm2_head = ['State','R_Interstate','R_Freeways_Exp','R_Principal_Art','R_Minor_Art','R_Major_Col',
                'R_Minor_Col','R_Local','R_Total','U_Interstate','U_Freeways_Exp','U_Principal_Art',
                'U_Minor_Art','U_Major_Col','U_Minor_Col','U_Local','U_Total','Total']
    vm2 = pd.read_csv(PATH_VM2,header=None,names=vm2_head)
    vm2['R_Interstate'] = vm2['R_Interstate']*1000000
    vm2['R_Freeways_Exp'] = vm2['R_Freeways_Exp']*1000000
    vm2['R_Principal_Art'] = vm2['R_Principal_Art']*1000000
    vm2['R_Minor_Art'] = vm2['R_Minor_Art']*1000000
    vm2['R_Major_Col'] = vm2['R_Major_Col']*1000000
    vm2['R_Minor_Col'] = vm2['R_Minor_Col']*1000000
    vm2['R_Local'] = vm2['R_Local']*1000000
    vm2['R_Total'] = vm2['R_Total']*1000000
    vm2['U_Interstate'] = vm2['U_Interstate']*1000000
    vm2['U_Freeways_Exp'] = vm2['U_Freeways_Exp']*1000000
    vm2['U_Principal_Art'] = vm2['U_Principal_Art']*1000000
    vm2['U_Minor_Art'] = vm2['U_Minor_Art']*1000000
    vm2['U_Major_Col'] = vm2['U_Major_Col']*1000000
    vm2['U_Minor_Col'] = vm2['U_Minor_Col']*1000000
    vm2['U_Local'] = vm2['U_Local']*1000000
    vm2['U_Total'] = vm2['U_Total']*1000000
    vm2['Total'] = vm2['Total']*1000000
    
    state_index = vm2[vm2['State']==states.get(SELECT_STATE)[0]].index.values[0]   # Total VMT by URB_RURAL+F_SYSTEM, of specific State 
    vm2 = vm2.loc[state_index]
    
    vm2_state = []
    vm2_state.append({'Urban_rural':'R', 'F_System':1, 'VMT':vm2.loc['R_Interstate']})
    vm2_state.append({'Urban_rural':'R', 'F_System':2, 'VMT':vm2.loc['R_Freeways_Exp']})
    vm2_state.append({'Urban_rural':'R', 'F_System':3, 'VMT':vm2.loc['R_Principal_Art']})
    vm2_state.append({'Urban_rural':'R', 'F_System':4, 'VMT':vm2.loc['R_Minor_Art']})
    vm2_state.append({'Urban_rural':'R', 'F_System':5, 'VMT':vm2.loc['R_Major_Col']})
    vm2_state.append({'Urban_rural':'R', 'F_System':6, 'VMT':vm2.loc['R_Minor_Col']})
    vm2_state.append({'Urban_rural':'R', 'F_System':7, 'VMT':vm2.loc['R_Local']})
    vm2_state.append({'Urban_rural':'R', 'F_System':-1, 'VMT':vm2.loc['R_Total']}) # not a functional class, it is the total (hence -1)
    vm2_state.append({'Urban_rural':'U', 'F_System':1, 'VMT':vm2.loc['U_Interstate']})
    vm2_state.append({'Urban_rural':'U', 'F_System':2, 'VMT':vm2.loc['U_Freeways_Exp']})
    vm2_state.append({'Urban_rural':'U', 'F_System':3, 'VMT':vm2.loc['U_Principal_Art']})
    vm2_state.append({'Urban_rural':'U', 'F_System':4, 'VMT':vm2.loc['U_Minor_Art']})
    vm2_state.append({'Urban_rural':'U', 'F_System':5, 'VMT':vm2.loc['U_Major_Col']})
    vm2_state.append({'Urban_rural':'U', 'F_System':6, 'VMT':vm2.loc['U_Minor_Col']})
    vm2_state.append({'Urban_rural':'U', 'F_System':7, 'VMT':vm2.loc['U_Local']})
    vm2_state.append({'Urban_rural':'U', 'F_System':-1, 'VMT':vm2.loc['U_Total']}) # not a functional class, it is the total (hence -1)
    vm2_state = pd.DataFrame(vm2_state)
    vm2_state.set_index(keys = ['Urban_rural', 'F_System'], inplace=True)
    
    #Reading county rd mileage data
    print ('Reading in HPMS County Rd Mileage')
    County_summary = pd.read_csv(PATH_COUNTY_MILEAGE,sep='|')
    #County_summary.rename(index=str, columns={'County_Code':'County_Cod'}, inplace=True)
    County_summary['Urban_rural']=''
    County_summary.loc[County_summary['Urban_Code']<99999, 'Urban_rural']='U'
    County_summary.loc[County_summary['Urban_Code']>=99999, 'Urban_rural']='R'
    State_County = County_summary.loc[County_summary['State_Code']==states.get(SELECT_STATE)[1]]
    ######################################################################
    #b1. Caluclate VMT for Functional Classification 1 to 5
    #Creating VM2 adjusting factors by comparing Annual VMT from HPMS and VM2
    
    print('Processing VMT for functional system 1-5')
    #create State_VMT Template
    State_VMT_Temp = []
    for county in State_County.County_Code.unique():
        for fclass in range(1,8):
            State_VMT_Temp.append({'County_Code':county, 'Urban_rural':'R', 'F_System':fclass})
        for fclass in range(1,8):
            State_VMT_Temp.append({'County_Code':county, 'Urban_rural':'U', 'F_System':fclass})
    
    
    #Calculate statewide VMT from VM2 table 
    State_VMT_Temp = pd.DataFrame(State_VMT_Temp)
    State_VMT_Temp = pd.merge(State_VMT_Temp, vm2_state, how="left", on=['Urban_rural','F_System'])[['County_Code', 'Urban_rural','F_System', 'VMT']]
    State_VMT_Temp.rename(columns={'VMT':'Statewide_VMT'}, inplace=True)
    
    # Calculate HPMS county VMT by urban-rural F-system classification
    HPMS_County_FClass_VMT = HPMS.groupby(['County_Code', 'Urban_rural','F_System'])['VMT'].sum().reset_index(drop=False)
    HPMS_County_FClass_VMT = HPMS_County_FClass_VMT.loc[HPMS_County_FClass_VMT['F_System'].between(1, 5, inclusive=True)]
    HPMS_County_FClass_VMT.reset_index(inplace=True)
    
    # Calculate HPMS statewide VMT by urban-rural F-system classification
    HPMS_FClass_VMT = HPMS.groupby(['Urban_rural','F_System'])['VMT'].sum().reset_index(drop=False)
    HPMS_FClass_VMT = HPMS_FClass_VMT.loc[(HPMS_FClass_VMT['F_System'].between(1, 5, inclusive=True)) | 
                                          ((HPMS_FClass_VMT['F_System']==6)&(HPMS_FClass_VMT['Urban_rural']=='U'))]
    HPMS_FClass_VMT.reset_index(inplace=True)
    
    # Merge those together with the template
    State_VMT_Temp = pd.merge(State_VMT_Temp, HPMS_County_FClass_VMT, how="left", on=['County_Code', 'Urban_rural','F_System'])[['County_Code', 'Urban_rural','F_System', 'Statewide_VMT', 'VMT']]
    State_VMT_Temp.rename(columns={'VMT':'County_Class_VMT'}, inplace=True)
    State_VMT_Temp = pd.merge(State_VMT_Temp, HPMS_FClass_VMT, how="left", on=['Urban_rural','F_System'])[['County_Code', 'Urban_rural','F_System', 'Statewide_VMT', 'County_Class_VMT', 'VMT']]
    State_VMT_Temp.rename(columns={'VMT':'Class_VMT'}, inplace=True)    
    
    # Drop unnecessary columns
    State_VMT_Temp['Adjusted_VMT'] = State_VMT_Temp['Statewide_VMT']*State_VMT_Temp['County_Class_VMT']/State_VMT_Temp['Class_VMT']
    State_VMT_Temp.rename(columns={'Adjusted_VMT':'VMT'}, inplace=True) 
    State_VMT_Temp = State_VMT_Temp[['County_Code', 'Urban_rural', 'F_System', 'Statewide_VMT', 'VMT']]
    
    print('Processing VMT for rural functional system 6 and urban and rural 7')
    State_County.rename(columns={'RMC_L_System_Length':'Miles'}, inplace=True)
    County_Summary_Miles = State_County.groupby(['County_Code', 'Urban_rural','F_System'])['Miles'].sum().reset_index(drop=False)
    County_Summary_Miles = County_Summary_Miles.loc[((County_Summary_Miles['F_System']==6)&(County_Summary_Miles['Urban_rural']=='R'))|
                                                    ((County_Summary_Miles['F_System']==7)&(County_Summary_Miles['Urban_rural'].isin(['R', 'U'])))]
    County_Summary_Miles.reset_index(inplace=True)
    FClass_Summary_Miles = State_County.groupby(['Urban_rural','F_System'])['Miles'].sum().reset_index(drop=False)
    FClass_Summary_Miles = FClass_Summary_Miles.loc[((County_Summary_Miles['F_System']==6)&(County_Summary_Miles['Urban_rural']=='R'))|
                                                    ((County_Summary_Miles['F_System']==7)&(County_Summary_Miles['Urban_rural'].isin(['R', 'U'])))]
    FClass_Summary_Miles.reset_index(inplace=True)
    
    State_VMT_Temp = pd.merge(State_VMT_Temp, County_Summary_Miles, how="left", on=['County_Code', 'Urban_rural','F_System'])[['County_Code', 'Urban_rural','F_System', 'Statewide_VMT', 'VMT', 'Miles']]
    State_VMT_Temp.rename(columns={'Miles':'County_Class_Miles'}, inplace=True)
    State_VMT_Temp = pd.merge(State_VMT_Temp, FClass_Summary_Miles, how="left", on=['Urban_rural','F_System'])[['County_Code', 'Urban_rural','F_System', 'Statewide_VMT', 'VMT', 'County_Class_Miles', 'Miles']]
    State_VMT_Temp.rename(columns={'Miles':'Class_Miles'}, inplace=True)
    
    State_VMT_Temp['Adjusted_VMT'] = State_VMT_Temp['Statewide_VMT']*State_VMT_Temp['County_Class_Miles']/State_VMT_Temp['Class_Miles']
    State_VMT_Temp.loc[((State_VMT_Temp['F_System']==6)&(State_VMT_Temp['Urban_rural']=='R'))|
                       ((State_VMT_Temp['F_System']==7)&(State_VMT_Temp['Urban_rural'].isin(['R', 'U']))), 'VMT'] = State_VMT_Temp['Adjusted_VMT']
    State_VMT_Temp = State_VMT_Temp[['County_Code', 'Urban_rural', 'F_System', 'VMT']]
    
    State_VMT_Temp.dropna(subset=['VMT'], inplace=True)
    State_VMT_Temp['roadTypeID']=''
    State_VMT_Temp.loc[(State_VMT_Temp['Urban_rural']=='U') & (State_VMT_Temp['F_System']<=2),'roadTypeID']=4
    State_VMT_Temp.loc[(State_VMT_Temp['Urban_rural']=='U') & (State_VMT_Temp['F_System']>2),'roadTypeID']=5
    State_VMT_Temp.loc[(State_VMT_Temp['Urban_rural']=='R') & (State_VMT_Temp['F_System']<=2),'roadTypeID']=2
    State_VMT_Temp.loc[(State_VMT_Temp['Urban_rural']=='R') & (State_VMT_Temp['F_System']>2),'roadTypeID']=3
    State_VMT=State_VMT_Temp.groupby(['County_Code','roadTypeID'], as_index=False)['VMT'].sum()
    
    now=lapTimer('  took: ',now)
    ####################################################################################################
    
    #c. Develop VMT Fractions
    print('Reading Composite Dataset')
    composite_df = pq.read_table(PATH_NPMRDS)
    composite_df = composite_df.to_pandas()
    now=lapTimer('  took: ',now)
    
    # Developing monthly vmt fractions
    print('Developing monthly VMT Fractions dataset')
    df_monthVMT = composite_df.groupby(['county','monthid']).agg({'PCT_TYPE10':'sum','PCT_TYPE25':'sum','PCT_TYPE40':'sum','PCT_TYPE50':'sum','PCT_TYPE60':'sum'})
    df_monthVMT_den = composite_df.groupby(['county']).agg({'PCT_TYPE10':'sum','PCT_TYPE25':'sum','PCT_TYPE40':'sum','PCT_TYPE50':'sum','PCT_TYPE60':'sum'})
    df_monthVMT_class = df_monthVMT.div(df_monthVMT_den, level=0)
    df_monthVMT_class.reset_index(inplace=True)
    
    df_monthVMT_class['11'] = df_monthVMT_class['PCT_TYPE10']
    df_monthVMT_class['21'] = df_monthVMT_class['PCT_TYPE25']
    df_monthVMT_class['31'] = df_monthVMT_class['PCT_TYPE25']
    df_monthVMT_class['32'] = df_monthVMT_class['PCT_TYPE25']
    df_monthVMT_class['41'] = df_monthVMT_class['PCT_TYPE40']
    df_monthVMT_class['42'] = df_monthVMT_class['PCT_TYPE40']
    df_monthVMT_class['43'] = df_monthVMT_class['PCT_TYPE40']
    df_monthVMT_class['51'] = df_monthVMT_class['PCT_TYPE50']
    df_monthVMT_class['52'] = df_monthVMT_class['PCT_TYPE50']
    df_monthVMT_class['53'] = df_monthVMT_class['PCT_TYPE50']
    df_monthVMT_class['54'] = df_monthVMT_class['PCT_TYPE50']
    df_monthVMT_class['61'] = df_monthVMT_class['PCT_TYPE60']
    df_monthVMT_class['62'] = df_monthVMT_class['PCT_TYPE60']
    
    #Wide to long format
    monthVMTFrac = pd.melt(df_monthVMT_class, id_vars=['county', 'monthid'], 
                                   value_vars=['11','21','31','32','41', '42', '43', '51', '52', '53', '54', '61', '62'], 
                                   var_name='sourceTypeID', value_name='monthVMTFraction')
    monthVMTFrac['sourceTypeID'] = monthVMTFrac['sourceTypeID'].astype('int64')
    
    monthVMTFrac.rename(columns = {'monthid':'monthID'}, inplace=True)
    
    # Export data
    df3_county = monthVMTFrac.groupby('county')
    df3_filepath = outputpath+SELECT_STATE+'_MONTH_VMT/'
    pathlib.Path(df3_filepath).mkdir(exist_ok=True) 
    monthVMTFrac.to_csv(outputpath+SELECT_STATE+'_MONTH_VMT.csv', index=False)
    for name, group in df3_county:
        group.to_csv(df3_filepath+SELECT_STATE+'_MONTH_VMT'+'_'+str(name)+'.csv', 
                     index=False, columns=['sourceTypeID','monthID','monthVMTFraction'])
    now=lapTimer('  took: ',now)
    
    ####################################################################################################
    
    #e. Developing Daily VMT fractions
    print('Developing daily VMT Fractions dataset')
    df_dayVMT = composite_df.groupby(['county', 'monthid', 'roadtypeid', 'dayid']).agg({'PCT_TYPE10':'sum','PCT_TYPE25':'sum','PCT_TYPE40':'sum','PCT_TYPE50':'sum','PCT_TYPE60':'sum'})
    df_dayVMT_class = df_dayVMT.groupby(level=[0,1,2]).apply(lambda x: x/x.sum())
    df_dayVMT_class.reset_index(inplace=True)
    
    df_dayVMT_class['11'] = df_dayVMT_class['PCT_TYPE10']
    df_dayVMT_class['21'] = df_dayVMT_class['PCT_TYPE25']
    df_dayVMT_class['31'] = df_dayVMT_class['PCT_TYPE25']
    df_dayVMT_class['32'] = df_dayVMT_class['PCT_TYPE25']
    df_dayVMT_class['41'] = df_dayVMT_class['PCT_TYPE40']
    df_dayVMT_class['42'] = df_dayVMT_class['PCT_TYPE40']
    df_dayVMT_class['43'] = df_dayVMT_class['PCT_TYPE40']
    df_dayVMT_class['51'] = df_dayVMT_class['PCT_TYPE50']
    df_dayVMT_class['52'] = df_dayVMT_class['PCT_TYPE50']
    df_dayVMT_class['53'] = df_dayVMT_class['PCT_TYPE50']
    df_dayVMT_class['54'] = df_dayVMT_class['PCT_TYPE50']
    df_dayVMT_class['61'] = df_dayVMT_class['PCT_TYPE60']
    df_dayVMT_class['62'] = df_dayVMT_class['PCT_TYPE60']
    
    #Wide to long format
    dayVMTFrac = pd.melt(df_dayVMT_class, id_vars=['county', 'monthid', 'roadtypeid', 'dayid'], 
                                   value_vars=['11','21','31','32','41', '42', '43', '51', '52', '53', '54', '61', '62'], 
                                   var_name='sourceTypeID', value_name='dayVMTFraction')
    dayVMTFrac['sourceTypeID'] = dayVMTFrac['sourceTypeID'].astype('int64')
    
    dayVMTFrac.rename(columns = {'monthid':'monthID', 'roadtypeid':'roadTypeID', 'dayid': 'dayID'}, inplace=True)
    
    #Exporting dataset
    df3_county = dayVMTFrac.groupby('county')
    df3_filepath = outputpath+SELECT_STATE+'_DAY_VMT/'
    pathlib.Path(df3_filepath).mkdir(exist_ok=True) 
    dayVMTFrac.to_csv(outputpath+SELECT_STATE+'_DAY_VMT.csv', index=False)
    for name, group in df3_county:
        group.to_csv(df3_filepath+SELECT_STATE+'_DAY_VMT'+'_'+str(name)+'.csv', index=False, columns=['sourceTypeID','monthID','roadTypeID','dayID','dayVMTFraction'])
    now=lapTimer('  took: ',now)
    
    ####################################################################################################
    
    #f. Developing hourly summaries
    print('Developing hourly VMT Fractions dataset')
    df_hourVMT = composite_df.groupby(['county','roadtypeid','dayid','hourid']).agg({'PCT_TYPE10':'sum','PCT_TYPE25':'sum','PCT_TYPE40':'sum','PCT_TYPE50':'sum','PCT_TYPE60':'sum'})
    df_hourVMT_class = df_hourVMT.div(df_hourVMT.groupby(['county','roadtypeid','dayid']).transform('sum'))
    df_hourVMT_class.reset_index(inplace=True)
    
    df_hourVMT_class['11'] = df_hourVMT_class['PCT_TYPE10']
    df_hourVMT_class['21'] = df_hourVMT_class['PCT_TYPE25']
    df_hourVMT_class['31'] = df_hourVMT_class['PCT_TYPE25']
    df_hourVMT_class['32'] = df_hourVMT_class['PCT_TYPE25']
    df_hourVMT_class['41'] = df_hourVMT_class['PCT_TYPE40']
    df_hourVMT_class['42'] = df_hourVMT_class['PCT_TYPE40']
    df_hourVMT_class['43'] = df_hourVMT_class['PCT_TYPE40']
    df_hourVMT_class['51'] = df_hourVMT_class['PCT_TYPE50']
    df_hourVMT_class['52'] = df_hourVMT_class['PCT_TYPE50']
    df_hourVMT_class['53'] = df_hourVMT_class['PCT_TYPE50']
    df_hourVMT_class['54'] = df_hourVMT_class['PCT_TYPE50']
    df_hourVMT_class['61'] = df_hourVMT_class['PCT_TYPE60']
    df_hourVMT_class['62'] = df_hourVMT_class['PCT_TYPE60']
    
    hourVMTFrac = pd.melt(df_hourVMT_class, id_vars=['county', 'roadtypeid', 'dayid', 'hourid'], 
                                   value_vars=['11','21','31','32','41', '42', '43', '51', '52', '53', '54', '61', '62'], 
                                   var_name='sourceTypeID', value_name='hourVMTFraction')
    hourVMTFrac['sourceTypeID'] = hourVMTFrac['sourceTypeID'].astype('int64')
    
    hourVMTFrac.rename(columns = {'roadtypeid':'roadTypeID', 'dayid': 'dayID', 'hourid': 'hourID'}, inplace=True)

    #Exporting datasets
    df3_county = hourVMTFrac.groupby('county')
    df3_filepath = outputpath+SELECT_STATE+'_HOUR_VMT/'
    pathlib.Path(df3_filepath).mkdir(exist_ok=True) 
    hourVMTFrac.to_csv(outputpath+SELECT_STATE+'_HOUR_VMT.csv', index=False)
    for name, group in df3_county:
        group.to_csv(df3_filepath+SELECT_STATE+'_HOUR_VMT'+'_'+str(name)+'.csv', index=False, columns=['sourceTypeID','roadTypeID','dayID','hourID','hourVMTFraction'])
    now=lapTimer('  took: ',now)
    
    ####################################################################################################
    
    #g. Developing Regional VMT sumarriess
    print('Developing Regional VMT summaries')
    State_VMT['state']=states.get(SELECT_STATE)[1]
    State_VMT.rename(columns={'County_Code': 'county'}, inplace=True)
    State_VMT_1 = State_VMT.groupby(['state','county'], as_index=False)['VMT'].sum()
    
    #g1. Aggregating all volume by County
    veh_county = composite_df.groupby('county').agg({'PCT_TYPE10':'sum','PCT_TYPE25':'sum','PCT_TYPE40':'sum','PCT_TYPE50':'sum','PCT_TYPE60':'sum'})
    veh_county.reset_index(inplace=True)
    veh_county.rename(index=str, columns={'PCT_TYPE10':'VEH10','PCT_TYPE25':'VEH25','PCT_TYPE40':'VEH40','PCT_TYPE50':'VEH50','PCT_TYPE60':'VEH60'}, inplace=True)
    #Creating classification percentages for County level
    veh_county['TOT_VEHS'] = veh_county['VEH10']+veh_county['VEH25']+veh_county['VEH40']+veh_county['VEH50']+veh_county['VEH60']
    veh_county['PCT10'] = veh_county['VEH10']/veh_county['TOT_VEHS']
    veh_county['PCT25'] = veh_county['VEH25']/veh_county['TOT_VEHS']
    veh_county['PCT40'] = veh_county['VEH40']/veh_county['TOT_VEHS']
    veh_county['PCT50'] = veh_county['VEH50']/veh_county['TOT_VEHS']
    veh_county['PCT60'] = veh_county['VEH60']/veh_county['TOT_VEHS']
    veh_county['TOT_PCT'] = veh_county['PCT10']+veh_county['PCT25']+veh_county['PCT40']+veh_county['PCT50']+veh_county['PCT60']
    
    #g3. Merging County (1st pass) and State (2nd pass) classification to VMT summary
    veh_tmc = pd.merge(State_VMT_1,veh_county, on='county', how='left')
    
    #g4. Calculating VMT by vehicle type
    veh_tmc['VMT10'] = veh_tmc['PCT10']*veh_tmc['VMT']
    veh_tmc['VMT25'] = veh_tmc['PCT25']*veh_tmc['VMT']
    veh_tmc['VMT40'] = veh_tmc['PCT40']*veh_tmc['VMT']
    veh_tmc['VMT50'] = veh_tmc['PCT50']*veh_tmc['VMT']
    veh_tmc['VMT60'] = veh_tmc['PCT60']*veh_tmc['VMT']
    veh_tmc['yearID'] = HPMS['year_record'].mode()[0]
    veh_tmc['baseYearOffNetVMT']=0
    veh_tmc.drop(['VMT','VEH10','VEH25','VEH40','VEH50','VEH60','TOT_VEHS','PCT10','PCT25',
                  'PCT40','PCT50','PCT60','TOT_PCT'], axis=1, inplace=True)
    veh_tmc.reset_index(inplace=True, drop=True)
    
    #g5. Creating County level summaries
    regionVMTFraction = pd.melt(veh_tmc, id_vars=['state', 'county', 'yearID', 'baseYearOffNetVMT'], 
                                   value_vars=['VMT10','VMT25','VMT40','VMT50','VMT60'], 
                                   var_name='VMT_TYPE', value_name='HPMSBaseYearVMT')
    regionVMTFraction.loc[regionVMTFraction['VMT_TYPE']=='VMT10', 'HPMSVtypeID']=10
    regionVMTFraction.loc[regionVMTFraction['VMT_TYPE']=='VMT25', 'HPMSVtypeID']=25
    regionVMTFraction.loc[regionVMTFraction['VMT_TYPE']=='VMT40', 'HPMSVtypeID']=40
    regionVMTFraction.loc[regionVMTFraction['VMT_TYPE']=='VMT50', 'HPMSVtypeID']=50
    regionVMTFraction.loc[regionVMTFraction['VMT_TYPE']=='VMT60', 'HPMSVtypeID']=60
    regionVMTFraction.drop('VMT_TYPE', axis=1, inplace=True)
    regionVMTFraction.sort_values(by='county', inplace=True)
    # Exporting data
    df3_county = regionVMTFraction.groupby('county')
    df3_filepath = outputpath+SELECT_STATE+'_REGIONAL_VMT/'
    pathlib.Path(df3_filepath).mkdir(exist_ok=True) 
    regionVMTFraction.to_csv(outputpath+SELECT_STATE+'_REGIONAL_VMT.csv', index=False)
    for name, group in df3_county:
        group.to_csv(df3_filepath+SELECT_STATE+'_REGIONAL_VMT'+'_'+str(name)+'.csv', index=False, columns=['county','HPMSVtypeID','yearID','HPMSBaseYearVMT','baseYearOffNetVMT'])
    now=lapTimer('  took: ',now)
    ####################################################################################################
    
    #h. Developing RoadType VMT sumarriess
    print('Developing RoadType VMT summaries')
    State_VMT['state']=states.get(SELECT_STATE)[1]
    State_VMT.rename(columns={'County_Cod': 'county'}, inplace=True)
    State_VMT_2 = State_VMT
    
    #h1. Aggregating all volume by County
    df_roadtypeVMT = composite_df.groupby(['county','roadtypeid']).agg({'PCT_TYPE10':'sum','PCT_TYPE25':'sum','PCT_TYPE40':'sum','PCT_TYPE50':'sum','PCT_TYPE60':'sum'})
    df_roadtypeVMT_class = df_roadtypeVMT.div(df_roadtypeVMT.groupby('county').transform('sum'))
    df_roadtypeVMT_class.reset_index(inplace=True)
    
    df_roadtypeVMT_class['11'] = df_roadtypeVMT_class['PCT_TYPE10']
    df_roadtypeVMT_class['21'] = df_roadtypeVMT_class['PCT_TYPE25']
    df_roadtypeVMT_class['31'] = df_roadtypeVMT_class['PCT_TYPE25']
    df_roadtypeVMT_class['32'] = df_roadtypeVMT_class['PCT_TYPE25']
    df_roadtypeVMT_class['41'] = df_roadtypeVMT_class['PCT_TYPE40']
    df_roadtypeVMT_class['42'] = df_roadtypeVMT_class['PCT_TYPE40']
    df_roadtypeVMT_class['43'] = df_roadtypeVMT_class['PCT_TYPE40']
    df_roadtypeVMT_class['51'] = df_roadtypeVMT_class['PCT_TYPE50']
    df_roadtypeVMT_class['52'] = df_roadtypeVMT_class['PCT_TYPE50']
    df_roadtypeVMT_class['53'] = df_roadtypeVMT_class['PCT_TYPE50']
    df_roadtypeVMT_class['54'] = df_roadtypeVMT_class['PCT_TYPE50']
    df_roadtypeVMT_class['61'] = df_roadtypeVMT_class['PCT_TYPE60']
    df_roadtypeVMT_class['62'] = df_roadtypeVMT_class['PCT_TYPE60']
    
    roadtypeVMTFrac = pd.melt(df_roadtypeVMT_class, id_vars=['county', 'roadtypeid'], 
                                   value_vars=['11','21','31','32','41', '42', '43', '51', '52', '53', '54', '61', '62'], 
                                   var_name='sourceTypeID', value_name='roadTypeVMTFraction')
    roadtypeVMTFrac['sourceTypeID'] = roadtypeVMTFrac['sourceTypeID'].astype('int64')
    
    roadtypeVMTFrac.sort_values(by=['county', 'roadtypeid', 'sourceTypeID'], inplace=True)
    
    hourVMTFrac.rename(columns = {'roadtypeid':'roadTypeID'}, inplace=True)
    
    df3_county = roadtypeVMTFrac.groupby('county')
    df3_filepath = outputpath+SELECT_STATE+'_ROADTYPE_VMT/'
    pathlib.Path(df3_filepath).mkdir(exist_ok=True) 
    roadtypeVMTFrac.to_csv(outputpath+SELECT_STATE+'_ROADTYPE_VMT.csv', index=False)
    for name, group in df3_county:
        group.to_csv(df3_filepath+SELECT_STATE+'_ROADTYPE_VMT'+'_'+str(name)+'.csv', index=False)
    now=lapTimer('  took: ',now)
    
    print('Outputs saved in Final Output\\')
    print('**********Process Completed**********')
    print('')