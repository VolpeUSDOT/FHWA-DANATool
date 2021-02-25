# -*- coding: utf-8 -*-
"""
Created by Cambridge Systematics

"""
import pandas as pd
import numpy as np
import pathlib
#import geopandas as gpd
#import pyarrow.parquet as pq
import time

def MOVES(SELECT_STATE, PATH_TMAS_CLASS_CLEAN, PATH_HPMS, PATH_VM2, PATH_COUNTY_MILEAGE): 
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
    vm2_state.append({'Urban_rural':'R', 'F_System':-1, 'VMT':vm2.loc['R_Total']})
    vm2_state.append({'Urban_rural':'U', 'F_System':1, 'VMT':vm2.loc['U_Interstate']})
    vm2_state.append({'Urban_rural':'U', 'F_System':2, 'VMT':vm2.loc['U_Freeways_Exp']})
    vm2_state.append({'Urban_rural':'U', 'F_System':3, 'VMT':vm2.loc['U_Principal_Art']})
    vm2_state.append({'Urban_rural':'U', 'F_System':4, 'VMT':vm2.loc['U_Minor_Art']})
    vm2_state.append({'Urban_rural':'U', 'F_System':5, 'VMT':vm2.loc['U_Major_Col']})
    vm2_state.append({'Urban_rural':'U', 'F_System':6, 'VMT':vm2.loc['U_Minor_Col']})
    vm2_state.append({'Urban_rural':'U', 'F_System':7, 'VMT':vm2.loc['U_Local']})
    vm2_state.append({'Urban_rural':'U', 'F_System':-1, 'VMT':vm2.loc['U_Total']})
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
    HPMS_FClass_VMT = HPMS_FClass_VMT.loc[HPMS_FClass_VMT['F_System'].between(1, 5, inclusive=True)]
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
    
    print('Processing VMT for functional system 6 and 7')
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
    State_VMT_Temp.loc[(State_VMT_Temp['Urban_rural']=='U') & (State_VMT_Temp['F_System']==1),'roadTypeID']=4
    State_VMT_Temp.loc[(State_VMT_Temp['Urban_rural']=='U') & (State_VMT_Temp['F_System']!=1),'roadTypeID']=5
    State_VMT_Temp.loc[(State_VMT_Temp['Urban_rural']=='R') & (State_VMT_Temp['F_System']==1),'roadTypeID']=2
    State_VMT_Temp.loc[(State_VMT_Temp['Urban_rural']=='R') & (State_VMT_Temp['F_System']!=1),'roadTypeID']=3
    State_VMT=State_VMT_Temp.groupby(['County_Code','roadTypeID'], as_index=False)['VMT'].sum()
    
    now=lapTimer('  took: ',now)
    ####################################################################################################
    
    #c. TMAS data
    print('Reading TMAS data')
    #c1. Select only State classification data
    tmas_class = pd.read_csv(PATH_TMAS_CLASS_CLEAN, dtype={'STATION_ID':str, 'LRS_ID':str,})
    tmas_class.drop(['LAT','LONG','ROUTE_SIGN','ROUTE_NUMBER','LOCATION'], axis=1, inplace=True)
    #tmas_class_cols=tmas_class.columns.tolist()
    tmas_class['monthID']=tmas_class['MONTH']
    tmas_class['hourID']=tmas_class['HOUR']+1
    tmas_class['roadTypeID']=np.nan
    tmas_class.loc[(tmas_class['URB_RURAL']=='U') & (tmas_class['F_SYSTEM']==1), 'roadTypeID'] = 4
    tmas_class.loc[(tmas_class['URB_RURAL']=='U') & (tmas_class['F_SYSTEM']!=1), 'roadTypeID'] = 5
    tmas_class.loc[(tmas_class['URB_RURAL']=='R') & (tmas_class['F_SYSTEM']==1), 'roadTypeID'] = 2
    tmas_class.loc[(tmas_class['URB_RURAL']=='R') & (tmas_class['F_SYSTEM']!=1), 'roadTypeID'] = 3
    tmas_class['dayID']=np.nan
    tmas_class.loc[tmas_class['DAY_TYPE']=='WD', 'dayID'] = 5
    tmas_class.loc[tmas_class['DAY_TYPE']=='WE', 'dayID'] = 2
    tmas_class['dayID']=tmas_class['dayID'].astype(int)
    tmas_class_state = tmas_class[tmas_class['STATE']==states.get(SELECT_STATE)[1]]
    
    now=lapTimer('  took: ',now)
    ####################################################################################################
    
    #d. Develop Monthly VMT fractions
    print('Developing monthly VMT Fractions dataset')
    #d0. create template
    sourceTypeID_list=[11, 21, 31, 32, 41, 42, 43, 51, 52, 53, 54, 61, 62]
    monthID_list=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    HPMScounty_temp=[]
    sourceType_temp=[]
    month_temp=[]
    for c in HPMScounty_list:
        for s in sourceTypeID_list:
            for m in monthID_list:
                HPMScounty_temp.append(c)
                sourceType_temp.append(s)
                month_temp.append(m)
    d={'county': HPMScounty_temp, 'sourceTypeID': sourceType_temp, 'monthID': month_temp}
    monthVMT_template=pd.DataFrame(d)
    
    #d1. 1st Pass - County
    df_monthVMT = tmas_class_state.groupby(['COUNTY','MONTH']).agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    df_monthVMT_den = tmas_class_state.groupby(['COUNTY']).agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    df_monthVMT_class = df_monthVMT.div(df_monthVMT_den, level=0)
    df_monthVMT_class.reset_index(inplace=True)
    df_monthVMT_class['LeapYear']=1
    df_monthVMT_class.rename(index=str, columns={'COUNTY':'county','MONTH':'monthID','HPMS_TYPE10':'VEH10',
                                                 'HPMS_TYPE25':'VEH25','HPMS_TYPE40':'VEH40','HPMS_TYPE50':'VEH50',
                                                 'HPMS_TYPE60':'VEH60'}, inplace=True)
    # Wide format to long format
    monthVMTFrac_1a=pd.melt(df_monthVMT_class, id_vars=['county', 'monthID', 'LeapYear'], 
                                   value_vars=['VEH10','VEH25','VEH40','VEH50','VEH60'], 
                                   var_name='VEH_TYPE', value_name='monthVMTFraction')
    monthVMTFrac_1a.sort_values(by=['county', 'monthID', 'VEH_TYPE'], inplace=True)
    monthVMTFrac_1a.loc[monthVMTFrac_1a['VEH_TYPE']=='VEH10', 'rep']=1
    monthVMTFrac_1a.loc[monthVMTFrac_1a['VEH_TYPE']=='VEH25', 'rep']=3
    monthVMTFrac_1a.loc[monthVMTFrac_1a['VEH_TYPE']=='VEH40', 'rep']=3
    monthVMTFrac_1a.loc[monthVMTFrac_1a['VEH_TYPE']=='VEH50', 'rep']=4
    monthVMTFrac_1a.loc[monthVMTFrac_1a['VEH_TYPE']=='VEH60', 'rep']=2
    monthVMTFrac_1a['rep']=monthVMTFrac_1a['rep'].astype('int64')
    monthVMTFrac_1b=monthVMTFrac_1a.reindex(monthVMTFrac_1a.index.repeat(monthVMTFrac_1a.rep))
    monthVMTFrac_1b.reset_index(drop=True, inplace=True)
    monthVMTFrac_1b['sourceTypeID']=''
    for idx, row in monthVMTFrac_1b.iterrows():
        if row['VEH_TYPE']=='VEH10':
            monthVMTFrac_1b.loc[idx, 'sourceTypeID']=11
        if row['VEH_TYPE']=='VEH25':
            if monthVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH25':
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=21
            elif monthVMTFrac_1b.loc[idx+1, 'VEH_TYPE']!='VEH25':
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=32
            else:
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=31
        if row['VEH_TYPE']=='VEH40':
            if monthVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH40':
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=41
            elif monthVMTFrac_1b.loc[idx+1, 'VEH_TYPE']!='VEH40':
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=43
            else:
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=42
        if row['VEH_TYPE']=='VEH50':
            if monthVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH50':
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=51
            elif monthVMTFrac_1b.loc[idx+1, 'VEH_TYPE']!='VEH50':
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=54
            elif monthVMTFrac_1b.loc[idx-2, 'VEH_TYPE']!='VEH50':
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=52
            else:
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=53
        if row['VEH_TYPE']=='VEH60':
            if monthVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH60':
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=61
            else:
                monthVMTFrac_1b.loc[idx, 'sourceTypeID']=62
    monthVMTFrac_1b['IsLeapYear'] = ''
    monthVMTFrac_1b.loc[monthVMTFrac_1b['LeapYear']==1.0, 'IsLeapYear'] = 'N'
    monthVMTFrac_1b.loc[monthVMTFrac_1b['LeapYear']==2.0, 'IsLeapYear'] = 'Y'
    monthVMTFrac_1b.drop(['LeapYear','VEH_TYPE','rep'], axis=1, inplace=True)
    
    #d2. 2nd Pass - State
    df_monthVMT = tmas_class_state.groupby(['MONTH']).agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    df_monthVMT_den = tmas_class_state.agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    df_monthVMT_class = df_monthVMT.div(df_monthVMT_den, level=0)
    df_monthVMT_class.reset_index(inplace=True)
    df_monthVMT_class['LeapYear']=1
    df_monthVMT_class.rename(index=str, columns={'MONTH':'monthID','HPMS_TYPE10':'VEH10','HPMS_TYPE25':'VEH25',
                                                 'HPMS_TYPE40':'VEH40','HPMS_TYPE50':'VEH50','HPMS_TYPE60':'VEH60'}, inplace=True)
    # Wide format to long format
    monthVMTFrac_2a=pd.melt(df_monthVMT_class, id_vars=['monthID', 'LeapYear'], 
                                   value_vars=['VEH10','VEH25','VEH40','VEH50','VEH60'], 
                                   var_name='VEH_TYPE', value_name='monthVMTFraction')
    monthVMTFrac_2a.sort_values(by=['monthID','VEH_TYPE'], inplace=True)
    monthVMTFrac_2a.loc[monthVMTFrac_2a['VEH_TYPE']=='VEH10', 'rep']=1
    monthVMTFrac_2a.loc[monthVMTFrac_2a['VEH_TYPE']=='VEH25', 'rep']=3
    monthVMTFrac_2a.loc[monthVMTFrac_2a['VEH_TYPE']=='VEH40', 'rep']=3
    monthVMTFrac_2a.loc[monthVMTFrac_2a['VEH_TYPE']=='VEH50', 'rep']=4
    monthVMTFrac_2a.loc[monthVMTFrac_2a['VEH_TYPE']=='VEH60', 'rep']=2
    monthVMTFrac_2a['rep']=monthVMTFrac_2a['rep'].astype('int64')
    monthVMTFrac_2b=monthVMTFrac_2a.reindex(monthVMTFrac_2a.index.repeat(monthVMTFrac_2a.rep))
    monthVMTFrac_2b.reset_index(drop=True, inplace=True)
    monthVMTFrac_2b['sourceTypeID']=''
    for idx, row in monthVMTFrac_2b.iterrows():
        if row['VEH_TYPE']=='VEH10':
            monthVMTFrac_2b.loc[idx, 'sourceTypeID']=11
        if row['VEH_TYPE']=='VEH25':
            if monthVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH25':
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=21
            elif monthVMTFrac_2b.loc[idx+1, 'VEH_TYPE']!='VEH25':
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=32
            else:
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=31
        if row['VEH_TYPE']=='VEH40':
            if monthVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH40':
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=41
            elif monthVMTFrac_2b.loc[idx+1, 'VEH_TYPE']!='VEH40':
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=43
            else:
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=42
        if row['VEH_TYPE']=='VEH50':
            if monthVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH50':
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=51
            elif monthVMTFrac_2b.loc[idx+1, 'VEH_TYPE']!='VEH50':
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=54
            elif monthVMTFrac_2b.loc[idx-2, 'VEH_TYPE']!='VEH50':
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=52
            else:
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=53
        if row['VEH_TYPE']=='VEH60':
            if monthVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH60':
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=61
            else:
                monthVMTFrac_2b.loc[idx, 'sourceTypeID']=62
    monthVMTFrac_2b['IsLeapYear'] = ''
    monthVMTFrac_2b.loc[monthVMTFrac_2b['LeapYear']==1.0, 'IsLeapYear'] = 'N'
    monthVMTFrac_2b.loc[monthVMTFrac_2b['LeapYear']==2.0, 'IsLeapYear'] = 'Y'
    monthVMTFrac_2b.drop(['LeapYear','VEH_TYPE','rep'], axis=1, inplace=True)
    
    #d3. Merge Template with 2 Passes
    monthVMTFrac_3=pd.merge(monthVMT_template, monthVMTFrac_1b, on=['county','sourceTypeID','monthID'],how='left')
    monthVMTFrac_3a=monthVMTFrac_3.loc[monthVMTFrac_3['monthVMTFraction'].notnull()]
    monthVMTFrac_3b=monthVMTFrac_3.loc[monthVMTFrac_3['monthVMTFraction'].isnull()]
    monthVMTFrac_3b.drop(columns=['monthVMTFraction','IsLeapYear'], inplace=True)
    monthVMTFrac_4=pd.merge(monthVMTFrac_3b, monthVMTFrac_2b, on=['sourceTypeID','monthID'], how='left')
    monthVMTFrac_4a=monthVMTFrac_4.loc[monthVMTFrac_4['monthVMTFraction'].notnull()]
    monthVMTFrac_4b=monthVMTFrac_4.loc[monthVMTFrac_4['monthVMTFraction'].isnull()]
    monthVMTFraction=pd.concat([monthVMTFrac_3a, monthVMTFrac_4a])
    # Export data
    df3_county = monthVMTFraction.groupby('county')
    df3_filepath = outputpath+SELECT_STATE+'_MONTH_VMT/'
    pathlib.Path(df3_filepath).mkdir(exist_ok=True) 
    monthVMTFraction.to_csv(outputpath+SELECT_STATE+'_MONTH_VMT.csv', index=False)
    for name, group in df3_county:
        group.to_csv(df3_filepath+SELECT_STATE+'_MONTH_VMT'+'_'+str(name)+'.csv', 
                     index=False, columns=['sourceTypeID','IsLeapYear','monthID','monthVMTFraction'])
    now=lapTimer('  took: ',now)
    ####################################################################################################
    
    #e. Developing Daily VMT fractions
    print('Developing daily VMT Fractions dataset')
    #e0. create template
    sourceTypeID_list=[11, 21, 31, 32, 41, 42, 43, 51, 52, 53, 54, 61, 62]
    monthID_list=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    roadTypeID_list=[2, 3, 4, 5]
    dayID_list=[2, 5]
    HPMScounty_temp=[]
    sourceType_temp=[]
    month_temp=[]
    roadType_temp=[]
    day_temp=[]
    for c in HPMScounty_list:
        for s in sourceTypeID_list:
            for m in monthID_list:
                for r in roadTypeID_list:
                    for d in dayID_list:
                        HPMScounty_temp.append(c)
                        sourceType_temp.append(s)
                        month_temp.append(m)
                        roadType_temp.append(r)
                        day_temp.append(d)
    d={'county': HPMScounty_temp, 'sourceTypeID': sourceType_temp, 'monthID': month_temp, 
       'roadTypeID': roadType_temp, 'dayID': day_temp}
    dayVMT_template=pd.DataFrame(d)
    
    #e1. 1st Pass - County
    df_dayVMT = tmas_class_state.groupby(['COUNTY','MONTH','roadTypeID','dayID']).agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    df_dayVMT_class = df_dayVMT.groupby(level=[0,1,2]).apply(lambda x: x/x.sum())
    df_dayVMT_class.reset_index(inplace=True)
    df_dayVMT_class['LeapYear']=1
    df_dayVMT_class.rename(index=str, columns={'COUNTY':'county','MONTH':'monthID','HPMS_TYPE10':'VEH10',
                                                 'HPMS_TYPE25':'VEH25','HPMS_TYPE40':'VEH40','HPMS_TYPE50':'VEH50',
                                                 'HPMS_TYPE60':'VEH60'}, inplace=True)
    # Wide format to long format
    dayVMTFrac_1a=pd.melt(df_dayVMT_class, id_vars=['county', 'monthID', 'roadTypeID', 'dayID', 'LeapYear'], 
                                   value_vars=['VEH10','VEH25','VEH40','VEH50','VEH60'], 
                                   var_name='VEH_TYPE', value_name='dayVMTFraction')
    dayVMTFrac_1a.sort_values(by=['county', 'monthID', 'roadTypeID', 'dayID', 'VEH_TYPE'], inplace=True)
    dayVMTFrac_1a.loc[dayVMTFrac_1a['VEH_TYPE']=='VEH10', 'rep']=1
    dayVMTFrac_1a.loc[dayVMTFrac_1a['VEH_TYPE']=='VEH25', 'rep']=3
    dayVMTFrac_1a.loc[dayVMTFrac_1a['VEH_TYPE']=='VEH40', 'rep']=3
    dayVMTFrac_1a.loc[dayVMTFrac_1a['VEH_TYPE']=='VEH50', 'rep']=4
    dayVMTFrac_1a.loc[dayVMTFrac_1a['VEH_TYPE']=='VEH60', 'rep']=2
    dayVMTFrac_1a['rep']=dayVMTFrac_1a['rep'].astype('int64')
    dayVMTFrac_1b=dayVMTFrac_1a.reindex(dayVMTFrac_1a.index.repeat(dayVMTFrac_1a.rep))
    dayVMTFrac_1b.reset_index(drop=True, inplace=True)
    dayVMTFrac_1b['sourceTypeID']=''
    for idx, row in dayVMTFrac_1b.iterrows():
        if row['VEH_TYPE']=='VEH10':
            dayVMTFrac_1b.loc[idx, 'sourceTypeID']=11
        if row['VEH_TYPE']=='VEH25':
            if dayVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH25':
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=21
            elif dayVMTFrac_1b.loc[idx+1, 'VEH_TYPE']!='VEH25':
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=32
            else:
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=31
        if row['VEH_TYPE']=='VEH40':
            if dayVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH40':
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=41
            elif dayVMTFrac_1b.loc[idx+1, 'VEH_TYPE']!='VEH40':
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=43
            else:
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=42
        if row['VEH_TYPE']=='VEH50':
            if dayVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH50':
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=51
            elif dayVMTFrac_1b.loc[idx+1, 'VEH_TYPE']!='VEH50':
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=54
            elif dayVMTFrac_1b.loc[idx-2, 'VEH_TYPE']!='VEH50':
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=52
            else:
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=53
        if row['VEH_TYPE']=='VEH60':
            if dayVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH60':
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=61
            else:
                dayVMTFrac_1b.loc[idx, 'sourceTypeID']=62
    dayVMTFrac_1b['IsLeapYear'] = ''
    dayVMTFrac_1b.loc[dayVMTFrac_1b['LeapYear']==1.0, 'IsLeapYear'] = 'N'
    dayVMTFrac_1b.loc[dayVMTFrac_1b['LeapYear']==2.0, 'IsLeapYear'] = 'Y'
    dayVMTFrac_1b.drop(['LeapYear','VEH_TYPE','rep'], axis=1, inplace=True)
    
    #e2. 2nd Pass - State
    df_dayVMT = tmas_class_state.groupby(['MONTH','roadTypeID','dayID']).agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    df_dayVMT_class = df_dayVMT.groupby(level=[0,1]).apply(lambda x: x/x.sum())
    df_dayVMT_class.reset_index(inplace=True)
    df_dayVMT_class['LeapYear']=1
    df_dayVMT_class.rename(index=str, columns={'MONTH':'monthID','HPMS_TYPE10':'VEH10',
                                                 'HPMS_TYPE25':'VEH25','HPMS_TYPE40':'VEH40','HPMS_TYPE50':'VEH50',
                                                 'HPMS_TYPE60':'VEH60'}, inplace=True)
    # Wide format to long format
    dayVMTFrac_2a=pd.melt(df_dayVMT_class, id_vars=['monthID', 'roadTypeID', 'dayID', 'LeapYear'], 
                                   value_vars=['VEH10','VEH25','VEH40','VEH50','VEH60'], 
                                   var_name='VEH_TYPE', value_name='dayVMTFraction')
    dayVMTFrac_2a.sort_values(by=['monthID', 'roadTypeID', 'dayID', 'VEH_TYPE'], inplace=True)
    dayVMTFrac_2a.loc[dayVMTFrac_2a['VEH_TYPE']=='VEH10', 'rep']=1
    dayVMTFrac_2a.loc[dayVMTFrac_2a['VEH_TYPE']=='VEH25', 'rep']=3
    dayVMTFrac_2a.loc[dayVMTFrac_2a['VEH_TYPE']=='VEH40', 'rep']=3
    dayVMTFrac_2a.loc[dayVMTFrac_2a['VEH_TYPE']=='VEH50', 'rep']=4
    dayVMTFrac_2a.loc[dayVMTFrac_2a['VEH_TYPE']=='VEH60', 'rep']=2
    dayVMTFrac_2a['rep']=dayVMTFrac_2a['rep'].astype('int64')
    dayVMTFrac_2b=dayVMTFrac_2a.reindex(dayVMTFrac_2a.index.repeat(dayVMTFrac_2a.rep))
    dayVMTFrac_2b.reset_index(drop=True, inplace=True)
    dayVMTFrac_2b['sourceTypeID']=''
    for idx, row in dayVMTFrac_2b.iterrows():
        if row['VEH_TYPE']=='VEH10':
            dayVMTFrac_2b.loc[idx, 'sourceTypeID']=11
        if row['VEH_TYPE']=='VEH25':
            if dayVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH25':
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=21
            elif dayVMTFrac_2b.loc[idx+1, 'VEH_TYPE']!='VEH25':
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=32
            else:
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=31
        if row['VEH_TYPE']=='VEH40':
            if dayVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH40':
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=41
            elif dayVMTFrac_2b.loc[idx+1, 'VEH_TYPE']!='VEH40':
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=43
            else:
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=42
        if row['VEH_TYPE']=='VEH50':
            if dayVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH50':
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=51
            elif dayVMTFrac_2b.loc[idx+1, 'VEH_TYPE']!='VEH50':
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=54
            elif dayVMTFrac_2b.loc[idx-2, 'VEH_TYPE']!='VEH50':
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=52
            else:
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=53
        if row['VEH_TYPE']=='VEH60':
            if dayVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH60':
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=61
            else:
                dayVMTFrac_2b.loc[idx, 'sourceTypeID']=62
    dayVMTFrac_2b['IsLeapYear'] = ''
    dayVMTFrac_2b.loc[dayVMTFrac_2b['LeapYear']==1.0, 'IsLeapYear'] = 'N'
    dayVMTFrac_2b.loc[dayVMTFrac_2b['LeapYear']==2.0, 'IsLeapYear'] = 'Y'
    dayVMTFrac_2b.drop(['LeapYear','VEH_TYPE','rep'], axis=1, inplace=True)
    
    #e3. Merge Template with 2 Passes
    dayVMTFrac_3=pd.merge(dayVMT_template, dayVMTFrac_1b, on=['county','sourceTypeID','monthID','roadTypeID','dayID'],how='left')
    dayVMTFrac_3a=dayVMTFrac_3.loc[dayVMTFrac_3['dayVMTFraction'].notnull()]
    dayVMTFrac_3b=dayVMTFrac_3.loc[dayVMTFrac_3['dayVMTFraction'].isnull()]
    dayVMTFrac_3b.drop(columns=['dayVMTFraction','IsLeapYear'], inplace=True)
    dayVMTFrac_4=pd.merge(dayVMTFrac_3b, dayVMTFrac_2b, on=['sourceTypeID','monthID','roadTypeID','dayID'], how='left')
    dayVMTFrac_4a=dayVMTFrac_4.loc[dayVMTFrac_4['dayVMTFraction'].notnull()]
    dayVMTFrac_4b=dayVMTFrac_4.loc[dayVMTFrac_4['dayVMTFraction'].isnull()]
    dayVMTFraction=pd.concat([dayVMTFrac_3a, dayVMTFrac_4a])
    
    #Exporting dataset
    df3_county = dayVMTFraction.groupby('county')
    df3_filepath = outputpath+SELECT_STATE+'_DAY_VMT/'
    pathlib.Path(df3_filepath).mkdir(exist_ok=True) 
    dayVMTFraction.to_csv(outputpath+SELECT_STATE+'_DAY_VMT.csv', index=False)
    for name, group in df3_county:
        group.to_csv(df3_filepath+SELECT_STATE+'_DAY_VMT'+'_'+str(name)+'.csv', index=False, columns=['sourceTypeID','monthID','roadTypeID','dayID','dayVMTFraction'])
    now=lapTimer('  took: ',now)
    ####################################################################################################
    
    #f. Developing hourly summaries
    print('Developing hourly VMT Fractions dataset')
    #f0. create template
    sourceTypeID_list=[11, 21, 31, 32, 41, 42, 43, 51, 52, 53, 54, 61, 62]
    roadTypeID_list=[2, 3, 4, 5]
    dayID_list=[2, 5]
    hourID_list=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24]
    HPMScounty_temp=[]
    sourceType_temp=[]
    roadType_temp=[]
    day_temp=[]
    hour_temp=[]
    for c in HPMScounty_list:
        for s in sourceTypeID_list:
            for r in roadTypeID_list:
                    for d in dayID_list:
                        for h in hourID_list:
                            HPMScounty_temp.append(c)
                            sourceType_temp.append(s)
                            roadType_temp.append(r)
                            day_temp.append(d)
                            hour_temp.append(h)
    d={'county': HPMScounty_temp, 'sourceTypeID': sourceType_temp, 'roadTypeID': roadType_temp, 'dayID': day_temp, 'hourID': hour_temp}
    hourVMT_template=pd.DataFrame(d)
    
    #f1. 1st Pass - County
    df_hourVMT = tmas_class_state.groupby(['COUNTY','roadTypeID','dayID','hourID']).agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    #df_hourVMT_den = tmas_class_state.groupby(['COUNTY','roadTypeID','dayID']).agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    df_hourVMT_class = df_hourVMT.div(df_hourVMT.groupby(['COUNTY','roadTypeID','dayID']).transform('sum'))
    df_hourVMT_class.reset_index(inplace=True)
    df_hourVMT_class['LeapYear']=1
    df_hourVMT_class.rename(index=str, columns={'COUNTY':'county','HPMS_TYPE10':'VEH10',
                                                 'HPMS_TYPE25':'VEH25','HPMS_TYPE40':'VEH40','HPMS_TYPE50':'VEH50',
                                                 'HPMS_TYPE60':'VEH60'}, inplace=True)
    # Wide format to long format
    hourVMTFrac_1a=pd.melt(df_hourVMT_class, id_vars=['county', 'roadTypeID', 'dayID', 'hourID', 'LeapYear'], 
                                   value_vars=['VEH10','VEH25','VEH40','VEH50','VEH60'], 
                                   var_name='VEH_TYPE', value_name='hourVMTFraction')
    hourVMTFrac_1a.sort_values(by=['county', 'roadTypeID', 'dayID', 'hourID', 'VEH_TYPE'], inplace=True)
    hourVMTFrac_1a.loc[hourVMTFrac_1a['VEH_TYPE']=='VEH10', 'rep']=1
    hourVMTFrac_1a.loc[hourVMTFrac_1a['VEH_TYPE']=='VEH25', 'rep']=3
    hourVMTFrac_1a.loc[hourVMTFrac_1a['VEH_TYPE']=='VEH40', 'rep']=3
    hourVMTFrac_1a.loc[hourVMTFrac_1a['VEH_TYPE']=='VEH50', 'rep']=4
    hourVMTFrac_1a.loc[hourVMTFrac_1a['VEH_TYPE']=='VEH60', 'rep']=2
    hourVMTFrac_1a['rep']=hourVMTFrac_1a['rep'].astype('int64')
    hourVMTFrac_1b=hourVMTFrac_1a.reindex(hourVMTFrac_1a.index.repeat(hourVMTFrac_1a.rep))
    hourVMTFrac_1b.reset_index(drop=True, inplace=True)
    hourVMTFrac_1b['sourceTypeID']=''
    for idx, row in hourVMTFrac_1b.iterrows():
        if row['VEH_TYPE']=='VEH10':
            hourVMTFrac_1b.loc[idx, 'sourceTypeID']=11
        if row['VEH_TYPE']=='VEH25':
            if hourVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH25':
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=21
            elif hourVMTFrac_1b.loc[idx+1, 'VEH_TYPE']!='VEH25':
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=32
            else:
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=31
        if row['VEH_TYPE']=='VEH40':
            if hourVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH40':
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=41
            elif hourVMTFrac_1b.loc[idx+1, 'VEH_TYPE']!='VEH40':
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=43
            else:
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=42
        if row['VEH_TYPE']=='VEH50':
            if hourVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH50':
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=51
            elif hourVMTFrac_1b.loc[idx+1, 'VEH_TYPE']!='VEH50':
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=54
            elif hourVMTFrac_1b.loc[idx-2, 'VEH_TYPE']!='VEH50':
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=52
            else:
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=53
        if row['VEH_TYPE']=='VEH60':
            if hourVMTFrac_1b.loc[idx-1, 'VEH_TYPE']!='VEH60':
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=61
            else:
                hourVMTFrac_1b.loc[idx, 'sourceTypeID']=62
    hourVMTFrac_1b['IsLeapYear'] = ''
    hourVMTFrac_1b.loc[hourVMTFrac_1b['LeapYear']==1.0, 'IsLeapYear'] = 'N'
    hourVMTFrac_1b.loc[hourVMTFrac_1b['LeapYear']==2.0, 'IsLeapYear'] = 'Y'
    hourVMTFrac_1b.drop(['LeapYear','VEH_TYPE','rep'], axis=1, inplace=True)
    
    #f2. 2nd Pass - State
    df_hourVMT = tmas_class_state.groupby(['roadTypeID','dayID','hourID']).agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    #df_hourVMT_den = tmas_class_state.groupby(['roadTypeID','dayID']).agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    df_hourVMT_class = df_hourVMT.div(df_hourVMT.groupby(['roadTypeID','dayID']).transform('sum'))
    df_hourVMT_class.reset_index(inplace=True)
    df_hourVMT_class['LeapYear']=1
    df_hourVMT_class.rename(index=str, columns={'HPMS_TYPE10':'VEH10',
                                                 'HPMS_TYPE25':'VEH25','HPMS_TYPE40':'VEH40','HPMS_TYPE50':'VEH50',
                                                 'HPMS_TYPE60':'VEH60'}, inplace=True)
    # Wide format to long format
    hourVMTFrac_2a=pd.melt(df_hourVMT_class, id_vars=['roadTypeID', 'dayID', 'hourID', 'LeapYear'], 
                                   value_vars=['VEH10','VEH25','VEH40','VEH50','VEH60'], 
                                   var_name='VEH_TYPE', value_name='hourVMTFraction')
    hourVMTFrac_2a.sort_values(by=['roadTypeID', 'dayID', 'hourID', 'VEH_TYPE'], inplace=True)
    hourVMTFrac_2a.loc[hourVMTFrac_2a['VEH_TYPE']=='VEH10', 'rep']=1
    hourVMTFrac_2a.loc[hourVMTFrac_2a['VEH_TYPE']=='VEH25', 'rep']=3
    hourVMTFrac_2a.loc[hourVMTFrac_2a['VEH_TYPE']=='VEH40', 'rep']=3
    hourVMTFrac_2a.loc[hourVMTFrac_2a['VEH_TYPE']=='VEH50', 'rep']=4
    hourVMTFrac_2a.loc[hourVMTFrac_2a['VEH_TYPE']=='VEH60', 'rep']=2
    hourVMTFrac_2a['rep']=hourVMTFrac_2a['rep'].astype('int64')
    hourVMTFrac_2b=hourVMTFrac_2a.reindex(hourVMTFrac_2a.index.repeat(hourVMTFrac_2a.rep))
    hourVMTFrac_2b.reset_index(drop=True, inplace=True)
    hourVMTFrac_2b['sourceTypeID']=''
    for idx, row in hourVMTFrac_2b.iterrows():
        if row['VEH_TYPE']=='VEH10':
            hourVMTFrac_2b.loc[idx, 'sourceTypeID']=11
        if row['VEH_TYPE']=='VEH25':
            if hourVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH25':
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=21
            elif hourVMTFrac_2b.loc[idx+1, 'VEH_TYPE']!='VEH25':
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=32
            else:
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=31
        if row['VEH_TYPE']=='VEH40':
            if hourVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH40':
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=41
            elif hourVMTFrac_2b.loc[idx+1, 'VEH_TYPE']!='VEH40':
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=43
            else:
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=42
        if row['VEH_TYPE']=='VEH50':
            if hourVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH50':
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=51
            elif hourVMTFrac_2b.loc[idx+1, 'VEH_TYPE']!='VEH50':
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=54
            elif hourVMTFrac_2b.loc[idx-2, 'VEH_TYPE']!='VEH50':
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=52
            else:
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=53
        if row['VEH_TYPE']=='VEH60':
            if hourVMTFrac_2b.loc[idx-1, 'VEH_TYPE']!='VEH60':
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=61
            else:
                hourVMTFrac_2b.loc[idx, 'sourceTypeID']=62
    hourVMTFrac_2b['IsLeapYear'] = ''
    hourVMTFrac_2b.loc[hourVMTFrac_2b['LeapYear']==1.0, 'IsLeapYear'] = 'N'
    hourVMTFrac_2b.loc[hourVMTFrac_2b['LeapYear']==2.0, 'IsLeapYear'] = 'Y'
    hourVMTFrac_2b.drop(['LeapYear','VEH_TYPE','rep'], axis=1, inplace=True)
    
    #f3. Merge Template with 2 Passes
    hourVMTFrac_3=pd.merge(hourVMT_template, hourVMTFrac_1b, on=['county','sourceTypeID','roadTypeID','dayID','hourID'],how='left')
    hourVMTFrac_3a=hourVMTFrac_3.loc[hourVMTFrac_3['hourVMTFraction'].notnull()]
    hourVMTFrac_3b=hourVMTFrac_3.loc[hourVMTFrac_3['hourVMTFraction'].isnull()]
    hourVMTFrac_3b.drop(columns=['hourVMTFraction','IsLeapYear'], inplace=True)
    hourVMTFrac_4=pd.merge(hourVMTFrac_3b, hourVMTFrac_2b, on=['sourceTypeID','roadTypeID','dayID','hourID'], how='left')
    hourVMTFrac_4a=hourVMTFrac_4.loc[hourVMTFrac_4['hourVMTFraction'].notnull()]
    hourVMTFrac_4b=hourVMTFrac_4.loc[hourVMTFrac_4['hourVMTFraction'].isnull()]
    hourVMTFraction=pd.concat([hourVMTFrac_3a, hourVMTFrac_4a])
    #Exporting datasets
    df3_county = hourVMTFraction.groupby('county')
    df3_filepath = outputpath+SELECT_STATE+'_HOUR_VMT/'
    pathlib.Path(df3_filepath).mkdir(exist_ok=True) 
    hourVMTFraction.to_csv(outputpath+SELECT_STATE+'_HOUR_VMT.csv', index=False)
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
    veh_county = tmas_class_state.groupby('COUNTY').agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    veh_county.reset_index(inplace=True)
    veh_county.rename(index=str, columns={'COUNTY':'county','HPMS_TYPE10':'VEH10','HPMS_TYPE25':'VEH25','HPMS_TYPE40':'VEH40','HPMS_TYPE50':'VEH50','HPMS_TYPE60':'VEH60'}, inplace=True)
    #Creating classification percentages for County level
    veh_county['TOT_VEHS'] = veh_county['VEH10']+veh_county['VEH25']+veh_county['VEH40']+veh_county['VEH50']+veh_county['VEH60']
    veh_county['PCT10'] = veh_county['VEH10']/veh_county['TOT_VEHS']
    veh_county['PCT25'] = veh_county['VEH25']/veh_county['TOT_VEHS']
    veh_county['PCT40'] = veh_county['VEH40']/veh_county['TOT_VEHS']
    veh_county['PCT50'] = veh_county['VEH50']/veh_county['TOT_VEHS']
    veh_county['PCT60'] = veh_county['VEH60']/veh_county['TOT_VEHS']
    veh_county['TOT_PCT'] = veh_county['PCT10']+veh_county['PCT25']+veh_county['PCT40']+veh_county['PCT50']+veh_county['PCT60']
    
    #g2. Aggregating classification data at the State level
    veh_state = tmas_class_state.groupby('STATE').agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    veh_state.reset_index(inplace=True)
    veh_state.rename(index=str, columns={'STATE':'state','HPMS_TYPE10':'VEH10','HPMS_TYPE25':'VEH25','HPMS_TYPE40':'VEH40','HPMS_TYPE50':'VEH50','HPMS_TYPE60':'VEH60'}, inplace=True)
    #Creating classification percentages for State level
    veh_state['TOT_VEHS'] = veh_state['VEH10']+veh_state['VEH25']+veh_state['VEH40']+veh_state['VEH50']+veh_state['VEH60']
    veh_state['PCT10'] = veh_state['VEH10']/veh_state['TOT_VEHS']
    veh_state['PCT25'] = veh_state['VEH25']/veh_state['TOT_VEHS']
    veh_state['PCT40'] = veh_state['VEH40']/veh_state['TOT_VEHS']
    veh_state['PCT50'] = veh_state['VEH50']/veh_state['TOT_VEHS']
    veh_state['PCT60'] = veh_state['VEH60']/veh_state['TOT_VEHS']
    veh_state['TOT_PCT'] = veh_state['PCT10']+veh_state['PCT25']+veh_state['PCT40']+veh_state['PCT50']+veh_state['PCT60']
    
    #g3. Merging County (1st pass) and State (2nd pass) classification to VMT summary
    vmt_merge1 = pd.merge(State_VMT_1,veh_county, on='county', how='left')
    vmt_good = vmt_merge1[pd.notnull(vmt_merge1['TOT_VEHS'])]       # County with avaialbe values
    vmt_bad = vmt_merge1[pd.isnull(vmt_merge1['TOT_VEHS'])]
    vmt_bad.drop(['VEH10','VEH25','VEH40','VEH50','VEH60','TOT_VEHS','PCT10','PCT25','PCT40','PCT50','PCT60','TOT_PCT'],axis=1,inplace=True)
    vmt_merge2 = pd.merge(vmt_bad, veh_state, on='state',how='left')
    veh_tmc=pd.concat([vmt_good, vmt_merge2])
    
    #g4. Calculating VMT by vehicle type
    veh_tmc['VMT10'] = veh_tmc['PCT10']*veh_tmc['VMT']
    veh_tmc['VMT25'] = veh_tmc['PCT25']*veh_tmc['VMT']
    veh_tmc['VMT40'] = veh_tmc['PCT40']*veh_tmc['VMT']
    veh_tmc['VMT50'] = veh_tmc['PCT50']*veh_tmc['VMT']
    veh_tmc['VMT60'] = veh_tmc['PCT60']*veh_tmc['VMT']
    veh_tmc['yearID']=2015
    veh_tmc['baseYearOffNetVMT']=0
    veh_tmc.drop(['VMT','VEH10','VEH25','VEH40','VEH50','VEH60','TOT_VEHS','PCT10','PCT25',
                  'PCT40','PCT50','PCT60','TOT_PCT'], axis=1, inplace=True)
    veh_tmc.reset_index(inplace=True, drop=True)
    
    #g5. Creating County level summaries
    regionVMTFraction = pd.melt(veh_tmc, id_vars=['state', 'county', 'yearID', 'baseYearOffNetVMT'], 
                                   value_vars=['VMT10','VMT25','VMT40','VMT50','VMT60'], 
                                   var_name='VMT_TYPE', value_name='HPMSBaseYearVMT')
    regionVMTFraction.loc[regionVMTFraction['VMT_TYPE']=='VMT10', 'HPMStypeID']=10
    regionVMTFraction.loc[regionVMTFraction['VMT_TYPE']=='VMT25', 'HPMStypeID']=25
    regionVMTFraction.loc[regionVMTFraction['VMT_TYPE']=='VMT40', 'HPMStypeID']=40
    regionVMTFraction.loc[regionVMTFraction['VMT_TYPE']=='VMT50', 'HPMStypeID']=50
    regionVMTFraction.loc[regionVMTFraction['VMT_TYPE']=='VMT60', 'HPMStypeID']=60
    regionVMTFraction.drop('VMT_TYPE', axis=1, inplace=True)
    regionVMTFraction.sort_values(by='county', inplace=True)
    # Exporting data
    df3_county = regionVMTFraction.groupby('county')
    df3_filepath = outputpath+SELECT_STATE+'_REGIONAL_VMT/'
    pathlib.Path(df3_filepath).mkdir(exist_ok=True) 
    regionVMTFraction.to_csv(outputpath+SELECT_STATE+'_REGIONAL_VMT.csv', index=False)
    for name, group in df3_county:
        group.to_csv(df3_filepath+SELECT_STATE+'_REGIONAL_VMT'+'_'+str(name)+'.csv', index=False, columns=['county','HPMStypeID','yearID','HPMSBaseYearVMT','baseYearOffNetVMT'])
    now=lapTimer('  took: ',now)
    ####################################################################################################
    
    #h. Developing RoadType VMT sumarriess
    print('Developing RoadType VMT summaries')
    State_VMT['state']=states.get(SELECT_STATE)[1]
    State_VMT.rename(columns={'County_Cod': 'county'}, inplace=True)
    State_VMT_2 = State_VMT
    
    #h1. Aggregating all volume by County
    veh_county = tmas_class_state.groupby(['COUNTY','roadTypeID']).agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    veh_county_class = veh_county.div(veh_county.groupby('COUNTY').transform('sum'))
    veh_county_class.reset_index(inplace=True)
    veh_county_class.rename(index=str, columns={'COUNTY':'county','HPMS_TYPE10':'VEH10','HPMS_TYPE25':'VEH25','HPMS_TYPE40':'VEH40','HPMS_TYPE50':'VEH50','HPMS_TYPE60':'VEH60'}, inplace=True)
    
    #h2. Aggregating classification data at the State level
    veh_state = tmas_class_state.groupby('roadTypeID').agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    veh_state_den = tmas_class_state.agg({'HPMS_TYPE10':'sum','HPMS_TYPE25':'sum','HPMS_TYPE40':'sum','HPMS_TYPE50':'sum','HPMS_TYPE60':'sum'})
    veh_state_class = veh_state.div(veh_state_den, level=0)
    veh_state_class.reset_index(inplace=True)
    veh_state_class.rename(index=str, columns={'HPMS_TYPE10':'VEH10_S','HPMS_TYPE25':'VEH25_S','HPMS_TYPE40':'VEH40_S','HPMS_TYPE50':'VEH50_S','HPMS_TYPE60':'VEH60_S'}, inplace=True)
    
    #h3. Normalize State & County %
    # Absolute State %
    roadVMTFrac_1=pd.merge(State_VMT_2, veh_state_class, on=['roadTypeID'], how='left')
    #roadVMTFrac_1.loc[:,'VEH10_S_TOT']=roadVMTFrac_1.groupby('county')['VEH10_S'].transform('sum')    # aggregate roadTypeID to county level, by HPMS veh type
    #roadVMTFrac_1.loc[:,'VEH10_S_PCT']=roadVMTFrac_1['VEH10_S']/roadVMTFrac_1['VEH10_S_TOT']
    roadVMTFrac_1.loc[:,'VEH10_S_PCT']=roadVMTFrac_1['VEH10_S']/roadVMTFrac_1.groupby('county')['VEH10_S'].transform('sum')
    roadVMTFrac_1.loc[:,'VEH25_S_PCT']=roadVMTFrac_1['VEH25_S']/roadVMTFrac_1.groupby('county')['VEH25_S'].transform('sum')
    roadVMTFrac_1.loc[:,'VEH40_S_PCT']=roadVMTFrac_1['VEH40_S']/roadVMTFrac_1.groupby('county')['VEH40_S'].transform('sum')
    roadVMTFrac_1.loc[:,'VEH50_S_PCT']=roadVMTFrac_1['VEH50_S']/roadVMTFrac_1.groupby('county')['VEH50_S'].transform('sum')
    roadVMTFrac_1.loc[:,'VEH60_S_PCT']=roadVMTFrac_1['VEH60_S']/roadVMTFrac_1.groupby('county')['VEH60_S'].transform('sum')
    roadVMTFrac_1=roadVMTFrac_1[['county','roadTypeID','VMT','state','VEH10_S_PCT','VEH25_S_PCT','VEH40_S_PCT','VEH50_S_PCT','VEH60_S_PCT']]
    # Absolute County %
    roadVMTFrac_2=pd.merge(roadVMTFrac_1, veh_county_class, on=['county','roadTypeID'], how='left')
    roadVMTFrac_2.loc[:,'VEH10_C_PCT']=roadVMTFrac_2['VEH10']/roadVMTFrac_2.groupby('county')['VEH10'].transform('sum')
    roadVMTFrac_2.loc[:,'VEH25_C_PCT']=roadVMTFrac_2['VEH25']/roadVMTFrac_2.groupby('county')['VEH25'].transform('sum')
    roadVMTFrac_2.loc[:,'VEH40_C_PCT']=roadVMTFrac_2['VEH40']/roadVMTFrac_2.groupby('county')['VEH40'].transform('sum')
    roadVMTFrac_2.loc[:,'VEH50_C_PCT']=roadVMTFrac_2['VEH50']/roadVMTFrac_2.groupby('county')['VEH50'].transform('sum')
    roadVMTFrac_2.loc[:,'VEH60_C_PCT']=roadVMTFrac_2['VEH60']/roadVMTFrac_2.groupby('county')['VEH60'].transform('sum')
    roadVMTFrac_2=roadVMTFrac_2[['county','roadTypeID','VMT','state','VEH10_S_PCT','VEH25_S_PCT','VEH40_S_PCT','VEH50_S_PCT','VEH60_S_PCT',
                                 'VEH10_C_PCT','VEH25_C_PCT','VEH40_C_PCT','VEH50_C_PCT','VEH60_C_PCT']]
    # Re-distribute County shares (leftover of State %) according to avaialble County % data
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH10_C_PCT'].notnull(),'cnt_available']=1
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH10_C_PCT'].isnull(),'cnt_available']=0
    roadVMTFrac_2.loc[:,'VEH10_CNT']=roadVMTFrac_2.groupby(['county','cnt_available'])['VEH10_S_PCT'].transform('sum')
    roadVMTFrac_2.loc[:,'VEH25_CNT']=roadVMTFrac_2.groupby(['county','cnt_available'])['VEH25_S_PCT'].transform('sum')
    roadVMTFrac_2.loc[:,'VEH40_CNT']=roadVMTFrac_2.groupby(['county','cnt_available'])['VEH40_S_PCT'].transform('sum')
    roadVMTFrac_2.loc[:,'VEH50_CNT']=roadVMTFrac_2.groupby(['county','cnt_available'])['VEH50_S_PCT'].transform('sum')
    roadVMTFrac_2.loc[:,'VEH60_CNT']=roadVMTFrac_2.groupby(['county','cnt_available'])['VEH60_S_PCT'].transform('sum')
    roadVMTFrac_2.loc[:,'VEH10_CNT']=roadVMTFrac_2.apply(lambda row: row['VEH10_CNT'] if row['cnt_available']==1 else 1-row['VEH10_CNT'], axis=1)
    roadVMTFrac_2.loc[:,'VEH25_CNT']=roadVMTFrac_2.apply(lambda row: row['VEH25_CNT'] if row['cnt_available']==1 else 1-row['VEH25_CNT'], axis=1)
    roadVMTFrac_2.loc[:,'VEH40_CNT']=roadVMTFrac_2.apply(lambda row: row['VEH40_CNT'] if row['cnt_available']==1 else 1-row['VEH40_CNT'], axis=1)
    roadVMTFrac_2.loc[:,'VEH50_CNT']=roadVMTFrac_2.apply(lambda row: row['VEH50_CNT'] if row['cnt_available']==1 else 1-row['VEH50_CNT'], axis=1)
    roadVMTFrac_2.loc[:,'VEH60_CNT']=roadVMTFrac_2.apply(lambda row: row['VEH60_CNT'] if row['cnt_available']==1 else 1-row['VEH60_CNT'], axis=1)
    
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH10_C_PCT'].isnull(),'VEH10_PCT']=roadVMTFrac_2['VEH10_S_PCT']
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH10_C_PCT'].notnull(),'VEH10_PCT']=roadVMTFrac_2['VEH10_C_PCT']*roadVMTFrac_2['VEH10_CNT']
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH25_C_PCT'].isnull(),'VEH25_PCT']=roadVMTFrac_2['VEH25_S_PCT']
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH25_C_PCT'].notnull(),'VEH25_PCT']=roadVMTFrac_2['VEH25_C_PCT']*roadVMTFrac_2['VEH25_CNT']
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH40_C_PCT'].isnull(),'VEH40_PCT']=roadVMTFrac_2['VEH40_S_PCT']
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH40_C_PCT'].notnull(),'VEH40_PCT']=roadVMTFrac_2['VEH40_C_PCT']*roadVMTFrac_2['VEH40_CNT']
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH50_C_PCT'].isnull(),'VEH50_PCT']=roadVMTFrac_2['VEH50_S_PCT']
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH50_C_PCT'].notnull(),'VEH50_PCT']=roadVMTFrac_2['VEH50_C_PCT']*roadVMTFrac_2['VEH50_CNT']
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH60_C_PCT'].isnull(),'VEH60_PCT']=roadVMTFrac_2['VEH60_S_PCT']
    roadVMTFrac_2.loc[roadVMTFrac_2['VEH60_C_PCT'].notnull(),'VEH60_PCT']=roadVMTFrac_2['VEH60_C_PCT']*roadVMTFrac_2['VEH60_CNT']
    
    roadVMTFrac_3=roadVMTFrac_2[['county','roadTypeID','VMT','state','VEH10_PCT','VEH25_PCT','VEH40_PCT','VEH50_PCT','VEH60_PCT']]
    #roadVMTFrac_3_qc=roadVMTFrac_3.groupby('county').agg('sum')
    
    #h5. Creating County level summaries
    roadVMTFrac_4=pd.melt(roadVMTFrac_3, id_vars=['county', 'roadTypeID', 'VMT', 'state'], 
                                   value_vars=['VEH10_PCT','VEH25_PCT','VEH40_PCT','VEH50_PCT','VEH60_PCT'], 
                                   var_name='VEH_TYPE', value_name='roadTypeVMTFraction')
    roadVMTFrac_4.sort_values(by=['county', 'roadTypeID','VEH_TYPE'], inplace=True)
    roadVMTFrac_4.loc[roadVMTFrac_4['VEH_TYPE']=='VEH10_PCT', 'rep']=1
    roadVMTFrac_4.loc[roadVMTFrac_4['VEH_TYPE']=='VEH25_PCT', 'rep']=3
    roadVMTFrac_4.loc[roadVMTFrac_4['VEH_TYPE']=='VEH40_PCT', 'rep']=3
    roadVMTFrac_4.loc[roadVMTFrac_4['VEH_TYPE']=='VEH50_PCT', 'rep']=4
    roadVMTFrac_4.loc[roadVMTFrac_4['VEH_TYPE']=='VEH60_PCT', 'rep']=2
    roadVMTFrac_4['rep']=roadVMTFrac_4['rep'].astype('int64')
    roadVMTFrac_5=roadVMTFrac_4.reindex(roadVMTFrac_4.index.repeat(roadVMTFrac_4.rep))
    roadVMTFrac_5.reset_index(drop=True, inplace=True)
    roadVMTFrac_5['sourceTypeID']=''
    for idx, row in roadVMTFrac_5.iterrows():
        if row['VEH_TYPE']=='VEH10_PCT':
            roadVMTFrac_5.loc[idx, 'sourceTypeID']=11
        if row['VEH_TYPE']=='VEH25_PCT':
            if roadVMTFrac_5.loc[idx-1, 'VEH_TYPE']!='VEH25_PCT':
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=21
            elif roadVMTFrac_5.loc[idx+1, 'VEH_TYPE']!='VEH25_PCT':
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=32
            else:
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=31
        if row['VEH_TYPE']=='VEH40_PCT':
            if roadVMTFrac_5.loc[idx-1, 'VEH_TYPE']!='VEH40_PCT':
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=41
            elif roadVMTFrac_5.loc[idx+1, 'VEH_TYPE']!='VEH40_PCT':
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=43
            else:
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=42
        if row['VEH_TYPE']=='VEH50_PCT':
            if roadVMTFrac_5.loc[idx-1, 'VEH_TYPE']!='VEH50_PCT':
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=51
            elif roadVMTFrac_5.loc[idx+1, 'VEH_TYPE']!='VEH50_PCT':
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=54
            elif roadVMTFrac_5.loc[idx-2, 'VEH_TYPE']!='VEH50_PCT':
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=52
            else:
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=53
        if row['VEH_TYPE']=='VEH60_PCT':
            if roadVMTFrac_5.loc[idx-1, 'VEH_TYPE']!='VEH60_PCT':
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=61
            else:
                roadVMTFrac_5.loc[idx, 'sourceTypeID']=62
    roadVMTFraction=roadVMTFrac_5[['county','sourceTypeID','roadTypeID','roadTypeVMTFraction','VMT']]
    # Exporting data
    df3_county = roadVMTFraction.groupby('county')
    df3_filepath = outputpath+SELECT_STATE+'_ROADTYPE_VMT/'
    pathlib.Path(df3_filepath).mkdir(exist_ok=True) 
    roadVMTFraction.to_csv(outputpath+SELECT_STATE+'_ROADTYPE_VMT.csv', index=False)
    for name, group in df3_county:
        group.to_csv(df3_filepath+SELECT_STATE+'_ROADTYPE_VMT'+'_'+str(name)+'.csv', index=False)
    now=lapTimer('  took: ',now)
    print('Outputs saved in Final Output\\')
    print('**********Process Completed**********')
    print('')