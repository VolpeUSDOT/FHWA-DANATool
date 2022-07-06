# -*- coding: utf-8 -*-
"""
Created on Tue Nov 12 13:20:03 2019

@author: rge
"""

"""
GUI interface to create the 'input.txt' file used to select TMC links in the NTD Dataset
Author: Cambridge Systematics
Modified By: Volpe National Transportation Systems Center
"""

versionNum = "2.0"

import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Polygon, LineString, Point
import os
import io
import time
import datetime as dt
import pathlib
import tkinter as tk
from tkinter import *
from tkinter import Tk,ttk,StringVar,filedialog
import re
import multiprocessing as mp
from tqdm.tk import tqdm
from tkcalendar import Calendar, DateEntry

from lib import NTD_00_TMAS
from lib import NTD_01_NPMRDS
from lib import NTD_02_MOVES
from lib import NTD_03_SPEED
from lib import NTD_04_NOISE

import sys
import traceback

if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")
#import pyarrow as pa
#import pyarrow.parquet as pq
#from shapely.geometry import Point

#filepath = ''
#pathlib.Path(filepath).mkdir(exist_ok=True)

class RedirectText(object):
    """"""
    #----------------------------------------------------------------------
    def __init__(self, queue):
        """Constructor"""
        self.out_queue = queue
        
    #----------------------------------------------------------------------
    def write(self, string):
        self.out_queue.put(string)
        
    def flush(self):
        pass        

#Function for creating path to the icon        
def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)

# Func - Popups
def PopUpCleanTMASSelection():
    popup = Tk()
    popup.wm_title("Warning")
    label = ttk.Label(popup, text="Please chose a processed TMAS File.")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop    
def PopUpCleanNPMRDSSelection():
    popup = Tk()
    popup.wm_title("Warning")
    label = ttk.Label(popup, text="Please run Step 1 to create processed NPMRDS File.")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop
def PopUp_Selection(txt_input):
    popup = Tk()
    popup.wm_title("Warning")
    label = ttk.Label(popup, text="No "+txt_input+" selected, please select again.")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop   
def PopUpMsg(txt_input):
    popup = Tk()
    popup.wm_title("Warning")
    label = ttk.Label(popup, text=txt_input)
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop

# Initialization
fn_output = ''
fn_tmas_station = ''
fn_tmas_class = ''
fn_npmrds_all = ''
fn_npmrds_pass = ''
fn_npmrds_truck = ''
fn_npmrds_tmc = ''
fn_npmrds_shp = ''
fn_emission = ''
fn_tmas_class_clean = ''
fn_npmrds_clean = ''
fn_tmas_station = ''
fn_hpms = ''
fn_vm2 = ''
fn_county_mileage = ''
fn_fips = ''
fn_nei = ''
step0 = '0. Pre-Process Raw TMAS Data (optional)'
step1 = '1. Process Raw NPMRDS Data'
step2 = '2. Produce MOVES Inputs'
step3 = '3. Produce Speed Distributions'
step4 = '4. Produce Noise Inputs'

# Func - File dialogs    
def f_output():
    global fn_output
    fn_output = filedialog.askdirectory(parent=root, title='Choose Output Folder', initialdir='/')
    pl_output_folder.config(text=fn_output.replace('/','\\'))
    pathlib.Path(fn_output).mkdir(exist_ok=True)
    with open(fn_output + '/progress_log.txt', 'a') as file:
        file.write('\n\n*********** New DANA TOOL Log ************')
        file.write('\n******** {} ********\n\n'.format(dt.datetime.now().strftime('%c')))
    #print (f)
def f_tmas_station():
    global fn_tmas_station
    fn_tmas_station = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose TMAS Station File',
        filetypes=[('dat file', '.dat'), ('station file', '.STA')])
    pl_tmas_station.config(text=fn_tmas_station.replace('/','\\'))
    #print (f)
def f_tmas_class():
    global fn_tmas_class
    fn_tmas_class = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose TMAS Class File',
        filetypes=[('dat file', '.dat'),('csv file', '.csv'), ('class file', '.CLS')])
    pl_tmas_class.config(text=fn_tmas_class.replace('/','\\'))
def f_npmrds_all():
    global fn_npmrds_all
    fn_npmrds_all = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose NPMRDS Passenger and Truck File',
        filetypes=[('csv file', '.csv')])
    pl_npmrds_all.config(text=fn_npmrds_all.replace('/','\\'))
def f_npmrds_pass():
    global fn_npmrds_pass
    fn_npmrds_pass = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose NPMRDS Passenger File',
        filetypes=[('csv file', '.csv')])
    pl_npmrds_pass.config(text=fn_npmrds_pass.replace('/','\\'))
def f_npmrds_truck():
    global fn_npmrds_truck
    fn_npmrds_truck = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose NPMRDS Truck File',
        filetypes=[('csv file', '.csv')])
    pl_npmrds_truck.config(text=fn_npmrds_truck.replace('/','\\'))
def f_npmrds_tmc():
    global fn_npmrds_tmc
    fn_npmrds_tmc = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose NPMRDS TMC Configuration',
        filetypes=[('csv file', '.csv')])
    pl_npmrds_tmc.config(text=fn_npmrds_tmc.replace('/','\\'))
def f_npmrds_shp():
    global fn_npmrds_shp
    fn_npmrds_shp = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose NPMRDS TMC shapefile',
        filetypes=[('shapefile file', '.shp')])
    pl_npmrds_shp.config(text=fn_npmrds_shp.replace('/','\\'))
def f_emission():
    global fn_emission
    fn_emission = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose Emission Rates File',
        filetypes=[('parquet file', '.parquet')])
    pl_emission.config(text=fn_emission.replace('/','\\'))
def f_tmas_class_clean():
    global fn_tmas_class_clean
    fn_tmas_class_clean = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose Processed TMAS Class File',
        filetypes=[('csv file', '.csv')])
    pl_tmas_class_clean_1.config(text=fn_tmas_class_clean.replace('/','\\'))
def f_npmrds_clean():
    global fn_npmrds_clean
    fn_npmrds_clean = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose Processed NPMRDS File',
        filetypes=[('parquet file', '.parquet'),('csv file', '.csv')])
    pl_npmrds_clean_1.config(text=fn_npmrds_clean.replace('/','\\'))
    pl_npmrds_clean_2.config(text=fn_npmrds_clean.replace('/','\\'))
    pl_npmrds_clean_3.config(text=fn_npmrds_clean.replace('/','\\'))
def f_tmas_station_state():
    global fn_tmas_station
    fn_tmas_station = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose Processed TMAS Station File',
        filetypes=[('csv file', '.csv')])
    pl_tmas_station_state_1.config(text=fn_tmas_station.replace('/','\\'))
    #pl_tmas_station_state_2.config(text=fn_tmas_station)
def f_hpms():
    global fn_hpms
    fn_hpms = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose HPMS File',
        filetypes=[('csv file', '.csv'), ('shape file', '.shp')])
    pl_hpms.config(text=fn_hpms.replace('/','\\'))
def f_vm2():
    global fn_vm2
    fn_vm2 = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose VM2 File',
        filetypes=[('csv file', '.csv')])
    pl_vm2.config(text=fn_vm2.replace('/','\\'))
def f_county_mileage():
    global fn_county_mileage
    fn_county_mileage = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose County Mileage File',
        filetypes=[('csv file', '.csv')])
    pl_county_mileage.config(text=fn_county_mileage.replace('/','\\'))
def f_fips():
    global fn_fips
    fn_fips = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose Federal Information Processing Standards (FIPS) File',
        filetypes=[('csv file', '.csv')])
    pl_fips_1.config(text=fn_fips.replace('/','\\'))
    pl_fips_2.config(text=fn_fips.replace('/','\\'))
def f_nei():
    global fn_nei
    fn_nei = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose Representative Counties File',
        filetypes=[('csv file', '.csv')])
    pl_nei_1.config(text=fn_nei.replace('/','\\'))
    pl_nei_2.config(text=fn_nei.replace('/','\\'))

# Enable TMAS Preprocessing
def enable_tmas_preprocess():
    global list_scripts
    if preprocess_checkvar.get() == 0:
        p0Canvas.grid_remove()
        w_tmas_station.grid_remove()
        w_tmas_class.grid_remove()
        w_fips_1.grid_remove()
        w_nei_1.grid_remove()
        
        pl_tmas_station.grid_remove()
        pl_tmas_class.grid_remove()
        pl_fips_1.grid_remove()
        pl_nei_1.grid_remove()

    else:
        p0Canvas.grid()
        w_tmas_station.grid()
        w_tmas_class.grid()
        w_fips_1.grid()
        w_nei_1.grid()
        
        pl_tmas_station.grid()
        pl_tmas_class.grid()
        pl_fips_1.grid()
        pl_nei_1.grid()

def autoDetectClick():
    if autoDetectDatesVar.get() == 1:
        calStart.configure(state='disabled')
        calEnd.configure(state='disabled')
    if autoDetectDatesVar.get() == 2:
        calStart.configure(state='normal')
        calEnd.configure(state='normal')

        
# Func - Print TMC Input
def PrintTMCinput(tmc_list):
    text = open('TMC input.txt', 'w')
    for i in tmc_list:
        text.write(i+'\n')
        
    text.close()

def checkProgress():
    global thread_queue
    global fn_npmrds_clean
    while not thread_queue.empty():
        line = thread_queue.get()
        output_text.insert(tk.END, line)
        with open(fn_output + '/progress_log.txt', 'a') as file:
            file.write(line)
        
    root.update_idletasks()
    canvasScrollRegion = (0, 0, 
                          max(canvas.winfo_width()-4, canvas.bbox('all')[2]),
                          max(canvas.winfo_height()-4, canvas.bbox('all')[3]))
    canvas.configure(scrollregion=canvasScrollRegion)
    TMCcanvasScrollRegion = (0, 0, 
                          max(TMCSelect_canvas.winfo_width()-4, TMCSelect_canvas.bbox('all')[2]-1),
                          max(TMCSelect_canvas.winfo_height()-4, TMCSelect_canvas.bbox('all')[3]-1))
    TMCSelect_canvas.configure(scrollregion=TMCcanvasScrollRegion)
    
    #There will only every be one process running at a time, but we look at all
    #running threads to make sure it is working cleanly
    removeList = []
    for i in range(len(runningThreads)):
        if not runningThreads[i].is_alive():
            removeList.append(i)
        else:
            disable_buttons(runningThreads[i].name)
    for i in removeList:
        print("*** {} has finished running ***".format(runningThreads[i].name))
        progBar.stop()
        enable_buttons()
        del runningThreads[i]
    
    global afterids
    afterids = []
    afterids.append(root.after(100, checkProgress))
        

def process_handler(proc_target, thread_queue, args): 
    redir = RedirectText(thread_queue)
    sys.stdout = redir
    #sys.stderr = sys.stdout
    try:
        proc_target(*args)
    except Exception as e:
        print(traceback.format_exc())            
    
# Func - ProcessData Function
def ProcessData(procNum):
    global runningThreads
    #output_text.delete('1.0', END)
    tmc_chars = set('0123456789+-PN, ')
    
    if StateValue.get() == '':
        PopUp_Selection("State")
        return
    if fn_output == '':
        PopUp_Selection("Output Folder Location")
        return
    
    progBar.start()
    if procNum == step0:
        if fn_tmas_station == '':
            PopUp_Selection("TMAS Station file")
        elif fn_tmas_class == '':
            PopUp_Selection("TMAS Class file")
        elif fn_fips == '':
            PopUp_Selection("Federal Information Processing Standards (FIPS) file")
        elif fn_nei == '':
            PopUp_Selection("National Emission Inventory")
        else:
            SELECT_STATE = StateValue.get()
            PATH_TMAS_STATION = fn_tmas_station
            PATH_TMAS_CLASS = fn_tmas_class
            PATH_FIPS = fn_fips 
            PATH_NEI = fn_nei
            PATH_OUTPUT = fn_output
            TMAS_Proc = mp.Process(target=process_handler, name=step0, args=(NTD_00_TMAS.TMAS, thread_queue, (SELECT_STATE, PATH_TMAS_STATION, PATH_TMAS_CLASS, PATH_FIPS, PATH_NEI, PATH_OUTPUT)))
            disable_buttons(step0)
            TMAS_Proc.start()
            runningThreads.append(TMAS_Proc)
            notebook.select('.!notebook.!frame2')
            
    elif procNum == step1:
        if fn_tmas_station == '' or fn_tmas_class_clean == '':
            PopUpCleanTMASSelection()            
        elif fn_npmrds_all == '':
            PopUp_Selection("NPMRDS All Data")
        elif fn_npmrds_pass == '':
            PopUp_Selection("NPMRDS Passenger Data")
        elif fn_npmrds_truck == '':
            PopUp_Selection("NPMRDS Truck Data")
        elif fn_npmrds_tmc == '':
            PopUp_Selection("NPMRDS TMC Configuration")
        # elif fn_npmrds_shp == '':
        #     PopUp_Selection("NPMRDS TMC Shapefile")
        elif fn_emission == '':
            PopUp_Selection("Emission Rates")
        elif fn_fips == '':
            PopUp_Selection("Federal Information Processing Standards (FIPS) file")
        elif fn_nei == '':
            PopUp_Selection("National Emission Inventory")
        else:
            SELECT_STATE = StateValue.get()
            PATH_npmrds_raw_all = fn_npmrds_all
            PATH_npmrds_raw_pass = fn_npmrds_pass
            PATH_npmrds_raw_truck = fn_npmrds_truck
            PATH_tmc_identification = fn_npmrds_tmc
            PATH_tmc_shp = fn_npmrds_shp
            PATH_emission = fn_emission
            PATH_TMAS_STATION = fn_tmas_station
            PATH_TMAS_CLASS_CLEAN = fn_tmas_class_clean
            PATH_FIPS = fn_fips 
            PATH_NEI = fn_nei
            PATH_OUTPUT = fn_output
            AUTO_DETECT_DATES = autoDetectDatesVar.get()==1
            DATE_START = calStart.get_date()
            DATE_END = calEnd.get_date()
            NPMRDS_Proc = mp.Process(target=process_handler, name=step1, args=(NTD_01_NPMRDS.NPMRDS, thread_queue, 
                                    (SELECT_STATE, PATH_tmc_identification, PATH_npmrds_raw_all, PATH_npmrds_raw_pass, PATH_npmrds_raw_truck, 
                                     PATH_emission, PATH_TMAS_STATION, PATH_TMAS_CLASS_CLEAN, PATH_FIPS, PATH_NEI, PATH_OUTPUT, AUTO_DETECT_DATES, DATE_START, DATE_END)))
            disable_buttons(step1)
            NPMRDS_Proc.start()
            runningThreads.append(NPMRDS_Proc)
            notebook.select('.!notebook.!frame2')
    
    elif procNum == step2:
        if fn_npmrds_clean == '':
            PopUpCleanNPMRDSSelection()
        else:
            SELECT_STATE = StateValue.get()
            PATH_NPMRDS = fn_npmrds_clean
            PATH_HPMS = fn_hpms
            PATH_VM2 = fn_vm2
            PATH_COUNTY_MILEAGE = fn_county_mileage
            PATH_OUTPUT = fn_output
            AUTO_DETECT_DATES = autoDetectDatesVar.get()==1
            DATE_START = calStart.get_date()
            DATE_END = calEnd.get_date()
            MOVES_Proc = mp.Process(target=process_handler, name=step2, args=(NTD_02_MOVES.MOVES, thread_queue, 
                                                                              (SELECT_STATE, PATH_NPMRDS, PATH_HPMS, PATH_VM2, PATH_COUNTY_MILEAGE, PATH_OUTPUT,
                                                                               AUTO_DETECT_DATES, DATE_START, DATE_END)))
            disable_buttons(step2)
            MOVES_Proc.start()
            runningThreads.append(MOVES_Proc)
            notebook.select('.!notebook.!frame2')
            
    elif procNum == step3:    # needs update
        SELECT_STATE = StateValue.get()
        if fn_npmrds_clean == '':
            PopUpCleanNPMRDSSelection()
        else:
            PATH_NPMRDS = fn_npmrds_clean
            PATH_OUTPUT = fn_output
            AUTO_DETECT_DATES = autoDetectDatesVar.get()==1
            DATE_START = calStart.get_date()
            DATE_END = calEnd.get_date()
            SPEED_Proc = mp.Process(target=process_handler, name=step3, args=(NTD_03_SPEED.SPEED, thread_queue, 
                                                                              (SELECT_STATE, PATH_NPMRDS, PATH_OUTPUT,
                                                                               AUTO_DETECT_DATES, DATE_START, DATE_END)))
            disable_buttons(step3)
            SPEED_Proc.start()
            runningThreads.append(SPEED_Proc)
            notebook.select('.!notebook.!frame2')

    elif procNum == step4:    # needs update
        SELECT_STATE = StateValue.get()
        if fn_npmrds_clean == '':
            PopUpCleanNPMRDSSelection()
        elif any((c not in tmc_chars) for c in tmcEntry.get()):
            PopUpMsg('Incorrect TMC name, please enter again.')
        else:
            SELECT_STATE = StateValue.get()
            SELECT_TMC = re.split(',\s+',tmcEntry.get())
            #PrintTMCinput(SELECT_TMC)
            PATH_NPMRDS = fn_npmrds_clean
            PATH_OUTPUT = fn_output
            AUTO_DETECT_DATES = autoDetectDatesVar.get()==1
            DATE_START = calStart.get_date()
            DATE_END = calEnd.get_date()
            NOISE_Proc = mp.Process(target=process_handler, name=step4, args=(NTD_04_NOISE.NOISE, thread_queue, 
                                                                              (SELECT_STATE, SELECT_TMC, PATH_NPMRDS, PATH_OUTPUT,
                                                                               AUTO_DETECT_DATES, DATE_START, DATE_END)))
            disable_buttons(step4)
            NOISE_Proc.start()
            runningThreads.append(NOISE_Proc)
            notebook.select('.!notebook.!frame2')
            
    else:
        PopUp_Selection("Step")
    
    #root.destroy()

def disable_buttons(step):
    p0startButton['state'] = DISABLED
    p1startButton['state'] = DISABLED
    p2startButton['state'] = DISABLED
    p3startButton['state'] = DISABLED
    p4startButton['state'] = DISABLED

    p0cancelButton["state"] = DISABLED
    p1cancelButton["state"] = DISABLED
    p2cancelButton["state"] = DISABLED
    p3cancelButton["state"] = DISABLED
    p4cancelButton["state"] = DISABLED
    
    if step == step0:
        p0cancelButton["state"] = NORMAL
        p0statusLabel.grid()
    elif step == step1:
        p1cancelButton["state"] = NORMAL
        p1statusLabel.grid()
    elif step == step2:
        p2cancelButton["state"] = NORMAL
        p2statusLabel.grid()
    elif step == step3:
        p3cancelButton["state"] = NORMAL
        p3statusLabel.grid()
    elif step == step4:
        p4cancelButton["state"] = NORMAL
        p4statusLabel.grid()

def enable_buttons():
    p0startButton['state'] = NORMAL
    p1startButton['state'] = NORMAL
    p2startButton['state'] = NORMAL
    p3startButton['state'] = NORMAL
    p4startButton['state'] = NORMAL

    p0cancelButton["state"] = NORMAL
    p1cancelButton["state"] = NORMAL
    p2cancelButton["state"] = NORMAL
    p3cancelButton["state"] = NORMAL
    p4cancelButton["state"] = NORMAL
    
    p0statusLabel.grid_remove()
    p1statusLabel.grid_remove()
    p2statusLabel.grid_remove()
    p3statusLabel.grid_remove()
    p4statusLabel.grid_remove()
    
def CancelProcess():
    global runningThreads
    for proc in runningThreads:
        while proc.is_alive():
            proc.terminate()
        
        if not proc.is_alive():
            print("************* Canceled Step {} **************".format(proc.name))
    

# TMC Selection Functions
################################################################################

fn_tmc_config = ''
fn_kml = ''
counties = ['']
roads = ['']
directions = ['']
kmlfiles = ['']
tmc = pd.DataFrame()

def f_tmc_config():
    global fn_tmc_config, tmc, geo_tmc, counties, roads, directions, kmlfiles
    fn_tmc_config = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose TMC Config File',
        filetypes=[('csv file', '.csv')])
    pl_tmc_config.config(text=fn_tmc_config.replace('/','\\'))
    tmc = pd.read_csv(fn_tmc_config)
    dir_dic = {'EASTBOUND':'EB', 'NORTHBOUND':'NB', 'WESTBOUND':'WB', 'SOUTHBOUND':'SB'}
    tmc['direction'].replace(dir_dic, inplace=True)
    tmc['direction']=tmc['direction'].str.extract('(EB|NB|SB|WB)')
    tmc['direction'].value_counts()
    #We create a column with all of the points together to create polygons from them
    tmc['start']= tmc.apply(lambda row: (row["start_longitude"],row["start_latitude"]),axis=1)
    tmc['end']= tmc.apply(lambda row: (row["end_longitude"],row["end_latitude"]),axis=1)
    # We create a new feature as shapely LineString from the start and end points
    tmc['geometry'] = tmc.apply(lambda row: LineString([row['start'],row['end']]),axis=1)
    #We create a GeoDataFrame with the polygon data
    crs = {'init' :'epsg:4326'}
    geo_tmc = GeoDataFrame(tmc, crs=crs, geometry='geometry')

    # We read the selection attributes from the tmc file
    counties = geo_tmc['county'].unique().tolist()
    counties.sort()
    counties.insert(0,'')
    roads = geo_tmc['road'].unique().tolist()
    roads.insert(0,'')
    directions = geo_tmc['direction'].unique().tolist()
    directions.insert(0,'')
    # kmlfiles = os.listdir('KML Polygon/')
    # kmlfiles.insert(0,"")
    
    county.config(values=counties)
    road.config(values=roads)
    direction.config(values=directions)
    
def f_kml():
    global fn_kml
    fn_kml = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose KML File',
        filetypes=[('kml file', '.kml')])
    pl_kml.config(text=fn_kml.replace('/','\\'))
    

#We create a function to crop tmc using kml polygon
def ReadPolygon():
    #Read kml polygon and parse it as an xml file
    kml_path = fn_kml
    tree_poly = ET.parse(kml_path)
    #Read the coordinates from xml file
    lineRing = tree_poly.findall('.//{http://www.opengis.net/kml/2.2}LinearRing')
    for attributes in lineRing:
        for subAttribute in attributes:
            if subAttribute.tag == '{http://www.opengis.net/kml/2.2}coordinates':
                coordinates_string = subAttribute.text
    #Separate coordinates from string into list
    coordinates = coordinates_string.split()
    sep_coord = []
    for i in coordinates:
        sep_coord.append(i.split(','))
    #Change coordinates from string to floats
    numb_coord = []
    for i in sep_coord:
        numb_coord.append([float(j) for j in i])
    #Create shapely polygon with coordinates
    Poly_Tuples = list(tuple(x[0:2]) for x in numb_coord)
    Poly = Polygon(Poly_Tuples)
    return Poly

def PrintResults(tmc_list, county, road, direction):
    outputpath = fn_output + '/TMC_Selection/'
    pathlib.Path(outputpath).mkdir(exist_ok=True)
    filename = 'TMCs_{}_{}_{}.txt'.format(county, road, direction)
    text = open(outputpath+filename, 'w')
    for i in tmc_list:
        text.write(i+',')
    text.close()
    #tmcEntry.set(','.join(tmc_list))
    PopUpCompletedSelection(outputpath, filename)

def PopUpNoSelection():
    popup = Tk()
    popup.wm_title("Warning")
    label = ttk.Label(popup, text="No selection criteria were defined, all of State TMC links will be used. You may close the tool, or select again.")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop
    
def PopUpWrongSelection():
    popup = Tk()
    popup.wm_title("Error")
    label = ttk.Label(popup, text="No links were found with the selection parameters. Please modify the selection parameters")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop
    
def PopUpNoFile():
    popup = Tk()
    popup.wm_title("Error")
    label = ttk.Label(popup, text="No input file chosen. Please choose a TMC or KML configuration file first.")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop
    
def PopUpCompletedSelection(outputpath, filename):
    popup = Tk()
    popup.wm_title("TMC Selection Complete")
    head = ttk.Label(popup, text='TMC Selection Completed'.format(outputpath+filename))
    head.pack(side="top", pady=5, padx=5)
    label = ttk.Label(popup, text='TMC Selection Results saved to {}'.format(outputpath+filename))
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop

# We create the CheckRoad function
def CheckIntersect(tmc_to_check, tmc):
    intersect = tmc_to_check[tmc_to_check.isin(tmc)]
    if len(intersect)==0:
        return None
    else:
        return intersect

# We create the SelectData function
def SelectData():
    #Read polygon file or pass if none is selected    
    if fn_kml == '':
        if tmc.empty:
            PopUpNoFile()
            return
        selected_tmc = tmc['tmc']
    else:
        #Read tmcs within polygon
        tmc_polygon = ReadPolygon()
        polygon_selection = geo_tmc['geometry'].within(tmc_polygon)
        selected_tmc = tmc.loc[polygon_selection,'tmc']
    #Read County name or pass if none is selected
    if CountyValue.get() == '':
        pass
    else:
        county_select = tmc.loc[tmc['county']==CountyValue.get(), 'tmc']
        county_tmc = CheckIntersect(county_select, selected_tmc)
        selected_tmc = county_tmc
    #Read Road name or pass if none is selected
    if RoadValue.get() == '':
        pass
    elif selected_tmc is None:
        pass
    else:
        road_select = tmc.loc[tmc['road']==RoadValue.get(), 'tmc']
        road_tmc = CheckIntersect(road_select, selected_tmc)
        selected_tmc = road_tmc
    #Read Direction name or pass if none is selected
    if DirectionValue.get() == '':
        pass
    elif selected_tmc is None:
        pass
    else:
        dir_select = tmc.loc[tmc['direction']==DirectionValue.get(), 'tmc']
        dir_tmc = CheckIntersect(dir_select, selected_tmc)
        selected_tmc = dir_tmc
    if selected_tmc is None:
        PopUpWrongSelection()
    elif len(selected_tmc)==len(tmc['tmc']):
        PopUpNoSelection()
        PrintResults(selected_tmc, CountyValue.get(), RoadValue.get(), DirectionValue.get())
    else:
        PrintResults(selected_tmc, CountyValue.get(), RoadValue.get(), DirectionValue.get())

# GUI
   
################################################################################
#Some important user interface callback functions

# Bind Mouse Wheel to GUI

def bind_tree(widget, event, callback):
    "Binds an event to a widget and all its descendants."

    widget.bind(event, callback)

    for child in widget.children.values():
        bind_tree(child, event, callback)

def main_mouse_wheel(event):
    #if main_container.winfo_height() 
    canvas.yview_scroll(int(-1*event.delta/120), "units")
    
def output_mouse_wheel(event):
    output_text.yview_scroll(int(-1*event.delta/120), "units")

def tmcselect_mouse_wheel(event):
    TMCSelect_canvas.yview_scroll(int(-1*event.delta/120), "units")
    
# bind ctrl events so the text can be coppied
def ctrlEvent(event):
    if(12==event.state and event.keysym=='c' ):
        return
    elif(12==event.state and event.keysym=='Tab'):
        return
    elif(event.keysym=='Left' or event.keysym=='Right' or event.keysym=='Up' or event.keysym=='Down'):
        return
    else:
        return "break"

################################################################################

root = Tk()
root.title("FHWA DANA Tool - v{}".format(versionNum))
root.grid_rowconfigure(0, weight=0)
root.grid_rowconfigure(1, weight=1)
root.grid_columnconfigure(0, weight=1)

iconPath = resource_path('lib\\dot.png')
p1 = tk.PhotoImage(file = iconPath)
root.iconphoto(True, p1)

headerFont = ("Ariel", 15, "bold")
ttk.Label(root, wraplength = 500, text="Welcome to FHWA's DANA Tool", font=headerFont).grid(row=0, column=0, columnspan= 1, sticky="w")
notebook = ttk.Notebook(root, height=500, width=1200)
notebook.grid(row=1, column=0, sticky="news")

main_container = tk.Frame(notebook)
main_container.grid(row=0, column=0, sticky="news")
main_container.grid_rowconfigure(0, weight=1)
main_container.grid_columnconfigure(0, weight=1)
notebook.add(main_container, text='Data Processing')

canvas = tk.Canvas(main_container)
main_scrollbar = tk.Scrollbar(main_container, orient="vertical", command=canvas.yview)
main_scrollbar.grid(row=0, column=1, sticky='ns')
canvas.grid(row=0, column=0, sticky="news")
canvas.configure(yscrollcommand = main_scrollbar.set)

mainframe = tk.Frame(canvas)
canvas.create_window((0,0), window=mainframe, anchor='nw')

## Progress Output Tab

output_container = tk.Frame(notebook)
output_container.grid(row=0, column=0, sticky="news")
output_container.grid_rowconfigure(0, weight=1)
output_container.grid_columnconfigure(0, weight=1)
notebook.add(output_container, text='Progress Log')

output_textcontainer = tk.Frame(output_container)
output_textcontainer.grid(row=0, column=0, sticky="news")
output_textcontainer.grid_rowconfigure(0, weight=1)
output_textcontainer.grid_columnconfigure(0, weight=1)

output_text = tk.Text(output_textcontainer, height=400, width=80,  wrap=WORD)
output_text.grid(row=0, column=0, sticky="news")
output_scrollbar = tk.Scrollbar(output_textcontainer, orient="vertical", command=output_text.yview)
output_scrollbar.grid(row=0, column=1, sticky='ns')
output_text.configure(yscrollcommand = output_scrollbar.set)
output_text.bind("<MouseWheel>", output_mouse_wheel)
output_text.bind("<Key>", lambda e: ctrlEvent(e))

progBarContainer = tk.Frame(output_container)
progBarContainer.grid(row=1, column = 0, sticky  = "news")

progBarContainer.grid_columnconfigure(0, weight=1)
progBarContainer.grid_columnconfigure(1, weight=1)
progBarContainer.grid_columnconfigure(2, weight=1)


progBar = ttk.Progressbar(progBarContainer, mode='indeterminate')
progBar.grid(row=0, column = 1, sticky  = "news")

# TMC Select GUI

TMCSelect_container = tk.Frame(notebook)
TMCSelect_container.grid(row=0, column=0, sticky="news")
TMCSelect_container.grid_rowconfigure(0, weight=1)
TMCSelect_container.grid_columnconfigure(0, weight=1)
notebook.add(TMCSelect_container, text='TMC Selection')

TMCSelect_canvas = tk.Canvas(TMCSelect_container)
TMCSelect_scrollbar = tk.Scrollbar(TMCSelect_container, orient="vertical", command=TMCSelect_canvas.yview)
TMCSelect_scrollbar.grid(row=0, column=1, sticky='ns')
TMCSelect_canvas.grid(row=0, column=0, sticky="news")
TMCSelect_canvas.configure(yscrollcommand = TMCSelect_scrollbar.set)

TMCSelection_frame = tk.Frame(TMCSelect_canvas)
TMCSelect_canvas.create_window((0,0), window=TMCSelection_frame, anchor='nw', )

ttk.Label(TMCSelection_frame, wraplength = 500, text='To select specific data from the National Traffic Dataset, please select the desired features').grid(row=0, column=0, columnspan= 5)
w_tmc_config = ttk.Button(TMCSelection_frame, text='Select TMC Config File', command=f_tmc_config).grid(column=0, row=1, columnspan=2, sticky="w")
pl_tmc_config = ttk.Label(TMCSelection_frame)
pl_tmc_config.grid(column=2, row=1, columnspan=3, sticky="w")

w_kml = ttk.Button(TMCSelection_frame, text='Select KML File', command=f_kml).grid(column=0, row=2, columnspan=2, sticky="w")
pl_kml = ttk.Label(TMCSelection_frame)
pl_kml.grid(column=2, row=2, columnspan=3, sticky="w")

ttk.Label(TMCSelection_frame, text='Select with County:').grid(row=3,column=0, columnspan=2, sticky="w")
ttk.Label(TMCSelection_frame, text='Select a Specific Road:').grid(row=4,column=0, columnspan=2, sticky="w")
ttk.Label(TMCSelection_frame, text='Select a Specific Direction:').grid(row=5,column=0, columnspan=2, sticky="w")

CountyValue = StringVar()
county = ttk.Combobox(TMCSelection_frame, textvariable=CountyValue, state='readonly', width=50)
county.grid(column=2, row=3, columnspan=3, sticky="w")

RoadValue = StringVar()
road = ttk.Combobox(TMCSelection_frame, textvariable=RoadValue, state='readonly', width=50)
road.grid(column=2, row=4, columnspan=3, sticky="w")

DirectionValue = StringVar()
direction = ttk.Combobox(TMCSelection_frame, textvariable=DirectionValue, state='readonly', width=50)
direction.grid(column=2, row=5, columnspan=3, sticky="w")

tmcButton = ttk.Button(TMCSelection_frame, text="Select Data", command=SelectData).grid(column=0, row=7, columnspan=5)

for child in TMCSelection_frame.winfo_children(): child.grid_configure(padx=2, pady=4)

#print(notebook.tabs())

##################################################

##################################################
#0. File location output
#ttk.Label(mainframe, text='Choose output file location').grid(row=0,column=0, columnspan=1, sticky="w")
ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=1,column=0, columnspan=5, sticky="ew")
#1. Header

ttk.Label(mainframe, text='Select State:').grid(row=2,column=0, columnspan=1, sticky="w")

ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=3,column=0, columnspan=5, sticky="ew")

autoDetectDatesVar = IntVar()
autoDetectDatesVar.set(1)
autoDetectDatesBox = ttk.Radiobutton(mainframe, text='Auto-detect date range from NPMRDS data or processed NPMRDS file.', value = 1, variable=autoDetectDatesVar, command=autoDetectClick)
autoDetectDatesBox.grid(row=4,column=0, columnspan=1, sticky="w")

# Date Range Selection
SelectRangeBox = ttk.Radiobutton(mainframe, text='Or, select a date range to process (must be within the maximum and minimum dates in the NPMRDS data input in Process 1 below).', value = 2, variable=autoDetectDatesVar, command=autoDetectClick)
SelectRangeBox.grid(row=5,column=0, columnspan=3, sticky="w")
drCanvas = tk.Canvas(mainframe)
drCanvas.grid(column=0, row=6, sticky='W', columnspan=2)
ttk.Label(drCanvas, text='    ').grid(row=0, column = 0)
ttk.Label(drCanvas, text='Start Date:').grid(row=0, column = 1)
calStart = DateEntry(drCanvas, width= 16, background= "blue", foreground= "white", bd=2)
calStart.grid(row=0, column=2, padx=8)
calStart.configure(state='disabled')
ttk.Label(drCanvas, text='End Date:').grid(row=0, column = 3)
calEnd = DateEntry(drCanvas, width= 16, background= "blue", foreground= "white", bd=2)
calEnd.grid(row=0, column=4, padx=8)
calEnd.configure(state='disabled')

#ttk.Label(mainframe, text='Select inputs under the desired process and press the “Run Process” button.').grid(row=6,column=0, columnspan=3, sticky="w")

ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=7,column=0, columnspan=5, sticky="ew")
#ttk.Label(mainframe, text=step0).grid(row=6,column=1, columnspan=1, sticky="w")
ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=13,column=0, columnspan=5, sticky="ew")
ttk.Label(mainframe, text=step1).grid(row=14,column=0, columnspan=1, sticky="w")
ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=25,column=0, columnspan=5, sticky="ew")
ttk.Label(mainframe, text=step2).grid(row=26,column=0, columnspan=1, sticky="w")
ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=31,column=0, columnspan=5, sticky="ew")
ttk.Label(mainframe, text=step3).grid(row=32,column=0, columnspan=1, sticky="w")
ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=34,column=0, columnspan=5, sticky="ew")
ttk.Label(mainframe, text=step4).grid(row=35,column=0, columnspan=1, sticky="w")

ttk.Label(mainframe, text=' Enter TMC Codes (separate by comma)').grid(row=38, column=0, columnspan=1, sticky="w")
ttk.Label(mainframe, text='          ').grid(row=39, column=0, columnspan=1, sticky="w")

##################################################

# 2. Combobox
# List of States Combobox
list_states = ['','AL','AZ','AR','CA','CO','CT','DE','DC','FL','GA','ID','IL','IN','IA','KS','KY','LA',
               'ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']
StateValue = StringVar()
w_state = ttk.Combobox(mainframe, textvariable=StateValue, state='readonly', width=30)
w_state['values'] = list_states
w_state.current(0)
w_state.grid(column=1, row=2, columnspan=1, sticky="w")

# outputFile
w_output_folder = ttk.Button(mainframe, text='Select Output Folder Location', command=f_output)
w_output_folder.grid(column=0, row=0, columnspan=1, sticky="w")

### checkbox for process 0

preprocess_checkvar = IntVar()
preprocess_checkvar.set(False)
preprocess_tmas_checkbox = ttk.Checkbutton(mainframe, text=step0, variable=preprocess_checkvar, command=enable_tmas_preprocess)
preprocess_tmas_checkbox.grid(row=8,column=0, columnspan=1, sticky="w")

########
# script 0
p0Canvas = tk.Canvas(mainframe)
p0Canvas.grid(column=1, row=8, sticky='W')
p0startButton = ttk.Button(p0Canvas, text="Run Process 0", command=lambda: ProcessData(step0))
p0startButton.grid(column=0, row=0, columnspan=1 ,sticky="W", padx=(0, 5))
p0cancelButton = ttk.Button(p0Canvas, text="Cancel Process 0", command=CancelProcess)
p0cancelButton.grid(column=1, row=0, columnspan=1, padx=(5, 5), sticky="W")
p0statusLabel = ttk.Label(p0Canvas, text="Process 0 Running", relief=SUNKEN)
p0statusLabel.grid(column=2, row=0, columnspan=1, padx=(5, 0), sticky="W")
p0statusLabel.grid_remove()

w_tmas_station = ttk.Button(mainframe, text='Select TMAS Station File', command=f_tmas_station)
w_tmas_station.grid(column=0, row=9, columnspan=1, sticky="w")
w_tmas_class = ttk.Button(mainframe, text='Select TMAS Class File', command=f_tmas_class)
w_tmas_class.grid(column=0, row=10, columnspan=1, sticky="w")
w_fips_1 = ttk.Button(mainframe, text='Select FIPS File', command=f_fips)
w_fips_1.grid(column=0, row=11, columnspan=1, sticky="w")
w_nei_1 = ttk.Button(mainframe, text='Select NEI Representative Counties', command=f_nei)
w_nei_1.grid(column=0, row=12, columnspan=1, sticky="w")

# script 1
p1Canvas = tk.Canvas(mainframe)
p1Canvas.grid(column=1, row=14, sticky='W')
p1startButton = ttk.Button(p1Canvas, text="Run Process 1", command=lambda: ProcessData(step1))
p1startButton.grid(column=0, row=0, columnspan=1 ,sticky="W", padx=(0, 5))
p1cancelButton = ttk.Button(p1Canvas, text="Cancel Process 1", command=CancelProcess)
p1cancelButton.grid(column=1, row=0, columnspan=1, padx=(5, 5), sticky="W")
p1statusLabel = ttk.Label(p1Canvas, text="Process 1 Running", relief=SUNKEN)
p1statusLabel.grid(column=2, row=0, columnspan=1, padx=(5, 0), sticky="W")
p1statusLabel.grid_remove()

w_tmas_station_state_1 = ttk.Button(mainframe, text='Select Processed TMAS Station', command=f_tmas_station_state).grid(column=0, row=15, columnspan=1, sticky="w")
w_tmas_class_clean_1 = ttk.Button(mainframe, text='Select Processed TMAS Class', command=f_tmas_class_clean).grid(column=0, row=16, columnspan=1, sticky="w")
w_npmrds_all = ttk.Button(mainframe, text='Select NPMRDS (All)', command=f_npmrds_all).grid(column=0, row=17, columnspan=1, sticky="w")
w_npmrds_pass = ttk.Button(mainframe, text='Select NPMRDS (Passenger)', command=f_npmrds_pass).grid(column=0, row=18, columnspan=1, sticky="w")
w_npmrds_truck = ttk.Button(mainframe, text='Select NPMRDS (Truck)', command=f_npmrds_truck).grid(column=0, row=19, columnspan=1, sticky="w")
w_npmrds_tmc = ttk.Button(mainframe, text='Select TMC Configuration', command=f_npmrds_tmc).grid(column=0, row=20, columnspan=1, sticky="w")
#w_npmrds_shp = ttk.Button(mainframe, text='Select TMC shapefile', command=f_npmrds_shp).grid(column=0, row=21, columnspan=1, sticky="w")
w_emission = ttk.Button(mainframe, text='Select Emission Rates', command=f_emission).grid(column=0, row=21, columnspan=1, sticky="w")
w_fips_2 = ttk.Button(mainframe, text='Select FIPS File', command=f_fips).grid(column=0, row=22, columnspan=1, sticky="w")
w_nei_2 = ttk.Button(mainframe, text='Select NEI Representative Counties', command=f_nei).grid(column=0, row=23, columnspan=1, sticky="w")

# script 2
#w_tmas_station_state_2 = ttk.Button(mainframe, text='Select Processed TMAS Station', command=f_tmas_station_state).grid(column=0, row=19, columnspan=1, sticky="w")
p2Canvas = tk.Canvas(mainframe)
p2Canvas.grid(column=1, row=26, sticky='W')
p2startButton = ttk.Button(p2Canvas, text="Run Process 2", command=lambda: ProcessData(step2))
p2startButton.grid(column=0, row=0, columnspan=1 ,sticky="W", padx=(0, 5))
p2cancelButton = ttk.Button(p2Canvas, text="Cancel Process 2", command=CancelProcess)
p2cancelButton.grid(column=1, row=0, columnspan=1, padx=(5, 5), sticky="W")
p2statusLabel = ttk.Label(p2Canvas, text="Process 2 Running", relief=SUNKEN)
p2statusLabel.grid(column=2, row=0, columnspan=1, padx=(5, 0), sticky="W")
p2statusLabel.grid_remove()

w_npmrds_clean_1 = ttk.Button(mainframe, text='Select Processed NPMRDS', command=f_npmrds_clean).grid(column=0, row=27, columnspan=1, sticky="w")
w_hpms = ttk.Button(mainframe, text='Select HPMS', command=f_hpms).grid(column=0, row=28, columnspan=1, sticky="w")
w_vm2 = ttk.Button(mainframe, text='Select Road Class Mileage', command=f_vm2).grid(column=0, row=29, columnspan=1, sticky="w")
w_county_mileage = ttk.Button(mainframe, text='Select County Mileage file', command=f_county_mileage).grid(column=0, row=30, columnspan=1, sticky="w")

# script 3
p3Canvas = tk.Canvas(mainframe)
p3Canvas.grid(column=1, row=32, sticky='W')
p3startButton = ttk.Button(p3Canvas, text="Run Process 3", command=lambda: ProcessData(step3))
p3startButton.grid(column=0, row=0, columnspan=1 ,sticky="W", padx=(0, 5))
p3cancelButton = ttk.Button(p3Canvas, text="Cancel Process 3", command=CancelProcess)
p3cancelButton.grid(column=1, row=0, columnspan=1, padx=(5, 5), sticky="W")
p3statusLabel = ttk.Label(p3Canvas, text="Process 3 Running", relief=SUNKEN)
p3statusLabel.grid(column=2, row=0, columnspan=1, padx=(5, 0), sticky="W")
p3statusLabel.grid_remove()

w_npmrds_clean_2 = ttk.Button(mainframe, text='Select Processed NPMRDS', command=f_npmrds_clean).grid(column=0, row=33, columnspan=1, sticky="w")

# script 4
p4Canvas = tk.Canvas(mainframe)
p4Canvas.grid(column=1, row=35, sticky='W')
p4startButton = ttk.Button(p4Canvas, text="Run Process 4", command=lambda: ProcessData(step4))
p4startButton.grid(column=0, row=0, columnspan=1 ,sticky="W", padx=(0, 5))
p4cancelButton = ttk.Button(p4Canvas, text="Cancel Process 4", command=CancelProcess)
p4cancelButton.grid(column=1, row=0, columnspan=1, padx=(5, 5), sticky="W")
p4statusLabel = ttk.Label(p4Canvas, text="Process 4 Running", relief=SUNKEN)
p4statusLabel.grid(column=2, row=0, columnspan=1, padx=(5, 0), sticky="W")
p4statusLabel.grid_remove()

tmc_selection_button = ttk.Button(mainframe, text = 'TMC Selection Tool', command=lambda: notebook.select('.!notebook.!frame3')).grid(column=0, row=36, columnspan=1, sticky="w")
w_npmrds_clean_3 = ttk.Button(mainframe, text='Select Processed NPMRDS', command=f_npmrds_clean).grid(column=0, row=37, columnspan=1, sticky="w")
# Entry
tmcEntry = StringVar()
ttk.Entry(mainframe, textvariable=tmcEntry).grid(column=1, row=38, columnspan=1, sticky="ew")
##################################################

# 4. Pathlabels
# output folder
pl_output_folder = ttk.Label(mainframe)
pl_output_folder.grid(column=1, row=0, columnspan=1, sticky="w")
# script 0
pl_tmas_station = ttk.Label(mainframe)
pl_tmas_station.grid(column=1, row=9, columnspan=1, sticky="w")
pl_tmas_class = ttk.Label(mainframe)
pl_tmas_class.grid(column=1, row=10, columnspan=1, sticky="w")
pl_fips_1 = ttk.Label(mainframe)
pl_fips_1.grid(column=1, row=11, columnspan=1, sticky="w")
pl_nei_1 = ttk.Label(mainframe)
pl_nei_1.grid(column=1, row=12, columnspan=1, sticky="w")
# script 1
pl_tmas_station_state_1 = ttk.Label(mainframe)
pl_tmas_station_state_1.grid(column=1, row=15, columnspan=1, sticky="w")
pl_tmas_class_clean_1 = ttk.Label(mainframe)
pl_tmas_class_clean_1.grid(column=1, row=16, columnspan=1, sticky="w")
pl_npmrds_all = ttk.Label(mainframe)
pl_npmrds_all.grid(column=1, row=17, columnspan=1, sticky="w")
pl_npmrds_pass = ttk.Label(mainframe)
pl_npmrds_pass.grid(column=1, row=18, columnspan=1, sticky="w")
pl_npmrds_truck = ttk.Label(mainframe)
pl_npmrds_truck.grid(column=1, row=19, columnspan=1, sticky="w")
pl_npmrds_tmc = ttk.Label(mainframe)
pl_npmrds_tmc.grid(column=1, row=20, columnspan=1, sticky="w")
#pl_npmrds_shp = ttk.Label(mainframe)
#pl_npmrds_shp.grid(column=1, row=19, columnspan=1, sticky="w")
pl_emission = ttk.Label(mainframe)
pl_emission.grid(column=1, row=21, columnspan=1, sticky="w")
pl_fips_2 = ttk.Label(mainframe)
pl_fips_2.grid(column=1, row=22, columnspan=1, sticky="w")
pl_nei_2 = ttk.Label(mainframe)
pl_nei_2.grid(column=1, row=23, columnspan=1, sticky="w")
# script 2
#pl_tmas_station_state_2 = ttk.Label(mainframe)
#pl_tmas_station_state_2.grid(column=1, row=19, columnspan=1, sticky="w")
pl_npmrds_clean_1 = ttk.Label(mainframe)
pl_npmrds_clean_1.grid(column=1, row=27, columnspan=1, sticky="w")
pl_hpms = ttk.Label(mainframe)
pl_hpms.grid(column=1, row=28, columnspan=1, sticky="w")
pl_vm2 = ttk.Label(mainframe)
pl_vm2.grid(column=1, row=29, columnspan=1, sticky="w")
pl_county_mileage = ttk.Label(mainframe)
pl_county_mileage.grid(column=1, row=30, columnspan=1, sticky="w")
# script 3
pl_npmrds_clean_2 = ttk.Label(mainframe)
pl_npmrds_clean_2.grid(column=1, row=33, columnspan=1, sticky="w")
# script 4
pl_npmrds_clean_3 = ttk.Label(mainframe)
pl_npmrds_clean_3.grid(column=1, row=37, columnspan=1, sticky="w")
##################################################

# 5. Check available pre-processed files
# TMAS station
pl_tmas_station_state_1.config(text='')
#pl_tmas_station_state_2.config(text='')

# TMAS Class
pl_tmas_class_clean_1.config(text='')

defaultpath = 'Default Input Files/'
pathlib.Path(defaultpath).mkdir(exist_ok=True) 
outputpath = fn_output
pathlib.Path(outputpath).mkdir(exist_ok=True) 

# FIPS
if ('FIPS_County_Codes.csv' in os.listdir('Default Input Files/')):
    pl_fips_1.config(text=os.getcwd()+'\\Default Input Files\\FIPS_County_Codes.csv')
    pl_fips_2.config(text=os.getcwd()+'\\Default Input Files\\FIPS_County_Codes.csv')
    fn_fips = 'Default Input Files/FIPS_County_Codes.csv'
else:
    pl_fips_1.config(text='')
    pl_fips_2.config(text='')
    
# NEI
if ('NEI2017_RepresentativeCounties.csv' in os.listdir('Default Input Files/')):
    pl_nei_1.config(text=os.getcwd()+'\\Default Input Files\\NEI2017_RepresentativeCounties.csv')
    pl_nei_2.config(text=os.getcwd()+'\\Default Input Files\\NEI2017_RepresentativeCounties.csv')
    fn_nei = 'Default Input Files/NEI2017_RepresentativeCounties.csv'
else:
    pl_nei_1.config(text='')
    pl_nei_2.config(text='')

# Emission Rates
if ('NEI2017_RepresentativeEmissionsRates.parquet' in os.listdir('Default Input Files/')):
    pl_emission.config(text=os.getcwd()+'\\Default Input Files\\NEI2017_RepresentativeEmissionsRates.parquet')
    fn_emission = 'Default Input Files/NEI2017_RepresentativeEmissionsRates.parquet'
else:
    pl_emission.config(text='')
       
if True:
    w_state.current(45)
    fn_tmas_station = 'C:/Users/William.Chupp/OneDrive - DOT OST/Documents/DANAToolTesting/FHWA-DANATool/Default Input Files/TMAS Data/TMAS 2019/TMAS_Station_2019.csv'
    pl_tmas_station_state_1.config(text=fn_tmas_station.replace('/','\\'))
    fn_tmas_class_clean = 'C:/Users/William.Chupp/OneDrive - DOT OST/Documents/DANAToolTesting/FHWA-DANATool/Default Input Files/TMAS Data/TMAS 2019/TMAS_Class_Clean_2019.csv'
    pl_tmas_class_clean_1.config(text=fn_tmas_class_clean.replace('/','\\'))
    fn_npmrds_all = 'H:/TestData/FairfaxCity_VA/NPMRDS Data/VA_FairfaxCity_2021_ALL.csv'
    pl_npmrds_all.config(text=fn_npmrds_all.replace('/','\\'))
    fn_npmrds_pass = 'H:/TestData/FairfaxCity_VA/NPMRDS Data/VA_FairfaxCity_2021_PASSENGER.csv'
    pl_npmrds_pass.config(text=fn_npmrds_pass.replace('/','\\'))
    fn_npmrds_truck = 'H:/TestData/FairfaxCity_VA/NPMRDS Data/VA_FairfaxCity_2021_TRUCKS.csv'
    pl_npmrds_truck.config(text=fn_npmrds_truck.replace('/','\\'))
    fn_npmrds_tmc = 'H:/TestData/FairfaxCity_VA/NPMRDS Data/TMC_Identification.csv'
    pl_npmrds_tmc.config(text=fn_npmrds_tmc.replace('/','\\'))
    fn_output = 'C:/Users/William.Chupp/OneDrive - DOT OST/Documents/DANAToolTesting/RunOutputs/TestingRun1_20220302'
    pl_output_folder.config(text=fn_output.replace('/','\\'))

    #fn_npmrds_shp = 'C:/Users/William.Chupp/Documents/DANAToolTesting/FHWA-DANATool/Default Input Files/National TMC Shapefile/NationalMerge.shp'
    #pl_npmrds_shp.config(text=fn_npmrds_shp.replace('/','\\'))

##################################################

# 6. Button
#ttk.Button(mainframe, text="Process Data", command=ProcessData).grid(column=0, row=35, columnspan=4)

##################################################
# pad each widget globally
for child in mainframe.winfo_children(): child.grid_configure(padx=2, pady=4)


enable_tmas_preprocess()

if __name__ == "__main__":
    mp.freeze_support()  
    thread_queue = mp.Queue()
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    redir = RedirectText(thread_queue)
    sys.stdout = redir
    sys.stderr = redir
    root.update_idletasks()
    bind_tree(main_container, "<MouseWheel>", main_mouse_wheel)
    bind_tree(TMCSelect_container, "<MouseWheel>", tmcselect_mouse_wheel)
    canvasScrollRegion = (0, 0, 
                          max(canvas.winfo_width()-4, canvas.bbox('all')[2]),
                          max(canvas.winfo_height()-4, canvas.bbox('all')[3]))
    canvas.configure(scrollregion=canvasScrollRegion)
    TMCcanvasScrollRegion = (0, 0, 
                          max(TMCSelect_canvas.winfo_width()-4, TMCSelect_canvas.bbox('all')[2]-1),
                          max(TMCSelect_canvas.winfo_height()-4, TMCSelect_canvas.bbox('all')[3]-1))
    runningThreads = []
    checkProgress()
    root.mainloop()
    sys.stdout = old_stdout
    for afterid in afterids:
        root.after_cancel(afterid)
