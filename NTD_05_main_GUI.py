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

versionNum = "2.1.1"

import pandas as pd
import numpy as np
import geopandas as gpd
from geopandas import GeoDataFrame
from shapely.geometry import Polygon, LineString, Point
from shapely import get_type_id
import os
import io
import time
import datetime as dt
import pathlib
import tkinter as tk
from tkinter import *
from tkinter import Tk,ttk,StringVar,filedialog,font
from ttkwidgets import Table
import tkintermapview
import re
import multiprocessing as mp
from tqdm.tk import tqdm
from tkcalendar import Calendar, DateEntry
import xml.etree.ElementTree as ET
import tkinter.font as tkFont
from matplotlib.figure import Figure
import pkg_resources
from matplotlib.backends.backend_tkagg import (FigureCanvasTkAgg, NavigationToolbar2Tk)
import importlib

from lib import NTD_00_TMAS
from lib import NTD_01_NPMRDS
from lib import NTD_02_MOVES
from lib import NTD_03_SPEED
from lib import call_TNMAide
from lib import load_shapes

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

# Function for converting string percent to float
def p2f(x):
    if x == "":
        return 0.0
    return float(x.strip('%'))/100

#Function for creating path to the icon        
def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    base_path = getattr(sys, '_MEIPASS', os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)

# Func - Popups
def PopUpCleanTMASSelection():
    popup = tk.Toplevel()
    popup.wm_title("Warning")
    label = ttk.Label(popup, text="Please chose a processed TMAS File.")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop()    
def PopUpCleanNPMRDSSelection():
    popup = tk.Toplevel()
    popup.wm_title("Warning")
    label = ttk.Label(popup, text="Please run Step 1 to create processed NPMRDS File.")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop()
def PopUp_Selection(txt_input):
    popup = tk.Toplevel()
    popup.wm_title("Warning")
    label = ttk.Label(popup, text="No "+txt_input+" selected, please select again.")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop()   
def PopUpMsg(txt_input):
    popup = tk.Toplevel()
    popup.wm_title("Warning")
    label = ttk.Label(popup, text=txt_input)
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop()

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

fn_tmc_config = ''

crs = 'epsg:4326'
PATH_tmc_shp = pkg_resources.resource_filename('lib', 'ShapeFilesCSV/')
usashp = load_shapes.load_shape_csv(PATH_tmc_shp, crs)

thread_queue = mp.Queue()
tnmaide_queue = mp.Queue()
tnmaide_result = None
tnmaide_group = None
afterids = []

# Func - File dialogs    
def f_output():
    global fn_output
    fn_output = filedialog.askdirectory(parent=root, title='Choose Output Folder', initialdir='/',)
    pl_output_folder.config(text=fn_output.replace('/','\\'))
    pathlib.Path(fn_output).mkdir(exist_ok=True)
    with open(fn_output + '/progress_log.txt', 'a') as file:
        file.write('\n\n*********** New DANA TOOL Log ************')
        file.write('\n******** Version Number: {} ********'.format(versionNum))
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
def f_default_speed():
    global fn_defualt_speed
    fn_defualt_speed = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose National Defualt Speed File',
        filetypes=[('csv file', '.csv')])
    pl_default_speed.config(text=fn_defualt_speed.replace('/','\\'))
def f_npmrds_tmc():
    global fn_npmrds_tmc
    fn_npmrds_tmc = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose NPMRDS TMC Configuration',
        filetypes=[('csv file', '.csv')])
    pl_npmrds_tmc.config(text=fn_npmrds_tmc.replace('/','\\'))
# def f_npmrds_shp():
#     global fn_npmrds_shp
#     fn_npmrds_shp = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose NPMRDS TMC shapefile',
#         filetypes=[('shapefile file', '.shp')])
#     pl_npmrds_shp.config(text=fn_npmrds_shp.replace('/','\\'))
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
        line = thread_queue.get(block=False)
        output_text.insert(tk.END, line)
        if fn_output != '':
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
    tnmaide_canvasScrollRegion = (0, 0, 
                          max(tnmaide_canvas.winfo_width()-4, tnmaide_canvas.bbox('all')[2]-1),
                          max(tnmaide_canvas.winfo_height()-4, tnmaide_canvas.bbox('all')[3]-1))
    tnmaide_canvas.configure(scrollregion=tnmaide_canvasScrollRegion)
    vis_canvasScrollRegion = (0, 0, 
                          max(vis_canvas.winfo_width()-4, vis_canvas.bbox('all')[2]-1),
                          max(vis_canvas.winfo_height()-4, vis_canvas.bbox('all')[3]-1))
    vis_canvas.configure(scrollregion=vis_canvasScrollRegion)
    TNMAide_statusLabel.grid_remove()
    #There will only every be one process running at a time, but we look at all
    #running threads to make sure it is working cleanly
    if fn_output != '':
        if os.path.exists(fn_output+'/Process1_linkLevelDataset') and f'{StateValue.get()}_Composite_Emissions.parquet' in os.listdir(fn_output+'/Process1_linkLevelDataset'):
            fn_npmrds_clean = fn_output + '/Process1_LinkLevelDataset/' + f'{StateValue.get()}_Composite_Emissions.parquet'
            pl_npmrds_clean_1.config(text=fn_npmrds_clean.replace('/','\\'))
            pl_npmrds_clean_2.config(text=fn_npmrds_clean.replace('/','\\'))
            pl_npmrds_clean_3.config(text=fn_npmrds_clean.replace('/','\\'))
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
    
    #Update TNMAide Total Percents
    sumLDNInputs = 0
    for r in ldnInTable:
        for c in r:
            sumLDNInputs = sumLDNInputs + p2f(c.get())
    
    totLDNInput.set(f"{round(100*sumLDNInputs, 2)} %")

    sumLDENInputs = 0
    for r in ldenInTable:
        for c in r:
            sumLDENInputs = sumLDENInputs + p2f(c.get())
    
    totLDENInput.set(f"{round(100*sumLDENInputs, 2)} %")

    global afterids
    afterids.append(root.after(100, checkProgress))
        

def process_handler(proc_target, thread_queue, args, return_queue = None):
    redir = RedirectText(thread_queue)
    sys.stdout = redir
    sys.stderr = sys.stdout
    if return_queue is None:
        try:
            proc_target(*args)
        except Exception as e:
            print(traceback.format_exc())
    else:
        try:
            result = proc_target(*args)
            return_queue.put(result)
        except Exception as e:
            print(traceback.format_exc())          
    
# Func - ProcessData Function
def ProcessData(procNum):
    global runningThreads
    #output_text.delete('1.0', END)
    tmc_chars = set('0123456789+-PN, ')
    
    if procNum != 'tnmaide':
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
            PATH_default_speeds = fn_defualt_speed
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
                                    (SELECT_STATE, PATH_tmc_identification, PATH_npmrds_raw_all, PATH_npmrds_raw_pass, PATH_npmrds_raw_truck, PATH_default_speeds,
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
                                                                              (SELECT_STATE, PATH_NPMRDS, PATH_HPMS, PATH_VM2, PATH_COUNTY_MILEAGE, PATH_OUTPUT)))
            disable_buttons(step2)
            MOVES_Proc.start()
            runningThreads.append(MOVES_Proc)
            notebook.select('.!notebook.!frame2')
            
    elif procNum == step3:
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
                                                                              (SELECT_STATE, PATH_NPMRDS, PATH_OUTPUT)))
            disable_buttons(step3)
            SPEED_Proc.start()
            runningThreads.append(SPEED_Proc)
            notebook.select('.!notebook.!frame2')
    
    elif procNum == 'tnmaide':
        if fn_npmrds_clean == '':
            PopUpCleanNPMRDSSelection()
        else:
            TNMAIDE_Proc = mp.Process(target=process_handler, name='tnmaide', 
                args = (call_TNMAide.get_TNMPyAide_inputs, thread_queue, 
                            (fn_npmrds_clean, TMC_1Entry.get(), TMC_2Entry.get()),
                            tnmaide_queue)
                    )
            disable_buttons('tnmaide')
            TNMAIDE_Proc.start()
            runningThreads.append(TNMAIDE_Proc)
            notebook.select('.!notebook.!frame2')

    else:
        PopUp_Selection("Step")
    
    #root.destroy()

def disable_buttons(step):
    w_output_folder['state'] = DISABLED
    
    autoDetectDatesBox['state'] = DISABLED
    SelectRangeBox['state'] = DISABLED
    
    w_state['state'] = DISABLED
    
    p0startButton['state'] = DISABLED
    p1startButton['state'] = DISABLED
    p2startButton['state'] = DISABLED
    p3startButton['state'] = DISABLED
    calcButton["state"] = DISABLED


    p0cancelButton["state"] = DISABLED
    p1cancelButton["state"] = DISABLED
    p2cancelButton["state"] = DISABLED
    p3cancelButton["state"] = DISABLED
    TNMAIDE_cancelButton["state"] = DISABLED

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
    elif step == "tnmaide":
        TNMAIDE_cancelButton["state"] = NORMAL
        TNMAide_statusLabel.grid()

def enable_buttons():
    
    w_output_folder['state'] = NORMAL
    
    autoDetectDatesBox['state'] = NORMAL
    SelectRangeBox['state'] = NORMAL
    
    w_state['state'] = NORMAL
    
    p0startButton['state'] = NORMAL
    p1startButton['state'] = NORMAL
    p2startButton['state'] = NORMAL
    p3startButton['state'] = NORMAL
    calcButton['state'] = NORMAL

    p0cancelButton["state"] = NORMAL
    p1cancelButton["state"] = NORMAL
    p2cancelButton["state"] = NORMAL
    p3cancelButton["state"] = NORMAL
    TNMAIDE_cancelButton["state"] = NORMAL
     
    p0statusLabel.grid_remove()
    p1statusLabel.grid_remove()
    p2statusLabel.grid_remove()
    p3statusLabel.grid_remove()
    TNMAide_statusLabel.grid_remove()
    
def CancelProcess():
    global runningThreads
    for proc in runningThreads:
        while proc.is_alive():
            proc.terminate()
        
        if not proc.is_alive():
            print("************* Canceled Step {} **************".format(proc.name))

def fitLabel(event): # not used for now
    label = event.widget
    print(tk.Label())
    if not hasattr(label, "original_text"):
        # preserve the original text so we can restore
        # it if the widget grows.
        label.original_text = label.cget("text")

    font = tkFont.nametofont(label.cget("font"))
    text = label.original_text
    max_width = event.width
    actual_width = font.measure(text)
    if actual_width <= max_width:
        # the original text fits; no need to add ellipsis
        label.configure(text=text)
    else:
        # the original text won't fit. Keep shrinking
        # until it does
        while actual_width > max_width and len(text) > 1:
            text = text[:-1]
            actual_width = font.measure(text + "...")
        label.configure(text=text+"...")

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
    global fn_tmc_config
    fn_tmc_config = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose TMC Config File',
        filetypes=[('csv file', '.csv')])
    if fn_tmc_config == '':
        return
    pl_tmc_config.config(text=fn_tmc_config.replace('/','\\'))
    try:
        proc_tmc_file()
    except Exception as e:
        print(traceback.format_exc())            
def proc_tmc_file():
    global tmc, geo_tmc, counties, roads, directions, kmlfiles
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

    countySelect.config(values=counties)
    road.config(values=roads)
    direction.config(values=directions)
    
def f_kml():
    global fn_kml
    fn_kml = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose KML File',
        filetypes=[('kml file', '.kml')])
    pl_kml.config(text=fn_kml.replace('/','\\'))
    if fn_kml == '':
        return
    pl_kml.config(text=fn_kml.replace('/','\\'))
    try:
        ReadPolygon()
    except Exception as e:
        print(traceback.format_exc())      

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
    res = tk.messagebox.askokcancel(title="Warning", message="No selection criteria were defined, all of State TMC links will be used. Press 'OK' to print all TMC ids to the output file. Press 'Cancel' to select more specific options.")

    return res

def PopUpWrongSelection():
    popup = tk.Toplevel()
    popup.wm_title("Error")
    label = ttk.Label(popup, text="No links were found with the selection parameters. Please modify the selection parameters")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop()

def PopUpNoGeoData():
    popup = tk.Toplevel()
    popup.wm_title("Error")
    label = ttk.Label(popup, text="There is no geographic data for the current selection parameters. \n"
                                  "You can still select TMC values, but you cannot view the links on a map.")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop()
    
def PopUpNoFile():
    popup = tk.Toplevel()
    popup.wm_title("Error")
    label = ttk.Label(popup, text="No input file chosen. Please choose a TMC or KML configuration file first.")
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop()
    
def PopUpCompletedSelection(outputpath, filename):
    popup = tk.Toplevel()
    popup.wm_title("TMC Selection Complete")
    head = ttk.Label(popup, text='TMC Selection Completed'.format(outputpath+filename))
    head.pack(side="top", pady=5, padx=5)
    label = ttk.Label(popup, text='TMC Selection Results saved to {}'.format(outputpath+filename))
    label.pack(side="top", pady=5, padx=5)
    popup.mainloop()

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
    if selected_tmc is None or len(selected_tmc)==0:
        PopUpWrongSelection()
    elif len(selected_tmc)==len(tmc['tmc']):
        if PopUpNoSelection():
            ClearData()
            for i in selected_tmc:
                TMCoutput_text.insert(tk.END, i+', ')
            PrintResults(selected_tmc, CountyValue.get(), RoadValue.get(), DirectionValue.get())
        else:
            pass
    else:
        ClearData()
        for i in selected_tmc:
            TMCoutput_text.insert(tk.END, i+', ')
        PrintResults(selected_tmc, CountyValue.get(), RoadValue.get(), DirectionValue.get())

def PrintSelectData():
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
        print(selected_tmc)
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
    if selected_tmc is None or len(selected_tmc)==0:
        PopUpWrongSelection()
    elif len(selected_tmc)==len(tmc['tmc']):
        if PopUpNoSelection():
            ClearData()
            for i in selected_tmc:
                TMCoutput_text.insert(tk.END, i+', ')
        else:
            pass
    else:
        ClearData()
        for i in selected_tmc:
            TMCoutput_text.insert(tk.END, i+', ')

def ClearData():
    TMCoutput_text.delete('@0,0', tk.END)

def ClearFilters():
    global fn_kml
    fn_kml = ''
    pl_kml.config(text='')
    CountyValue.set('')
    RoadValue.set('')
    DirectionValue.set('')

def updateMapSize(popup):
    w = popup.winfo_width
    h = popup.winfo_height

def MapTMCs():    
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
    if selected_tmc is None or len(selected_tmc)==0:
        PopUpWrongSelection()
        return
    else:
        selectedGeoTMC = usashp.loc[usashp['Tmc'].isin(selected_tmc)]
        crs = {'init' :'epsg:4326'}
        selectedGeoTMC = gpd.GeoDataFrame(selectedGeoTMC, geometry='geometry', crs=crs)
    if selectedGeoTMC is None or len(selectedGeoTMC)==0:
        PopUpNoGeoData()
        return
        
    ClearData()
    for i in selected_tmc:
        TMCoutput_text.insert(tk.END, i+', ')
    mappopup=tk.Toplevel(root)
    mappopup.wm_title("Selected TMCs")
    iconPath = resource_path('lib\\dot.png')
    p1 = tk.PhotoImage(file = iconPath)
    mappopup.iconphoto(False, p1)
    mappopup.wm_resizable(width=False, height=False)

    global map_widget
    map_widget = tkintermapview.TkinterMapView(mappopup, width=1000, height=750, corner_radius=0)
    map_widget.grid(column=0, row=0, columnspan=1,rowspan=1, sticky="news")
    tmc_paths = []
    for t, g in zip(selectedGeoTMC.Tmc, selectedGeoTMC.geometry):
        if get_type_id(g) == 1:
            path = map_widget.set_path([(c[1], c[0]) for c in list(g.coords)], data={'tmc': t, 'clicked':False})
        if get_type_id(g) == 5:
            pathList = []
            for geom in g.geoms:
                for c in list(geom.coords):
                    pathList.append((c[1], c[0]))
            path = map_widget.set_path(pathList, data={'tmc': t, 'clicked':False})
        path.map_widget.canvas.tag_bind(path.canvas_line, "<Enter>", path.mouse_enter)
        path.map_widget.canvas.tag_bind(path.canvas_line, "<Leave>", path.mouse_leave)
        path.map_widget.canvas.tag_bind(path.canvas_line, "<Button-1>", lambda e: path_clicked(e, e.widget.find_withtag('current')[0], tmc_paths))
        tmc_paths.append(path)
    center = selectedGeoTMC.dissolve().centroid[0]
    map_widget.set_position(center.y, center.x)
    bounds = selectedGeoTMC.total_bounds
    map_widget.fit_bounding_box((bounds[3], bounds[0]), (bounds[1], bounds[2]))
    mapButton['state'] = DISABLED
    root.wait_window(mappopup)
    mapButton['state'] = NORMAL

def path_clicked(event, pathnum, tmc_paths):
    for path in tmc_paths:
        if pathnum==path.canvas_line and not path.data['clicked']:
            markerPoint = map_widget.convert_canvas_coords_to_decimal_coords(event.x, event.y)
            pathMarker = map_widget.set_marker(markerPoint[0], markerPoint[1], text=path.data['tmc'])
            map_widget.canvas.itemconfigure(path.canvas_line, fill="red")
            path.data['marker'] = pathMarker
            path.data['clicked'] = True
        elif pathnum==path.canvas_line and path.data['clicked']:
            map_widget.canvas.itemconfigure(path.canvas_line, fill="#3E69CB")
            path.data['marker'].delete()
            path.data['clicked'] = False

def calc_tnmaide(start=False):
    global tnmaide_result
    global tnmaide_group
    if start:
        ProcessData('tnmaide')
    if tnmaide_queue.empty():
        global afterids
        afterids.append(root.after(100, lambda: calc_tnmaide(False)))
    else:
        tnmaide_group = tnmaide_queue.get(block=False)
        tnmaide_group.columns = tnmaide_group.columns.str.upper()
        tnmaide_result = call_TNMAide.call_TNMAide(tnmaide_group.copy(), (roadgrade1.get(), roadgrade2.get()), medwidth.get(), (NumLanes1.get(), NumLanes2.get()))
        LAeqOutput.set(str(round(tnmaide_result.average_day.LAEQ_24_HR, 2)))
        LdnOutput.set(str(round(tnmaide_result.average_day.LDN, 2)))
        LdenOutput.set(str(round(tnmaide_result.average_day.LDEN, 2)))
        WorstHourOut.set(str(tnmaide_result.average_day.worst_hour))
        WorstDayOut.set(str(tnmaide_result.worst_day.day))

        CurrentAADTOut.set(str(sum(tnmaide_group.AADT.unique())))
        # Average day worst hour traffic 
        
        wh_data = tnmaide_result.df_Traffic_Noise.loc[tnmaide_result.average_day.worst_hour_idx]
        whOutTable[0][0].set(f"{round((wh_data['VOL_AT_L1'] + wh_data['VOL_AT_L2'])/2)}")
        whOutTable[0][1].set(f"{round((wh_data['VOL_MT_L1'] + wh_data['VOL_MT_L2'])/2)}")
        whOutTable[0][2].set(f"{round((wh_data['VOL_HT_L1'] + wh_data['VOL_HT_L2'])/2)}")
        whOutTable[0][3].set(f"{round((wh_data['VOL_BUS_L1'] + wh_data['VOL_BUS_L2'])/2)}")
        whOutTable[0][4].set(f"{round((wh_data['VOL_MC_L1'] + wh_data['VOL_MC_L2'])/2)}")
        whOutTable[1][0].set(f"{round((wh_data['SPD_AT_L1'] + wh_data['SPD_AT_L2'])/2)}")
        whOutTable[1][1].set(f"{round((wh_data['SPD_HT_L1'] + wh_data['SPD_HT_L2'])/2)}")
        whOutTable[1][2].set(f"{round((wh_data['SPD_HT_L1'] + wh_data['SPD_HT_L2'])/2)}")
        whOutTable[1][3].set(f"{round((wh_data['SPD_ALL_L1'] + wh_data['SPD_ALL_L2'])/2)}")
        whOutTable[1][4].set(f"{round((wh_data['SPD_ALL_L1'] + wh_data['SPD_ALL_L2'])/2)}")

        # Year Breakdown
        yrOutTable[0][0].set(f"{round(100*sum(list(tnmaide_group.PCT_NOISE_AUTO*tnmaide_group.MAADT))/(sum(tnmaide_group.MAADT)/24), 2)} %")        
        yrOutTable[0][1].set(f"{round(100*sum(list(tnmaide_group.PCT_NOISE_MED_TRUCK*tnmaide_group.MAADT))/(sum(tnmaide_group.MAADT)/24), 2)} %")
        yrOutTable[0][2].set(f"{round(100*sum(list(tnmaide_group.PCT_NOISE_HVY_TRUCK*tnmaide_group.MAADT))/(sum(tnmaide_group.MAADT)/24), 2)} %")
        yrOutTable[0][3].set(f"{round(100*sum(list(tnmaide_group.PCT_NOISE_BUS*tnmaide_group.MAADT))/(sum(tnmaide_group.MAADT)/24), 2)} %")
        yrOutTable[0][4].set(f"{round(100*sum(list(tnmaide_group.PCT_NOISE_MC*tnmaide_group.MAADT))/(sum(tnmaide_group.MAADT)/24), 2)} %")

        traffic_df = tnmaide_result.df_Traffic_Noise
        traffic_df['VOL_L1'] = traffic_df['VOL_AT_L1'] + traffic_df['VOL_MT_L1'] + traffic_df['VOL_HT_L1'] + traffic_df['VOL_BUS_L1'] + traffic_df['VOL_MC_L1']
        traffic_df['VOL_L2'] = traffic_df['VOL_AT_L2'] + traffic_df['VOL_MT_L2'] + traffic_df['VOL_HT_L2'] + traffic_df['VOL_BUS_L2'] + traffic_df['VOL_MC_L2']

        # LDN
        traffic_df_day = traffic_df[traffic_df['HOUR'].between(7, 21)]
        traffic_df_night = traffic_df[~traffic_df['HOUR'].between(7, 21)]

        ldnOutTable[0][0].set(f"{round(100*(traffic_df_day['VOL_AT_L1'].sum() + traffic_df_day['VOL_AT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldnOutTable[1][0].set(f"{round(100*(traffic_df_night['VOL_AT_L1'].sum() + traffic_df_night['VOL_AT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldnOutTable[0][1].set(f"{round(100*(traffic_df_day['VOL_MT_L1'].sum() + traffic_df_day['VOL_MT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldnOutTable[1][1].set(f"{round(100*(traffic_df_night['VOL_MT_L1'].sum() + traffic_df_night['VOL_MT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldnOutTable[0][2].set(f"{round(100*(traffic_df_day['VOL_HT_L1'].sum() + traffic_df_day['VOL_HT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldnOutTable[1][2].set(f"{round(100*(traffic_df_night['VOL_HT_L1'].sum() + traffic_df_night['VOL_HT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldnOutTable[0][3].set(f"{round(100*(traffic_df_day['VOL_BUS_L1'].sum() + traffic_df_day['VOL_BUS_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldnOutTable[1][3].set(f"{round(100*(traffic_df_night['VOL_BUS_L1'].sum() + traffic_df_night['VOL_BUS_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldnOutTable[0][4].set(f"{round(100*(traffic_df_day['VOL_MC_L1'].sum() + traffic_df_day['VOL_MC_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldnOutTable[1][4].set(f"{round(100*(traffic_df_night['VOL_MC_L1'].sum() + traffic_df_night['VOL_MC_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")

        del traffic_df_day
        del traffic_df_night

        # LDEN
        traffic_df_day = traffic_df[traffic_df['HOUR'].between(7, 18)]
        traffic_df_eve = traffic_df[traffic_df['HOUR'].between(19, 21)]
        traffic_df_night = traffic_df[~traffic_df['HOUR'].between(7, 21)]

        ldenOutTable[0][0].set(f"{round(100*(traffic_df_day['VOL_AT_L1'].sum() + traffic_df_day['VOL_AT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[1][0].set(f"{round(100*(traffic_df_eve['VOL_AT_L1'].sum() + traffic_df_eve['VOL_AT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[2][0].set(f"{round(100*(traffic_df_night['VOL_AT_L1'].sum() + traffic_df_night['VOL_AT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[0][1].set(f"{round(100*(traffic_df_day['VOL_MT_L1'].sum() + traffic_df_day['VOL_MT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[1][1].set(f"{round(100*(traffic_df_eve['VOL_MT_L1'].sum() + traffic_df_eve['VOL_MT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[2][1].set(f"{round(100*(traffic_df_night['VOL_MT_L1'].sum() + traffic_df_night['VOL_MT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[0][2].set(f"{round(100*(traffic_df_day['VOL_HT_L1'].sum() + traffic_df_day['VOL_HT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[1][2].set(f"{round(100*(traffic_df_eve['VOL_HT_L1'].sum() + traffic_df_eve['VOL_HT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[2][2].set(f"{round(100*(traffic_df_night['VOL_HT_L1'].sum() + traffic_df_night['VOL_HT_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[0][3].set(f"{round(100*(traffic_df_day['VOL_BUS_L1'].sum() + traffic_df_day['VOL_BUS_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[1][3].set(f"{round(100*(traffic_df_eve['VOL_BUS_L1'].sum() + traffic_df_eve['VOL_BUS_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[2][3].set(f"{round(100*(traffic_df_night['VOL_BUS_L1'].sum() + traffic_df_night['VOL_BUS_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[0][4].set(f"{round(100*(traffic_df_day['VOL_MC_L1'].sum() + traffic_df_day['VOL_MC_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[1][4].set(f"{round(100*(traffic_df_eve['VOL_MC_L1'].sum() + traffic_df_eve['VOL_MC_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")
        ldenOutTable[2][4].set(f"{round(100*(traffic_df_night['VOL_MC_L1'].sum() + traffic_df_night['VOL_MC_L2'].sum())/(traffic_df['VOL_L1'].sum() + traffic_df['VOL_L2'].sum()), 2)} %")

def calc_future_noise_LDN():
    print("*** Calculate Future Noise Metrics ***")
    if CurrentAADTOut.get() == '' or futureAADT.get() == '':
        PopUpMsg('Calculate current noise metrics and fill in future AADT before calculating future noise metrics.')
        return
    traffic_df_day = tnmaide_group[tnmaide_group['HOUR'].between(7, 21)]
    traffic_df_night = tnmaide_group[~tnmaide_group['HOUR'].between(7, 21)]
    cols = ['PCT_NOISE_AUTO', 'PCT_NOISE_MED_TRUCK', 'PCT_NOISE_HVY_TRUCK', 'PCT_NOISE_BUS', 'PCT_NOISE_MC']

    print(" Calculating")
    for i in range(len(cols)):
        traffic_df_day[cols[i]] = .01*float(ldnInTable[0][i].get())/15
    for i in range(len(cols)):
        traffic_df_night[cols[i]] = .01*float(ldnInTable[1][i].get())/9

    group_future_ldn = pd.concat([traffic_df_day, traffic_df_night])
    group_future_ldn['MAADT'] = float(futureAADT.get())
    future_result_ldn = call_TNMAide.call_TNMAide(group_future_ldn, (roadgrade1.get(), roadgrade2.get()), medwidth.get(), (NumLanes1.get(), NumLanes2.get()))
    print(" Setting")
    FleetBreakdownUsed.set('LDN Time Period Distribution')
    LAeqFutureOutput.set(str(round(future_result_ldn.average_day.LAEQ_24_HR, 2)))
    LdnFutureOutput.set(str(round(future_result_ldn.average_day.LDN, 2)))
    LdenFutureOutput.set(str(round(future_result_ldn.average_day.LDEN, 2)))

def calc_future_noise_LDEN():
    print("*** Calculate Future Noise Metrics ***")
    if CurrentAADTOut.get() == '' or futureAADT.get() == '':
        PopUpMsg('Calculate current noise metrics and fill in future AADT before calculating future noise metrics.')
        return
    traffic_df_day = tnmaide_group[tnmaide_group['HOUR'].between(7, 18)]
    traffic_df_eve = tnmaide_group[~tnmaide_group['HOUR'].between(19, 21)]
    traffic_df_night = tnmaide_group[~tnmaide_group['HOUR'].between(7, 21)]
    cols = ['PCT_NOISE_AUTO', 'PCT_NOISE_MED_TRUCK', 'PCT_NOISE_HVY_TRUCK', 'PCT_NOISE_BUS', 'PCT_NOISE_MC']

    print(" Calculating")
    for i in range(len(cols)):
        traffic_df_day[cols[i]] = .01*float(ldenInTable[0][i].get())/12
    for i in range(len(cols)):
        traffic_df_eve[cols[i]] = .01*float(ldenInTable[1][i].get())/3
    for i in range(len(cols)):
        traffic_df_night[cols[i]] = .01*float(ldenInTable[2][i].get())/9

    print(len(traffic_df_day), len(traffic_df_eve), len(traffic_df_night))
    group_future_lden = pd.concat([traffic_df_day, traffic_df_eve, traffic_df_night])
    group_future_lden['MAADT'] = float(futureAADT.get())
    future_result_lden = call_TNMAide.call_TNMAide(group_future_lden, (roadgrade1.get(), roadgrade2.get()), medwidth.get(), (NumLanes1.get(), NumLanes2.get()))
    print(" Outputting Results")
    FleetBreakdownUsed.set('LDEN Time Period Distribution')
    LAeqFutureOutput.set(str(round(future_result_lden.average_day.LAEQ_24_HR, 2)))
    LdnFutureOutput.set(str(round(future_result_lden.average_day.LDN, 2)))
    LdenFutureOutput.set(str(round(future_result_lden.average_day.LDEN, 2)))

def fill_current_year():
    futureAADT.set(CurrentAADTOut.get())

    for r in range(len(ldnOutTable)):
        for c in range(len(ldnOutTable[r])):
            ldnInTable[r][c].set(round(100*p2f(ldnOutTable[r][c].get()), 2))
    
    for r in range(len(ldenOutTable)):
        for c in range(len(ldenOutTable[r])):
            ldenInTable[r][c].set(round(100*p2f(ldenOutTable[r][c].get()), 2))

def vis_plot():
    if tnmaide_result is None:
        PopUpMsg('Please Calculate TNMAide results succesfully before attempting to plot noise results.')
        return
    
    plotOptions = ['Average Day Hourly SPL', 'Average Day Hourly Speed', 
                   'Hourly Speed Histograms', 'Hourly SPL Histograms']
    if plotChoice.get() == 'Average Day Hourly SPL':
        figs, axs = tnmaide_result.Plot_Avg_Day_Hourly_SPL()
    if plotChoice.get() == 'Average Day Hourly Speed':
        figs, axs = tnmaide_result.Plot_Avg_Day_Hourly_Speed()
    if plotChoice.get() == 'Hourly Speed Histograms':
        figs, axs = tnmaide_result.Plot_Hourly_Speed_Histograms()
    if plotChoice.get() == 'Hourly SPL Histograms':
        figs, axs = tnmaide_result.Plot_Hourly_SPL_Histograms()
    
    #Show the Plot
    for child in plotFrame.winfo_children():
        child.destroy()

    i = 0
    for fig in figs:
        fig.set_tight_layout(True)
        canvas = FigureCanvasTkAgg(fig, master = plotFrame)  
        canvas.draw()
        # placing the canvas on the Tkinter window
        canvas.get_tk_widget().grid(row=i, column = 0, pady=(5, 0), padx=(5, 0))
        i+=1
        toolbar = NavigationToolbar2Tk(canvas, plotFrame, pack_toolbar=False)
        toolbar.update()
        # placing the toolbar on the Tkinter window
        toolbar.grid(row=i, column = 0, pady=(5, 0))
        i+=1
    bind_tree(vis, "<MouseWheel>", vis_mouse_wheel)


# GUI
   
################################################################################
#Some important user interface callback functions

# Bind Mouse Wheel to GUI

def bind_tree(widget, event, callback, filter=None):
    "Binds an event to a widget and all its descendants."

    if filter is not None:        
        if isinstance(widget, filter):
            
            widget.bind(event, callback)

            for child in widget.children.values():
                bind_tree(child, event, callback, filter = filter)
    else:
        widget.bind(event, callback)

        for child in widget.children.values():
            bind_tree(child, event, callback)

def main_mouse_wheel(event):
    #if main_container.winfo_height() 
    canvas.yview_scroll(int(-1*event.delta/120), "units")
    
def output_mouse_wheel(event):
    output_text.yview_scroll(int(-1*event.delta/120), "units")

def TMCoutput_mouse_wheel(event):
    TMCoutput_text.yview_scroll(int(-1*event.delta/120), "units")

def tmcselect_mouse_wheel(event):
    TMCSelect_canvas.yview_scroll(int(-1*event.delta/120), "units")

def tnmaide_mouse_wheel(event):
    tnmaide_canvas.yview_scroll(int(-1*event.delta/120), "units")

def vis_mouse_wheel(event):
    vis_canvas.yview_scroll(int(-1*event.delta/120), "units")
    
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
if __name__ == "__main__":
    mp.freeze_support()  
    
    root = Tk()
    root.title("FHWA DANA Tool - v{}".format(versionNum))
    root.grid_rowconfigure(0, weight=0)
    root.grid_rowconfigure(1, weight=1)
    root.grid_columnconfigure(0, weight=1)
    style = ttk.Style()

    iconPath = resource_path('lib\\dot.png')
    p1 = tk.PhotoImage(file = iconPath)
    root.iconphoto(False, p1)

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
    TMCSelect_scrollbar.grid(row=0, column=2, sticky='ns')
    TMCSelect_canvas.grid(row=0, column=0, sticky="news")
    TMCSelect_canvas.configure(yscrollcommand = TMCSelect_scrollbar.set)
    
    TMCSelection_window = tk.Frame(TMCSelect_canvas)
    TMCSelect_canvas.create_window((0,0), window=TMCSelection_window, anchor='nw', )

    TMCSelection_frame = tk.Frame(TMCSelection_window)
    TMCSelection_frame.grid(column=0, row=0, sticky='nw', padx=(0,10))
    
    ttk.Label(TMCSelection_frame, wraplength = 500, text='To select specific data from the National Traffic Dataset, please select the desired features').grid(row=0, column=0, columnspan= 5)
    w_tmc_config = ttk.Button(TMCSelection_frame, text='Select TMC Config File', command=f_tmc_config).grid(column=0, row=1, columnspan=2, sticky="w")
    pl_tmc_config = ttk.Label(TMCSelection_frame)
    pl_tmc_config.grid(column=2, row=1, columnspan=4, sticky="w")
    
    w_kml = ttk.Button(TMCSelection_frame, text='Select KML File', command=f_kml).grid(column=0, row=2, columnspan=2, sticky="w")
    pl_kml = ttk.Label(TMCSelection_frame)
    pl_kml.grid(column=2, row=2, columnspan=3, sticky="w")
    
    ttk.Label(TMCSelection_frame, text='Select with County:').grid(row=4,column=0, columnspan=2, sticky="w")
    ttk.Label(TMCSelection_frame, text='Select a Specific Road:').grid(row=5,column=0, columnspan=2, sticky="w")
    ttk.Label(TMCSelection_frame, text='Select a Specific Direction:').grid(row=6,column=0, columnspan=2, sticky="w")
    
    CountyValue = StringVar()
    countySelect = ttk.Combobox(TMCSelection_frame, textvariable=CountyValue, state='readonly', width=50)
    countySelect.grid(column=2, row=4, columnspan=3, sticky="w")
    
    RoadValue = StringVar()
    road = ttk.Combobox(TMCSelection_frame, textvariable=RoadValue, state='readonly', width=50)
    road.grid(column=2, row=5, columnspan=3, sticky="w")
    
    DirectionValue = StringVar()
    direction = ttk.Combobox(TMCSelection_frame, textvariable=DirectionValue, state='readonly', width=50)
    direction.grid(column=2, row=6, columnspan=3, sticky="w")
    
    tmcbuttoncanvas = tk.Canvas(TMCSelection_frame)
    tmcbuttoncanvas.grid(column=0, row=7, sticky="W", columnspan=5)

    mapButton = ttk.Button(tmcbuttoncanvas, text="Map Selected TMCs", command=MapTMCs)
    mapButton.grid(column=0, row=0, columnspan=1, sticky='w', padx=(0, 5))

    tmcButton = ttk.Button(tmcbuttoncanvas, text="Select TMC IDs", command=PrintSelectData)
    tmcButton.grid(column=1, row=0, columnspan=1, sticky='w', padx=(0, 5))

    saveButton = ttk.Button(tmcbuttoncanvas, text="Save Selected TMCs", command=SelectData)
    saveButton.grid(column=2, row=0, columnspan=1, sticky='w', padx=(0, 5))

    clearButton = ttk.Button(tmcbuttoncanvas, text="Clear Selected TMCs", command=ClearData)
    clearButton.grid(column=3, row=0, columnspan=1, sticky='w', padx=(0, 5))

    clearButton = ttk.Button(tmcbuttoncanvas, text="Clear Filters", command=ClearFilters)
    clearButton.grid(column=4, row=0, columnspan=1, sticky='w', padx=(0, 5))

    TMCOutput_Container = tk.Frame(TMCSelection_frame)
    TMCOutput_Container.grid(row=8, column=0, columnspan=5, sticky="news")
    TMCOutput_Container.grid_rowconfigure(0, weight=1)
    TMCOutput_Container.grid_columnconfigure(0, weight=1)
    
    TMCoutput_text = tk.Text(TMCOutput_Container, height=10, width=80, wrap=WORD)
    TMCoutput_text.grid(row=0, column=0, sticky="news")
    TMCoutput_scrollbar = tk.Scrollbar(TMCOutput_Container, orient="vertical", command=TMCoutput_text.yview)
    TMCoutput_scrollbar.grid(row=0, column=1, sticky='ns')
    TMCoutput_text.configure(yscrollcommand = TMCoutput_scrollbar.set)
    TMCoutput_text.bind("<MouseWheel>", TMCoutput_mouse_wheel)
    TMCoutput_text.bind("<Key>", lambda e: ctrlEvent(e))

    for child in TMCSelection_frame.winfo_children(): child.grid_configure(padx=2, pady=4)
    
    ########################
    ###### TNM_AID GUI TAB
    
    tnmaide = tk.Frame(notebook)
    tnmaide.grid(row=0, column=0, sticky="news")
    tnmaide.grid_rowconfigure(0, weight=1)
    tnmaide.grid_columnconfigure(0, weight=1)
    notebook.add(tnmaide, text='TNMAide')

    tnmaide_canvas = tk.Canvas(tnmaide)
    tnmaide_scrollbar = tk.Scrollbar(tnmaide, orient="vertical", command=tnmaide_canvas.yview)
    tnmaide_scrollbar.grid(row=0, column=1, sticky='ns')
    tnmaide_canvas.grid(row=0, column=0, sticky="news")
    tnmaide_canvas.configure(yscrollcommand = tnmaide_scrollbar.set)
    
    tnmaideframe = tk.Frame(tnmaide_canvas)
    tnmaide_canvas.create_window((0,0), window=tnmaideframe, anchor='nw')

    # Choose TMCs
    tnmaide_headerFont = ("Ariel", 13, "bold")
    ttk.Label(tnmaideframe, text='TNMAide - Estimate characteristic noise metrics near the roadway.', font=tnmaide_headerFont).grid(row=0, column=0, columnspan=2, sticky="w")
    ttk.Separator(tnmaideframe, orient=HORIZONTAL).grid(row=1,column=0, columnspan=5, sticky="ew")

    tmc_selection_button = ttk.Button(tnmaideframe, text = 'TMC Selection Tool', command=lambda: notebook.select('.!notebook.!frame3')).grid(column=0, row=2, columnspan=1, sticky="w")
    w_npmrds_clean_3 = ttk.Button(tnmaideframe, text='Select Processed NPMRDS', command=f_npmrds_clean).grid(column=0, row=3, columnspan=1, sticky="w")
    pl_npmrds_clean_3 = ttk.Label(tnmaideframe)
    pl_npmrds_clean_3.grid(column=1, row=3, columnspan=1, sticky="w")

    medwidthentry = ttk.Label(tnmaideframe, text='Median Width (ft): ').grid(column = 0, row=4, sticky="w")
    medwidth = DoubleVar()
    ttk.Entry(tnmaideframe, textvariable=medwidth).grid(column=1, row=4, sticky="w")
    
    ttk.Label(tnmaideframe, text='Enter TMC Information').grid(row=5, column=0, columnspan=2, sticky="ew")
    tmcentercanvas = tk.Canvas(tnmaideframe)
    tmcentercanvas.grid(column=0, row=6, columnspan=2, sticky="w")

    # Entry
    tmc1entrylabel = ttk.Label(tmcentercanvas, text='TMC 1: ').grid(row=0, column=1)
    tmc2entrylabel = ttk.Label(tmcentercanvas, text='TMC 2: ').grid(row=0, column=2)
    TMC_1Entry = StringVar()
    TMC_2Entry = StringVar()
    tmcIDLabel = ttk.Label(tmcentercanvas, text='Enter TMC IDs: ').grid(row=1, column=0, sticky='w', padx=(0, 5))
    ttk.Entry(tmcentercanvas, textvariable=TMC_1Entry).grid(column=1, row=1, sticky="w", padx=(5, 5))
    ttk.Entry(tmcentercanvas, textvariable=TMC_2Entry).grid(column=2, row=1, sticky="w", padx=(5, 0))

    lanesentry = ttk.Label(tmcentercanvas, text='Number of Lanes in Each Direction: ').grid(column = 0, row=2, sticky="w", padx=(0, 5))
    NumLanes1 = IntVar()
    ttk.Entry(tmcentercanvas, textvariable=NumLanes1).grid(column=1, row = 2, sticky="w", padx=(5, 5))
    NumLanes2 = IntVar()
    ttk.Entry(tmcentercanvas, textvariable=NumLanes2).grid(column=2, row = 2, sticky="w", padx=(5, 0))

    gradeentry = ttk.Label(tmcentercanvas, text='Roadway Grade: ').grid(column = 0, row=3, sticky="w", padx=(0, 5))
    roadgrade1 = DoubleVar()
    ttk.Entry(tmcentercanvas, textvariable=roadgrade1).grid(column=1, row=3, sticky="w", padx=(5, 5))
    roadgrade2 = DoubleVar()
    ttk.Entry(tmcentercanvas, textvariable=roadgrade2).grid(column=2, row=3, sticky="w", padx=(5, 5))
    helpgrade = ttk.Label(tmcentercanvas, text='in direction of near lanes, include - or +').grid(column = 3, row=3, sticky="w", padx=(5, 0))

    ########## Calculate and Outputs ###############
    ttk.Separator(tnmaideframe, orient=HORIZONTAL).grid(row=9,column=0, columnspan=5, sticky="ew")
    calcButton = ttk.Button(tnmaideframe, text='Calculate TNMAide Outputs', command=lambda: calc_tnmaide(True), style='h2.TButton')
    style.configure('h2.TButton', font=tnmaide_headerFont)

    calcButton.grid(column=0, row=10, columnspan=1, sticky='w', padx=(0, 5))
    TNMAIDE_cancelButton = ttk.Button(tnmaideframe, text="Cancel TNMAide Calculation ", command=CancelProcess)
    TNMAIDE_cancelButton.grid(column=1, row=10, columnspan=1, sticky='w', padx=(5, 5))
    TNMAide_statusLabel = ttk.Label(tnmaideframe, text="TNMAide Calculating", relief=SUNKEN)
    TNMAide_statusLabel.grid(column=2, row=10, columnspan=1, padx=(5, 0), sticky="W")
    TNMAide_statusLabel.grid_remove()
    
    tnmaide_headerFont2 = ("Ariel", 11, "bold")
    ttk.Label(tnmaideframe, text='Worst Hour Noise Metrics at Reference Location: ', font=tnmaide_headerFont2).grid(row=11, column=0, columnspan=2, sticky="w")
    
    #Output Labels
    ttk.Label(tnmaideframe, text='LAeq: ').grid(row=12, column=0, columnspan=1, sticky="w")
    ttk.Label(tnmaideframe, text='Ldn: ').grid(row=13, column=0, columnspan=1, sticky="w")
    ttk.Label(tnmaideframe, text='Lden: ').grid(row=14, column=0, columnspan=1, sticky="w")
    ttk.Label(tnmaideframe, text='Worst Hour: ').grid(row=15, column=0, columnspan=1, sticky="w")
    ttk.Label(tnmaideframe, text='Worst Day: ').grid(row=16, column=0, columnspan=1, sticky="w")
    
    # Outputs
    LAeqOutput = StringVar()
    ttk.Entry(tnmaideframe, textvariable=LAeqOutput, state="readonly").grid(row=12, column=1, sticky="w")
    LdnOutput = StringVar()
    ttk.Entry(tnmaideframe, textvariable=LdnOutput, state="readonly").grid(row=13, column=1, sticky="w")
    LdenOutput = StringVar()
    ttk.Entry(tnmaideframe, textvariable=LdenOutput, state="readonly").grid(row=14, column=1, sticky="w")
    WorstHourOut = StringVar()
    ttk.Entry(tnmaideframe, textvariable=WorstHourOut, state="readonly").grid(row=15, column=1, sticky="w")
    WorstDayOut = StringVar()
    ttk.Entry(tnmaideframe, textvariable=WorstDayOut, state="readonly").grid(row=16, column=1, sticky="w") 
    
    ttk.Label(tnmaideframe, text="Traffic Information", font=tnmaide_headerFont2).grid(row=17, column=0, columnspan=2, sticky="w")   
    ttk.Label(tnmaideframe, text='Current AADT: ').grid(row=18, column=0, columnspan=1, sticky="w")
    CurrentAADTOut = StringVar()
    ttk.Entry(tnmaideframe, textvariable=CurrentAADTOut, state="readonly").grid(row=18, column=1, sticky="w")

    ttk.Label(tnmaideframe, text="Average Day Worst Hour Traffic Conditions: ", font='Ariel 10 bold').grid(row=19, column=0, columnspan = 2, sticky="W")

    wh = tk.Canvas(tnmaideframe)
    wh.grid(column=0, row=20, columnspan=5, rowspan=3, sticky="w")
    ttk.Label(wh, text='Average Day Worst Hour Volume: ').grid(row=1, column=0, columnspan=1, sticky="w")
    ttk.Label(wh, text='Average Day Worst Hour Average Speed: ').grid(row=2, column=0, columnspan=1, sticky="w")
    ttk.Label(wh, text = "Auto").grid(column=1, row=0, sticky="w")
    ttk.Label(wh, text = "Medium Trucks").grid(column=2, row=0, sticky="w")
    ttk.Label(wh, text = "Heavy Trucks").grid(column=3, row=0, sticky="w")
    ttk.Label(wh, text = "Buses").grid(column=4, row=0, sticky="w")
    ttk.Label(wh, text = "Motor Cycles").grid(column=5, row=0, sticky="w")

    whOutTable = []
    for r in range(2):
        whOutRow = []
        for c in range(5):
            strvar = tk.StringVar()
            whOutRow.append(strvar)
            ttk.Entry(wh, textvariable=strvar, state="readonly").grid(row=1+r, column=1+c, sticky="w")
        whOutTable.append(whOutRow)

    ttk.Label(tnmaideframe, text="Yearly Vehicle Mix (%): ", font='Ariel 10 bold').grid(row=23, column=0, columnspan = 2, sticky="W")
    
    yearbreakdown = tk.Canvas(tnmaideframe)
    yearbreakdown.grid(column=0, row=24, columnspan=5, rowspan=2, sticky="w")
    ttk.Label(yearbreakdown, text='Percent Vehicles in the Current Year: ').grid(row=1, column=0, columnspan=1, sticky="w")
    ttk.Label(yearbreakdown, text = "Auto").grid(column=1, row=0, sticky="w")
    ttk.Label(yearbreakdown, text = "Medium Trucks").grid(column=2, row=0, sticky="w")
    ttk.Label(yearbreakdown, text = "Heavy Trucks").grid(column=3, row=0, sticky="w")
    ttk.Label(yearbreakdown, text = "Buses").grid(column=4, row=0, sticky="w")
    ttk.Label(yearbreakdown, text = "Motor Cycles").grid(column=5, row=0, sticky="w")

    yrOutTable = []
    for r in range(1):
        yrOutRow = []
        for c in range(5):
            strvar = tk.StringVar()
            yrOutRow.append(strvar)
            ttk.Entry(yearbreakdown, textvariable=strvar, state="readonly").grid(row=1+r, column=1+c, sticky="w")
        yrOutTable.append(yrOutRow)
    
    # ttk.Label(tnmaideframe, text='Cars: ').grid(row=20, column=0, columnspan=1, sticky="w")
    # ttk.Label(tnmaideframe, text='Medium Trucks: ').grid(row=21, column=0, columnspan=1, sticky="w")
    # ttk.Label(tnmaideframe, text='Heavy Trucks: ').grid(row=22, column=0, columnspan=1, sticky="w")
    # ttk.Label(tnmaideframe, text='Buses: ').grid(row=23, column=0, columnspan=1, sticky="w")
    # ttk.Label(tnmaideframe, text='Motorcycles: ').grid(row=24, column=0, columnspan=1, sticky="w")
    # ttk.Label(tnmaideframe, text='Total: ').grid(row=25, column=0, columnspan=1, sticky="w")
    
    # CarsOut = StringVar()
    # ttk.Entry(tnmaideframe, textvariable=CarsOut, state="readonly").grid(row=20, column=1, sticky="w")
    # MedTrucksOut = StringVar()
    # ttk.Entry(tnmaideframe, textvariable=MedTrucksOut, state="readonly").grid(row=21, column=1, sticky="w")
    # HeavyTrucksOut = StringVar()
    # ttk.Entry(tnmaideframe, textvariable=HeavyTrucksOut, state="readonly").grid(row=22, column=1, sticky="w")
    # BusesOut = StringVar()
    # ttk.Entry(tnmaideframe, textvariable=BusesOut, state="readonly").grid(row=23, column=1, sticky="w")
    # MotoOut = StringVar()
    # ttk.Entry(tnmaideframe, textvariable=MotoOut, state="readonly").grid(row=24, column=1, sticky="w")
    # TotalOut = StringVar()
    # ttk.Entry(tnmaideframe, textvariable=TotalOut, state="readonly").grid(row=25, column=1, sticky="w")

    ttk.Label(tnmaideframe, text='LDN Time Period Distribution', font='Ariel 10 bold').grid(row=26, column=0, columnspan=2, sticky="w")

    daybreakdown = tk.Canvas(tnmaideframe)
    daybreakdown.grid(column=0, row=27, columnspan=5, rowspan=3, sticky="w")
    ttk.Label(daybreakdown, text='Percent Vehicles in the Current Year, DAYTIME: ').grid(row=1, column=0, columnspan=1, sticky="w")
    ttk.Label(daybreakdown, text='Percent Vehicles in the Current Year, NIGHTTIME: ').grid(row=2, column=0, columnspan=1, sticky="w")
    ttk.Label(daybreakdown, text = "Auto").grid(column=1, row=0, sticky="w")
    ttk.Label(daybreakdown, text = "Medium Trucks").grid(column=2, row=0, sticky="w")
    ttk.Label(daybreakdown, text = "Heavy Trucks").grid(column=3, row=0, sticky="w")
    ttk.Label(daybreakdown, text = "Buses").grid(column=4, row=0, sticky="w")
    ttk.Label(daybreakdown, text = "Motor Cycles").grid(column=5, row=0, sticky="w")

    ldnOutTable = []
    for r in range(2):
        ldnOutRow = []
        for c in range(5):
            strvar = tk.StringVar()
            ldnOutRow.append(strvar)
            ttk.Entry(daybreakdown, textvariable=strvar, state="readonly").grid(row=1+r, column=1+c, sticky="w")
        ldnOutTable.append(ldnOutRow)

    ttk.Label(tnmaideframe, text='LDEN Time Period Distribution', font='Ariel 10 bold').grid(row=30, column=0, columnspan=2, sticky="w")

    denbreakdown = tk.Canvas(tnmaideframe)
    denbreakdown.grid(column=0, row=31, columnspan=5, rowspan=4, sticky="w")
    ttk.Label(denbreakdown, text='Percent Vehicles in the Current Year, DAYTIME: ').grid(row=1, column=0, columnspan=1, sticky="w")
    ttk.Label(denbreakdown, text='Percent Vehicles in the Current Year, EVENING: ').grid(row=2, column=0, columnspan=1, sticky="w")
    ttk.Label(denbreakdown, text='Percent Vehicles in the Current Year, NIGHTTIME: ').grid(row=3, column=0, columnspan=1, sticky="w")
    ttk.Label(denbreakdown, text = "Auto").grid(column=1, row=0, sticky="w")
    ttk.Label(denbreakdown, text = "Medium Trucks").grid(column=2, row=0, sticky="w")
    ttk.Label(denbreakdown, text = "Heavy Trucks").grid(column=3, row=0, sticky="w")
    ttk.Label(denbreakdown, text = "Buses").grid(column=4, row=0, sticky="w")
    ttk.Label(denbreakdown, text = "Motor Cycles").grid(column=5, row=0, sticky="w")

    ldenOutTable = []
    for r in range(3):
        ldenOutRow = []
        for c in range(5):
            strvar = tk.StringVar()
            ldenOutRow.append(strvar)
            ttk.Entry(denbreakdown, textvariable=strvar, state="readonly").grid(row=1+r, column=1+c, sticky="w")
        ldenOutTable.append(ldenOutRow)

    ########## Calculate Future Metrics ###############
    ttk.Separator(tnmaideframe, orient=HORIZONTAL).grid(row=35,column=0, columnspan=5, sticky="ew")
    ttk.Label(tnmaideframe, text='Estimate noise levels with future AADT breakdown', font=tnmaide_headerFont).grid(row=36, column=0, columnspan=2, sticky="w")
    fillCurrentButton = ttk.Button(tnmaideframe, text='Fill from current year', command=fill_current_year)
    fillCurrentButton.grid(column=1, row=37, columnspan=1, sticky='w', padx=(5, 5))
    ttk.Label(tnmaideframe, text='Future Year AADT: ').grid(row=38, column=0, columnspan=2, sticky="w")
    futureAADT = tk.StringVar()
    ttk.Entry(tnmaideframe, textvariable=futureAADT).grid(row=38, column=1, sticky="w")
    
    ttk.Label(tnmaideframe, text='LDN Time Period Distribution', font='Ariel 10 bold').grid(row=39, column=0, columnspan=2, sticky="w")
    futureLDNInput = tk.Canvas(tnmaideframe)
    futureLDNInput.grid(column=0, row=40, columnspan=5, rowspan=3, sticky="w")
    ttk.Label(futureLDNInput, text='Percent Vehicles in the Future Year, DAYTIME: ').grid(row=1, column=0, columnspan=1, sticky="w")
    ttk.Label(futureLDNInput, text='Percent Vehicles in the Future Year, NIGHTTIME: ').grid(row=2, column=0, columnspan=1, sticky="w")
    ttk.Label(futureLDNInput, text = "Auto").grid(column=1, row=0, sticky="w")
    ttk.Label(futureLDNInput, text = "Medium Trucks").grid(column=2, row=0, sticky="w")
    ttk.Label(futureLDNInput, text = "Heavy Trucks").grid(column=3, row=0, sticky="w")
    ttk.Label(futureLDNInput, text = "Buses").grid(column=4, row=0, sticky="w")
    ttk.Label(futureLDNInput, text = "Motor Cycles").grid(column=5, row=0, sticky="w")

    ldnInTable = []
    for r in range(2):
        ldnInRow = []
        for c in range(5):
            strvar = tk.StringVar()
            ldnInRow.append(strvar)
            ttk.Entry(futureLDNInput, textvariable=strvar).grid(row=1+r, column=1+c, sticky="w")
        ldnInTable.append(ldnInRow)

    totLDNInput = tk.StringVar()
    ttk.Label(tnmaideframe, text='Total Percent: ').grid(row=44, column=0, columnspan=1, sticky="w")
    ttk.Entry(tnmaideframe, textvariable=totLDNInput, state="readonly").grid(row=44, column=1, sticky="w")

    ttk.Label(tnmaideframe, text='LDEN Time Period Distribution', font='Ariel 10 bold').grid(row=45, column=0, columnspan=2, sticky="w")
    futureLDENInput = tk.Canvas(tnmaideframe)
    futureLDENInput.grid(column=0, row=46, columnspan=5, rowspan=4, sticky="w")
    ttk.Label(futureLDENInput, text='Percent Vehicles in the Future Year, DAYTIME: ').grid(row=1, column=0, columnspan=1, sticky="w")
    ttk.Label(futureLDENInput, text='Percent Vehicles in the Future Year, EVENING: ').grid(row=2, column=0, columnspan=1, sticky="w")
    ttk.Label(futureLDENInput, text='Percent Vehicles in the Future Year, NIGHTTIME: ').grid(row=3, column=0, columnspan=1, sticky="w")
    ttk.Label(futureLDENInput, text = "Auto").grid(column=1, row=0, sticky="w")
    ttk.Label(futureLDENInput, text = "Medium Trucks").grid(column=2, row=0, sticky="w")
    ttk.Label(futureLDENInput, text = "Heavy Trucks").grid(column=3, row=0, sticky="w")
    ttk.Label(futureLDENInput, text = "Buses").grid(column=4, row=0, sticky="w")
    ttk.Label(futureLDENInput, text = "Motor Cycles").grid(column=5, row=0, sticky="w")

    ldenInTable = []
    for r in range(3):
        ldenInRow = []
        for c in range(5):
            strvar = tk.StringVar()
            ldenInRow.append(strvar)
            ttk.Entry(futureLDENInput, textvariable=strvar).grid(row=1+r, column=1+c, sticky="w")
        ldenInTable.append(ldenInRow)
    
    totLDENInput = tk.StringVar()
    ttk.Label(tnmaideframe, text='Total Percent: ').grid(row=50, column=0, columnspan=1, sticky="w")
    ttk.Entry(tnmaideframe, textvariable=totLDENInput, state="readonly").grid(row=50, column=1, sticky="w")

    calcFutureLDNButton = ttk.Button(tnmaideframe, text='Calculate with LDN Distributions', command=lambda: calc_future_noise_LDN(), style='h2.TButton')
    calcFutureLDNButton.grid(column=0, row=51, columnspan=1, sticky='w', padx=(0, 5))

    calcFutureLDENButton = ttk.Button(tnmaideframe, text='Calculate with LDEN Distributions', command=lambda: calc_future_noise_LDEN(), style='h2.TButton')
    calcFutureLDENButton.grid(column=1, row=51, columnspan=1, sticky='w', padx=(0, 5))
    
    ttk.Label(tnmaideframe, text='Worst Hour Noise Metrics at Reference Location: ', font=tnmaide_headerFont2).grid(row=52, column=0, columnspan=2, sticky="w")

    #Output Labels
    ttk.Label(tnmaideframe, text='Future Fleet Distribution Based On: ').grid(row=53, column=0, columnspan=1, sticky="w")
    ttk.Label(tnmaideframe, text='LAeq: ').grid(row=54, column=0, columnspan=1, sticky="w")
    ttk.Label(tnmaideframe, text='Ldn: ').grid(row=55, column=0, columnspan=1, sticky="w")
    ttk.Label(tnmaideframe, text='Lden: ').grid(row=56, column=0, columnspan=1, sticky="w")

    # Outputs
    FleetBreakdownUsed = StringVar()
    ttk.Entry(tnmaideframe, textvariable=FleetBreakdownUsed, state="readonly").grid(row=53, column=1, sticky="w", columnspan=2)
    LAeqFutureOutput = StringVar()
    ttk.Entry(tnmaideframe, textvariable=LAeqFutureOutput, state="readonly").grid(row=54, column=1, sticky="w")
    LdnFutureOutput = StringVar()
    ttk.Entry(tnmaideframe, textvariable=LdnFutureOutput, state="readonly").grid(row=55, column=1, sticky="w")
    LdenFutureOutput = StringVar()
    ttk.Entry(tnmaideframe, textvariable=LdenFutureOutput, state="readonly").grid(row=56, column=1, sticky="w")

    for child in tnmaideframe.winfo_children(): child.grid_configure(padx=2, pady=4)
    ##################################################
    
    ###### Visualization Tab
    vis = tk.Frame(notebook)
    vis.grid(row=0, column=0, sticky="news")
    vis.grid_rowconfigure(0, weight=1)
    vis.grid_columnconfigure(0, weight=1)
    notebook.add(vis, text='Data Visualization')

    vis_canvas = tk.Canvas(vis)
    vis_scrollbar = tk.Scrollbar(vis, orient="vertical", command=vis_canvas.yview)
    vis_scrollbar.grid(row=0, column=1, sticky='ns')
    vis_canvas.grid(row=0, column=0, sticky="news")
    vis_canvas.configure(yscrollcommand = vis_scrollbar.set)
    
    visframe = tk.Frame(vis_canvas)
    vis_canvas.create_window((0,0), window=visframe, anchor='nw')

    plotOptions = ['Average Day Hourly SPL', 'Average Day Hourly Speed', 
                   'Hourly Speed Histograms', 'Hourly SPL Histograms']

    ttk.Label(visframe, text='What would you like to plot? ').grid(row=0, column=0, columnspan=1, sticky="w")
    plotChoice = StringVar()
    plot_dropdown = ttk.Combobox(visframe, textvariable=plotChoice, values=plotOptions)
    plot_dropdown.grid(row=0, column=1, sticky='w')

    plotButton = ttk.Button(visframe, text='Create Plot', command=lambda: vis_plot(), style='h2.TButton')
    plotButton.grid(column=0, row=1, columnspan=1, sticky='w', padx=(0, 5))
    plotFrame = tk.Frame(visframe)
    plotFrame.grid(column=0, row=2, columnspan=2, sticky="ns")

    for child in visframe.winfo_children(): child.grid_configure(padx=2, pady=4)

    #print(notebook.tabs())
    
    ##################################################
    
    ##################################################
    #0. File location output
    #ttk.Label(mainframe, text='Choose output file location').grid(row=0,column=0, columnspan=1, sticky="w")
    ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=1,column=0, columnspan=5, sticky="ew")
    #1. Header
    
    ttk.Label(mainframe, text='Select State:').grid(row=2,column=0, columnspan=1, sticky="w")
    
    ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=3,column=0, columnspan=5, sticky="ew")
    
    #ttk.Label(mainframe, text='Select inputs under the desired process and press the “Run Process” button.').grid(row=6,column=0, columnspan=3, sticky="w")
    
    #ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=4,column=0, columnspan=5, sticky="ew")
    #ttk.Label(mainframe, text=step0).grid(row=6,column=1, columnspan=1, sticky="w")
    ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=9,column=0, columnspan=5, sticky="ew")
    ttk.Label(mainframe, text=step1).grid(row=10,column=0, columnspan=1, sticky="w")
    ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=25,column=0, columnspan=5, sticky="ew")
    ttk.Label(mainframe, text=step2).grid(row=26,column=0, columnspan=1, sticky="w")
    ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=31,column=0, columnspan=5, sticky="ew")
    ttk.Label(mainframe, text=step3).grid(row=32,column=0, columnspan=1, sticky="w")
    
    ##################################################
    
    # outputFile
    w_output_folder = ttk.Button(mainframe, text='Select Output Folder Location', command=f_output)
    w_output_folder.grid(column=0, row=0, columnspan=1, sticky="w")
    
    # 2. Combobox
    # List of States Combobox
    list_states = ['','AK','AL','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA',
                   'ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']
    StateValue = StringVar()
    w_state = ttk.Combobox(mainframe, textvariable=StateValue, state='readonly', width=30)
    w_state['values'] = list_states
    w_state.current(0)
    w_state.grid(column=1, row=2, columnspan=1, sticky="w")
    
    ### checkbox for process 0
    
    preprocess_checkvar = IntVar()
    preprocess_checkvar.set(False)
    preprocess_tmas_checkbox = ttk.Checkbutton(mainframe, text=step0, variable=preprocess_checkvar, command=enable_tmas_preprocess)
    preprocess_tmas_checkbox.grid(row=4,column=0, columnspan=1, sticky="w")
    
    ########
    # script 0
    p0Canvas = tk.Canvas(mainframe)
    p0Canvas.grid(column=1, row=4, sticky='W')
    p0startButton = ttk.Button(p0Canvas, text="Run Process 0", command=lambda: ProcessData(step0))
    p0startButton.grid(column=0, row=0, columnspan=1 ,sticky="W", padx=(0, 5))
    p0cancelButton = ttk.Button(p0Canvas, text="Cancel Process 0", command=CancelProcess)
    p0cancelButton.grid(column=1, row=0, columnspan=1, padx=(5, 5), sticky="W")
    p0statusLabel = ttk.Label(p0Canvas, text="Process 0 Running", relief=SUNKEN)
    p0statusLabel.grid(column=2, row=0, columnspan=1, padx=(5, 0), sticky="W")
    p0statusLabel.grid_remove()
    
    w_tmas_station = ttk.Button(mainframe, text='Select TMAS Station File', command=f_tmas_station)
    w_tmas_station.grid(column=0, row=5, columnspan=1, sticky="w")
    w_tmas_class = ttk.Button(mainframe, text='Select TMAS Class File', command=f_tmas_class)
    w_tmas_class.grid(column=0, row=6, columnspan=1, sticky="w")
    w_fips_1 = ttk.Button(mainframe, text='Select FIPS File', command=f_fips)
    w_fips_1.grid(column=0, row=7, columnspan=1, sticky="w")
    w_nei_1 = ttk.Button(mainframe, text='Select NEI Representative Counties', command=f_nei)
    w_nei_1.grid(column=0, row=8, columnspan=1, sticky="w")
    
    # script 1
    p1Canvas = tk.Canvas(mainframe)
    p1Canvas.grid(column=1, row=10, sticky='W')
    p1startButton = ttk.Button(p1Canvas, text="Run Process 1", command=lambda: ProcessData(step1))
    p1startButton.grid(column=0, row=0, columnspan=1 ,sticky="W", padx=(0, 5))
    p1cancelButton = ttk.Button(p1Canvas, text="Cancel Process 1", command=CancelProcess)
    p1cancelButton.grid(column=1, row=0, columnspan=1, padx=(5, 5), sticky="W")
    p1statusLabel = ttk.Label(p1Canvas, text="Process 1 Running", relief=SUNKEN)
    p1statusLabel.grid(column=2, row=0, columnspan=1, padx=(5, 0), sticky="W")
    p1statusLabel.grid_remove()
    
        # Auto detect
    autoDetectDatesVar = IntVar()
    autoDetectDatesVar.set(1)
    autoDetectDatesBox = ttk.Radiobutton(mainframe, text='Auto-detect date range from NPMRDS data.', value = 1, variable=autoDetectDatesVar, command=autoDetectClick)
    autoDetectDatesBox.grid(row=11,column=0, columnspan=1, sticky="w")
    
        # Date Range Selection
    SelectRangeBox = ttk.Radiobutton(mainframe, text='Or, select a date range to process (must be within the minimum and maximum dates in the NPMRDS data).', value = 2, variable=autoDetectDatesVar, command=autoDetectClick)
    SelectRangeBox.grid(row=12,column=0, columnspan=3, sticky="w")
    drCanvas = tk.Canvas(mainframe)
    drCanvas.grid(column=0, row=13, sticky='W', columnspan=2)
    ttk.Label(drCanvas, text='    ').grid(row=0, column = 0)
    ttk.Label(drCanvas, text='Start Date:').grid(row=0, column = 1)
    calStart = DateEntry(drCanvas, width= 16, background= "blue", foreground= "white", bd=2)
    calStart.grid(row=0, column=2, padx=8)
    calStart.configure(state='disabled')
    ttk.Label(drCanvas, text='End Date:').grid(row=0, column = 3)
    calEnd = DateEntry(drCanvas, width= 16, background= "blue", foreground= "white", bd=2)
    calEnd.grid(row=0, column=4, padx=8)
    calEnd.configure(state='disabled')
    
        # file buttons 
    
    w_tmas_station_state_1 = ttk.Button(mainframe, text='Select Processed TMAS Station', command=f_tmas_station_state).grid(column=0, row=15, columnspan=1, sticky="w")
    w_tmas_class_clean_1 = ttk.Button(mainframe, text='Select Processed TMAS Class', command=f_tmas_class_clean).grid(column=0, row=16, columnspan=1, sticky="w")
    w_npmrds_all = ttk.Button(mainframe, text='Select NPMRDS (All)', command=f_npmrds_all).grid(column=0, row=17, columnspan=1, sticky="w")
    w_npmrds_pass = ttk.Button(mainframe, text='Select NPMRDS (Passenger)', command=f_npmrds_pass).grid(column=0, row=18, columnspan=1, sticky="w")
    w_npmrds_truck = ttk.Button(mainframe, text='Select NPMRDS (Truck)', command=f_npmrds_truck).grid(column=0, row=19, columnspan=1, sticky="w")
    w_default_speed = ttk.Button(mainframe, text='Select Default Speeds', command=f_default_speed).grid(column=0, row=20, columnspan=1, sticky="w")
    w_npmrds_tmc = ttk.Button(mainframe, text='Select TMC Configuration', command=f_npmrds_tmc).grid(column=0, row=21, columnspan=1, sticky="w")
    #w_npmrds_shp = ttk.Button(mainframe, text='Select TMC shapefile', command=f_npmrds_shp).grid(column=0, row=21, columnspan=1, sticky="w")
    w_emission = ttk.Button(mainframe, text='Select Emission Rates', command=f_emission).grid(column=0, row=22, columnspan=1, sticky="w")
    w_fips_2 = ttk.Button(mainframe, text='Select FIPS File', command=f_fips).grid(column=0, row=23, columnspan=1, sticky="w")
    w_nei_2 = ttk.Button(mainframe, text='Select NEI Representative Counties', command=f_nei).grid(column=0, row=24, columnspan=1, sticky="w")
    
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
    
    
    
    # 4. Pathlabels
    # output folder
    pl_output_folder = ttk.Label(mainframe)
    pl_output_folder.grid(column=1, row=0, columnspan=1, sticky="w")
    # script 0
    pl_tmas_station = ttk.Label(mainframe)
    pl_tmas_station.grid(column=1, row=5, columnspan=1, sticky="w")
    pl_tmas_class = ttk.Label(mainframe)
    pl_tmas_class.grid(column=1, row=6, columnspan=1, sticky="w")
    pl_fips_1 = ttk.Label(mainframe)
    pl_fips_1.grid(column=1, row=7, columnspan=1, sticky="w")
    pl_nei_1 = ttk.Label(mainframe)
    pl_nei_1.grid(column=1, row=8, columnspan=1, sticky="w")
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
    pl_default_speed = ttk.Label(mainframe)
    pl_default_speed.grid(column=1, row=20, columnspan=1, sticky="w")
    pl_npmrds_tmc = ttk.Label(mainframe)
    pl_npmrds_tmc.grid(column=1, row=21, columnspan=1, sticky="w")
    #pl_npmrds_shp = ttk.Label(mainframe)
    #pl_npmrds_shp.grid(column=1, row=19, columnspan=1, sticky="w")
    pl_emission = ttk.Label(mainframe)
    pl_emission.grid(column=1, row=22, columnspan=1, sticky="w")
    pl_fips_2 = ttk.Label(mainframe)
    pl_fips_2.grid(column=1, row=23, columnspan=1, sticky="w")
    pl_nei_2 = ttk.Label(mainframe)
    pl_nei_2.grid(column=1, row=24, columnspan=1, sticky="w")
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
    #Default Speeds
    if ('National_Default_Roadway_Operating_Speed.csv' in os.listdir('Default Input Files/')):
        pl_default_speed.config(text=os.getcwd()+'\\Default Input Files\\National_Default_Roadway_Operating_Speed.csv')
        fn_defualt_speed = 'Default Input Files/National_Default_Roadway_Operating_Speed.csv'
    else:
        pl_nei_1.config(text='')
        pl_nei_2.config(text='')    
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
           
    if False:
        testOption = 1

        roadgrade1.set(0)
        roadgrade2.set(0)
        medwidthentry = 6
        NumLanes1.set(3)
        NumLanes2.set(3)

        if testOption == 1:
            tmas_year = 2021
            npmrds_year = 2018
            state  = 'MA'
            county = 'Middlesex'

            TMC_1Entry.set('129-04136')
            TMC_2Entry.set('129+04137')
  
        elif testOption == 2:
            tmas_year = 2019
            npmrds_year = 2017
            state  = 'DC'
            county = 'District'
        elif testOption == 3:
            tmas_year = 2019
            npmrds_year = 2019
            state  = 'VA'
            county = 'Alexandria'
        elif testOption == 4:
            tmas_year = 2019
            npmrds_year = 2018
            state = 'OR'
            county = 'Marion'

            TMC_1Entry.set('114-04428')
            TMC_2Entry.set('114+04429')
        elif testOption == 5:
            tmas_year = 2019
            npmrds_year = 2021
            state = 'LA'
            county = 'LaSalle'
        elif testOption == 6:
            tmas_year = 2019
            npmrds_year = 2021
            state = 'VA'
            county = '3Counties'
        elif testOption == 7:
            tmas_year = 2019
            npmrds_year = 2021
            state = 'VA'
            county = 'FairfaxCity'    
        elif testOption == 8:
            tmas_year = 2019
            npmrds_year = 2019
            state = 'CA'
            county = 'LosAngeles'    
        elif testOption == 9:
            tmas_year = 2019
            npmrds_year = 2019
            state = 'IL'
            county = 'State'
        elif testOption == 10:
            tmas_year = 2019
            npmrds_year = 2019
            state = 'MD'
            county = 'State'
            
        elif testOption == 11:
            tmas_year = 2019
            npmrds_year = 2019
            state = 'RI'
            county = 'Providence'
            
        elif testOption == 12:
            tmas_year = 2019
            npmrds_year = 2019
            state = 'OK'
            county = 'Oklahoma'
            
        elif testOption == 13:
            tmas_year = 2020
            npmrds_year = 2020
            state = 'VA'
            county = 'Richmond'
            
        elif testOption == 14:
            tmas_year = 2019
            npmrds_year = 2020
            state = 'NJ'
            county = 'Morris'
        elif testOption == 15:
            tmas_year = 2020
            npmrds_year = 2020
            state = 'MA'
            county = '1TMC'
            dateStart = dt.date(2020, 3, 20)
            dateEnd = dt.date(2020, 3, 22)
            
        elif testOption == 16:
            tmas_year = 2017
            npmrds_year = 2017
            state = 'AK'
            county = 'Anchorage'

        elif testOption == 17:
            tmas_year = 2020
            npmrds_year = 2020
            state = 'VA'
            county = 'RingRoads'
                
        
        w_state.set(state)
        fn_tmas_station = f'C:/Users/William.Chupp/OneDrive - DOT OST/Documents/DANAToolTesting/FHWA-DANATool/Default Input Files/TMAS Data/TMAS {tmas_year}/TMAS_Station_{tmas_year}.csv'
        pl_tmas_station_state_1.config(text=fn_tmas_station.replace('/','\\'))
        fn_tmas_class_clean = f'C:/Users/William.Chupp/OneDrive - DOT OST/Documents/DANAToolTesting/FHWA-DANATool/Default Input Files/TMAS Data/TMAS {tmas_year}/TMAS_Class_Clean_{tmas_year}.csv'
        pl_tmas_class_clean_1.config(text=fn_tmas_class_clean.replace('/','\\'))
        fn_npmrds_all = f'H:/TestData/{county}_{state}/NPMRDS Data/{state}_{county}_{npmrds_year}_ALL.csv'
        pl_npmrds_all.config(text=fn_npmrds_all.replace('/','\\'))
        fn_npmrds_pass = f'H:/TestData/{county}_{state}/NPMRDS Data/{state}_{county}_{npmrds_year}_PASSENGER.csv'
        pl_npmrds_pass.config(text=fn_npmrds_pass.replace('/','\\'))
        fn_npmrds_truck = f'H:/TestData/{county}_{state}/NPMRDS Data/{state}_{county}_{npmrds_year}_TRUCKS.csv'
        pl_npmrds_truck.config(text=fn_npmrds_truck.replace('/','\\'))
        fn_npmrds_tmc = f'H:/TestData/{county}_{state}/NPMRDS Data/TMC_Identification.csv'
        pl_npmrds_tmc.config(text=fn_npmrds_tmc.replace('/','\\'))
        fn_output = f'H:/DANATool/Outputs/TESTNEW_20240709'
        pl_output_folder.config(text=fn_output.replace('/','\\'))

        fn_tmc_config = fn_npmrds_tmc
        medwidth.set(6)
        pl_tmc_config.config(text=fn_tmc_config.replace('/','\\'))
        proc_tmc_file()

        #fn_npmrds_shp = 'C:/Users/William.Chupp/Documents/DANAToolTesting/FHWA-DANATool/Default Input Files/National TMC Shapefile/NationalMerge.shp'
        #pl_npmrds_shp.config(text=fn_npmrds_shp.replace('/','\\'))
    
    ##################################################
    
    # 6. Button
    #ttk.Button(mainframe, text="Process Data", command=ProcessData).grid(column=0, row=35, columnspan=4)
    
    ##################################################
    # pad each widget globally
    for child in mainframe.winfo_children(): child.grid_configure(padx=2, pady=4)
    
    
    enable_tmas_preprocess()

    old_stdout = sys.stdout
    old_stderr = sys.stderr
    redir = RedirectText(thread_queue)
    sys.stdout = redir
    sys.stderr = redir
    root.update_idletasks()
    bind_tree(main_container, "<MouseWheel>", main_mouse_wheel)
    bind_tree(TMCSelect_container, "<MouseWheel>", tmcselect_mouse_wheel)
    bind_tree(tnmaide, "<MouseWheel>", tnmaide_mouse_wheel)
    bind_tree(vis, "<MouseWheel>", vis_mouse_wheel)
    canvasScrollRegion = (0, 0, 
                          max(canvas.winfo_width()-4, canvas.bbox('all')[2]),
                          max(canvas.winfo_height()-4, canvas.bbox('all')[3]))
    canvas.configure(scrollregion=canvasScrollRegion)
    TMCcanvasScrollRegion = (0, 0, 
                          max(TMCSelect_canvas.winfo_width()-4, TMCSelect_canvas.bbox('all')[2]-1),
                          max(TMCSelect_canvas.winfo_height()-4, TMCSelect_canvas.bbox('all')[3]-1))
    TMCSelect_canvas.configure(scrollregion=TMCcanvasScrollRegion)
    tnmaide_canvasScrollRegion = (0, 0, 
                          max(tnmaide_canvas.winfo_width()-4, tnmaide_canvas.bbox('all')[2]-1),
                          max(tnmaide_canvas.winfo_height()-4, tnmaide_canvas.bbox('all')[3]-1))
    tnmaide_canvas.configure(scrollregion=tnmaide_canvasScrollRegion)
    vis_canvasScrollRegion = (0, 0, 
                          max(vis_canvas.winfo_width()-4, vis_canvas.bbox('all')[2]-1),
                          max(vis_canvas.winfo_height()-4, vis_canvas.bbox('all')[3]-1))
    vis_canvas.configure(scrollregion=vis_canvasScrollRegion)
    runningThreads = []
    checkProgress()
    if '_PYIBoot_SPLASH' in os.environ and importlib.util.find_spec("pyi_splash"):
        import pyi_splash
        pyi_splash.update_text('UI Loaded ...')
        pyi_splash.close()
    root.mainloop()
    sys.stdout = old_stdout
    for afterid in afterids:
        root.after_cancel(afterid)