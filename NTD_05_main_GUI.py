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
    label = ttk.Label(popup, text="Please run Step 1 to create processed TMAS File.")
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
step0 = '0. Pre-Process Raw TMAS Data'
step1 = '1. Process Raw NPMRDS Data'
step2 = '2. Produce MOVES Inputs'
step3 = '3. Produce Speed Distributions'
step4 = '4. Produce Noise Inputs'

# Func - File dialogs    
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
        filetypes=[('csv file', '.csv')])
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
        filetypes=[('csv file', '.csv')])
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
    fn_nei = filedialog.askopenfilename(parent=root, initialdir=os.getcwd(),title='Choose National Emission Inventory File',
        filetypes=[('xlsx file', '.xlsx')])
    pl_nei_1.config(text=fn_nei.replace('/','\\'))
    pl_nei_2.config(text=fn_nei.replace('/','\\'))

# Enable TMAS Preprocessing
def enable_tmas_preprocess():
    global list_scripts
    if preprocess_checkvar.get() == 0:
        w_tmas_station.grid_remove()
        w_tmas_class.grid_remove()
        w_fips_1.grid_remove()
        w_nei_1.grid_remove()
        
        pl_tmas_station.grid_remove()
        pl_tmas_class.grid_remove()
        pl_fips_1.grid_remove()
        pl_nei_1.grid_remove()
        
        list_scripts = ['', step1, step2, step3, step4]
        w_script['values'] = list_scripts

    else:
        w_tmas_station.grid()
        w_tmas_class.grid()
        w_fips_1.grid()
        w_nei_1.grid()
        
        pl_tmas_station.grid()
        pl_tmas_class.grid()
        pl_fips_1.grid()
        pl_nei_1.grid()
        
        list_scripts = ['', step0, step1, step2, step3, step4]
        w_script['values'] = list_scripts    
        
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
        output_text.insert(tk.END, thread_queue.get())
    
    root.update_idletasks()
    canvasScrollRegion = (0, 0, 
                          max(canvas.winfo_width()-4, canvas.bbox('all')[2]),
                          max(canvas.winfo_height()-4, canvas.bbox('all')[3]))
    canvas.configure(scrollregion=canvasScrollRegion)
    TMCcanvasScrollRegion = (0, 0, 
                          max(TMCSelect_canvas.winfo_width()-4, TMCSelect_canvas.bbox('all')[2]-1),
                          max(TMCSelect_canvas.winfo_height()-4, TMCSelect_canvas.bbox('all')[3]-1))
    TMCSelect_canvas.configure(scrollregion=TMCcanvasScrollRegion)
    
    removeList = []
    for i in range(len(runningThreads)):
        if not runningThreads[i].is_alive():
            removeList.append(i)
        else:
            statusLabel["text"] = "Step {} is currently running".format(runningThreads[i].name)
    for i in removeList:
        print("*** {} has finished running ***".format(runningThreads[i].name))
        del runningThreads[i]
        startButton["state"] = NORMAL
        statusLabel["text"] = "No Process Currently Running"
    
    root.after(100, checkProgress)
        

def process_handler(proc_target, thread_queue, args): 
    redir = RedirectText(thread_queue)
    sys.stdout = redir
    #sys.stderr = sys.stdout
    try:
        proc_target(*args)
    except Exception as e:
        print(traceback.format_exc())            
    
# Func - ProcessData Function
def ProcessData():
    global runningThreads
    #output_text.delete('1.0', END)
    tmc_chars = set('0123456789+-PN, ')
    if StateValue.get() == '':
        PopUp_Selection("State")
    elif ScriptValue.get() == step0:
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
            TMAS_Proc = mp.Process(target=process_handler, name=step0, args=(NTD_00_TMAS.TMAS, thread_queue, (SELECT_STATE, PATH_TMAS_STATION, PATH_TMAS_CLASS, PATH_FIPS, PATH_NEI)))
            startButton["state"] = DISABLED
            TMAS_Proc.start()
            runningThreads.append(TMAS_Proc)
            notebook.select('.!notebook.!frame2')
            
    elif ScriptValue.get() == step1:
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
            NPMRDS_Proc = mp.Process(target=process_handler, name=step1, args=(NTD_01_NPMRDS.NPMRDS, thread_queue, (SELECT_STATE, PATH_tmc_identification, PATH_npmrds_raw_all, PATH_npmrds_raw_pass, PATH_npmrds_raw_truck, PATH_emission, PATH_TMAS_STATION, PATH_TMAS_CLASS_CLEAN, PATH_FIPS, PATH_NEI)))
            startButton["state"] = DISABLED
            NPMRDS_Proc.start()
            runningThreads.append(NPMRDS_Proc)
            notebook.select('.!notebook.!frame2')
    
    elif ScriptValue.get() == step2:
        if fn_npmrds_clean == '':
            PopUpCleanNPMRDSSelection()
        else:
            SELECT_STATE = StateValue.get()
            PATH_NPMRDS = fn_npmrds_clean
            PATH_HPMS = fn_hpms
            PATH_VM2 = fn_vm2
            PATH_COUNTY_MILEAGE = fn_county_mileage
            MOVES_Proc = mp.Process(target=process_handler, name=step2, args=(NTD_02_MOVES.MOVES, thread_queue, (SELECT_STATE, PATH_NPMRDS, PATH_HPMS, PATH_VM2, PATH_COUNTY_MILEAGE)))
            startButton["state"] = DISABLED
            MOVES_Proc.start()
            runningThreads.append(MOVES_Proc)
            notebook.select('.!notebook.!frame2')
            
    elif ScriptValue.get() == step3:    # needs update
        SELECT_STATE = StateValue.get()
        if fn_npmrds_clean == '':
            PopUpCleanNPMRDSSelection()
        else:
            PATH_NPMRDS = fn_npmrds_clean
            SPEED_Proc = mp.Process(target=process_handler, name=step3, args=(NTD_03_SPEED.SPEED, thread_queue, (SELECT_STATE, PATH_NPMRDS)))
            startButton["state"] = DISABLED
            SPEED_Proc.start()
            runningThreads.append(SPEED_Proc)
            notebook.select('.!notebook.!frame2')

    elif ScriptValue.get() == step4:    # needs update
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
            NOISE_Proc = mp.Process(target=process_handler, name=step4, args=(NTD_04_NOISE.NOISE, thread_queue, (SELECT_STATE, SELECT_TMC, PATH_NPMRDS)))
            startButton["state"] = DISABLED
            NOISE_Proc.start()
            runningThreads.append(NOISE_Proc)
            notebook.select('.!notebook.!frame2')
            
    else:
        PopUp_Selection("Step")
    #root.destroy()
    
def CancelProcess():
    global runningThreads
    for proc in runningThreads:
        while proc.is_alive():
            proc.terminate()
        
        if not proc.is_alive():
            print("************* Canceled Step {} **************".format(proc.name))
            startButton["state"] = NORMAL
            statusLabel["text"] = "No Process Currently Running"

    runningThreads = []
    

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
    outputpath = 'Final Output/TMC_Selection/'
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
    else:
        return "break"

################################################################################

root = Tk()
root.title("DANA Tool")
root.grid_rowconfigure(0, weight=1)
root.grid_columnconfigure(0, weight=1)

iconPath = resource_path('lib\\dot.png')
p1 = tk.PhotoImage(file = iconPath)
root.iconphoto(True, p1)

notebook = ttk.Notebook(root, width=1000, height=700)
notebook.grid(row=0, column=0, sticky="news")

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

output_container = tk.Frame(notebook)
output_container.grid(row=0, column=0, sticky="news")
output_container.grid_rowconfigure(0, weight=1)
output_container.grid_columnconfigure(0, weight=1)
notebook.add(output_container, text='Progress Log')

output_text = tk.Text(output_container, height=400, width=80,  wrap=WORD)
output_text.grid(row=0, column=0, sticky="news")
output_scrollbar = tk.Scrollbar(output_container, orient="vertical", command=output_text.yview)
output_scrollbar.grid(row=0, column=1, sticky='ns')
output_text.configure(yscrollcommand = output_scrollbar.set)
output_text.bind("<MouseWheel>", output_mouse_wheel)
output_text.bind("<Key>", lambda e: ctrlEvent(e))

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

# checkbox for process 1

preprocess_checkvar = IntVar()
preprocess_checkvar.set(False)
preprocess_tmas_checkbox = ttk.Checkbutton(mainframe, text=step0, variable=preprocess_checkvar, command=enable_tmas_preprocess)
preprocess_tmas_checkbox.grid(row=6,column=0, columnspan=1, sticky="w")

##################################################

#1. Header
ttk.Label(mainframe, wraplength = 500, text='To select the desired script and inputs').grid(row=0, column=0, columnspan= 1, sticky="w")
ttk.Label(mainframe, text='Select State:').grid(row=1,column=0, columnspan=1, sticky="w")
ttk.Label(mainframe, text='Select Processing Steps:').grid(row=2,column=0, columnspan=1, sticky="w")
ttk.Label(mainframe, text='Select inputs under the selected step and press the process data button.  Repeat for each step that you want to run.').grid(row=3,column=0, columnspan=3, sticky="w")
startButton = ttk.Button(mainframe, text="Process Data", command=ProcessData)
startButton.grid(column=0, row=4, columnspan=1 ,sticky="w")
cancelButton = ttk.Button(mainframe, text="Cancel Data Processing", command=CancelProcess).grid(column=0, row=4, columnspan=1 ,sticky="E")
statusLabel = ttk.Label(mainframe, text="No Process Currently Running", relief=SUNKEN)
statusLabel.grid(column=1, row=4, columnspan=1, sticky="W")

ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=5,column=0, columnspan=5, sticky="ew")
#ttk.Label(mainframe, text=step0).grid(row=6,column=1, columnspan=1, sticky="w")
ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=11,column=0, columnspan=5, sticky="ew")
ttk.Label(mainframe, text=step1).grid(row=12,column=0, columnspan=1, sticky="w")
ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=23,column=0, columnspan=5, sticky="ew")
ttk.Label(mainframe, text=step2).grid(row=24,column=0, columnspan=1, sticky="w")
ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=29,column=0, columnspan=5, sticky="ew")
ttk.Label(mainframe, text=step3).grid(row=30,column=0, columnspan=1, sticky="w")
ttk.Separator(mainframe, orient=HORIZONTAL).grid(row=32,column=0, columnspan=5, sticky="ew")
ttk.Label(mainframe, text=step4).grid(row=33,column=0, columnspan=1, sticky="w")

ttk.Label(mainframe, text=' Enter TMC Codes (separate by comma)').grid(row=35, column=0, columnspan=1, sticky="w")
ttk.Label(mainframe, text='          ').grid(row=36,column=0, columnspan=1, sticky="w")

##################################################

# 2. Combobox
# List of States Combobox
list_states = ['','AL','AZ','AR','CA','CO','CT','DE','DC','FL','GA','ID','IL','IN','IA','KS','KY','LA',
               'ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']
StateValue = StringVar()
w_state = ttk.Combobox(mainframe, textvariable=StateValue, state='readonly', width=60)
w_state['values'] = list_states
w_state.current(0)
w_state.grid(column=1, row=1, columnspan=1, sticky="w")
# List of Scripts Combobox
list_scripts = ['', step0, step1, step2, step3, step4]
ScriptValue = StringVar()
w_script = ttk.Combobox(mainframe, textvariable=ScriptValue, state='readonly', width=60)
w_script['values'] = list_scripts
w_script.current(0)
w_script.grid(column=1, row=2, columnspan=1, sticky="w")
##################################################

# 3. File Browsing Buttons
# script 0
w_tmas_station = ttk.Button(mainframe, text='Select TMAS Station File', command=f_tmas_station)
w_tmas_station.grid(column=0, row=7, columnspan=1, sticky="w")
w_tmas_class = ttk.Button(mainframe, text='Select TMAS Class File', command=f_tmas_class)
w_tmas_class.grid(column=0, row=8, columnspan=1, sticky="w")
w_fips_1 = ttk.Button(mainframe, text='Select FIPS File', command=f_fips)
w_fips_1.grid(column=0, row=9, columnspan=1, sticky="w")
w_nei_1 = ttk.Button(mainframe, text='Select NEI Representative Counties', command=f_nei)
w_nei_1.grid(column=0, row=10, columnspan=1, sticky="w")

# script 1
w_tmas_station_state_1 = ttk.Button(mainframe, text='Select Processed TMAS Station', command=f_tmas_station_state).grid(column=0, row=13, columnspan=1, sticky="w")
w_tmas_class_clean_1 = ttk.Button(mainframe, text='Select Processed TMAS Class', command=f_tmas_class_clean).grid(column=0, row=14, columnspan=1, sticky="w")
w_npmrds_all = ttk.Button(mainframe, text='Select NPMRDS (All)', command=f_npmrds_all).grid(column=0, row=15, columnspan=1, sticky="w")
w_npmrds_pass = ttk.Button(mainframe, text='Select NPMRDS (Passenger)', command=f_npmrds_pass).grid(column=0, row=16, columnspan=1, sticky="w")
w_npmrds_truck = ttk.Button(mainframe, text='Select NPMRDS (Truck)', command=f_npmrds_truck).grid(column=0, row=17, columnspan=1, sticky="w")
w_npmrds_tmc = ttk.Button(mainframe, text='Select TMC Configuration', command=f_npmrds_tmc).grid(column=0, row=18, columnspan=1, sticky="w")
#w_npmrds_shp = ttk.Button(mainframe, text='Select TMC shapefile', command=f_npmrds_shp).grid(column=0, row=19, columnspan=1, sticky="w")
w_emission = ttk.Button(mainframe, text='Select Emission Rates', command=f_emission).grid(column=0, row=20, columnspan=1, sticky="w")
w_fips_2 = ttk.Button(mainframe, text='Select FIPS File', command=f_fips).grid(column=0, row=21, columnspan=1, sticky="w")
w_nei_2 = ttk.Button(mainframe, text='Select NEI Representative Counties', command=f_nei).grid(column=0, row=22, columnspan=1, sticky="w")

# script 2
#w_tmas_station_state_2 = ttk.Button(mainframe, text='Select Processed TMAS Station', command=f_tmas_station_state).grid(column=0, row=19, columnspan=1, sticky="w")
w_npmrds_clean_1 = ttk.Button(mainframe, text='Select Processed NPMRDS', command=f_npmrds_clean).grid(column=0, row=25, columnspan=1, sticky="w")
w_hpms = ttk.Button(mainframe, text='Select HPMS', command=f_hpms).grid(column=0, row=26, columnspan=1, sticky="w")
w_vm2 = ttk.Button(mainframe, text='Select VM2', command=f_vm2).grid(column=0, row=27, columnspan=1, sticky="w")
w_county_mileage = ttk.Button(mainframe, text='Select County Mileage file', command=f_county_mileage).grid(column=0, row=28, columnspan=1, sticky="w")

# script 3
w_npmrds_clean_2 = ttk.Button(mainframe, text='Select Processed NPMRDS', command=f_npmrds_clean).grid(column=0, row=31, columnspan=1, sticky="w")

# script 4
w_npmrds_clean_3 = ttk.Button(mainframe, text='Select Processed NPMRDS', command=f_npmrds_clean).grid(column=0, row=34, columnspan=1, sticky="w")
tmc_selection_button = ttk.Button(mainframe, text = 'TMC Selection Tool', command=lambda: notebook.select('.!notebook.!frame3')).grid(column=1, row=33, columnspan=1, sticky="w")
# Entry
tmcEntry = StringVar()
ttk.Entry(mainframe, textvariable=tmcEntry).grid(column=1, row=35, columnspan=1, sticky="ew")
##################################################

# 4. Pathlabels
# script 0
pl_tmas_station = ttk.Label(mainframe)
pl_tmas_station.grid(column=1, row=7, columnspan=1, sticky="w")
pl_tmas_class = ttk.Label(mainframe)
pl_tmas_class.grid(column=1, row=8, columnspan=1, sticky="w")
pl_fips_1 = ttk.Label(mainframe)
pl_fips_1.grid(column=1, row=9, columnspan=1, sticky="w")
pl_nei_1 = ttk.Label(mainframe)
pl_nei_1.grid(column=1, row=10, columnspan=1, sticky="w")
# script 1
pl_tmas_station_state_1 = ttk.Label(mainframe)
pl_tmas_station_state_1.grid(column=1, row=13, columnspan=1, sticky="w")
pl_tmas_class_clean_1 = ttk.Label(mainframe)
pl_tmas_class_clean_1.grid(column=1, row=14, columnspan=1, sticky="w")
pl_npmrds_all = ttk.Label(mainframe)
pl_npmrds_all.grid(column=1, row=15, columnspan=1, sticky="w")
pl_npmrds_pass = ttk.Label(mainframe)
pl_npmrds_pass.grid(column=1, row=16, columnspan=1, sticky="w")
pl_npmrds_truck = ttk.Label(mainframe)
pl_npmrds_truck.grid(column=1, row=17, columnspan=1, sticky="w")
pl_npmrds_tmc = ttk.Label(mainframe)
pl_npmrds_tmc.grid(column=1, row=18, columnspan=1, sticky="w")
#pl_npmrds_shp = ttk.Label(mainframe)
#pl_npmrds_shp.grid(column=1, row=19, columnspan=1, sticky="w")
pl_emission = ttk.Label(mainframe)
pl_emission.grid(column=1, row=20, columnspan=1, sticky="w")
pl_fips_2 = ttk.Label(mainframe)
pl_fips_2.grid(column=1, row=21, columnspan=1, sticky="w")
pl_nei_2 = ttk.Label(mainframe)
pl_nei_2.grid(column=1, row=22, columnspan=1, sticky="w")
# script 2
#pl_tmas_station_state_2 = ttk.Label(mainframe)
#pl_tmas_station_state_2.grid(column=1, row=19, columnspan=1, sticky="w")
pl_npmrds_clean_1 = ttk.Label(mainframe)
pl_npmrds_clean_1.grid(column=1, row=25, columnspan=1, sticky="w")
pl_hpms = ttk.Label(mainframe)
pl_hpms.grid(column=1, row=26, columnspan=1, sticky="w")
pl_vm2 = ttk.Label(mainframe)
pl_vm2.grid(column=1, row=27, columnspan=1, sticky="w")
pl_county_mileage = ttk.Label(mainframe)
pl_county_mileage.grid(column=1, row=28, columnspan=1, sticky="w")
# script 3
pl_npmrds_clean_2 = ttk.Label(mainframe)
pl_npmrds_clean_2.grid(column=1, row=31, columnspan=1, sticky="w")
# script 4
pl_npmrds_clean_3 = ttk.Label(mainframe)
pl_npmrds_clean_3.grid(column=1, row=34, columnspan=1, sticky="w")
##################################################

# 5. Check available pre-processed files
# TMAS station
pl_tmas_station_state_1.config(text='')
#pl_tmas_station_state_2.config(text='')

# TMAS Class
pl_tmas_class_clean_1.config(text='')

defaultpath = 'Default Input Files/'
pathlib.Path(defaultpath).mkdir(exist_ok=True) 
outputpath = 'Final Output/'
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
if ('NEI2017_RepresentativeEmissionsRates.csv' in os.listdir('Default Input Files/')):
    pl_emission.config(text=os.getcwd()+'\\Default Input Files\\NEI2017_RepresentativeEmissionsRates.csv')
    fn_emission = 'Default Input Files/NEI2017_RepresentativeEmissionsRates.csv'
else:
    pl_emission.config(text='')
       
if False:
    w_state.current(22)
    fn_tmas_station = 'C:/Users/William.Chupp/Documents/DANAToolTesting/FHWA-DANATool/Default Input Files/TMAS Data/TMAS 2017/TMAS_Station_2017.csv'
    pl_tmas_station_state_1.config(text=fn_tmas_station.replace('/','\\'))
    fn_tmas_class_clean = 'C:/Users/William.Chupp/Documents/DANAToolTesting/FHWA-DANATool/Default Input Files/TMAS Data/TMAS 2017/TMAS_Class_Clean_2017.csv'
    pl_tmas_class_clean_1.config(text=fn_tmas_class_clean.replace('/','\\'))
    fn_npmrds_all = 'C:/Users/William.Chupp/Documents/DANAToolTesting/FHWA-DANATool/User Input Files/Example_MiddlesexCounty_Massachusetts/2018 NPMRDS Data/MA_MIDDLESEX_2018_ALL.csv'
    pl_npmrds_all.config(text=fn_npmrds_all.replace('/','\\'))
    fn_npmrds_pass = 'C:/Users/William.Chupp/Documents/DANAToolTesting/FHWA-DANATool/User Input Files/Example_MiddlesexCounty_Massachusetts/2018 NPMRDS Data/MA_MIDDLESEX_2018_PASSENGER.csv'
    pl_npmrds_pass.config(text=fn_npmrds_pass.replace('/','\\'))
    fn_npmrds_truck = 'C:/Users/William.Chupp/Documents/DANAToolTesting/FHWA-DANATool/User Input Files/Example_MiddlesexCounty_Massachusetts/2018 NPMRDS Data/MA_MIDDLESEX_2018_TRUCKS.csv'
    pl_npmrds_truck.config(text=fn_npmrds_truck.replace('/','\\'))
    fn_npmrds_tmc = 'C:/Users/William.Chupp/Documents/DANAToolTesting/FHWA-DANATool/User Input Files/Example_MiddlesexCounty_Massachusetts/2018 NPMRDS Data/TMC_Identification.csv'
    pl_npmrds_tmc.config(text=fn_npmrds_tmc.replace('/','\\'))
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
    redir = RedirectText(thread_queue)
    sys.stdout = redir
    sys.stderr = sys.stdout
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
