"""
GUI interface to create the 'input.txt' file used to select TMC links in the NTD Dataset
Author: Cambridge Systematics
"""
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Polygon, LineString
import os
from tkinter import *
from tkinter import Tk,ttk,StringVar,filedialog
import xml.etree.ElementTree as ET


fn_tmc_config = ''
fn_kml = ''
counties = ['']
roads = ['']
directions = ['']
kmlfiles = ['']

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
    #kmlfiles = os.listdir('KML Polygon/')
    #kmlfiles.insert(0,"")
    
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

def PrintResults(tmc_list):
    
    text = open('FinalTMC_Selection_Results.txt', 'w')
    for i in tmc_list:
        text.write(i+',')
    text.close()

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
        PrintResults(selected_tmc)
        PopUpNoSelection()
    else:
        PrintResults(selected_tmc)
        root.destroy()

#We create the GUI
root = Tk()
root.title("Data Selection Tool")
TMCSelection_frame = ttk.Frame(root)
TMCSelection_frame.grid(sticky=(N, W, E, S))
TMCSelection_frame.columnconfigure(0, weight=1)
TMCSelection_frame.rowconfigure(0, weight=1)

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

ttk.Button(TMCSelection_frame, text="Select Data", command=SelectData).grid(column=0, row=7, columnspan=5)

for child in TMCSelection_frame.winfo_children(): child.grid_configure(padx=2, pady=4)

root.mainloop()