from lib import NTD_00_TMAS
from lib import NTD_01_NPMRDS
from lib import NTD_02_MOVES
from lib import NTD_03_SPEED
from lib import call_TNMAide
from lib import load_shapes
import os
import sys
import datetime as dt
import matplotlib.pyplot as plt

NTD_01_NPMRDS.NPMRDS(
    'TX', 
    r'H:\DANATool\ValVerdeTexas_Run_ROADII\TMC_Identification.csv', 
    r'H:\DANATool\ValVerdeTexas_Run_ROADII\ValVerdeTexas_2022_ALL.csv', 
    r'H:\DANATool\ValVerdeTexas_Run_ROADII\ValVerdeTexas_2022_PASS.csv', 
    r'H:\DANATool\ValVerdeTexas_Run_ROADII\ValVerdeTexas_2022_TRUCKS.csv', 
    r"C:\Users\William.Chupp\OneDrive - DOT OST\DANA Version 2.1\DANA Tool Final Packages\Default Input Files\National_Default_Roadway_Operating_Speed.csv",
    r"C:\Users\William.Chupp\OneDrive - DOT OST\DANA Version 2.1\DANA Tool Final Packages\Default Input Files\NEI2017_RepresentativeEmissionsRates.parquet", 
    r"C:\Users\William.Chupp\OneDrive - DOT OST\DANA Version 2.1\DANA Tool Final Packages\Default Input Files\TMAS Data\TMAS 2022\TMAS_Station_2022.csv", 
    r"C:\Users\William.Chupp\OneDrive - DOT OST\DANA Version 2.1\DANA Tool Final Packages\Default Input Files\TMAS Data\TMAS 2022\TMAS_Class_Clean_2022.csv", 
    r"C:\Users\William.Chupp\OneDrive - DOT OST\DANA Version 2.1\DANA Tool Final Packages\Default Input Files\FIPS_County_Codes.csv", 
    r"C:\Users\William.Chupp\OneDrive - DOT OST\DANA Version 2.1\DANA Tool Final Packages\Default Input Files\NEI2017_RepresentativeCounties.csv",
    'H:\DANATool\ValVerdeTexas_Run_ROADII')