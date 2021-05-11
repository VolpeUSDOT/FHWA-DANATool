# FHWA's DANA Tool

The Database for Air Quality and Noise Analysis (DANA) tool provides traffic-related inputs to the Motor Vehicle Emission Simulator (MOVES) vehicle emissions model and the Traffic Noise Model Aide (TNMAide). DANA provides real-world measurements of traffic conditions for use in environmental analyses. In the past, analysts relied almost exclusively on transportation models to generate base year traffic data, an often-cited shortcoming of transportation emission and noise analyses. By having these data already compiled, environmental analysts are spared the task of assembling the data. 

DANA creates two air quality-related data sets and a combined traffic data for a spreadsheet-based noise tool, TNMAide. The datasets are: 

- Link-Level Data Set — Three output files are provided: 1) hour-by-hour detailed output file (parquet format) with combined traffic data and emission rates by vehicle type for each NPMRDS link (NHS roadways); 2) sample output file (csv format) with the first 1000 and last 1000 rows of the detailed output data; and 3) annual summary output file (csv format) with average speed over the year, the annual average daily traffic, and the cumulative emissions over the year of each of the pollutants (CO, NOX, VOC, PM2.5, PM10, CO2, CH4, N2O, and CO2eq) for each NPMRDS link. 

- MOVES County-Level Input Data Set — the following MOVES input types are provided: Average Speed Distribution; Vehicle Type VMT, Road Type Distribution; Hour VMT Fraction; Day VMT Fraction; and Month VMT Fraction. 

- Traffic data summaries for TNMAide - TNMAide is a separate spreadsheet tool that provides: Single Noise Worst Case Hour Analysis and 24-Hour Traffic Distribution for Noise Analysis based on one year of real-world traffic data provided by the DANA tool.  

## Installation

The fully compiled, standalone DANA tool can be downloaded and installed from FHWA's main page:

[https://www.fhwa.dot.gov/environment/air_quality/methodologies/dana](https://www.fhwa.dot.gov/environment/air_quality/methodologies/dana)

A full user guide describing the data inputs and outputs of the DANA tool as well as the usage of the GUI can be found at the above page as well.

If you would like to use the source python scripts in the DANA tool library, you may clone this repository and use the 5 data processing functions defined in the library in your own Python scripts or projects.
```bash
git clone https://github.com/VolpeUSDOT/FHWA-DANATool.git
```

## Usage
To run the graphical user interface from a Python environment:
```bash
python NTD_05_main_GUI.py
```
To use the data processing functions separately:
```python
from lib import NTD_00_TMAS
from lib import NTD_01_NPMRDS
from lib import NTD_02_MOVES
from lib import NTD_03_SPEED
from lib import NTD_04_NOISE
```
See the [RUN_DANA.py](https://github.com/VolpeUSDOT/FHWA-DANATool/blob/main/RUN_DANA.py) file included in this repository for examples of how each processing function is used.

## Built With
- Python on Anaconda
- Tkinter and TTK
- Geopandas
- Apache Parquet

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change. 

Contact [David Kall](mailto:david.kall@dot.gov) with questions about contributions or permissions.

## License
[MIT](https://choosealicense.com/licenses/mit/)
