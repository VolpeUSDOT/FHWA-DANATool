# FHWA's DANA Tool

The Databaser for Air Quality and Noise Analysis (DANA) tool provides traffic-related inputs to the Motor Vehicle Emission Simulator (MOVES) vehicle emissions model and the Traffic Noise Model Aide (TNMAide). DANA provides real-world measurements of traffic conditions for use in environmental analyses. In the past, analysts relied almost exclusively on transportation models to generate base year traffic data, an often-cited shortcoming of transportation emission and noise analyses. By having these data already compiled, environmental analysts are spared the task of assembling the data. 

DANA creates two air quality-related data sets and a combined traffic data for a spreadsheet-based noise tool, TNMAide. The datasets are: 

- Link-Level Data Set — For every NPMRDS link (NHS roadways) for every hour of the year, combined traffic data and emission rates by vehicle type are provided. Due to the large size of this link-level dataset, the process saves the full output as an Apache Parquet file, which users may find difficult to open or use themselves. To make the resulting dataset easier to examine, the tool also outputs a sample of the final dataset consisting of the first 1000 and last 1000 rows data, as well as a summary file consisting of an annual aggregation of data for each roadway link in the final dataset. The summary contains the average speed over the year, the annual average daily traffic, and the cumulative emissions of each of the pollutants included in the emissions data for each link. 

- MOVES County-Level Input Data Set — the following MOVES input types are provided: Average Speed Distribution; Vehicle Type VMT, Road Type Distribution; Hour VM- T Fraction; Day VMT Fraction; and Month VMT Fraction. 

- Traffic data summaries for TNMAide - TNMAide is a separate spreadsheet tool that provides: Single Noise Worst Case Hour Analysis and 24-Hour Traffic Distribution for Noise Analysis based on one year of real-world traffic data provided by the DANA tool.  

## Installation

The fully compiled, standalone DANA tool can be downloaded and installed from FHWA's main page:

[UPDATE LINK](https://www.fhwa.dot.gov/environment/)

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

Contact [David Kall](david.kall@dot.gov) with questions about contributions or permissions. 

## License
[MIT](https://choosealicense.com/licenses/mit/)
