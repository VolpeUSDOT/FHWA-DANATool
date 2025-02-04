import sys
sys.path.append('C:/Users/andrew.decandia/Documents/NERO Dev/FHWA-DANATool/lib/TNMPyAide')
from DANA_Noise_Data import DANA_Noise_Data as DND
import regex as re
import pandas as pd
import pyproj

# TODO: Throw a real error when multiple tmcs are passed
class RoadLink():
    '''
    NERO Road segment class designed to handle ONE TMC and related geometry. Do not create with paired links
    '''

    def __init__(self, df_DANA, geometry, link_grade=None, link_name="RoadLink"):
        self.df_DANA = df_DANA
        self.geometry = self.convert_geometry(geometry)
        self.TNMGeometry = self.convert_geometry_TNM(geometry)
        self.name = link_name

        num_links = len(list(pd.unique(self.df_DANA['tmc'])))
        if num_links > 1:
            print('Error, RoadLink Object is intended for a single TMC / Road Link')
            return

        if link_grade == None:
            self.link_grade = 0
        else:
            if check_Link_Grade(link_grade):
                self.link_grade = link_grade
            else:
                print('Error, Link Grade passed is invalid. Pass a float between -8.0 and 8.0. Proceeding with link grade of 0')
                self.link_grade = 0

        dndobj = DND(self.df_DANA, self.link_grade, robust_speeds = False)
        self.df_traffic_noise = dndobj.df_Traffic_Noise

    def convert_geometry(self, geometry):
        '''
        Separates out the geometry data into a list of sections of the segment for later processing
        '''
        if isinstance(geometry, str):
            geo_str = geometry
        else:
            geo_str = geometry.values[0]
        line_segs = re.findall(r'(?:,\s{1}|\()(.+?,\s.+?)(?:,\s|\))', geo_str, overlapped=True)
        for i, line in enumerate(line_segs):
            line_segs[i] = line.split(", ")
            for k, point in enumerate(line_segs[i]):
                line_segs[i][k] = tuple(float(s) for s in point.split(' '))
        return line_segs

    def convert_geometry_TNM(self, geometry):
        '''
        Separates out the geometry data into a list of points for later processing
        '''
        if isinstance(geometry, str):
            geo_str = geometry
        else:
            geo_str = geometry.values[0]
        line_segs = re.findall(r'(?:,\s{1}|\()(.+?)(?:,\s|\))', geo_str)
        for i, point in enumerate(line_segs):
            line_segs[i] = tuple(float(s) for s in point.split(' '))

        return line_segs

    def select_hour(self, hour):
        if hour != None:
            self.df_DANA = self.df_DANA.loc[(self.df_DANA['HOUR'] == hour)]
            self.df_traffic_noise = self.df_traffic_noise.loc[(self.df_traffic_noise['HOUR'] == hour)]

    def createTNMImportDF(self, offset = (0,0), target_projection = "EPSG:3857"):
        '''
        function to calculate the x, y coords from lat-long, apply an offest to match to a different projection if applicable

        Accepts:
            offset - A tuple of x and y offset to allow projection to be shifted to match TNM projection, default is (0,0)
            target_projection - A string that represents the desired map projection out, default is web mercator

        Returns:
            A Pandas df containing one row per geometry segment with info about this road link

        '''
        # Create lat_long DF
        df_geom = pd.DataFrame(self.TNMGeometry, columns=["X", "Y"])
        # Create a transformer object for the desired projection
        transformer = pyproj.Transformer.from_crs("EPSG:4326", target_projection)
        # Convert coordinates
        df_geom["X"], df_geom["Y"] = transformer.transform(df_geom["Y"], df_geom["X"])

        # Convert meters to feet
        df_geom["X"] = df_geom["X"] * 3.28084
        df_geom["Y"] = df_geom["Y"] * 3.28084

        # Apply offest
        df_geom["X"] = df_geom["X"] + offset[0]
        df_geom["Y"] = df_geom["Y"] + offset[1]

        # Make a new df of the correct size
        len_geom = len(self.TNMGeometry)

        vehicle_type_dict = {"Auto": 'at',
                             "MediumTruck": 'mt',
                             "HeavyTruck": 'ht',
                             "Bus": 'bus',
                             "Motorcycle": 'mc'}

        for long_name, vehicle_type in vehicle_type_dict.items():

            traffic_vol_col_name = 'MAADT_L1'

            perc_col_name = 'PCT_' + vehicle_type.upper() + '_L1'

            if vehicle_type.upper() == 'AT':
                spd_col_name = 'SPD_AT_L1'
            elif vehicle_type.upper() == 'MC':
                spd_col_name = 'SPD_ALL_L1'
            else:
                spd_col_name = 'SPD_HT_L1'
            hourly_traffic_vol = self.df_traffic_noise[traffic_vol_col_name] * \
                self.df_traffic_noise[perc_col_name]

            hourly_traffic_spd = self.df_traffic_noise[spd_col_name]

            vehicle_type_dict[long_name] = (hourly_traffic_vol.mean(), hourly_traffic_spd.mean())
        # print(vehicle_type_dict)

        data = {"Type": ["Roadway"]*len_geom,
        "Name": [self.name]*len_geom,
        "X": df_geom["X"],
        "Y": df_geom["Y"],
        "Z": [1]*len_geom,   # Update to make this setable
        "Width": [12]*len_geom,  # Update to make this setable
        "PavementType": ["Average"]*len_geom,
        "OnStructure": [False]*len_geom,
        "AutoVolume": [vehicle_type_dict['Auto'][0]]*len_geom,
        "AutoSpeedMph": [vehicle_type_dict['Auto'][1]]*len_geom,
        "MediumTruckVolume": [vehicle_type_dict['MediumTruck'][0]]*len_geom,
        "MediumTruckSpeedMph": [vehicle_type_dict['MediumTruck'][1]]*len_geom,
        "HeavyTruckVolume": [vehicle_type_dict['HeavyTruck'][0]]*len_geom,
        "HeavyTruckSpeedMph": [vehicle_type_dict['HeavyTruck'][1]]*len_geom,
        "BusVolume": [vehicle_type_dict['Bus'][0]]*len_geom,
        "BusSpeedMph": [vehicle_type_dict['Bus'][1]]*len_geom,
        "MotorcycleVolume": [vehicle_type_dict['Motorcycle'][0]]*len_geom,
        "MotorcycleSpeedMph": [vehicle_type_dict['Motorcycle'][1]]*len_geom,
        "ControlDevice": ["None"]*len_geom,
        "AffectedVehicles": [100]*len_geom,
        "ConstraintSpeedMph": [0]*len_geom
        }

        roadLinkDF = pd.DataFrame(data)

        return roadLinkDF


def check_Link_Grade(link_grade):
    '''
    Returns:
        boolean True if valid False if invalid
    '''
    if link_grade <= 8 and link_grade >= -8:
        return True
    return False

if __name__ == "__main__":
    filename = '../KY_Composite_Emissions_SUMMARY.csv'
    geo_df = pd.read_csv(filename)
    geo_df = geo_df[['tmc', 'geometry']]
    geo_df = geo_df.set_index('tmc')

    filename = '../KY_Composite_Emissions.parquet'
    df = pd.read_parquet(filename)
    # Make date column out of separate day, month, year cols
    df['date'] = df['month'].astype(str) + '/' + df['day'].astype(str) + '/' + df['year'].astype(str)
    # Trim cols to match unit tests
    df = df[['tmc', 'date', 'hour', 'road', 'direction', 'state', 'county',
           'start_latitude', 'start_longitude', 'end_latitude', 'end_longitude',
           'tmc_length', 'road_order', 'f_system', 'thrulanes', 'MAADT', 'aadt',
           'aadt_singl', 'aadt_combi', 'travel_time_all', 'travel_time_pass',
           'travel_time_truck', 'speed_all', 'speed_pass', 'speed_truck',
           'PCT_NOISE_AUTO', 'PCT_NOISE_MED_TRUCK', 'PCT_NOISE_HVY_TRUCK',
           'PCT_NOISE_BUS', 'PCT_NOISE_MC']]

    # print(pd.unique(df["tmc"]))
    grouped = df.groupby('tmc', sort=False)
    df_DANA = grouped.get_group('121N04350')

    rl = RoadLink(df_DANA, geo_df.loc['121N04350'])

    rl.createTNMImportDF()
