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
        self.m_map_projection = "EPSG:3857"
        self.geom_offset = (0,0)
        self.name = link_name
        self.df_DANA = df_DANA

        if isinstance(geometry, str):
            self.geo_str = geometry
        else:
            self.geo_str = geometry.values[0]
        self.geometry = self.convert_geometry(self.geo_str)
        self.TNMGeometry = self.convert_geometry_TNM(self.geo_str)

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

    def setGeomOffset(self, offset):
        if self.geom_offset == offset:
            return

        self.geom_offset = offset
        self.geometry = self.convert_geometry(self.geo_str)

    def geomOffset(self):
         return self.geom_offset

    def setProjection(self, map_projection):
        '''
        update projection and x, y coords to match new projection
        '''
        if self.m_map_projection == map_projection:
             return

        self.m_map_projection = map_projection
        self.geometry = self.convert_geometry(self.geo_str)

    def projection(self):
         '''
         returns current map projection of the receiver
         '''
         return self.m_map_projection

    def convert_geometry(self, geometry):
        '''
        Separates out the geometry data into a list of sections of the segment for later processing
        '''
        line_segs = re.findall(r'(?:,\s{1}|\()(.+?,\s.+?)(?:,\s|\))', geometry, overlapped=True)
        for i, line in enumerate(line_segs):
            line_segs[i] = line.split(", ")
            for k, point in enumerate(line_segs[i]):
                temp_tuple = tuple(float(s) for s in point.split(' '))
                x, y = self.convertCoords(temp_tuple[0], temp_tuple[1])
                line_segs[i][k] = (x, y)
        return line_segs

    def convert_geometry_TNM(self, geometry):
        '''
        Separates out the geometry data into a list of points for later processing
        '''
        line_segs = re.findall(r'(?:,\s{1}|\()(.+?)(?:,\s|\))', geometry)
        for i, point in enumerate(line_segs):
            line_segs[i] = tuple(float(s) for s in point.split(' '))

        return line_segs

    def convertCoords(self, lat, long):
        '''
            function to calculate the x, y coords from lat-long, apply an offset to match to a different projection if applicable

            Target Projection is set on the road link object with the setProjection function

            Offset is set with setGeomOffset function

        '''
                # Create a transformer object for the desired projection
        transformer = pyproj.Transformer.from_crs("EPSG:4326", self.m_map_projection)

        crs_obj = pyproj.CRS(self.m_map_projection)
        # print(self.name)
        # print(crs_obj.axis_info)

        # Convert coordinates
        x, y = transformer.transform(long, lat)

        # Convert meters to feet
        x = x * 3.28084
        y = y * 3.28084

        x = x + self.geom_offset[0]
        y = y + self.geom_offset[1]

        return x, y

    def select_hour(self, hour):
        if hour != None:
            self.df_DANA = self.df_DANA.loc[(self.df_DANA['HOUR'] == hour)]
            self.df_traffic_noise = self.df_traffic_noise.loc[(self.df_traffic_noise['HOUR'] == hour)]

    def createTNMImportDF(self):
        '''
        function to calculate the x, y coords from lat-long, apply an offset to match to a different projection if applicable

        Returns:
            A Pandas df containing one row per geometry segment with info about this road link

        '''
        # Create lat_long DF
        df_geom = pd.DataFrame(self.TNMGeometry, columns=["X", "Y"])
        # Create a transformer object for the desired projection
        transformer = pyproj.Transformer.from_crs("EPSG:4326", self.m_map_projection)
        # Convert coordinates
        df_geom["X"], df_geom["Y"] = transformer.transform(df_geom["Y"], df_geom["X"])

        # Convert meters to feet
        df_geom["X"] = df_geom["X"] * 3.28084
        df_geom["Y"] = df_geom["Y"] * 3.28084

        # Apply offset
        df_geom["X"] = df_geom["X"] + self.geom_offset[0]
        df_geom["Y"] = df_geom["Y"] + self.geom_offset[1]

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

    rdf = rl.createTNMImportDF(target_projection="ESRI:103286")
    print(rdf)
