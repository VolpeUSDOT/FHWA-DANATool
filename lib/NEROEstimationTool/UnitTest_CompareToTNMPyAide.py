# Compare with TNMPyAide

from NERO_SPL_Aquisition_Loop import *
from TNMPyAide.TNMPyAide import TNMPyAide
import numpy as np

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


    receiver_list = [Receiver(-85.581518, 38.312084), Receiver(-85.588119, 38.310498), Receiver(-85.577600, 38.314593)]

    # print(pd.unique(df["tmc"]))
    grouped = df.groupby('tmc', sort=False)
    df_DANA = grouped.get_group('121N04350')

    # Make scenario where receiver is 50 ft from road and the entire link is one line segment from end to end
    i = min(df_DANA.index)
    a, b, x, y = df_DANA['start_latitude'][i], df_DANA['start_longitude'][i], df_DANA['end_latitude'][i], df_DANA['end_longitude'][i]
    geom = f'({b} {a}, {y} {x})'
    print(geom)

    test_link = RoadLink(df_DANA, geom)
    segment = test_link.geometry[0]
    receiver = Receiver(-85.586150, 38.313200)

    dist = receiver.Find_Perp_Dist(segment)
    alpha = receiver.Find_Angle_Alpha(segment)
    print("Receiver Distance: " + str(dist))
    print("Receiver Angle: " + str(alpha) + "Radians,   In Degrees: " + str(np.rad2deg(alpha)))
    print("Expected effect of angle on SPL: " + str(10 * np.log10(alpha/np.pi)))

    receiver_SPL_Dict = Find_Worst_Hour_All_Receivers([receiver], [test_link])
    linkResults = TNMPyAide(df_DANA, link_grade=0, meta= 0, median_width=0.0, robust_speeds = False, detailed_log=False)
    print(receiver_SPL_Dict)
    print(linkResults.average_day)




    # For data to importer: data for one hour that you want to compare
    # when converted, geometry can not be overlapping b/c it causes issues with tnm
