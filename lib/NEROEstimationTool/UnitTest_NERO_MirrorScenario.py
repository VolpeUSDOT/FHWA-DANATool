# Compare with TNMPyAide

from NERO_SPL_Aquisition_Loop import *

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


    grouped = df.groupby('tmc', sort=False)
    for tmc, df_DANA in grouped:
        # print(df_DANA)
        pass

    # mirrored link testing
    segments = "(-85.60000 38.30000, -85.55000 38.30500)"
    test_mirrored_link1 = RoadLink(df_DANA, segments)

    segments = "(-85.55000 38.30500, -85.50000 38.30000)"
    test_mirrored_link2 = RoadLink(df_DANA, segments)

    receiver = Receiver(-85.55000, 38.25000)
    receiver_SPL_Dict = Find_Worst_Hour_All_Receivers([receiver], [test_mirrored_link1])
    print(receiver_SPL_Dict)
    receiver_SPL_Dict = Find_Worst_Hour_All_Receivers([receiver], [test_mirrored_link2])
    print(receiver_SPL_Dict)
    receiver_SPL_Dict = Find_Worst_Hour_All_Receivers([receiver], [test_mirrored_link1, test_mirrored_link2])
    print(receiver_SPL_Dict)
