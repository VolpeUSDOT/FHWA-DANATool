import pandas as pd
from NERO_Receiver import Receiver
from NERO_Pair_Calculate import RRLPair, Add_Log_Vals
from NERO_RoadLink import RoadLink
import pyproj
import time


# NOTE: link_grade is described as a tuple in TNMPyAide, but it is actually read as a list of floats when used. link_grade_list as such should be a list of lists of floats.

def calc_hourly_totals(receiver, road_link_list, hour = None):
    '''
    Calculate the total SPL at a receiver from all road links

    Optional: Specify a specific hour to run. Default behavior runs for 24 hours

    Returns:
        hourly_totals: Dictionary with hours as key and SPL as values
    '''
    hourly_totals = {}
    for road_link in road_link_list:
        road_link.select_hour(hour)
        pair = RRLPair(road_link=road_link, receiver=receiver)
        hourly_values = pair.calc_Hourly_SPL()
        for key in hourly_values:
            if key in hourly_totals:
                hourly_totals[key] = Add_Log_Vals(hourly_totals[key], hourly_values[key])
            else:
                hourly_totals[key] = hourly_values[key]
    return hourly_totals

def Find_Reference_Worst_Hour(receiver_list, road_link_list):
    '''
    Get total SPL at all receiver locations based on worst hour at reference receiver location

    Returns:
        receiver_SPL_Dict: Dict of SPL values for the worst hour at each receiver
    '''
    # First receiver in list is reference receiver, determine worst hour at this location
    receiver = receiver_list[0]
    hourly_totals = calc_hourly_totals(receiver, road_link_list)
    print(hourly_totals)

    worst_hour = max(hourly_totals, key=hourly_totals.get)
    receiver_worst_hour_data = hourly_totals[worst_hour]

    receiver_SPL_Dict = {receiver: receiver_worst_hour_data}
    if len(receiver_list) == 1:
        return worst_hour, receiver_SPL_Dict
    else:
        for receiver in receiver_list[1:]:
            hourly_totals = calc_hourly_totals(receiver, road_link_list, worst_hour)
            receiver_SPL_Dict[receiver] = hourly_totals[worst_hour]

    return worst_hour, receiver_SPL_Dict

def Find_Absolute_Worst_Hour(receiver_list, road_link_list):
    '''
    Get total SPL at all receiver locations based on aboslute worst hour

    Returns:
        receiver_SPL_Dict: Dict of SPL values for the worst hour at each receiver
    '''
    # Find all totals and worst hour for all receivers
    receivers_all_data = {}
    for receiver in receiver_list:
        hourly_totals = calc_hourly_totals(receiver, road_link_list)
        worst_hour = max(hourly_totals, key=hourly_totals.get)
        receivers_all_data[receiver] = (worst_hour, hourly_totals)

    # Determine absolute worst hour from any receiver of group
    worst_hour = None
    worst_SPL = 0

    for comp in receivers_all_data.values():
        receiver_worst_hour = comp[0]
        if worst_hour != receiver_worst_hour:
            if worst_hour == None:
                worst_hour = receiver_worst_hour
                worst_SPL = comp[1][worst_hour]
            elif worst_SPL < comp[1][receiver_worst_hour]:
                worst_hour = receiver_worst_hour
                worst_SPL = comp[1][worst_hour]
            # No else b/c if it's not worse we don't need to update anything

    # Select data from found worst hour
    receiver_SPL_Dict = {}
    for receiver in receivers_all_data:
        receiver_SPL_Dict[receiver] = receivers_all_data[receiver][1][worst_hour]

    return worst_hour, receiver_SPL_Dict

def Find_Worst_Hour_All_Receivers(receiver_list, road_link_list):
    '''
    get total SPL at all receivers for their individual worst hours

    Returns:
        receiver_SPL_Dict: Dict of SPL values for the worst hour at each receiver
    '''
    # Find all totals and worst hour for all receivers
    receiver_SPL_Dict = {}
    for receiver in receiver_list:
        hourly_totals = calc_hourly_totals(receiver, road_link_list)
        worst_hour = max(hourly_totals, key=hourly_totals.get)
        receiver_SPL_Dict[receiver] = hourly_totals[worst_hour]

    return worst_hour, receiver_SPL_Dict

def convertXYtoLatLong(x, y, offset = (0,0), converted_projection = "EPSG:3857"):

    # Create a transformer object for the desired projection "EPSG:4326"
    transformer = pyproj.Transformer.from_crs(converted_projection, "EPSG:4326")

    # Convert feet to meters
    x = x / 3.28084
    y = y / 3.28084

    # Convert coordinates
    lat, long = transformer.transform(x, y)
    print(lat, long)
    return lat, long



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


    receiver_lat_long_coords = {}
    receiver_lat_long_coords[1] = tuple(convertXYtoLatLong(4967202, 4000683, converted_projection="ESRI:103288"))
    receiver_lat_long_coords[2] = tuple(convertXYtoLatLong(4968385, 4001336, converted_projection="ESRI:103288"))
    receiver_lat_long_coords[3] = tuple(convertXYtoLatLong(4968876, 4001251, converted_projection="ESRI:103288"))

    # print(receiver_lat_long_coords)

    receiver_list = [Receiver(receiver_lat_long_coords[1][1], receiver_lat_long_coords[1][0], receiverName="Receiver 1 - 4607 Crossfield Circle"),
                     Receiver(receiver_lat_long_coords[2][1], receiver_lat_long_coords[2][0], receiverName="Receiver 2 - 4501 Springdale Road"),
                     Receiver(receiver_lat_long_coords[3][1], receiver_lat_long_coords[3][0], receiverName="Receiver 3 - Crowne Apts at Springdale")]

    # receiver_list = [Receiver(-85.581518, 38.312084, receiverName="Receiver1"), Receiver(-85.588119, 38.310498, receiverName="Receiver2"), Receiver(-85.577600, 38.314593, receiverName="Receiver3")]

    grouped = df.groupby('tmc', sort=False)
    list_of_links = []
    for tmc, df_DANA in grouped:
        rs = RoadLink(df_DANA, geo_df.loc[tmc], link_name=tmc)
        rs.setProjection("ESRI:103288")
        list_of_links.append(rs)
    # start = time.time()
    # receiver_SPL_Dict = Find_Reference_Worst_Hour(receiver_list, list_of_links)
    # end = time.time()
    # print(end - start)


    for receiver in receiver_list:
        receiver.setProjection("ESRI:103288")

    start = time.time()
    worst_hour, receiver_SPL_Dict = Find_Absolute_Worst_Hour(receiver_list, list_of_links)
    print("Worst Hour: " + str(worst_hour))
    for receiver, spl in receiver_SPL_Dict.items():
        print(receiver.name + ": x = " + str(receiver.x) + " y = " + str(receiver.y))
        print("SPL: " + str(spl))
    end = time.time()
    print("time to compute:")
    print(end - start)
    print("Number of Raod Links:")
    print(len(list_of_links))

    tnmReceiverImportdf = None
    for receiver in receiver_list:
        df_line = receiver.createTNMImportDF()
        if not isinstance(tnmReceiverImportdf, pd.DataFrame):
            tnmReceiverImportdf = df_line
        else:
            tnmReceiverImportdf = pd.concat([tnmReceiverImportdf, df_line])

    tnmRoadLinkImportdf = None
    for roadLink in list_of_links:
        roadLink.select_hour(worst_hour)
        df_link = roadLink.createTNMImportDF()
        if not isinstance(tnmRoadLinkImportdf, pd.DataFrame):
            tnmRoadLinkImportdf = df_link
        else:
            tnmRoadLinkImportdf = pd.concat([tnmRoadLinkImportdf, df_link])

    tnmReceiverImportdf.to_csv("receiver_nero_to_TNM.csv", index=False)
    tnmRoadLinkImportdf.to_csv("road_link_nero_to_TNM.csv", index=False)
