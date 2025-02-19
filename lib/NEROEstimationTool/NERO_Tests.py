import pandas as pd
from NERO_Pair_Calculate import *
from NERO_Receiver import Receiver
from NERO_RoadLink import RoadLink



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
# Merge geodata to df
# df = df.merge(geo_df, on= 'tmc')
receiver_list = [Receiver(-85.581518, 38.312084, "receiver1"), Receiver(-85.588119, 38.310498, "receiver2"), Receiver(-85.577600, 38.314593, "receiver3")]

grouped = df.groupby('tmc', sort=False)
list_of_links = []
for tmc, df_DANA in grouped:
    # TODO: Keep as object instead of string
    #rs = RoadLink(df_DANA, str(geo_df.loc[tmc]))
    rs = RoadLink(df_DANA, '(-85.58886 38.31122, -85.58789 38.3117, -85.58689 38.31122)')
    list_of_links.append(rs)

# list_of_links[0].geometry = list_of_links[0].convert_geometry('')

simplified_link = list_of_links[0]
receiver = receiver_list[0]

print(simplified_link.geometry)
seg_section = simplified_link.geometry[0]
print(seg_section)
dndobj = DND(simplified_link.df_DANA, simplified_link.link_grade, robust_speeds = False)
df_traffic_noise = dndobj.df_Traffic_Noise
print(df_traffic_noise)
df_traffic_noise.to_csv('debug.csv')
alpha = receiver.Find_Angle_Alpha(seg_section)
print(alpha)
distance =receiver.Find_Perp_Dist(seg_section)
print(distance)

relative_attenuation = receiver.Compute_Rel_Attenuation(alpha, distance)
hourly_totals = {}
hours_grouped = df_traffic_noise.groupby(['HOUR'])
for name, group in hours_grouped:
    name = name[0] # Name is in a tuple which we don't want
    # Apply relative attenuation, then find the logarithmic mean for each hour
    mean_val = 10*np.log10(group.filter(regex='SPL_Total.*').assign(value = lambda x: x+relative_attenuation)['value'].apply(lambda x: 10**(x/10)).mean())
    # Add hourly averages of different pieces of road together
    if name in hourly_totals:
        hourly_totals[name] = Add_Log_Vals(hourly_totals[name], mean_val)
    else:
        hourly_totals[name] = mean_val

print(hourly_totals)

relative_attenuation = receiver.Compute_Rel_Attenuation(alpha, distance * 2)
hourly_totals_double_dist = {}
hours_grouped = df_traffic_noise.groupby(['HOUR'])
for name, group in hours_grouped:
    name = name[0] # Name is in a tuple which we don't want
    # Apply relative attenuation, then find the logarithmic mean for each hour
    mean_val = 10*np.log10(group.filter(regex='SPL_Total.*').assign(value = lambda x: x+relative_attenuation)['value'].apply(lambda x: 10**(x/10)).mean())
    # Add hourly averages of different pieces of road together
    if name in hourly_totals_double_dist:
        hourly_totals_double_dist[name] = Add_Log_Vals(hourly_totals_double_dist[name], mean_val)
    else:
        hourly_totals_double_dist[name] = mean_val

print(hourly_totals_double_dist)

relative_attenuation = receiver.Compute_Rel_Attenuation(alpha * 0.5, distance)
hourly_totals_half_alpha = {}
hours_grouped = df_traffic_noise.groupby(['HOUR'])
for name, group in hours_grouped:
    name = name[0] # Name is in a tuple which we don't want
    # Apply relative attenuation, then find the logarithmic mean for each hour
    mean_val = 10*np.log10(group.filter(regex='SPL_Total.*').assign(value = lambda x: x+relative_attenuation)['value'].apply(lambda x: 10**(x/10)).mean())
    # Add hourly averages of different pieces of road together
    if name in hourly_totals_half_alpha:
        hourly_totals_half_alpha[name] = Add_Log_Vals(hourly_totals_half_alpha[name], mean_val)
    else:
        hourly_totals_half_alpha[name] = mean_val

print(hourly_totals_half_alpha)
