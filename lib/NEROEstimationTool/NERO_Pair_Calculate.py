
import numpy as np
# from TNMPyAide.DANA_Noise_Data import DANA_Noise_Data as DND

class RRLPair():
    '''
    Receiver + RoadLink Pair
    road_link: A NERO RoadLink object
    receiver: A NERO Receiver object
    '''
    def __init__(self, road_link, receiver):
        self.road_link = road_link
        self.receiver = receiver

    def calc_Hourly_SPL(self):
        '''
        Calculate the SPL for the given SR pair going through all geometry of the road link
        '''
        # dndobj = DND(self.road_link.df_DANA, self.road_link.link_grade, robust_speeds = False)
        df_traffic_noise = self.road_link.df_traffic_noise
        hourly_totals = {}
        for seg_section in self.road_link.geometry:
            alpha = self.receiver.Find_Angle_Alpha(seg_section)
            distance = self.receiver.Find_Perp_Dist(seg_section)

            relative_attenuation = self.receiver.Compute_Rel_Attenuation(alpha, distance)

            # Average traffic noise by hour
            hours_grouped = df_traffic_noise.groupby(['HOUR'])
            for name, group in hours_grouped:
                name = name[0] # Name is in a tuple which we don't want
                # Filter out cols to just spl_totals
                group = group.filter(regex='SPL_Total.*')
                # Apply relative attenuation, then find the logarithmic mean for each hour
                try:
                    mean_val = 10*np.log10(group.assign(value = lambda x: x+relative_attenuation)['value'].apply(lambda x: 10**(x/10)).mean())
                except RuntimeWarning:
                    print("-"*80)
                    for val in group['SPL_Total_L1'].values:
                        if val <= 1:
                            print(val)
                    print("-"*80)
                # Add hourly averages of different pieces of road together
                if name in hourly_totals:
                    hourly_totals[name] = Add_Log_Vals(hourly_totals[name], mean_val)
                else:
                    hourly_totals[name] = mean_val

        return hourly_totals


def Add_Log_Vals_DF_Rows(df):
    '''
    Written to handle both individual links and link pairs are in data.
    '''
    if len(df.columns.values) == 2:
        sums = Add_Log_Vals(df['SPL_Total_L1'], df['SPL_Total_L2'])
    elif len(df.columns.values) == 1:
        sums = df['SPL_Total_L1']
    else:
        print('Error, empty dataframe received')
        return
    return sums

def Add_Log_Vals(x, y):
    try:
        return 10*np.log10(10**(x/10) + 10**(y/10))
    except RuntimeWarning:
        print("="*80)
        print(x)
        print(y)
        print("="*80)


if __name__ == "__main__":
    x = 44
    y = 44
    z = Add_Log_Vals(x, y)
    print(z)

    x = 0
    y = 44
    z = Add_Log_Vals(x, y)
    print(z)
