import numpy as np
import pandas as pd
import pyproj


class Receiver():
    '''
    Receiver item class for NERO TNMAide modeling
    '''

    def __init__(self, latitude, longitude, receiverName = "Receiver"):
        self.lat = latitude
        self.long = longitude
        self.name = receiverName
        self.worst_hour = None

    def Compute_Rel_Attenuation(self, alpha, distance):
        '''
        value to modify traffic noise values to account for angle and distance from roadway
        '''
        ra = (15 * np.log10(50/distance)) + (10 * np.log10(alpha/np.pi))

        return ra

    def Find_Angle_Alpha(self, segment):
        '''
        find the angle from a receiver to either end of a link
        NOTE: Currently angle is returned in radians
        TODO: Write some test cases to ensure all angles work (90, 45, 179)
        '''
        # Find lengths of all 3 sides in common units
        link_start = (segment[0][0], segment[0][1])
        link_end = (segment[1][0], segment[1][1])
        receiver_pos = (self.lat, self.long)

        def Distance_Formula(point1, point2):
            x1 = point1[0]
            x2 = point2[0]
            y1 = point1[1]
            y2 = point2[1]
            x_diff = (x2 - x1)**2
            y_diff = (y2 - y1)**2
            distance = np.sqrt(x_diff + y_diff)
            return distance

        link_len = Distance_Formula(link_start, link_end)
        start_to_mic = Distance_Formula(link_start, receiver_pos)
        end_to_mic = Distance_Formula(link_end, receiver_pos)

        # Calculate angle
        alpha = np.arccos(((start_to_mic**2) + (end_to_mic**2) - (link_len**2)) / (2 * start_to_mic * end_to_mic))
        return alpha

    def Find_Perp_Dist(self, segment):
        '''
        Compute the perpenicular distance from the receiver to the road link in feet

        '''
        # define points
        x1, y1 = segment[0][0], segment[0][1]
        x2, y2 = segment[1][0], segment[1][1]
        x3, y3 = self.lat, self.long

        # Calculate point of intersection with the straight line #TODO: Check that this works on lat/long
        dx, dy = x2-x1, y2-y1
        det = dx*dx + dy*dy
        a = (dy*(y3-y1)+dx*(x3-x1))/det
        x4, y4 = x1+a*dx, y1+a*dy

        # Calculate lat and long differences between intersection and receiver point
        diff_lat, diff_long = x3-x4, y3-y4

        # Convert to radians
        x3, y3 = np.radians(x3), np.radians(y3)
        x4, y4 = np.radians(x4), np.radians(y4)
        diff_lat, diff_long = np.radians(diff_lat), np.radians(diff_long)

        # Use Haversine Formula to find distance
        # R = 6371 # km
        R = 20902211 # ft

        a = ((np.sin(diff_lat / 2))**2) + (np.cos(x3) * np.cos(x4) * ((np.sin(diff_long / 2))**2))
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
        distance = R * c
        return distance

        # Kentucky North: ESRI:103286 from epsg.io

    def createTNMImportDF(self, offset = (0,0), target_projection = "EPSG:3857"):
        '''
        function to calculate the x, y coords from lat-long, apply an offest to match to a different projection if applicable

        Accepts:
            offset - A tuple of x and y offset to allow projection to be shifted to match TNM projection, default is (0,0)
            target_projection - A string that represents the desired map projection out, default is web mercator

        Returns:
            A Pandas df containing one row with info about this receiver
        '''
        # Create a transformer object for the desired projection
        transformer = pyproj.Transformer.from_crs("EPSG:4326", target_projection)

        # Convert coordinates
        x, y = transformer.transform(self.long, self.lat)

        # Convert meters to feet
        x = x * 3.28084
        y = y * 3.28084

        x = x + offset[0]
        y = y + offset[1]

        data = {"Type": ["Receiver"],
                "ReceiverName": [self.name],
                "X": [x],
                "Y": [y],
            	"Z": [0],   # Update to make this setable
                "Height": [5],  # Update to make this setable
                "DwellingUnits": [0],
                "ExistingLevel": [0],
                "NoiseReductionGoal": [0],
                "AbsoluteCriterion": [0],
                "RelativeCriterion": [0]
                }

        receiverDF = pd.DataFrame(data)

        return receiverDF




if __name__ == "__main__":
    # Unit Tests (show 4.5 db decrease per dist doubling), (3db per angular doubling) (General unit testing)
    segment = [(-85.58886, 38.31122), (-85.58789, 38.3117)]
    receiver = Receiver(-85.581518, 38.312084)
    alpha = receiver.Find_Angle_Alpha(segment)
    dist = receiver.Find_Perp_Dist(segment)
    ra = receiver.Compute_Rel_Attenuation(alpha, dist)
    ra_dist_check = receiver.Compute_Rel_Attenuation(alpha, dist*2)
    ra_angle_check = receiver.Compute_Rel_Attenuation(alpha*2, dist)
    print('Angle Alpha:')
    print(np.rad2deg(alpha))
    print('Distance:')
    print(dist)
    print('Relative Attenuation:')
    print(ra)
    print('Relative Attenuation double distance check: (should be -4.5)')
    print(ra_dist_check-ra)
    print('Relative Attenuation double angle check: (should be 3)')
    print(ra_angle_check-ra)

    # Test angle calc:
    print('='*80)
    print('Angle Function Check')
    segment = [(-85.60000, 38.30000), (-85.50000, 38.30000)]
    receiver = Receiver(-85.55000, 38.35000)
    alpha = receiver.Find_Angle_Alpha(segment)
    print('90 Degree:')
    print(np.rad2deg(alpha))

    receiver = Receiver(-85.55000, 38.42071)
    alpha = receiver.Find_Angle_Alpha(segment)
    print('45 Degree:')
    print(np.rad2deg(alpha))

    receiver = Receiver(-85.55000, 38.17929)
    alpha = receiver.Find_Angle_Alpha(segment)
    print('45 Degrees: (opposite side of road)')
    print(np.rad2deg(alpha))

    receiver = Receiver(-85.55000, 38.30000)
    alpha = receiver.Find_Angle_Alpha(segment)
    print('180 Degree: (on roadway line)')
    print(np.rad2deg(alpha))

    # Test dist calc:
    print('='*80)
    print('Distance Function Check')
    receiver = Receiver(-85.55000, 38.30000)
    dist = receiver.Find_Perp_Dist(segment)
    print('on roadway line')
    print(dist)

    receiver = Receiver(-85.65000, 38.30000)
    dist = receiver.Find_Perp_Dist(segment)
    print('on roadway line past end of segment')
    print(dist)

    receiver = Receiver(-85.55000, 38.303533)
    dist = receiver.Find_Perp_Dist(segment)
    print('100ft')
    print(dist)

    receiver = Receiver(-85.55000, 38.296467)
    dist = receiver.Find_Perp_Dist(segment)
    print('100ft, opposite side of road')
    print(dist)

    # Mirrored Road Segment check
    segment1 = [(-85.60000, 38.30000), (-85.55000, 38.30500)]
    segment2 = [(-85.55000, 38.30500), (-85.50000, 38.30000)]
    receiver = Receiver(-85.55000, 38.25000)
    alpha = receiver.Find_Angle_Alpha(segment1)
    dist = receiver.Find_Perp_Dist(segment1)
    ra1 = receiver.Compute_Rel_Attenuation(alpha, dist)
    alpha = receiver.Find_Angle_Alpha(segment2)
    dist = receiver.Find_Perp_Dist(segment2)
    ra2 = receiver.Compute_Rel_Attenuation(alpha, dist)
    print("=" *80)
    print("Mirrored road segments")
    print("Segment 1: " + str(ra1))
    print("Segment 2: " + str(ra2))
    log_sum = 10*np.log10(10**(ra1/10) + 10**(ra2/10))
    print("Log sum (Should be +3 db) = " + str(log_sum))
