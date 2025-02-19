import numpy as np
import pandas as pd
import pyproj


class Receiver():
    '''
    Receiver item class for NERO TNMAide modeling
    '''

    def __init__(self, latitude, longitude, receiverName = "Receiver"):
        self.m_map_projection = "EPSG:3857"
        self.geom_offset = (0,0)
        self.name = receiverName
        self.x, self.y = self.convertCoords(lat=latitude, long=longitude)
        self.lat = latitude
        self.long = longitude
        self.worst_hour = None

    def setGeomOffset(self, offset):
        if self.geom_offset == offset:
            return

        self.geom_offset = offset
        self.x, self.y = self.convertCoords(self.lat, self.long)

    def geomOffset(self):
         return self.geom_offset

    def setProjection(self, map_projection):
        '''
        update projection and x, y coords to match new projection
        '''
        if self.m_map_projection == map_projection:
             return

        self.m_map_projection = map_projection
        self.x, self.y = self.convertCoords(self.lat, self.long)

    def projection(self):
         '''
         returns current map projection of the receiver
         '''
         return self.m_map_projection

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
        receiver_pos = (self.x, self.y)

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
        x3, y3 = self.x, self.y

        # Calculate point of intersection with the straight line
        dx, dy = x2-x1, y2-y1
        det = dx*dx + dy*dy
        a = (dy*(y3-y1)+dx*(x3-x1))/det
        x4, y4 = x1+a*dx, y1+a*dy

        # find distance
        distance = Distance_Formula((x3, y3), (x4, y4))
        return distance

        # Kentucky North: ESRI:103286 from epsg.io

    def convertCoords(self, lat, long):
        '''
            function to calculate the x, y coords from lat-long, apply an offset to match to a different projection if applicable

            Target Projection is set on the receiver object with the setProjection function

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

    def createTNMImportDF(self):
        '''
        Returns:
            A Pandas df containing one row with info about this receiver
        '''
        data = {"Type": ["Receiver"],
                "ReceiverName": [self.name],
                "X": [self.x],
                "Y": [self.y],
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


def Distance_Formula(point1, point2):
            x1 = point1[0]
            x2 = point2[0]
            y1 = point1[1]
            y2 = point2[1]
            x_diff = (x2 - x1)**2
            y_diff = (y2 - y1)**2
            distance = np.sqrt(x_diff + y_diff)
            return distance


if __name__ == "__main__":



    # Unit Tests (show 4.5 db decrease per dist doubling), (3db per angular doubling) (General unit testing)
    segment = [(-85.58886, 38.31122), (-85.58789, 38.3117)]

    receiver = Receiver(-85.581518, 38.312084)
    receiver.setProjection("ESRI:103286")
    for i, point in enumerate(segment):
        x, y = receiver.convertCoords(point[0], point[1])
        segment[i] = (x, y)
    print(segment)
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
    receiver.setProjection("ESRI:103286")
    for i, point in enumerate(segment):
        x, y = receiver.convertCoords(point[0], point[1])
        segment[i] = (x, y)
    alpha = receiver.Find_Angle_Alpha(segment)
    print('90 Degree:')
    print(np.rad2deg(alpha))

    receiver = Receiver(-85.55000, 38.42071)
    receiver.setProjection("ESRI:103286")
    alpha = receiver.Find_Angle_Alpha(segment)
    print('45 Degree:')
    print(np.rad2deg(alpha))

    receiver = Receiver(-85.55000, 38.17929)
    receiver.setProjection("ESRI:103286")
    alpha = receiver.Find_Angle_Alpha(segment)
    print('45 Degrees: (opposite side of road)')
    print(np.rad2deg(alpha))

    receiver = Receiver(-85.55000, 38.30000)
    receiver.setProjection("ESRI:103286")
    alpha = receiver.Find_Angle_Alpha(segment)
    print('180 Degree: (on roadway line)')
    print(np.rad2deg(alpha))

    # Test dist calc:
    print('='*80)
    print('Distance Function Check')
    receiver.setProjection("ESRI:103286")
    receiver = Receiver(-85.55000, 38.30000)
    dist = receiver.Find_Perp_Dist(segment)
    print('on roadway line')
    print(dist)

    receiver = Receiver(-85.65000, 38.30000)
    receiver.setProjection("ESRI:103286")
    dist = receiver.Find_Perp_Dist(segment)
    print('on roadway line past end of segment')
    print(dist)

    receiver = Receiver(-85.55000, 38.303533)
    receiver.setProjection("ESRI:103286")
    dist = receiver.Find_Perp_Dist(segment)
    print('100ft')
    print(dist)

    receiver = Receiver(-85.55000, 38.296467)
    receiver.setProjection("ESRI:103286")
    dist = receiver.Find_Perp_Dist(segment)
    print('100ft, opposite side of road')
    print(dist)

    # Mirrored Road Segment check
    segment1 = [(-85.60000, 38.30000), (-85.55000, 38.30500)]
    segment2 = [(-85.55000, 38.30500), (-85.50000, 38.30000)]
    receiver = Receiver(-85.55000, 38.25000)
    receiver.setProjection("ESRI:103286")
    for i, point in enumerate(segment1):
        x, y = receiver.convertCoords(point[0], point[1])
        segment1[i] = (x, y)
    for i, point in enumerate(segment2):
        x, y = receiver.convertCoords(point[0], point[1])
        segment2[i] = (x, y)
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
