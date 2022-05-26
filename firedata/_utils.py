import numpy as np
import pandas as pd


select_columns = {
    'latitude': 'float32',
    'longitude': 'float32',
    'scan': 'float32',
    'track': 'float32',
    'satellite': object,
    'confidence': 'int8',
    'frp': 'float32',
    'daynight': object,
    'type': 'int8',
    }

dataset_dtypes = {
    'VIIRS_dtypes' : {
        'latitude': 'float32',
        'longitude': 'float32',
        'bright_ti4': 'float32',
        'bright_ti5': 'float32',
        'acq_date': object,
        'acq_time': object,
        'scan': 'float32',
        'track': 'float32',
        'satellite': object,
        'instrument': object,
        'confidence': object,
        'version': 'int8',
        'frp': 'float32',
        'daynight': object,
        'type': 'int8',
        },

    'MODIS_dtypes' : {
        'latitude': 'float32',
        'longitude': 'float32',
        'brightness': 'float32',
        'acq_date': object,
        'acq_time': object,
        'scan': 'float32',
        'track': 'float32',
        'satellite': object,
        'instrument': object,
        'confidence': 'int8',
        'version': 'float32',
        'bright_t31': 'float32',
        'frp': 'float32',
        'daynight': object,
        'type': 'int8',
        },

    'MODIS_nrt_dtypes' : {
        'latitude': 'float32',
        'longitude': 'float32',
        'brightness': 'float32',
        'acq_date': object,
        'acq_time': object,
        'scan': 'float32',
        'track': 'float32',
        'satellite': object,
        'instrument': object,
        'confidence': 'int8',
        'version': object,
        'bright_t31': 'float32',
        'frp': 'float32',
        'daynight': object,
        },

    'VIIRS_nrt_dtypes' : {
        'latitude': 'float32',
        'longitude': 'float32',
        'bright_ti4': 'float32',
        'acq_date': object,
        'acq_time': object,
        'scan': 'float32',
        'track': 'float32',
        'satellite': object,
        'instrument': object,
        'confidence': object,
        'version': object,
        'bright_ti5': 'float32',
        'frp': 'float32',
        'daynight': object,
        },
    }

def spatial_subset_dfr(dfr, bbox):
    """
    Selects data within spatial bbox. bbox coords must be given as
    positive values for the Northern hemisphere, and negative for
    Southern. West and East both positive - Note - the method is
    naive and will only work for bboxes fully fitting in the Eastern hemisphere!!!
    Args:
        dfr - pandas dataframe
        bbox - (list) [North, West, South, East]
    Returns:
        pandas dataframe
    """
    dfr = dfr[(dfr['latitude'] < bbox[0]) &
                            (dfr['latitude'] > bbox[2])]
    dfr = dfr[(dfr['longitude'] > bbox[1]) &
                            (dfr['longitude'] < bbox[3])]
    return dfr

class FireDate(object):
    """Class for datetime conversions
    """

    def __init__(self, base_date='2002-01-01'):
        self.base_date = pd.Timestamp(base_date, tz='utc')

    @classmethod
    def fire_dates(self, dfr):
        dates = pd.to_datetime(
            dfr['acq_date'] + ' ' +
            dfr.loc[:, 'acq_time'].astype(str).str.zfill(4), utc=True
            )
        return dates

    def days_since(self, dates):
        """Calculates days from base_date

        Args:
            dates (array like): pandas timedelta objects with dates

        Returns:
            day_since : (array): with total days from 
            self.base_date to the date of fire detection.
        """
        day_since = (dates - self.base_date).dt.days
        return day_since


class ModisGrid(object):
    """Class used for calculating position on MODIS
    sinusoidal grid..."""

    def __init__(self):
        # height and width of MODIS tile in the projection plane (m)
        self.tile_size = 1111950
        # the western limit ot the projection plane (m)
        self.x_min = -20015109
        # the northern limit ot the projection plane (m)
        self.y_max = 10007555
        # the actual size of a "500-m" MODIS sinusoidal grid cell
        self.w_size = 463.31271653
        # the radius of the idealized sphere representing the Earth
        self.earth_r = 6371007.181
        # DBSCAN eps in radians = 650 meters / earth radius
        self.eps = 750 / self.earth_r
        self.basedate = pd.Timestamp('2002-01-01')

    def modis_sinusoidal_grid_index(self, longitudes, latitudes):
        """
        Calculates the position of the points given in longitude latitude
        columns on the global MODIS 500 metre resolution sinusoidal grid.
        Follows formalism given in the MCD64A1 product ATBD document (Giglio
        et al., 2016). Global grid indices are obtained by conversion from
        MODIS sinusoidal grid tile and within-tile row/column index to global
        index on the entire grid row/column.

        Parameters
        ----------
        longitudes : (array) with longitudes.
        latitudes : (array) with latitudes.

        Returns
        -------
        index_x : Array with pixel positions on global MODIS grid
            along x axis.
        index_y : Array with pixel positions on global MODIS grid
            along y axis.

        """
        lon_rad = np.deg2rad(longitudes)
        lat_rad = np.deg2rad(latitudes)
        x = self.earth_r * lon_rad * np.cos(lat_rad)
        y = self.earth_r * lat_rad
        tile_h = (np.floor((x - self.x_min) /
                  self.tile_size)).astype(int)
        tile_v = (np.floor((self.y_max - y) /
                  self.tile_size)).astype(int)
        i_top = (self.y_max - y) % self.tile_size
        j_top = (x - self.x_min) % self.tile_size
        indy = (np.floor((i_top / self.w_size) - 0.5)).astype(int)
        indx = (np.floor((j_top / self.w_size) - 0.5)).astype(int)
        index_x = indx + (tile_h * 2400)
        index_y = indy + (tile_v * 2400)
        return index_x, index_y
