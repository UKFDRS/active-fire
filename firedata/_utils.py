import numpy as np
import pandas as pd
from pathlib import Path

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

    'VIIRS_NOAA_nrt_dtypes' : {
        'latitude': 'float32',
        'longitude': 'float32',
        'brightness': 'float32',
        'acq_date': object,
        'acq_time': object,
        'scan': 'float32',
        'track': 'float32',
        'satellite': object,
        'instrument': object,
        'confidence': object,
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
        }
    }

sql_datatypes = {
    'SQL_rename': {
        'GEOUNIT': 'admin',
        },
    'SQL_detections_dtypes': {
        'id': 'int',
        'latitude': 'float32',
        'longitude': 'float32',
        'frp': 'float32',
        'daynight': 'int',
        'type': 'int',
        'date': 'int',
        'lc': 'int',
        'admin': object,
        'event': 'int',
        },
    'SQL_events_dtypes': {
        'event': 'int',
        'active': 'int',
        'size': 'int',
        'latitude': 'float32',
        'longitude': 'float32',
        'duration': 'float32'
        }
    }

def get_project_root() -> Path:
    return Path(__file__).parent.parent

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

class ModisGrid(object):
    """Class used for calculating position on MODIS
    sinusoidal grid..."""
    # height and width of MODIS tile in the projection plane (m)
    tile_size = 1111950
    # the western limit ot the projection plane (m)
    x_min = -20015109
    # the northern limit ot the projection plane (m)
    y_max = 10007555
    # the actual size of a "500-m" MODIS sinusoidal grid cell
    w_size = 463.31271653
    # the radius of the idealized sphere representing the Earth
    earth_r = 6371007.181
    # DBSCAN eps in radians = 650 meters / earth radius
    eps = 750 / earth_r

    @classmethod
    def modis_sinusoidal_coords(cls, longitudes, latitudes):
        """
        Calculates the position of the points given in longitude latitude
        columns on the MODIS 500 metre resolution sinusoidal grid.
        Follows formalism given in the MCD64A1 product ATBD document (Giglio
        et al., 2016). 
        Parameters
        ----------
        longitudes : (array) with longitudes.
        latitudes : (array) with latitudes.
        Returns
        -------
        tile_h : Array with MODIS 10 degree tile horizontal index.
        tile_v : Array with MODIS 10 degree tile vertical index.
        indx : Array with within-tile pixel positions along x axis.
        indy : Array with within-tile pixel positions along y axis.
        """
        lon_rad = np.deg2rad(longitudes)
        lat_rad = np.deg2rad(latitudes)
        x = cls.earth_r * lon_rad * np.cos(lat_rad)
        y = cls.earth_r * lat_rad
        tile_h = (np.floor((x - cls.x_min) /
                  cls.tile_size)).astype(int)
        tile_v = (np.floor((cls.y_max - y) /
                  cls.tile_size)).astype(int)
        i_top = (cls.y_max - y) % cls.tile_size
        j_top = (x - cls.x_min) % cls.tile_size
        indy = (np.floor((i_top / cls.w_size) - 0.5)).astype(int)
        indx = (np.floor((j_top / cls.w_size) - 0.5)).astype(int)
        return tile_h, tile_v, indx, indy


    @classmethod
    def modis_sinusoidal_grid_index(cls, longitudes, latitudes):
        """
        Global grid indices are obtained by conversion from
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
        tile_h, tile_v, indx, indy = cls.modis_sinusoidal_coords(longitudes, latitudes)
        index_x = indx + (tile_h * 2400)
        index_y = indy + (tile_v * 2400)
        return index_x, index_y

class FireDate(object):
    """Class for datetime conversions
    """
    base_date = pd.Timestamp('1970-01-01', tz='utc')

    @classmethod
    def fire_dates(cls, dfr):
        """Converts FIRMS active fire date and time stored
        as strings in 'acq_date' and 'acq_time' columns to
        datetimes.
        Args:
            dfr: pandas dataframe with 'acq_date' and 'acq_time' columns
        Returns:
            dates: pandas Series with datetimes.
        """
        dates = pd.to_datetime(
            dfr['acq_date'] + ' ' +
            dfr.loc[:, 'acq_time'].astype(str).str.zfill(4), utc=True
            )
        return dates

    @classmethod
    def days_since(cls, dates):
        """Calculates days from base_date
        Args:
            dates (array like): pandas timedelta objects with dates
        Returns:
            day_since : (array): with total days from 
            self.base_date to the date of fire detection.
        """
        day_since = (dates - cls.base_date).dt.days
        return day_since
    
    @classmethod
    def unix_time(cls, datetimes):
        """pandas datetime to unix time conversion
        Args:
            datetimes: pandas Series with datetimes.
        Returns:
            unix time
        """
        return (datetimes - cls.base_date).dt.total_seconds().astype(int)
        
