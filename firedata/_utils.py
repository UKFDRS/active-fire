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
        },
    }

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
