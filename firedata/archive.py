import os
import glob
import pandas as pd
from utils import fire_date

columns_dtypes = {
        'latitude': 'float32',
        'longitude': 'float32',
        'scan': 'float32',
        'track': 'float32',
        'satellite': object,
        'instrument': object,
        'frp': 'float32',
        'daynight': object,
        }


data_types = {
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
        }


def prepare_modis_archive(data_path, columns_dtypes):
    for year in range(2002, 2022):
        print(f'processing modis year {year}')
        fname = os.path.join(data_path, f'fire_archive_M6_{year}.csv')
        dfr = pd.read_csv(fname)
        # drop low confidence detections
        dfr = dfr.loc[dfr.confidence >= 30, :].copy()
        # keeping only "vegetation fires"
        dfr = dfr.loc[dfr.type == 0, :].copy()
        dfr = fire_date(dfr)
        dfr.set_index('date', drop=True, inplace=True)
        dfr.sort_index(inplace=True)
        dfr = dfr[columns_dtypes.keys()]
        dfr = dfr.astype(columns_dtypes)
        dfr.to_parquet(os.path.join(data_path,
                                    f'modis_archive_{year}.parquet'))


def prepare_viirs_snpp(data_path):
    """
    TODO finish
    """
    fnames = glob.glob(os.path.join(data_path, 'fire_archive_SV-C2*.csv'))
    for fname in fnames:
        dfr = pd.read_csv(fname)
        dfr = fire_date(dfr)
        # drop low confidence detections
        dfr = dfr.loc[dfr.confidence != 'l', :].copy()
        # keeping only "vegetation fires"
        dfr = dfr.loc[dfr.type == 0, :].copy()
        # set daynight values to strings as in the Modis product
        dfr.loc[dfr.daynight == 1, 'daynight'] = 'D'
        dfr.loc[dfr.daynight == 0, 'daynight'] = 'N'
        dfr = fire_date(dfr)
        dfr.set_index('date', drop=True, inplace=True)
        dfr.sort_index(inplace=True)
        dfr = dfr[columns_dtypes.keys()]


prepare_modis_archive('data', columns_dtypes)
