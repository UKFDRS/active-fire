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


def prepare_modis_archive(data_path, year, fname, columns_dtypes):
    print(f'processing modis year {year}')
    # fname = os.path.join(data_path, f'fire_archive_M6_{year}.csv')
    dfr = pd.read_csv(fname)
    # Leave the bellow for later stages
    # drop low confidence detections
    # dfr = dfr.loc[dfr.confidence >= 30, :].copy()
    # keeping only "vegetation fires"
    # dfr = dfr.loc[dfr.type == 0, :].copy()
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


def prepare_modis_archive_all_years():
    """
    Pre-processing of MODIS archive active fire dataset.
    Reads yearly csv files and writes parquet.
    """
    fnames = glob.glob('data/fire_archive_M6_*.csv')
    for fname in fnames:
        year = fname.split('.')[0].split('_')[-1]
        print(fname, year)
        prepare_modis_archive('data', year, fname, columns_dtypes)


def prepare_modis_nrt(nrt_fname):
    nrt = pd.read_csv(nrt_fname)
    nrt = fire_date(nrt)
    nrt.to_parquet('data/nrt_complete.parquet')


# prepare_modis_archive_all_years()
# nrt_fname = '/home/tadas/activefire/firedata/data/fire_nrt_M-C61_238232.csv'
# prepare_modis_nrt(nrt_fname)
