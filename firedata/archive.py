import os
import glob
import pandas as pd
from _utils import MCD14DL_dtypes, FireDate


def parse_modis(fname, index_increment, MCD14DL_dtypes):
    dfr = pd.read_csv(fname, dtype=MCD14DL_dtypes)
    # Leave the bellow for later stages
    # drop low confidence detections
    # dfr = dfr.loc[dfr.confidence >= 30, :].copy()
    # keeping only "vegetation fires"
    # dfr = dfr.loc[dfr.type == 0, :].copy()
    dfr.index = dfr.index + index_increment
    dfr = dfr[MCD14DL_dtypes.keys()]
    dfr = dfr.astype(MCD14DL_dtypes)
    dfr['date'] = FireDate.fire_dates(dfr)
    dfr = dfr.sort_values(by='date').reset_index()
    return dfr


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
        dfr = dfr[MCD14DL_dtypes.keys()]


def prepare_modis_archive_all_years():
    """
    Pre-processing of MODIS archive active fire dataset.
    Reads yearly csv files and writes parquet.
    """
    index_increment = 0
    years = range(2002, 2022, 1)
    for year in years:
        fname = f'data/fire_archive_M6_{year}.csv'
        print(fname, year)
        dfr = parse_modis(fname, index_increment, MCD14DL_dtypes)
        index_increment = dfr.index.stop
        print(index_increment, dfr.index)
        dfr.to_parquet(os.path.join('data',
                       f'modis_archive_{year}.parquet'))


def prepare_modis_nrt(nrt_fname, last_archive_fname):
    nrt = pd.read_csv(nrt_fname)
    nrt['type'] = 4
    nrt = nrt[MCD14DL_dtypes.keys()]
    nrt = nrt.astype(MCD14DL_dtypes)
    nrt['date'] = FireDate.fire_dates(nrt)
    nrt = nrt.sort_values(by='date').reset_index()
    index_increment = pd.read_parquet(last_archive_fname).index.stop
    nrt.index = nrt.index + index_increment
    # TODO defer the bellow to later stages?
    nrt.to_parquet('data/nrt_complete.parquet')


# prepare_modis_archive_all_years()
# TODO run the bellow setting dtypes when reading csv
nrt_fname = '/home/tadas/activefire/firedata/data/fire_nrt_M-C61_238232.csv'
last_archive_fname = '/home/tadas/activefire/firedata/data/modis_archive_2021.parquet'
prepare_modis_nrt(nrt_fname, last_archive_fname)
# dfr = pd.read_parquet('data/modis_archive_2002.parquet')
