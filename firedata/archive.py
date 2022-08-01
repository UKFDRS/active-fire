"""
Process and prepare archive active fire datasets from FIRMS.

author: tadas.nik@gmail.com

"""

import os
import glob
import pandas as pd
from _utils import VIIRS_dtypes, MCD14DL_dtypes, MCD14DL_nrt_dtypes, FireDate


def parse_modis(fname, index_increment, MCD14DL_dtypes):
    """Read and prepare active fire modis dataset.""" 
    dfr = pd.read_csv(fname, dtype=MCD14DL_dtypes)
    dfr = dfr[MCD14DL_dtypes.keys()]
    dfr = dfr.astype(MCD14DL_dtypes)
    dfr['date'] = FireDate.fire_dates(dfr)
    dfr = dfr.sort_values(by='date').reset_index(drop=True)
    # add index increment
    dfr.index = dfr.index + index_increment
    return dfr


def prepare_viirs_snpp(data_path):
    """Read and prepare VIIRS_NPP archive files found in 'data_path'.
    The datasets must be split per-year"""
    fnames = glob.glob(os.path.join(data_path, 'fire_archive_SV-C2*.csv'))
    for fname in fnames:
        print(fname)
        dfr = pd.read_csv(fname, dtype=VIIRS_dtypes)
        dfr = dfr.astype(VIIRS_dtypes)
        dfr['date'] = FireDate.fire_dates(dfr)
        year = dfr.date[0].year
        dfr.to_parquet(os.path.join(data_path,
            f'fire_archive_SV-C2_{year}.parquet'))



def prepare_modis_archive_all_years():
    """ Pre-processing of MODIS archive active fire dataset.
    Reads yearly csv files and writes parquet. TODO change 
    process per file rather than per year."""
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


def prepare_modis_nrt(nrt_fname, last_archive_fname, MCD14DL_nrt_dtypes):
    """Format the modis nrt dataset. TODO should not be used.
    Import from fetch.py instead."""
    nrt = pd.read_csv(nrt_fname, dtype=MCD14DL_nrt_dtypes)
    nrt = nrt[MCD14DL_nrt_dtypes.keys()]
    nrt = nrt.astype(MCD14DL_nrt_dtypes)
    nrt['type'] = 4
    nrt['date'] = FireDate.fire_dates(nrt)
    nrt = nrt.sort_values(by='date').reset_index(drop=True)
    index_increment = pd.read_parquet(last_archive_fname).index.stop
    nrt.index = nrt.index + index_increment
    nrt.to_parquet('data/nrt_complete.parquet')

if __name__ == "__main__":
# prepare_modis_archive_all_years()
# TODO run the bellow setting dtypes when reading csv
# nrt_fname = '/home/tadas/activefire/firedata/data/fire_nrt_M-C61_238232.csv'
# last_archive_fname = '/home/tadas/activefire/firedata/data/modis_archive_2021.parquet'
# prepare_modis_nrt(nrt_fname, last_archive_fname, MCD14DL_nrt_dtypes)
# prepare_viirs_snpp('data/VIIRS')
