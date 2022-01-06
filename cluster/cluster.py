import os
import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import LabelEncoder
import dask.dataframe as dd


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


    def add_days_since(self, dfr):
        """Calculates days from 2002-01-01 to January 1st of
        the year of the annual dataframe

        Args:
            dfr (dataframe): dataframe with annual burned data.
            Must have 'year' column, 'date' column with day of year,
            or datetime date. The function attempts to convert
            column 'date' to day of year, and failing this it is
            assumed that the 'date' column contains day of year.
            Must by passed data from a single year only for this to make sense.

        Returns:
            dfr (datafram): the same dateframe with added column
            'days_since' with total days from 2002-01-01 to the day of burn.
        """
        year_date = pd.Timestamp('{0}-01-01'.format(dfr.year[0]))
        add_days = (year_date - self.base_date).days
        day_of_year = dfr.index.dayofyear
        dfr['day_since'] = day_of_year + add_days
        return dfr


    def modis_sinusoidal_grid_index(self, dfr):
        """
        Calculates the position of the points given in longitude latitude
        columns on the global MODIS 500 metre resolution sinusoidal grid.
        Follows formalism given in the MCD64A1 product ATBD document (Giglio
        et al., 2016). Global grid indices are obtained by conversion from
        MODIS sinusoidal grid tile and within-tile row/column index to global
        index on the entire grid row/column.

        Parameters
        ----------
        dfr : Pandas DataFrame
            dataset with longitude and latitude columns.
            Column names are inferred using regex.

        Returns
        -------
        dfr : Pandas DataFrame
            The same dataset with columns 'x' and 'y' containing
            the grid cell indices in the global Modis 500m grid.
        """
        latitude = dfr.filter(regex=r'(?i:l)at')
        longitude = dfr.filter(regex=r'(?i:l)on')
        lon_rad = np.deg2rad(longitude)
        lat_rad = np.deg2rad(latitude)
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
        dfr['x'] = indx + (tile_h * 2400)
        dfr['y'] = indy + (tile_v * 2400)
        return dfr


class SplitDBSCAN(DBSCAN):
    def __init__(
        self,
        eps=0.5,
        edge_eps=0.5,
        split_dimension=0,
        min_samples=5,
        metric="euclidean",
    ):
        super().__init__(
                eps=eps,
                min_samples=min_samples,
                metric=metric,
                )
        self.eps = eps
        self.edge_eps = edge_eps
        self.split_dimension = split_dimension,


    def split(self, chunk, end):
        """Splits clusters into completed and active parts.
        The group membership of active points may change when
        clustering the following chunk or with influx of new data.

        Args:
            chunk (dataframe): a clustered chunk with and 'group' columns.

        Returns:
            completed (dataframe): part of the chunk which is completed.
            active (dataframe): part of the chunk with points belonging to
            active clusters.
        """
        print('chunk', chunk.shape[0])
        print('end', end)
        edge = chunk[split_dimension].max()
        active_groups = chunk[(chunk[self.split_dimension] >=
                               edge - self.edge_eps)]['group'].unique()
        active = chunk.loc[(chunk.group.isin(active_groups)), :].copy()
        print('active', active.shape[0])
        completed = chunk.loc[~(chunk.group.isin(active_groups)), :].copy()
        print('completed', completed.shape[0])
        return completed, active



def split_overlap(chunk, end):

def chunk_splits(dfr, chunk_size, split_column):
    # Upside-down floor division
    chunks = -(-len(dfr.index) // chunk_size)
    splits = [(x.values[0], x.values[-1]) for x in
              np.array_split(dfr[split_column], chunks, axis=0)]
    return splits

def split_overlap(chunk, end):
    """Splits clustered chunk into completed and "overlap" parts.
    The overlap points may be part of groups in the following chunk
    and need to be clustered again (with the following chunk).
    The overlap is any points in the last nMaxUncertainty + 2 days of the chunk,
    plus any other points that are part of groups present in that period.

    Args:
        chunk (dataframe): a clustered chunk with 'date' and 'group' columns.

    Returns:
        completed (dataframe): part of the chunk which is completed.
        overlap (dataframe): part of the chunk with points to re-clustered.
    """
    print('chunk', chunk.shape[0])
    print('end', end)
    overlap_groups = chunk[(chunk.day_dense >= end - 2)]['group'].unique()
    overlap = chunk.loc[(chunk.group.isin(overlap_groups)), :].copy()
    print('overlap', overlap.shape[0])
    completed = chunk.loc[~(chunk.group.isin(overlap_groups)), :].copy()
    print('completed', completed.shape[0])
    return completed, overlap


def cluster_in_chunks(dfr, chunk_size, max_group_last, eps, last_year=False):
    """
    Routine for clustering one year of ba data in chunks

    Parameters:
        dfr (DataFrame): A dataset to be clustered.
        chunk_size (int): target number of rows for a single chunk to cluster.
        Overlap (DataFrame): overlap points to be added to the first chunk.
        max_group_last (int): Group number increment. This will be added
            group labels to have unique group id across the record.
        eps (float): Distance threshold for DBSCAN clustering. Passed to
        cluster function.

    Returns:
        completed (dataframe): clustered dfr dataframe, without
        overlap if not last year.
        overlap (dataframe): densified points to be clustered
        with the following chunk.

    Todo:
        This does not have to be year based, since we have "day_since" column
        which gives a unique id in time for each pixel. Any consecutive
        time block would work. Chunking should be done on days_since!
    """
    labeler = LabelEncoder()
    overlap = None
    # determine number of chunks
    print(splits)
    done_ba = []
    done_af = []
    for nr, (split_start, split_end) in enumerate(splits, 1):
        print('Doing chunk ', nr)
        # if last chunk, do not extend beyond the split_end date
        if nr == len(splits):
            print('here')
            split_end += 1
            overlap_end = split_end - 21
        else:
            overlap_end = split_end
        print('splits', split_start, split_end)
        chunk = dfr.loc[(dfr.day_dense >= split_start) & (dfr.day_dense < split_end), :]
        if overlap is not None:
            chunk = pd.concat([overlap.drop('group', axis = 1), chunk])
        chunk['group'] = cluster(chunk[['x', 'y', 'day_dense']], eps)
        print('slave group: ', chunk.group.isnull().any())
        if (nr == len(splits)) and last_year:
            completed = chunk
            overlap = None
        else:
            completed, overlap = split_overlap(chunk, overlap_end)
        completed['group'] = labeler.fit_transform(completed.group)
        #increment group labels
        if max_group_last:
            completed['group'] += max_group_last
        max_group_last = completed['group'].max() + 1
        #un-densify completed part
        #completed = completed.groupby(['x', 'y', 'day_since'])['group'].first().reset_index()
        done_ba.append(completed_ba)
        done_af.append(completed_af)
    completed_ba = pd.concat(done_ba)
    pixel = completed_ba[(completed_ba.x == 27918)&(completed_ba.y == 24501)&(completed_ba.day_since == 699)]
    print(pixel)
    completed_af = pd.concat(done_af)
    elapsed = timeit.default_timer() - tt
    print('total {0} run time: {1:.2f}s'.format(year, elapsed))
    return completed_ba, completed_af, overlap


def read_dataframes(data_path, base_name):
    dfr = dd.read_parquet(os.path.join(data_path, base_name +
                                       '*' + '.parquet'))
#splits = chunk_splits(dfr, chunk_size, split_column)
