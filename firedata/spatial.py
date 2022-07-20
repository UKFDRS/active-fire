import os
import numpy as np
import pandas as pd


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
        self.base_date = pd.Timestamp('2002-01-01')

    def days_since_base_date(self, dfr, dates):
        """Calculates days from base_date (start of the record)

        Args:
            dates (array like): array with

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
