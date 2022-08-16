"""
Fetches near-real time active fire data from FIRMS.
Place your LANCE NRT token as variable (NRT_TOKEN) in
.env file in project root. The below uses dotenv to read
the token.

author: tadas.nik@gmail.com

"""
import os
import logging
import requests
import pandas as pd
from io import BytesIO
from requests.auth import AuthBase
from dotenv import dotenv_values
from configparser import ConfigParser
from _utils import dataset_dtypes, FireDate

# Place your LANCE NRT token as variable (NRT_TOKEN) in
# .env file in project root. The below uses dotenv to read
# the token.


class NRTAuth(AuthBase):
    """Implementation of authorization using the LANCE (eosdis)
    nrt (near-real time) data access token.
    This is called during http request setup"""
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


class FetchNRT():
    """Class for fetching active fires near-real time (nrt) data from FIRMS"""
    def __init__(self, sensor, nrt_token, data_path,
                 base_url, nrt_dataset, archive_end):
        self.date_now = pd.Timestamp.utcnow()
        self.sensor = sensor
        self.auth = NRTAuth(nrt_token)
        self.base_url = base_url
        self.nrt_dataset_path = os.path.join(data_path, nrt_dataset)
        self.archive_end = archive_end
        self.logger = self.setup_logger()

    def setup_logger(self):
        """Sutup and return logger"""
        log_file_name = self.__class__.__name__ + '.log'
        logger = logging.getLogger(f'{self.sensor}')
        logger.setLevel(logging.INFO)
        # create console handler and set level to debug
        log_handler = logging.FileHandler(filename=log_file_name)
        log_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s: %(message)s')
        log_handler.setFormatter(formatter)
        logger.addHandler(log_handler)
        return logger

    @classmethod
    def nrt_end_date_log(cls):
        """TODO Not used"""
        with open(cls.__class__.__name__) as logfile:
            lines = logfile.read().splitlines()
            last_line = lines[-1]
        return pd.Timestamp(last_line.split('NRT last datetime')[1])


    def day_url(self, date):
        """active fire data url for the date"""
        year = date.year
        doy = date.day_of_year
        return self.base_url + f'{year}{doy:03d}.txt'

    def fetch_day_nrt(self, date):
        """Retrieves nrt active fire data for the day (date) from FIRMS
        Args:
            date: Pandas Timestamp.
        Returns:
            DataFrame with available active fire data for the day.
        """
        url = self.day_url(date)
        try:
            response = requests.get(url, auth=self.auth)
            response.raise_for_status()
            dfr = pd.read_table(BytesIO(response.content),
                                sep=',', header=0)
            self.logger.info('fetched nrt for day: ' +
                             date.strftime('%Y-%m-%d'))
        except requests.exceptions.HTTPError as err:
            dfr = None
            self.logger.warning('fetching error: ' +
                             str(err))
        return dfr

    def log_nrt_end_date(self, dataset):
        """Logs nrt record end date"""
        self.logger.info('nrt end datetime: ' +
                         dataset.date.max().strftime('%Y-%m-%d %H:%M'))

    def prepare_nrt_dataset(self, dataset):
        """Adds required columns and sets data types of the nrt dataset"""
        # In case dataset doesn't have instrument column:
        dataset['instrument'] = self.sensor
        sensor_str = self.sensor.split('_')[0]
        dataset = dataset.astype(dataset_dtypes[f'{sensor_str}_nrt_dtypes'])
        dataset['type'] = 4
        dataset['date'] = FireDate.fire_dates(dataset)
        # Dropping original date/time columns
        dataset = dataset.drop(['acq_date', 'acq_time'], axis=1)
        dataset = dataset.sort_values(by='date').reset_index(drop=True)
        return dataset

    def nrt_last_date(self):
        """Return nrt record end datetime."""
        try:
            dataset = pd.read_parquet(self.nrt_dataset_path)
            nrt_end_date = dataset.date.max()
        except FileNotFoundError:
            nrt_end_date = pd.Timestamp(self.archive_end)
        return nrt_end_date

    def fetch(self):
        """The main function performing data fetching. For each day (date)
        calls fetch_day_nrt method and appends the retrieved datasets.
        Finally, the fetched data is merged into nrt_completed dataset
        """
        self.logger.info(f'Running fetch')
        nrt_end_date = self.nrt_last_date()
        days_to_fetch = pd.date_range(nrt_end_date.date(),
                self.date_now.date())
        datasets = []
        for date in days_to_fetch:
            print('fetching :', date)
            dataset = self.fetch_day_nrt(date)
            if dataset is not None:
                datasets.append(dataset)
        if len(datasets) > 0:
            nrt_new = pd.concat(datasets)
            nrt_new = self.prepare_nrt_dataset(nrt_new)
            self.logger.info(f'fetched {nrt_new.shape[0]} fire detections')
            if os.path.isfile(self.nrt_dataset_path):
                self.merge_nrt(nrt_new)
            else:
                nrt_new.to_parquet(self.nrt_dataset_path)
                self.log_nrt_end_date(nrt_new)
        else:
            self.logger.warning(f'No new data fetched, nothing to add to the record.')

    def merge_nrt(self, nrt_new):
        """Merges the fetched data (nrt_new) to nrt_completed and
        overwrites the nrt_completed file at nrt_dataset_path
        """
        nrt_completed = pd.read_parquet(self.nrt_dataset_path)
        # Is index increment needed?
        index_increment = nrt_completed.index[0]
        nrt_updated = pd.concat([nrt_completed, nrt_new])
        tot_rows = nrt_updated.shape[0]
        nrt_updated = nrt_updated.drop_duplicates()
        rows_dropped = tot_rows - nrt_updated.shape[0]
        rows_added = nrt_new.shape[0] - rows_dropped
        if rows_added > 0:
            self.logger.info(f'Adding {rows_added} active fires to nrt record')
            # write the updated file
            nrt_updated = nrt_updated.reset_index(drop=True)
            nrt_updated.index = nrt_updated.index + index_increment
            nrt_updated.to_parquet(self.nrt_dataset_path)
            self.log_nrt_end_date(nrt_updated)
        else:
            self.logger.info(f'No new fire detections, snooze')

    def drop_in_archive_nrt(self):
        """Select nrt data starting after archive_end datetime.
        Used to reduce nrt dataset after archive update.
        WARNING: overwrites the file at nrt_dataset_path!
        """
        nrt = pd.read_parquet(self.nrt_dataset_path)
        archive_end_dt = pd.Timestamp(self.archive_end)
        nrt_selected = nrt[nrt.date.values > archive_end_dt]
        nrt_selected.to_parquet(self.nrt_dataset_path)


if __name__ == "__main__":
    # The below is run by a cron job daily. Perhaps better to
    # move this to a separate script file?
    env_vars = dotenv_values('.env')
    config = ConfigParser()
    config.read('config.ini')
    # Suomi NPP is out of action...
    # for sensor in ['MODIS', 'VIIRS_NPP', 'VIIRS_NOAA']:
    for sensor in ['MODIS', 'VIIRS_NOAA']:
        nrt = FetchNRT(sensor, env_vars['nrt_token'], 
                       config['OS']['data_path'], **config[sensor])
        nrt.fetch()

