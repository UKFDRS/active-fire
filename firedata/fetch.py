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
    def __init__(self, sensor, nrt_token, base_url):
        self.sensor = sensor
        self.auth = NRTAuth(nrt_token)
        self.base_url = base_url
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

    def fetch(self, start_date, end_date):
        """The main function performing data fetching. For each day (date)
        between start_date and end_date (inclusively) calls fetch_day_nrt
        method appends the retrieved datasets.
        """
        nrt_new = None
        self.logger.info(f'Running fetch')
        days_to_fetch = pd.date_range(start_date, end_date)
        datasets = []
        for date in days_to_fetch:
            print('fetching :', date)
            dataset = self.fetch_day_nrt(date)
            if dataset is not None:
                datasets.append(dataset)
        if len(datasets) > 0:
            nrt_new = pd.concat(datasets)
            self.logger.info(f'fetched {nrt_new.shape[0]} fire detections')
        else:
            self.logger.warning(f'No new data fetched, nothing to add to the record.')
        return nrt_new


