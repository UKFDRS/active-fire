import logging
import requests
import pandas as pd
from io import BytesIO
from dotenv import dotenv_values
from utils import fire_date

# Place your LANCE NRT token as variable (NRT_TOKEN) in
# .env file in project root. The below uses dotenv to read
# the token into config dict.


class NRTAuth(requests.auth.AuthBase):
    """Implementation of authorization using the LANCE (eosdis)
    nrt data access token. This is called during http request setup"""
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


class FetchNRT():
    def __init__(self, nrt_token, modis_base_url, nrt_dataset_path):
        self.date_now = pd.Timestamp.utcnow()
        self.base_url = modis_base_url
        self.auth = NRTAuth(nrt_token)
        self.nrt_dataset_path = nrt_dataset_path
        self.log_file_name = self.__class__.__name__ + '.log'
        logging.basicConfig(filename=self.log_file_name,
                            level=logging.INFO,
                            format='%(asctime)s %(levelname)s: %(message)s',
                            filemode="a")
        self.logger = logging.getLogger(self.log_file_name)

    @classmethod
    def nrt_end_date_log(cls):
        """TODO Not used"""
        with open(cls.__class__.__name__) as logfile:
            lines = logfile.read().splitlines()
            last_line = lines[-1]
        return pd.Timestamp(last_line.split('NRT last datetime')[1])

    def nrt_last_date(self):
        dataset = pd.read_parquet(self.nrt_dataset_path)
        return dataset.date.max()

    def day_url(self, date):
        year = date.year
        doy = date.day_of_year
        day_url = f'MODIS_C6_1_Global_MCD14DL_NRT_{year}{doy}.txt'
        return self.base_url + day_url

    def day_nrt(self, date):
        url = self.day_url(date)
        try:
            response = requests.get(url, auth=self.auth)
            response.raise_for_status()
            dfr = pd.read_table(BytesIO(response.content),
                                sep=',', header=0)
            self.logger.info('fetched nrt for day: ' +
                             date.strftime('%Y-%m-%d'))
        except requests.exceptions.HTTPError as err:
            raise SystemExit(err)
        return dfr

    def log_nrt_end_date(self, dataset):
        self.logger.info('nrt end datetime: ' +
                         dataset.date.max().strftime('%Y-%m-%d %H:%M'))

    def fetch(self):
        days_to_fetch = pd.date_range(self.nrt_last_date(), self.date_now)
        datasets = []
        for day in days_to_fetch:
            dataset = self.day_nrt(day)
            dataset = fire_date(dataset)
            datasets.append(dataset)
        nrt_new = pd.concat(datasets)
        self.merge_nrt(nrt_new)

    def merge_nrt(self, nrt_new):
        nrt_completed = pd.read_parquet(self.nrt_dataset_path)
        self.logger.info(f'adding {nrt_new.shape[0]} rows to nrt completed')
        nrt_updated = pd.concat([nrt_completed, nrt_new])
        tot_rows = nrt_updated.shape[0]
        nrt_updated = nrt_updated.drop_duplicates()
        rows_dropped = tot_rows - nrt_updated.shape[0]
        self.logger.info(f'dropped {rows_dropped} duplicate rows')
        nrt_updated.to_parquet(self.nrt_dataset_path)
        self.log_nrt_end_date(nrt_updated)


config = dotenv_values('../.env')
nrt = FetchNRT(**config)
nrt.fetch()
# nrt.merge_nrt(nrt_new)
