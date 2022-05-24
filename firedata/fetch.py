import logging
import requests
import pandas as pd
from io import BytesIO
from dotenv import dotenv_values
from _utils import MCD14DL_nrt_dtypes, FireDate

# Place your LANCE NRT token as variable (NRT_TOKEN) in
# .env file in project root. The below uses dotenv to read
# the token.


class NRTAuth(requests.auth.AuthBase):
    """Implementation of authorization using the LANCE (eosdis)
    nrt data access token. This is called during http request setup"""
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


class FetchNRT():
    def __init__(self, nrt_token, modis_base_url, nrt_dataset_path, archive_end):
        self.date_now = pd.Timestamp.utcnow()
        self.base_url = modis_base_url
        self.auth = NRTAuth(nrt_token)
        self.nrt_dataset_path = nrt_dataset_path
        self.archive_end = archive_end
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
        day_url = f'MODIS_C6_1_Global_MCD14DL_NRT_{year}{doy:03d}.txt'
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
            dfr = None
            # raise SystemExit(err)
        return dfr

    def log_nrt_end_date(self, dataset):
        self.logger.info('nrt end datetime: ' +
                         dataset.date.max().strftime('%Y-%m-%d %H:%M'))

    def prepare_nrt_dataset(self, dataset):
        dataset['instrument'] = 'MODIS'
        dataset = dataset.astype(MCD14DL_nrt_dtypes)
        dataset['type'] = 4
        dataset['date'] = FireDate.fire_dates(dataset)
        dataset = dataset.sort_values(by='date').reset_index(drop=True)
        return dataset

    def fetch(self):
        self.logger.info(f'Running fetch')
        days_to_fetch = pd.date_range(self.nrt_last_date().date(),
                self.date_now.date())
        datasets = []
        for day in days_to_fetch:
            print('fetching :', day)
            dataset = self.day_nrt(day)
            if dataset is not None:
                datasets.append(dataset)
        nrt_new = pd.concat(datasets)
        # return nrt_new
        nrt_new = self.prepare_nrt_dataset(nrt_new)
        self.merge_nrt(nrt_new)

    def merge_nrt(self, nrt_new):
        nrt_completed = pd.read_parquet(self.nrt_dataset_path)
        self.logger.info(f'adding {nrt_new.shape[0]} rows to nrt completed')
        index_increment = nrt_completed.index.start
        nrt_updated = pd.concat([nrt_completed, nrt_new])
        tot_rows = nrt_updated.shape[0]
        nrt_updated = nrt_updated.drop_duplicates()
        rows_dropped = tot_rows - nrt_updated.shape[0]
        self.logger.info(f'dropped {rows_dropped} duplicate rows')
        nrt_updated = nrt_updated.reset_index(drop=True)
        nrt_updated.index = nrt_updated.index + index_increment
        nrt_updated.to_parquet(self.nrt_dataset_path)
        self.log_nrt_end_date(nrt_updated)

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
# TODO run the bellow setting dtypes when reading csv
    config = dotenv_values('../.env')
    nrt = FetchNRT(**config)
    dfr = nrt.fetch()
