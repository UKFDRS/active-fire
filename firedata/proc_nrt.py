import pandas as pd
from configuration import Config
from _utils import dataset_dtypes, FireDate

class ProcParquet(object):
    def __init__(self, sensor):
        data_path = Config.config['OS']['data_path']
        self.nrt_dataset = Config.config[sensor]['nrt_dataset']
        self.base_url = Config.config[sensor]['base_url']
        self.archive_end = Config.config[sensor]['archive_end']
        self.nrt_dataset_path = os.path.join(data_path, nrt_dataset)
        self.date_now = pd.Timestamp.utcnow()

    def proc_nrt(self):
        nrt_end_date = self.nrt_last_date()
        fetcher = FetchNRT(self.sensor, self.nrt_token, self.base_url)
        nrt_dataset = fetcher.fetch(nrt_end_date.date(), self.date_now.date())
        self.write_nrt_parquet(nrt_dataset)

    def nrt_last_date(self):
        """Return nrt record end datetime."""
        try:
            dataset = pd.read_parquet(self.nrt_dataset_path)
            nrt_end_date = dataset.date.max()
        except FileNotFoundError:
            nrt_end_date = pd.Timestamp(self.archive_end)
        return nrt_end_date

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

    def write_nrt_parquet(selp, nrt_new):
        nrt_new = self.prepare_nrt_dataset(nrt_new)
        if os.path.isfile(self.nrt_dataset_path):
            self.merge_nrt(nrt_new)
        else:
            nrt_new.to_parquet(self.nrt_dataset_path)
            self.log_nrt_end_date(nrt_new)

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
    # Suomi NPP is out of action...
    # for sensor in ['MODIS', 'VIIRS_NPP', 'VIIRS_NOAA']:
    for sensor in ['MODIS', 'VIIRS_NOAA']:
        nrt = FetchNRT(sensor, nrt_token, base_url)
        # nrt.fetch()
