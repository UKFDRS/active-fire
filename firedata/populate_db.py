"""
The file contains methods to populate 
fire database from fire detections archive and nrt datasets.
tadasnik tadas.nik@gmail.com
"""
import os
import glob
import time

import numpy as np
import pandas as pd

import config
from firedata import fetch
from firedata import database
from firedata import prepare
from firedata import _utils
from cluster import split_dbscan


class ProcSQL(prepare.PrepData):
    def __init__(self, sensor: str):
        self.config = config.config_dict
        self.eps = self.config["CLUSTER"]["eps"]
        self.min_samples = self.config["CLUSTER"]["min_samples"]
        self.chunk_size = self.config["CLUSTER"]["chunk_size"]
        self.sensor = sensor
        self.db = database.DataBase(sensor)

    def get_nrt(self):
        """Fetches near-real time active fire data from FIRMS. The data
        is fetched for each day (inclusive) between the last day of
        data stored in the database and current day."""
        base_url = self.config[self.sensor]["base_url"]
        fetcher = fetch.FetchNRT(self.sensor, self.config["nrt_token"], base_url)
        start_date = pd.Timestamp(self.last_date(), tz="utc")
        end_date = pd.Timestamp.utcnow()
        dfr = fetcher.fetch(start_date, end_date)
        if dfr is not None:
            dfr = dfr.reset_index(drop=True)
        return dfr

    def cluster_dataframe(self, dfr: pd.DataFrame):
        """Convenience method to cluster dataset passed as pandas DataFrame (dfr).
        Must contain longitude, latitude and date columns. Date is assumed to
        be unixepoch.
        """
        indx, indy = _utils.ModisGrid.modis_sinusoidal_grid_index(
            dfr.longitude, dfr.latitude
        )
        day_since = (dfr.date / 86400).astype(int)
        ars = np.column_stack([day_since, indx, indy])
        cl = split_dbscan.SplitDBSCAN(eps=self.eps, min_samples=self.min_samples)
        cl.fit(ars)
        active_mask = cl.split(ars)
        return cl.labels_.astype(int), active_mask

    def event_ids(self, dfr: pd.DataFrame):
        """Re-label events according to their first detection id. This
        is done to keep reference to the same events through reclustering.
        """
        event_id = dfr.groupby("event")["id"].transform("first").values
        return event_id

    def last_event(self):
        """Return max event label"""
        sql_string = "SELECT max(event) FROM events WHERE active = 0"
        try:
            max_event = self.db.return_single_value(sql_string)
        except:
            max_event = 0
        return max_event

    def last_id(self):
        """Query max id from extinct and active detections.
        Returns only max id of the two.
        """
        sql_ext = "SELECT max(id) FROM detections_extinct"
        sql_act = "SELECT max(id) FROM detections_active"
        max_id_ext = self.db.return_single_value(sql_ext)
        max_id_act = self.db.return_single_value(sql_act)
        if (max_id_ext is None) and (max_id_act is None):
            return 0
        else:
            max_id = max(max_id_ext, max_id_act)
            return max_id

    def last_date(self):
        """Return record end datetime."""
        sql_string = "SELECT date(max(date), 'unixepoch')" "FROM detections_active"
        max_date = self.db.return_single_value(sql_string)
        return max_date

    def active_detections(self):
        """Return all fire records from detections_active as DataFrame"""
        sql_string = """SELECT * FROM detections_active"""
        active = self.db.return_many_values(sql_string)
        return active

    def delete_active(self):
        """Delete active events and delete detections_active table and create an empty one"""
        print("deleting active events")
        sql_string = """DELETE FROM events
                        WHERE events.active = 1"""
        self.db.execute_sql(sql_string)
        print("deleting active detections")
        sql_string = "DROP TABLE detections_active"
        self.db.execute_sql(sql_string)
        print("deleting done")
        create_active = self.config["SQL"]["sql_create_active_table"]
        self.db.execute_sql(create_active)

    def increment_index(self, dataset):
        """Add record id column to the detections dataset
        starting with highest id already in the database"""
        id_increment = self.last_id()
        if id not in dataset:
            dataset["id"] = range(1, (len(dataset) + 1))
        dataset["id"] += id_increment
        return dataset

    def consistency_check(self, dfr):
        """Verify if the fire detections dataset (dfr) is consistent
        in time with the records in detections_active in the database.
        Checks if min date in the dfr is monotonicly increasing and follows
        the max date in the database"""
        if not self.last_date():
            print("Database does not exist or is empty, skipping checks")
            return
        max_date_db = pd.Timestamp(self.last_date())
        min_date_dfr = pd.to_datetime(dfr.date.min(), unit="s")
        days_dif = (min_date_dfr - max_date_db).days
        print(f"last date in db {max_date_db}")
        print(f"first date in nrt {min_date_dfr}")
        print("difference in days", days_dif)
        assert -1 < days_dif < 2, "DataFrame is not consistent with db"

    def populate_archive(self):
        """Populate database with active fire archive"""
        archive_dir = os.path.join(
            self.config["OS"]["data_path"], self.sensor, "fire_archive*.parquet"
        )
        arch_files = glob.glob(archive_dir)
        arch_files.sort()
        print(arch_files)
        for file_name in arch_files:
            print(f"proc file {file_name}")
            dfr = pd.read_parquet(file_name)
            dfr = self.prepare_detections_dataset(dfr)
            self.dataframe_to_db(dfr)

    def dataframe_to_db(self, dfr: pd.DataFrame):
        """Prepare, cluster and insert active fire detections
        stored in Pandas DataFrame into the database
        """
        start_g = time.time()
        chunks = -(-len(dfr.index) // self.chunk_size)
        dfrs = np.array_split(dfr, chunks, axis=0)
        for nr, chunk in enumerate(dfrs):
            print(f"doing chunk {nr}", chunk.shape)
            self.consistency_check(chunk)
            # increment index only to new data
            chunk = self.increment_index(chunk)

            # get active events
            active = self.active_detections()
            chunk = pd.concat([active, chunk])
            # drop duplicates
            chunk = chunk.drop_duplicates(subset=["longitude", "latitude", "date"])
            # cluster new chunk and active
            event, active_flag = self.cluster_dataframe(chunk)
            chunk["event"] = event.astype(int)
            # Try to preserve past event labels
            chunk["event"] = self.event_ids(chunk)
            chunk["active"] = active_flag.astype(int)

            events_chunk = self.prepare_event_dataset(chunk)

            # delete active detections from the database
            self.delete_active()

            self.db.insert_events(events_chunk)
            self.db.insert_active(chunk[chunk.active == 1])
            self.db.insert_extinct(chunk[chunk.active == 0])
