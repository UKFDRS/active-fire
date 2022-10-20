"""
The file contains script to populate 
fire database from fire detections archive and nrt datasets.
tadasnik tadas.nik@gmail.com
"""
import os
import glob
import time
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from firedata.database import DataBase
from firedata.prepare import PrepData
from firedata.fetch import FetchNRT
from cluster.split_dbscan import SplitDBSCAN
from firedata._utils import ModisGrid

class ProcSQL(PrepData):
    def __init__(self, sensor):
        self.eps = self.config().getint('CLUSTER', 'eps')
        self.min_samples = self.config().getint('CLUSTER', 'min_samples')
        self.chunk_size = self.config().getint('CLUSTER', 'chunk_size')
        self.sensor = sensor
        self.db = DataBase(f'{sensor}')

    def cluster_dataframe(self, dfr):
        """Convenience method to cluster dataset passed as pandas DataFrame (dfr).
        Must contain longitude, latitude and date columns. Date is assumed to
        be unixepoch.
        """
        indx, indy = ModisGrid.modis_sinusoidal_grid_index(dfr.longitude, dfr.latitude)
        day_since = (dfr.date / 86400).astype(int)
        ars = np.column_stack([day_since, indx, indy])
        cl = SplitDBSCAN(eps=self.eps, min_samples=self.min_samples)
        cl.fit(ars)
        active_mask = cl.split(ars)
        return cl.labels_.astype(int), active_mask

    def event_ids(self, dfr):
        """Re-label events according to their first detection id. This
        is done to keep reference to the same events through reclustering.
        """
        event_id = dfr.groupby('event')['id'].transform('first')
        return event_id

    def last_event(self):
        """Return max event label"""
        sql_string = ("SELECT max(event) FROM events WHERE active = 0")
        try:
            max_event = self.db.return_single_value(sql_string)
        except:
            max_event = 0
        return max_event

    def last_id(self):
        """Query max id from extinct and active detections.
        Returns only one max value.
        """
        sql_ext = ("SELECT max(id) FROM detections_extinct")
        sql_act = ("SELECT max(id) FROM detections_active")
        try:
            max_id_ext = self.db.return_single_value(sql_ext)
            max_id_act = self.db.return_single_value(sql_act)
            max_id = max(max_id_ext, max_id_act)
        # if database does not exist
        except:
            max_id = 0
        # if no records in database:
        if not max_id:
            max_id = 0
        return max_id

    def last_date(self):
        """Return record end datetime."""
        sql_string = ("SELECT date(max(date), 'unixepoch')"
                        "FROM detections_active")
        max_date = self.db.return_single_value(sql_string)
        return max_date
    
    def active_detections(self):
        """Return all fire records from detections_active as DataFrame"""
        sql_string = """SELECT * FROM detections_active"""
        active = self.db.return_many_values(sql_string)
        return active

    def delete_active(self):
        """Delete detections active table and create an empty one"""
        print('deleting active events')
        sql_string = """DELETE FROM events
                        WHERE events.active = 1"""
        self.db.execute_sql(sql_string)
        print('deleting active detections')
        sql_string = "DROP TABLE detections_active"
        self.db.execute_sql(sql_string)
        print('deleting done')
        create_active = Config.config().get(section='SQL',
                option='sql_create_active_table')
        self.db.execute_sql(create_active)
        
    def incement_index(self, dataset):
        """Add record id column to the detections dataset
        starting with highest id already in the database"""
        id_increment = self.last_id()
        if id not in dataset:
            dataset['id'] = range(1, (len(dataset) + 1))
        dataset['id'] += id_increment 
        return dataset
    
    def consistency_check(self, dfr):
        """Verify if the fire detections dataset (dfr) is consistent
        in time with the records in detections_active in the database.
        Checks if min date in the dfr is monotonicly increasing and follows
        the max date in the database"""
        max_date_db = pd.Timestamp(self.last_date())
        min_date_dfr = pd.to_datetime(dfr.date.min(), unit='s')
        days_dif = (min_date_dfr - max_date_db).days
        print('difference in days', days_dif)
        assert (-1 < days_dif < 2), 'DataFrame is not consistent with db'

    def populate_archive(self):
        """Populate database with active fire archive"""
        archive_dir = os.path.join(self.data_path,
                                   self.sensor, 'fire_archive*.parquet')
        arch_files = glob.glob(archive_dir)
        arch_files.sort()
        print(arch_files)
        for file_name in arch_files:
            print(f'proc file {file_name}')
            dfr = pd.read_parquet(file_name)
            assert dfr.date.is_monotonic_increasing, 'Must be sorted by date'
            self.dataframe_to_db(dfr)

    def dataframe_to_db(self, dfr):
        """Prepare, cluster and insert active fire detections
        stored in Pandas DataFrame into the database
        """ 
        assert dfr.date.is_monotonic_increasing, 'The date column is not ordered'
        chunks = -(-len(dfr.index) // self.chunk_size)
        dfrs = np.array_split(dfr, chunks, axis=0)
        for nr, chunk in enumerate(dfrs):
            print(f'doing chunk {nr}', chunk.shape) 

            start_g = time.time()
            # TODO move prepare outside this method. Needs to happen
            # before to the whole dfr
            chunk = self.prepare_detections_dataset(chunk)

            end = time.time()
            print("The time of execution of prepare is :",
                  (end-start_g),"s")

            start = time.time()
            # get active events
            active = self.active_detections()
            end = time.time()
            print("The time of execution of get active is :",
                  (end-start), "s")
            #concat active and new chunk

            start = time.time()
            self.consistency_check(chunk)
            chunk = pd.concat([active, chunk])
            self.consistency_check(chunk)
            # drop duplicates
            chunk = chunk.drop_duplicates(subset=['longitude',
                                                  'latitude',
                                                  'date'])
            chunk = self.incement_index(chunk)
            # cluster new chunk and active
            print('clustering')
            event, active_flag = self.cluster_dataframe(chunk)
            print('clustering done')
            chunk['event'] = event.astype(int)
            chunk['event'] = self.event_ids(chunk)
            chunk['active'] = active_flag.astype(int)
            end = time.time()
            print("The time of execution of processing/clustering is :",
                  (end-start), "s")

            start = time.time()
            events_chunk = self.prepare_event_dataset(chunk)
            events_chunk = self.columns_dtypes(events_chunk,
                                               'SQL_events_dtypes')
            end = time.time()
            print("The time of execution of events prep is :",
                  (end-start), "s")
            start = time.time()
            self.db.insert_events(events_chunk.values.tolist())
            end = time.time()
            print("The time of execution of events insert is :",
                  (end-start), "s")
            
            start = time.time()
            chunk_active = chunk[chunk.active == 1].copy()
            chunk_active = self.columns_dtypes(chunk_active,
                                               'SQL_detections_dtypes')
            self.delete_active()
            self.db.insert_active(chunk_active.values.tolist())
            end = time.time()
            print("The time of execution of insert active is :",
                  (end-start), "s")

            start = time.time()
            chunk = chunk[chunk.active == 0]
            chunk = self.columns_dtypes(chunk, 'SQL_detections_dtypes')
            self.db.insert_extinct(chunk.values.tolist())
            end = time.time()
            print("The time of execution of insert extinct is :",
                  (end-start), "s")
            print("TOTAL time is :",
                  (end-start_g),"s")

        pass
    
    def get_nrt(self):
        base_url = self.config().get(self.sensor, 'base_url')
        fetcher = FetchNRT(self.sensor, self.nrt_token, base_url)
        start_date = self.last_date()
        end_date = pd.Timestamp.utcnow()
        dfr = fetcher.fetch(start_date, end_date)
        return dfr


if __name__ == "__main__":
    pc = ProcSQL('VIIRS_NPP')
    nrt = pc.get_nrt()
    pc.dataframe_to_db(dfr)
# sql_strings = [x[1] for x in Config.config().items('SQL')]
# pc.db.spin_up_fire_database(sql_strings)
# pc.populate_archive()
# dfr = pd.read_parquet('firedata/data/VIIRS_NPP/fire_archive_SV-C2_2022.parquet')
# pc.dataframe_to_db(dfr)

