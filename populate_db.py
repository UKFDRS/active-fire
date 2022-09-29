"""
The file contains script to populate 
fire database from fire detections archive and nrt datasets.
tadasnik tadas.nik@gmail.com
"""
import os
import glob
import pandas as pd
import numpy as np
from configuration import Config
from sklearn.preprocessing import LabelEncoder
from firedata.database import DataBase
from firedata.prepare import PrepData
from cluster.split_dbscan import SplitDBSCAN
from firedata._utils import ModisGrid

sql_create_detections_table = """ CREATE TABLE IF NOT EXISTS detections (
                                    id        integer PRIMARY KEY,
                                    latitude  real    NOT NULL,
                                    longitude real    NOT NULL,
                                    frp       real    NOT NULL,
                                    daynight  integer NOT NULL,
                                    type      integer NOT NULL,
                                    date      integer NOT NULL,
                                    lc        integer NOT NULL,
                                    admin     integer,
                                    event     integer NOT NULL,
                                    active    integer NOT NULL,
                                    FOREIGN KEY (event) REFERENCES events (event)
                                    ); """

sql_create_events_table = """ CREATE TABLE IF NOT EXISTS events (
                                    event      integer PRIMARY KEY,
                                    active     integer NOT NULL,
                                    tot_size   integer NOT NULL,
                                    max_size   integer NOT NULL,
                                    start_date integer NOT NULL,
                                    last_date  integer NOT NULL,
                                    latitude   real NOT NULL,
                                    longitude  real NOT NULL,
                                    continent  text,
                                    name       text
                                    );"""

class ProcSQL(PrepData):
    def __init__(self, sensor):
        self.sensor = sensor
        self.db = DataBase('trial_populate')
        # TODO this should be set in config
        self.eps = 5

    def cluster_dataframe(self, dfr):
        """Convenience method to cluster dataset passed as pandas DataFrame (dfr).
        Must contain longitude, latitude and date columns. Date is assumed to
        be unixepoch.
        """
        indx, indy = ModisGrid.modis_sinusoidal_grid_index(dfr.longitude, dfr.latitude)
        day_since = (dfr.date / 86400).astype(int)
        ars = np.column_stack([day_since, indx, indy])
        cl = SplitDBSCAN(eps = self.eps, min_samples=1)
        cl.fit(ars)
        active_mask = cl.split(ars)
        return cl.labels_.astype(int), active_mask

    def event_ids(self, dfr):
        """Re-label events according to their first detection id. This
        is done to keep reference to the same events through reclustering.
        """
        event_id = dfr.groupby('event')['id'].transform('first')
        return event_id

    def populate_db(self, dfr, event, active):
        dfr['events_post'] = event
        dfr['active_post'] = active.astype(int)
        past_active = dfr



    def transform_labels(self, dataset):
        """obsolete"""
        done_mask = dataset.active == False
        le = LabelEncoder()
        done_labels = le.fit_transform(dataset[done_mask]['event'])
        done_labels = done_labels + self.last_event() + 1
        dataset.loc[done_mask, 'event'] = done_labels
        done_events = dataset[done_mask]

    def last_event(self):
        """Return max event label"""
        sql_string = ("SELECT max(event) FROM events WHERE active = 0;")
        try:
            max_event = self.db.return_single_value(sql_string)
        except:
            max_event = 0
        return max_event

    def last_id(self):
        """Return max event label"""
        sql_string = ("SELECT max(id) FROM detections")
        try:
            max_id = self.db.return_single_value(sql_string)
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
                        "FROM detections")
        max_date = self.db.return_single_value(sql_string)
        return max_date
    
    
    def active_detections(self):
        """Return active fires from detections as DataFrame"""
        #sql_string = """SELECT * FROM detections WHERE active=1"""
        sql_string = """SELECT 
                          d.id AS id,
                          d.latitude AS latitude,
                          d.longitude AS longitude,
                          d.frp AS frp,
                          d.daynight AS daynight,
                          d.type AS type,
                          d.date AS date,
                          d.lc AS lc,
                          d.admin AS admin,
                          d.event AS event
                        FROM detections d
                          INNER JOIN events
                          ON d.event == events.event
                          WHERE events.active == 1
                          """
        active = self.db.return_many_values(sql_string)
        return active


    def delete_active(self):
        print('deleting active events')
        sql_string = """DELETE FROM events
                        WHERE events.active = 1"""
        self.db.run_sql(sql_string)
        print('deleting active detections')
        sql_string = """DELETE FROM detections
                        WHERE detections.active = 1"""
        self.db.run_sql(sql_string)
        print('deleting done')
        
    def incement_index(self, dataset):
        id_increment = self.last_id()
        if id not in dataset:
            dataset['id'] = range(1, (len(dataset) + 1))
        dataset['id'] += id_increment 
        return dataset

    

eps = 5
chunk_size = 1000000
dfr = pd.read_parquet('firedata/data/VIIRS_NPP/fire_archive_SV-C2_2012.parquet')
dfr = dfr.sort_values('date')
chunks = -(-len(dfr.index) // chunk_size)
dfrs = np.array_split(dfr, chunks, axis=0)
pc = ProcSQL('VIIRS_NPP')
pc.db.spin_up_fire_database([sql_create_detections_table, sql_create_events_table])
for nr, dfr in enumerate(dfrs):
    print(f'doing chunk {nr}', dfr.shape) 
    dfr = pc.incement_index(dfr)
    dfr = pc.prepare_detections_dataset(dfr)
# get active events
    active = pc.active_detections()
#concat active and new chunk
    dfr = pd.concat([active, dfr])
# cluster new chunk and active
    print('clustiring')
    event, active_flag = pc.cluster_dataframe(dfr)
    print('clustiring done')
    dfr['event'] = event.astype(int)
    dfr['event'] = pc.event_ids(dfr)
    dfr['active'] = active_flag.astype(int)
    gdfr = pc.prepare_event_dataset(dfr)

    dfr = pc.columns_dtypes(dfr, 'SQL_detections_dtypes')
    gdfr = pc.columns_dtypes(gdfr, 'SQL_events_dtypes')
    pc.delete_active()

    print('insert events')
    pc.db.insert_events(gdfr.values.tolist())
    print('insert detections')
    pc.db.insert_detections(dfr.values.tolist())
    print('insert done')
