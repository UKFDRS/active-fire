import os
import sqlite3
from sqlite3 import Error
from firedata._utils import FireDate, sql_datatypes
from configuration import Config

class DataBase(object):
    def __init__(self, name):
        self.name = name
        data_path = Config.config().get(section='OS', option='data_path')
        self.__db_file = os.path.join(data_path, name + '.db')

    def create_connection(self):
        """ create a database connection to the SQLite database
            specified by __db_file
        wreturn: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(self.__db_file)
            return conn
        except Error as e:
            print(e)
        return conn

    def create_table(self, create_table_sql):
        """ create a table from the create_table_sql statement
        :param conn: Connection object
        :param create_table_sql: a CREATE TABLE statement
        :return:
        """
        with self.create_connection() as conn:
            try:
                c = conn.cursor()
                c.execute(create_table_sql)
            except Error as e:
                print(e)

    def return_single_value(self, sql_string):
        """Query a single value""" 
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql_string)
            value = cur.fetchone()[0]
            return value

    def insert_detections(self, fires_records):
        """Insert fire detections into the database"""
        columns = sql_datatypes['SQL_detections_dtypes'].keys()
        qmks = ', '.join(['?'] * len(columns))
        sql = f"""INSERT INTO detections VALUES ({qmks})""" 
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.executemany(sql, fires_records)
            conn.commit()

    def create_fire_tables(self, sql_list):
        """Create a table for each sql_string in sql_list"""
        for sql_string in sql_list:
            self.create_table(sql_string) 

    def spin_up_fire_database(self, sql_list):
        """Convenience methot to create database and create the tables
        given as sql strings in sql_list"""
        self.create_connection()
        self.create_fire_tables(sql_list)


if __name__ == '__main__':
    name = 'test'
    db = DataBase(name)

    detections_table_global = """CREATE TABLE IF NOT EXISTS detections (
                                    id        integer PRIMARY KEY,
                                    latitude  real    NOT NULL,
                                    longitude real    NOT NULL,
                                    frp       real    NOT NULL,
                                    daynight  integer NOT NULL,
                                    type      integer NOT NULL,
                                    date      integer NOT NULL,
                                    lc        integer NOT NULL,
                                    admin_lc  integer,
                                    admin     text    NOT NULL,
                                    cont      text    NOT NULL,
                                    event     integer NOT NULL,
                                    FOREIGN KEY (event) REFERENCES events (event_id)
                                    unique (latitude, longitude, date)
                                    ); """


    sql_create_detections_table = """ CREATE TABLE IF NOT EXISTS detections (
                                        id        integer PRIMARY KEY,
                                        latitude  real    NOT NULL,
                                        longitude real    NOT NULL,
                                        frp       real    NOT NULL,
                                        daynight  integer NOT NULL,
                                        type      integer NOT NULL,
                                        date      integer NOT NULL,
                                        lc        integer NOT NULL,
                                        admin     text    NOT NULL,
                                        event     integer NOT NULL,
                                        FOREIGN KEY (event) REFERENCES events (event_id)
                                        unique (latitude, longitude, date)
                                        ); """

    sql_create_events_table = """ CREATE TABLE IF NOT EXISTS events (
                                        event_id  integer PRIMARY KEY,
                                        active    integer NOT NULL
                                        );"""
    
    #db.spin_up_fire_database([sql_create_detections_table, sql_create_events_table])
    # db.create_table(sql_create_detections_table)
    #wdb.create_table(sql_create_events_table)
