import os
import sqlite3
import pandas as pd
from sqlite3 import Error
from firedata._utils import sql_datatypes
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

    def execute_sql(self, sql_string):
        """ Execute sql_string
        """
        with self.create_connection() as conn:
            try:
                c = conn.cursor()
                c.execute(sql_string)
            except Error as e:
                print(e)

    def run_sql(self, sql_string):
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql_string)
            conn.commit()

    def return_single_value(self, sql_string):
        """Query a single value""" 
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.execute(sql_string)
            value = cur.fetchone()[0]
            return value

    def return_many_values(self, sql_string):
        """Query a single value""" 
        with self.create_connection() as conn:
            dataset = pd.read_sql_query(sql_string, conn)
            #cur = conn.cursor()
            #cur.execute(sql_string)
            #values = cur.fetchall()
            return dataset

    def insert_events(self, fires_records):
        """Insert fire events into the database"""
        columns = sql_datatypes['SQL_events_dtypes'].keys()
        qmks = ', '.join(['?'] * len(columns))
        sql = f"""INSERT INTO events VALUES ({qmks})""" 
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.executemany(sql, fires_records)
            conn.commit()


    def insert_records(self, table, records, columns):
        """Insert records into the database"""
        qmks = ', '.join(['?'] * len(columns))
        sql = f"""INSERT INTO {table} VALUES ({qmks})""" 
        with self.create_connection() as conn:
            cur = conn.cursor()
            cur.executemany(sql, records)
            conn.commit()

    def insert_extinct(self, records):
        columns = sql_datatypes['SQL_detections_dtypes'].keys()
        self.insert_records('detections_extinct', records, columns)

    def insert_active(self, records):
        columns = sql_datatypes['SQL_detections_dtypes'].keys()
        self.insert_records('detections_active', records, columns)

    def insert_events(self, records):
        columns = sql_datatypes['SQL_events_dtypes'].keys()
        self.insert_records('events', records, columns)

    def spin_up_fire_database(self, sql_list):
        """Convenience methot to create database and create the tables
        given as sql strings in sql_list"""
        self.create_connection()
        for sql_string in sql_list:
            self.execute_sql(sql_string) 


if __name__ == '__main__':
    name = 'test'
    db = DataBase(name)
    #db.spin_up_fire_database([sql_create_detections_table, sql_create_events_table])
    # db.create_table(sql_create_detections_table)
    #wdb.create_table(sql_create_events_table)
