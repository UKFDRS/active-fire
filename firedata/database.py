import os
import sqlite3
from sqlite3 import Error
from configparser import ConfigParser
from _utils import FireDate

class DataBase(object):
    def __init__(self, data_path, name):
        self.name = name
        self.db_file = os.path.join(data_path, name + '.db')

    def create_connection(self):
        """ create a database connection to the SQLite database
            specified by db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
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

    def insert_detections(self, fires):
        """
        Create a new task
        :param conn:
        :param task:
        :return:
        """
        sql = ''' INSERT INTO tasks(name,priority,status_id,project_id,begin_date,end_date)
                  VALUES(?,?,?,?,?,?) ''' 
        with self.create_connection() as conn:
            task_1 = ('Analyze the requirements of the app', 1, 1, project_id, '2015-01-01', '2015-01-02')
            cur = conn.cursor()
            cur.execute(sql, task)
            conn.commit()

    def create_fire_tables(self, sql_list):
        for sql_string in sql_list:
            self.create_table(sql_string) 

    def spin_up_fire_database(self, sql_list):
        self.create_connection()
        self.create_fire_tables(sql_list)


def main():
    database = r"C:\sqlite\db\pythonsqlite.db"

    sql_create_noaa_table = """ CREATE TABLE IF NOT EXISTS noaa (
                                        id integer PRIMARY KEY,
                                        name text NOT NULL,
                                        begin_date text,
                                        end_date text
                                    ); """

    sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS tasks (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    priority integer,
                                    status_id integer NOT NULL,
                                    project_id integer NOT NULL,
                                    begin_date text NOT NULL,
                                    end_date text NOT NULL,
                                    FOREIGN KEY (project_id) REFERENCES projects (id)
                                );"""

    # create a database connection
    conn = create_connection(database)

    # create tables
    if conn is not None:
        # create projects table
        create_table(conn, sql_create_projects_table)

        # create tasks table
        create_table(conn, sql_create_tasks_table)
    else:
        print("Error! cannot create the database connection.")


if __name__ == '__main__':
    config = ConfigParser()
    config.read('config.ini')
    data_path = config['OS']['data_path']
    name = 'test'
    db = DataBase(data_path, name)


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
    
    db.spin_up_fire_database([sql_create_detections_table, sql_create_events_table])
    #db.create_table(sql_create_detections_table)
    #db.create_table(sql_create_events_table)
    #main()
