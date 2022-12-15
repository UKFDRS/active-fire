import pandas as pd
from firedata import database
from firedata import populate_db

continents_dict = {}
sensor = "VIIRS_NPP"
pc = populate_db.ProcSQL(sensor)
# ad = pc.active_detections()
# sql_str = "SELECT * FROM events WHERE active = 1  GROUP BY continent order by tot_size desc limit 10"
sql_str = """
SELECT * FROM (
    SELECT *,
           row_number() over (
               partition by continent order by tot_size desc
               ) as continent_count
    from events WHERE active = 1) ranks
where continent_count <= 50;
"""
dfr = pc.db.return_many_values(sql_str)
# dfr = pc.active_detections()
