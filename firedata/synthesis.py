import pandas as pd
from firedata import database
from firedata import populate_db

continents_dict = {}
sensor = "MODIS"
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
events_sql = "SELECT * FROM events WHERE tot_size > 20"

sql_uk = """SELECT * FROM detections_extinct WHERE admin == 826"""
dfr = pc.db.return_many_values(sql_uk)
# dfr = pd.read_parquet("firedata/data/active_detections.parquet")
