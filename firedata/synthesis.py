import pandas as pd
from firedata import database
from firedata import populate_db

continents_dict = {}
sensor = "VIIRS_NPP"
pc = populate_db.ProcSQL(sensor)
# ad = pc.active_detections()
sql_str = "SELECT * FROM events WHERE active = 1 order by tot_size desc limit 100"
dfr = pc.db.return_many_values(sql_str)
# dfr = pc.active_detections()
