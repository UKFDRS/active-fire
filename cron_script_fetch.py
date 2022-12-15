import pandas as pd
from firedata import populate_db

print("test")
pc = populate_db.ProcSQL("VIIRS_NPP")
nrt = pc.get_nrt()
nrt = pc.prepare_detections_dataset(nrt)
assert nrt.date.is_monotonic_increasing, "The date column is not ordered"
pc.dataframe_to_db(nrt)
