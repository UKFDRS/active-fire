import pandas as pd

def fire_date(dfr):
    dates = pd.to_datetime(
        dfr['acq_date'] + ' ' +
        dfr.loc[:, 'acq_time'].astype(str).str.zfill(4), utc=True
        )
    return dates
