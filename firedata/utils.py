import pandas as pd


def fire_date(dataset):
    dataset['date'] = pd.to_datetime(
        dataset['acq_date'] + ' ' +
        dataset.loc[:, 'acq_time'].astype(str).str.zfill(4), utc=True
        )
    dataset = dataset.drop(['acq_time', 'acq_date'], axis=1)
    return dataset
