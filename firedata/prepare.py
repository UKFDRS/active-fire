import os
import glob
import pathlib

import numpy as np
import pandas as pd
from pyhdf import SD

from .. import config
from ._utils import dataset_dtypes, sql_datatypes, ModisGrid, FireDate


def group_mode(
    dfr: pd.DataFrame, group_cols: list[str], value_col: str, count_col: str
) -> pd.DataFrame:
    return (
        dfr.groupby(group_cols + [value_col])
        .size()
        .to_frame(count_col)
        .reset_index()
        .sort_values(count_col, ascending=False)
        .drop_duplicates(subset=group_cols)
    )


def read_hdf4(dataset_path: str, dataset=None):
    """
    Reads Scientific Data Set(s) stored in a HDF-EOS (HDF4) file
    defined by the file_name argument. Returns SDS(s) given
    name string provided by dataset argument. If
    no dataset is given, the function returns pyhdf
    SD instance of the HDF-EOS file open in read mode.
    """
    try:
        product = SD.SD(dataset_path)
        if dataset == "all":
            dataset = list(product.datasets().keys())
        if isinstance(dataset, list):
            datasetList = []
            for sds in dataset:
                selection = product.select(sds).get()
                datasetList.append(selection)
            return datasetList
        elif dataset:
            selection = product.select(dataset).get()
            return selection
        return product
    except OSError as exc:
        print(f"Could not read dataset {dataset_path} {exc}")
        raise


def add_admin(detections: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """Add a column with country code to the events dataset"""
    cn = group_mode(detections, ["event"], "admin", "gadmin")
    events = pd.merge(events, cn[["event", "admin"]], on="event", how="left")
    print(events.head)
    return events


def add_lc1(detections: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """Add 'lc1' column with dominant (mode) land cover type for the event"""
    lc = group_mode(detections, ["event"], "lc", "glc")
    lc = lc.rename({"lc": "lc1"}, axis=1)
    events = pd.merge(events, lc[["event", "lc1"]], on="event", how="left")
    return events


def add_vegetation_ratio(
    detections: pd.DataFrame, events: pd.DataFrame
) -> pd.DataFrame:
    """
    Determine proportion of vegetation fire detections
    and add a column to the events dataset.
    """
    vegetation_lcs = list(range(1, 13))
    vegetation_lcs.append(14)
    lc_count = detections.groupby(["event"])["lc"].value_counts().unstack(fill_value=0)
    veg_rat = lc_count.loc[:, lc_count.columns.isin(vegetation_lcs)].sum(
        axis=1
    ) / lc_count.sum(axis=1)
    veg_rat = veg_rat.reset_index(name="veg_ratio")
    events = pd.merge(events, veg_rat, on="event", how="left")
    return events


class PrepData:
    def __init__(self, sensor: str):
        self.sensor = sensor
        self.config = config.config_dict
        self.date_now = pd.Timestamp.utcnow()

    def columns_dtypes(self, dataset, dtypes_dict_key):
        """Selects required columns and sets data types as per
        dtype dictionary.
        """
        sql_dtypes = sql_datatypes[dtypes_dict_key]
        dataset = dataset[sql_dtypes.keys()]
        dataset = dataset.astype(sql_dtypes)
        return dataset

    def prepare_detections_dataset(self, dataset):
        """Convenience method combining several processes into one call.
        Adds missing columns and additional information to
        the detections dataset. Sets data types and returns the required
        columns only. Works (or at least should) both with archive and nrt
        datasets. TODO a lot going on here, perhaps split.
        """
        # If no date column add one
        if "date" not in dataset:
            dataset["date"] = FireDate.fire_dates(dataset)
        # If no type column assume nrt dataset
        if "type" not in dataset:
            dataset["type"] = 4
        # add placeholder event/active columns
        if "event" not in dataset:
            dataset["event"] = -1
        if "id" not in dataset:
            dataset["id"] = -1
        # remap daynight to integer
        daynight_map = {"D": 1, "N": 0}
        dataset["daynight"] = dataset["daynight"].map(daynight_map)
        # Add country code
        dataset["admin"] = self.country_code(dataset)
        # Add land cover
        dataset = self.modis_lulc(dataset)
        # datetime to unix time
        dataset["date"] = FireDate.unix_time(dataset["date"])
        # sort by date
        dataset = dataset.sort_values(by="date").reset_index(drop=True)
        # select required columns
        dataset = self.columns_dtypes(dataset, "SQL_detections_dtypes")
        return dataset

    def prepare_event_dataset(self, dataset: pd.DataFrame) -> pd.DataFrame:
        """Generate per event dataset."""
        dfg = dataset.groupby("event")
        dfg = dfg.agg(
            tot_size=("type", "count"),
            start_date=("date", "min"),
            last_date=("date", "max"),
            active=("active", "first"),
            longitude=("longitude", "median"),
            latitude=("latitude", "median"),
        ).reset_index()
        # dfg = pd.merge(dfg, lc[['event', 'lc']], on = 'event', how = 'left')
        dfg = add_admin(dataset, dfg)
        dfg = add_lc1(dataset, dfg)
        dfg = add_vegetation_ratio(dataset, dfg)
        dfg = self.add_continent(dfg)
        # dfg['duration'] = dfg.last_date - dfg.start_date
        max_size = dataset.groupby(["event", "date"])["type"].count()
        dfg["max_size"] = max_size.groupby(level=0).max().values
        dfg["name"] = None
        dfg = self.columns_dtypes(dfg, "SQL_events_dtypes")
        return dfg

    def filter_non_vegetation_events(self, dfr):
        """
        Drop fire events which are primarily non_vegetation detections,
        (water and urban) and also unclassified
        """
        vegetation_lcs = list(range(1, 13))
        vegetation_lcs.append(14)
        lc_count = dfr.groupby(["event"])["lc"].value_counts().unstack(fill_value=0)
        veg_rat = lc_count[vegetation_lcs] / lc_count.sum(axis=1)
        # drop detections which are classed as static
        # drop urban events (> 50% urban detections)
        if 13 in lc_count:
            urban_rat = lc_count[13] / (lc_count.sum(axis=1))
            urban_rat = urban_rat.reset_index(name="urban_ratio")
            dfr = dfr.merge(urban_rat, on="event")
            dfr = dfr[dfr.urban_ratio < 0.5]
            dfr = dfr.drop("urban_ratio", axis=1)
        if any(x in [15, 16, 17] for x in lc_count):
            barren_rat = lc_count[[15, 16, 17]].sum(axis=1) / (lc_count.sum(axis=1))
            barren_rat = barren_rat.reset_index(name="barren_ratio")
            dfr = dfr.merge(barren_rat, on="event")
            dfr = dfr[dfr.barren_ratio < 0.5]
            dfr = dfr.drop("barren_ratio", axis=1)

        # drop water events and unclassified (> 50% water detections)
        if 0 in lc_count:
            water_rat = lc_count[0] / (lc_count.sum(axis=1))
            water_rat = water_rat.reset_index(name="water_ratio")
            dfr = dfr.merge(water_rat, on="event")
            dfr = dfr[dfr.water_ratio < 0.5]
            dfr = dfr.drop("water_ratio", axis=1)
        if 255 in lc_count:
            unclass_rat = lc_count[255] / (lc_count.sum(axis=1))
            water_rat = unclass_rat.reset_index(name="unclass_rat")
            dfr = dfr.merge(unclass_rat, on="event")
            dfr = dfr[dfr.unclass_rat < 0.5]
            dfr = dfr.drop("unclass_ratio", axis=1)
        return dfr

    def modis_lulc(self, dataset):
        """Add land cover from MODIS MCD12Q1 product"""
        lulc_data_path = self.config["OS"]["lulc_data_path"]
        tile_h, tile_v, indx, indy = ModisGrid.modis_sinusoidal_coords(
            dataset.longitude, dataset.latitude
        )
        # Create a dataframe with grid indices
        dfr = pd.DataFrame(
            {"tile_h": tile_h, "tile_v": tile_v, "indx": indx, "indy": indy}
        )
        dfr.index = dataset.index
        grouped = dfr.groupby(["tile_h", "tile_v"])
        dfrs = []
        lulc_year = self.modis_lulc_year(dataset)
        for name, gr in grouped:
            tile_h = name[0]
            tile_v = name[1]
            lulc_fname = pathlib.Path(
                lulc_data_path, f"MCD12Q1.A{lulc_year}001.h{tile_h:02}v{tile_v:02}*"
            )
            try:
                lulc_fname = glob.glob(str(lulc_fname))[0]
                dslc = read_hdf4(lulc_fname)
                gr["lc"] = dslc.select("LC_Type1").get()[gr["indy"], gr["indx"]]
            except IndexError:
                print("tile not found: ", lulc_fname)
                gr["lc"] = 0
            gr = gr.drop(["tile_h", "tile_v", "indx", "indy"], axis=1)
            dfrs.append(gr)
        dfr = pd.concat(dfrs)
        dataset = dataset.join(dfr["lc"])
        return dataset

    def modis_lulc_year(self, dataset):
        """Returns the closest year in available MCD12Q1 product to
        mode year of the fire detections dataset"""
        lulc_data_path = pathlib.Path(self.config["OS"]["lulc_data_path"], "*.hdf")
        file_names = glob.glob(str(lulc_data_path))
        years = [int(x.split(".A")[1][:4]) for x in file_names]
        years_unique = np.unique(years)
        dataset_year = dataset["date"].dt.year.value_counts().index[0]
        lulc_year = years_unique[np.argmin(np.abs((years_unique - dataset_year)))]
        return lulc_year

    def country_code(self, dfr):
        """
        Supplementary country information added to the dataset
        """
        file_path = pathlib.Path(
            self.config["OS"]["admin_data_path"],
            "gpw_v4_national_identifier_grid_rev11_30_sec.tif",
        )
        dfr[["longitude", "latitude"]].to_csv(
            "input.csv", sep=" ", index=False, header=False
        )
        os.system(
            r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s'
            % (file_path, "input.csv", "output.csv")
        )
        admin = np.loadtxt("output.csv")
        return admin.astype(int)

    def add_continent(self, dfr: pd.DataFrame):
        """Add a column with event continent"""
        continents_path = pathlib.Path(
            self.config["OS"]["admin_data_path"], "countries_continents.parquet"
        )
        cids = pd.read_parquet(continents_path)
        cids = cids.rename({"Value": "admin", "Continent_Name": "continent"}, axis=1)
        cids = cids.loc[(cids.continent.notna()), :]
        cidsg = cids.groupby(["admin"])["continent"].first().reset_index()
        dfr = pd.merge(dfr, cidsg[["admin", "continent"]], on="admin", how="left")
        # Russia east of 50deg longitude is considered Asia
        dfr.loc[((dfr.admin == 643) & (dfr.longitude > 50)), "continent"] = "Asia"
        # Kazakhstan is considered Asia
        dfr.loc[(dfr.admin == 398), "continent"] = "Asia"
        dfr.loc[dfr.continent == -999, "continent"] = "None"
        return dfr

    def prepare_nrt_dataset(self, dataset):
        """Adds required columns and sets data types of the nrt dataset"""
        # In case dataset doesn't have instrument column:
        dataset["instrument"] = self.sensor
        sensor_str = self.sensor.split("_")[0]
        dataset = dataset.astype(dataset_dtypes[f"{sensor_str}_nrt_dtypes"])
        dataset["type"] = 4
        dataset["date"] = FireDate.fire_dates(dataset)
        # Dropping original date/time columns
        dataset = dataset.drop(["acq_date", "acq_time"], axis=1)
        dataset = dataset.sort_values(by="date").reset_index(drop=True)
        return dataset

    def merge_nrt(self, nrt_new):
        """Merges the fetched data (nrt_new) to nrt_completed and
        overwrites the nrt_completed file at nrt_dataset_path
        """
        nrt_completed = pd.read_parquet(self.nrt_dataset_path)
        # Is index increment needed?
        index_increment = nrt_completed.index[0]
        nrt_updated = pd.concat([nrt_completed, nrt_new])
        tot_rows = nrt_updated.shape[0]
        nrt_updated = nrt_updated.drop_duplicates()
        rows_dropped = tot_rows - nrt_updated.shape[0]
        rows_added = nrt_new.shape[0] - rows_dropped
        if rows_added > 0:
            self.logger.info(f"Adding {rows_added} active fires to nrt record")
            # write the updated file
            nrt_updated = nrt_updated.reset_index(drop=True)
            nrt_updated.index = nrt_updated.index + index_increment
            nrt_updated.to_parquet(self.nrt_dataset_path)
            self.log_nrt_end_date(nrt_updated)
        else:
            self.logger.info(f"No new fire detections, snooze")

    def drop_in_archive_nrt(self):
        """Select nrt data starting after archive_end datetime.
        Used to reduce nrt dataset after archive update.
        WARNING: overwrites the file at nrt_dataset_path!
        """
        nrt = pd.read_parquet(self.nrt_dataset_path)
        archive_end_dt = pd.Timestamp(self.archive_end)
        nrt_selected = nrt[nrt.date.values > archive_end_dt]
        nrt_selected.to_parquet(self.nrt_dataset_path)
