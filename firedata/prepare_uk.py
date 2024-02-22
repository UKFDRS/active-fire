import os

import pandas as pd
import geopandas as gpd
from activefire.firedata import populate_db
from activefire.firedata._utils import spatial_subset_dfr

class ProcSQLUK(populate_db.ProcSQL):
    def __init__(self, sensor: str):
        super().__init__(sensor)

    def get_uk_fire_detections(self, max_id: int):
        """Extract UK fire detections with id more than max_id 
        from the database. UK country code is 826"""
        sql_str = f"SELECT * FROM detections_extinct WHERE id>{max_id} AND admin=826"
        sql_stra = f"SELECT * FROM detections_active WHERE admin=826"
        dfr = self.db.return_many_values(sql_str)
        dfr['active'] = 0
        dfra = self.db.return_many_values(sql_stra)
        dfra['active'] = 0
        print('data extracted')
        if (len(dfr) > 0) and (len(dfra) > 0):
            dfr = pd.concat([dfr, dfra])
        if len(dfr)>0:
            return dfr
        else:
            return pd.DataFrame()

    def transform_uk_nrt(self, dfr: pd.DataFrame):
        """Prepares near-real time active fire data for UK."""
        dfr = dfr.rename({"lc": "lc_m"}, axis=1)
        lc = self.uk_ceh_lc(dfr,)
        dfr["lc"] = lc
        cor_lc = self.corine_lc(dfr,)
        dfr["lc_c"] = cor_lc
        dfr = self.get_uk_country(dfr,)
        print('country')
        dfr = self.get_UK_climate_region(dfr,)
        print('region')
        dfr = self.clean_nrt(dfr)
        print('clean nrt')
        if len(dfr)>0:
            return dfr
        else:
            return pd.DataFrame()


    
    def load_nrt(self):
        """Loads transformed near-real FIRMS fire data to the database. A wrapper
        around self.populate_db"""
        transformed_detections_file_name = Path(self.config["OS"]["data_path"], self.config["TASKS"]["transformed_detections_nrt_data"])
        transformed_events_file_name = Path(self.config["OS"]["data_path"], self.config["TASKS"]["transformed_events_nrt_data"])
        dataset = pd.read_parquet(transformed_detections_file_name)
        max_date_dfr = pd.to_datetime(dataset.date.max(), unit="s")
        min_date_dfr = pd.to_datetime(dataset.date.min(), unit="s")
        print("load reading transformed detections max date : ", dataset.shape, max_date_dfr)
        print("load reading transformed detections min date : ", dataset.shape, min_date_dfr)
        events_dataset = pd.read_parquet(transformed_events_file_name)
        # delete active detections from the database
        self.delete_active()

        print("last date in db before insert: ", pd.Timestamp(self.last_date(), tz="utc"))
        self.db.insert_events(events_dataset)
        self.db.insert_active(dataset[dataset.active == 1])
        self.db.insert_extinct(dataset[dataset.active == 0])
        print("last date in db after insert: ", pd.Timestamp(self.last_date(), tz="utc"))

        # remove transformed datasets
        transformed_detections_file_name.unlink()
        transformed_events_file_name.unlink()

    def uk_ceh_lc(self, dfr):
        uk_lc_fname = self.config['OS']['uk_lc_fname']
        dfr[["longitude", "latitude"]].to_csv(
            "input_ceh.csv", sep=" ", index=False, header=False
        )
        os.system(
            r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s'
            % (uk_lc_fname, "input_ceh.csv", "output_ceh.csv")
        )
        return pd.read_csv("output_ceh.csv", header=None)

    def corine_lc(self, dfr):
        corine_lc_fname = self.config['OS']['corine_lc_fname']
        dfr[["longitude", "latitude"]].to_csv(
            "input_cor.csv", sep=" ", index=False, header=False
        )
        os.system(
            r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s'
            % (corine_lc_fname, "input_cor.csv", "output_cor.csv")
        )
        return pd.read_csv("output_cor.csv", header=None)

    def get_uk_country(self, dfr):
        countries = gpd.read_file(
            self.config['OS']['countries_fname']
        )
        uk = countries[countries.ADMIN == "United Kingdom"][["GEOUNIT", "geometry"]].copy()
        uk = uk.to_crs("EPSG:4326")
        gdf = gpd.GeoDataFrame(
            dfr, geometry=gpd.points_from_xy(dfr.longitude, dfr.latitude), crs=4326
        )
        pts = gpd.sjoin(gdf, uk)
        pts = pts.drop(["geometry", "index_right"], axis=1)
        df = pd.DataFrame(pts)
        return df

    def get_UK_climate_region(self, dfr):
        regions = gpd.read_file(self.config['OS']['uk_regions_file'])
        regions = regions.set_crs('EPSG:27700')
        regions = regions.to_crs('EPSG:4326')
        geometry = gpd.points_from_xy(dfr.longitude, dfr.latitude)
        gdf = gpd.GeoDataFrame(dfr, geometry=geometry, crs=4326)
        pts = gpd.sjoin(regions, gdf)
        pts = pts.drop(['geometry', 'index_right'], axis=1)
        df = pd.DataFrame(pts)
        return df

    def clean_nrt(self, dfr):
        """
        Drop fire events which are primarily stationary detections,
        water ant urban
        """
        lc_count = dfr.groupby(["event"])["lc"].value_counts().unstack(fill_value=0)
        # drop detections which are classed as static
        print(dfr.shape)
        dfr = dfr[dfr.type != 2]
        print(dfr.shape)
        # drop urban events (> 50% urban detections)
        if 20 in lc_count.columns or 21 in lc_count.columns:
            urban_rat = lc_count.loc[:, 20:].sum(axis=1) / (lc_count.sum(axis=1))
            urban_rat = urban_rat.reset_index(name="urban_ratio")
            dfr = dfr.merge(urban_rat, on="event")
            dfr = dfr[dfr.urban_ratio < 0.5]
        else:
            dfr['urban_ratio'] = 0
        print(dfr.shape)
        # drop water events (> 90% water detections)
        if 0 in lc_count.columns:
            water_rat = lc_count[0] / (lc_count.sum(axis=1))
            water_rat = water_rat.reset_index(name="water_ratio")
            dfr = dfr.merge(water_rat, on="event")
            dfr = dfr[dfr.water_ratio < 0.5]
        else:
            dfr['water_ratio'] = 0
        print(dfr.shape)
        # Filter out Fife NGL plant area
        bbox = [56.11, -33.3, 56.08, -3.28]
        fife = spatial_subset_dfr(dfr, bbox)
        dfr = dfr[~dfr.isin(fife)].dropna()
        print(dfr.shape)
        return dfr


