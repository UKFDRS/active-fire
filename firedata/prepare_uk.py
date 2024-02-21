from activefire.firedata import populate_db

def uk_ceh_lc(dfr):
    lc_fname = "/home/tadas/modFire/lc_agb/data/LCD_2018.tif"
    dfr[["longitude", "latitude"]].to_csv(
        "input_ceh.csv", sep=" ", index=False, header=False
    )
    os.system(
        r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s'
        % (lc_fname, "input_ceh.csv", "output_ceh.csv")
    )
    return pd.read_csv("output_ceh.csv", header=None)

def corine_lc(dfr):
    lc_fname = "/home/tadas/modFire/data/corine_land_cover/U2018_CLC2018_V2020_20u1.tif"
    dfr[["longitude", "latitude"]].to_csv(
        "input_cor.csv", sep=" ", index=False, header=False
    )
    os.system(
        r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s'
        % (lc_fname, "input_cor.csv", "output_cor.csv")
    )
    return pd.read_csv("output_cor.csv", header=None)

def get_uk_country(dfr):
    countries = gpd.read_file(
        "/home/tadas/modFire/fdi/data/ne_50m_admin_0_map_subunits.shp"
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

def get_UK_climate_region(dfr):
    uk_regions_file_name = config_dict['OS']['uk_regions_file']
    regions = gpd.read_file(uk_regions_file_name)
    regions = regions.set_crs('EPSG:27700')
    regions = regions.to_crs('EPSG:4326')
    geometry = gpd.points_from_xy(dfr.longitude, dfr.latitude)
    gdf = gpd.GeoDataFrame(dfr, geometry=geometry, crs=4326)
    pts = gpd.sjoin(regions, gdf)
    pts = pts.drop(['geometry', 'index_right'], axis=1)
    df = pd.DataFrame(pts)
    return df

def clean_nrt(dfr):
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
    urban_rat = (lc_count[20] + lc_count[21]) / (lc_count.sum(axis=1))
    urban_rat = urban_rat.reset_index(name="urban_ratio")
    dfr = dfr.merge(urban_rat, on="event")
    dfr = dfr[dfr.urban_ratio < 0.5]
    # dfr = dfr.drop("urban_ratio", axis=1)
    print(dfr.shape)
    # drop water events (> 90% water detections)
    water_rat = lc_count[0] / (lc_count.sum(axis=1))
    water_rat = water_rat.reset_index(name="water_ratio")
    dfr = dfr.merge(water_rat, on="event")
    dfr = dfr[dfr.water_ratio < 0.5]
    # dfr = dfr.drop("water_ratio", axis=1)
    print(dfr.shape)
    # Filter out Fife NGL plant area
    bbox = [56.11, -33.3, 56.08, -3.28]
    fife = spatial_subset_dfr(dfr, bbox)
    dfr = dfr[~dfr.isin(fife)].dropna()
    print(dfr.shape)
    return dfr

class ProcSQLUK(populate_db.ProcSQL):
    def __init__(self, sensor: str):
        super().__init__(sensor)

    def get_nrt(self):
        """Faetches near-real time active fire data from FIRMS. The data
        is fetched for each day (inclusive) between the last day of
        data stored in the database and current day."""
        nrt_file_name = Path(self.config["OS"]["data_path"], self.config["TASKS"]["fetch_nrt_data"])
        base_url = self.config[self.sensor]["base_url"]
        fetcher = fetch.FetchNRT(self.sensor, self.config["nrt_token"], base_url)
        start_date = pd.Timestamp(self.last_date(), tz="utc")
        end_date = pd.Timestamp.utcnow()
        dfr = fetcher.fetch(start_date, end_date)
        dfr = self.prepare_detections_dataset(dfr)
        new_data = self.new_data_check(dfr)
        print("new_data_check: ", new_data)
        if dfr is not None:
            dfr = dfr.reset_index(drop=True)
        if len(dfr)>0 and new_data:
            print("fetch - writing nrt data to file")
            dfr.to_parquet(nrt_file_name)
            return True
        else:
            return False

    def transform_nrt(self):
        """Prepares near-real time active fire data from FIRMS. A wrapper
        around prepare_detections_dataset and cluster routines"""
        raw_nrt_file_name = Path(self.config["OS"]["data_path"], self.config["TASKS"]["fetch_nrt_data"])
        transformed_detections_file_name = Path(self.config["OS"]["data_path"], self.config["TASKS"]["transformed_detections_nrt_data"])
        transformed_events_file_name = Path(self.config["OS"]["data_path"], self.config["TASKS"]["transformed_events_nrt_data"])
        dataset = pd.read_parquet(raw_nrt_file_name)
        self.consistency_check(dataset)
        dataset = self.increment_index(dataset)

        active = self.active_detections()
        min_date_active = pd.to_datetime(active.date.min(), unit="s")
        max_date_active = pd.to_datetime(active.date.max(), unit="s")
        print("active shape: ", active.shape, min_date_active, max_date_active)
        dataset = pd.concat([active, dataset])
        # drop duplicates
        dataset = dataset.drop_duplicates(subset=["longitude", "latitude", "date"])
        # cluster new chunk and active
        event, active_flag = self.cluster_dataframe(dataset)
        dataset["event"] = event.astype(int)
        # Try to preserve past event labels
        dataset["event"] = self.event_ids(dataset)
        dataset["active"] = active_flag.astype(int)

        events_dataset = self.prepare_event_dataset(dataset)

        max_date_dfr = pd.to_datetime(dataset.date.max(), unit="s")
        print("writing transformed detections max date : ", max_date_dfr)
        dataset.to_parquet(transformed_detections_file_name)
        events_dataset.to_parquet(transformed_events_file_name)

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


