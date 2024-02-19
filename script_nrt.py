import os
import glob

from pathlib import Path

import pandas as pd
import numpy as np
import geopandas as gpd
from activefire.config import config_dict
from firedata.database import DataBase
from firedata._utils import FireDate, ModisGrid, spatial_subset_dfr, sql_datatypes
from cluster.split_dbscan import SplitDBSCAN


def write_event(event, index, fname):
    """Writes 'label' array as dataframe using 'index'
    to file named 'fname'"""
    dfr = pd.DataFrame(event, columns=["label"], index=index)
    dfr.to_parquet(fname)


def cluster(dfr, eps):
    # mo = ModisGrid()
    # fd = FireDate()
    indx, indy = ModisGrid.modis_sinusoidal_grid_index(dfr.longitude, dfr.latitude)
    day_since = FireDate.days_since(dfr.date)
    ars = np.column_stack([day_since, indx, indy])
    sd = SplitDBSCAN(eps=eps, edge_eps=eps, split_dim=0, min_samples=1)
    sd.fit(ars)
    active_mask = sd.split(ars)
    return sd.labels_, active_mask

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


def uk_fires_archive(data_path):
    bbox = [58.7, -7.6, 49.96, 1.7]
    uks = []
    fnames = glob.glob(os.path.join(data_path, "fire_archive*.parquet"))
    for fname in fnames:
        ars = pd.read_parquet(fname)
        uk = spatial_subset_dfr(ars, bbox)
        uks.append(uk)
    return pd.concat(uks)


def uk_fires_viirs_npp():
    bbox = [58.7, -7.6, 49.96, 1.7]
    uks = []
    for year in range(2012, 2022, 1):
        print(year)
        ars = pd.read_parquet(f"firedata/data/VIIRS/fire_archive_SV-C2_{year}.parquet")
        uk = spatial_subset_dfr(ars, bbox)
        uks.append(uk)
    return pd.concat(uks)


def nrt_fire_record_uk(nrt_fname):
    bbox = [58.7, -7.6, 49.96, 1.7]
    uks = []
    ars = pd.read_parquet(nrt_fname)
    uk = spatial_subset_dfr(ars, bbox)
    return uk


def fire_record_merged_uk(archive_fname, nrt_fname):
    columns = ["latitude", "longitude", "frp", "daynight", "type", "date"]
    uk_arc = pd.read_parquet(archive_fname, columns=columns)
    uk_nrt = nrt_fire_record_uk(nrt_fname)
    uk_nrt = uk_nrt[columns].copy()
    uk_merged = pd.concat(
        [uk_arc.reset_index(drop=True), uk_nrt.reset_index(drop=True)]
    )
    uk_merged = uk_merged.drop_duplicates()
    return uk_merged


def uk_fires():
    bbox = [58.7, -7.6, 49.96, 1.7]
    uks = []
    for year in range(2002, 2022, 1):
        print(year)
        ars = pd.read_parquet(f"firedata/data/modis_archive_{year}.parquet")
        if year == 2021:
            print("add nrt")
            nrt = pd.read_parquet("firedata/data/nrt_complete.parquet")
            nrt["version"] = 6.2
            ars = pd.concat([ars, nrt])
        uk = spatial_subset_dfr(ars, bbox)
        uks.append(uk)
    return pd.concat(uks)


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


def detections_id(dfr, db):
    index_max = db.select_single_value("max", "id", "detections")


def fire_detections_to_db(dfr, db):
    """Prepare fire detections for insertion into the database.
    TODO Does quite a lot, perhaps split."""
    dfr = dfr.rename(sql_datatypes["SQL_rename"], axis=1)
    dfr = dfr.reset_index(drop=True)
    # check max value of id in database
    max_id = db.return_single_value("select max(id) from detections")
    # if max id not None, add it + 1 to the index
    if max_id:
        dfr.index += max_id + 1
    dfr.insert(0, "id", dfr.index)
    dfr_dt_columns = sql_datatypes["SQL_detections_dtypes"].keys()
    dfr_dt = dfr[dfr_dt_columns].copy()
    # remap daynight to integer
    daynight_map = {"D": 1, "N": 0}
    dfr_dt["daynight"] = dfr_dt["daynight"].map(daynight_map)
    # datetime to unix time
    dfr_dt["date"] = FireDate.unix_time(dfr_dt["date"])
    dfr_dt = dfr_dt.astype(sql_datatypes["SQL_detections_dtypes"])
    return dfr_dt


def fire_events_to_db(dfr):
    """TODO finish"""
    # dataframe with events
    dfr_ev_columns = sql_datatypes["SQL_events_dtypes"].keys()
    dfr_ev = dfr[dfr.columns.intersection(dfr_ev_columns)].copy()
    dfr_ev = dfr_ev.groupby("event")["active"].first().reset_index()
    # TODO not finnished
    wfr_ev = dfr_ev.astype(sql_datatypes["SQL_events_dtypes"])
    return dfr_ev


def clean_viirs(dfr):
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
    try:
        dfr = dfr.drop("urban_ratio", axis=1)
    except:
        pass
    urban_rat = urban_rat.reset_index(name="urban_ratio")
    dfr = dfr.merge(urban_rat, on="event")
    dfr = dfr[dfr.urban_ratio < 0.5]
    print(dfr.shape)
    # drop water events (> 50% water detections)
    water_rat = lc_count[0] / (lc_count.sum(axis=1))
    try:
        dfr = dfr.drop("water_ratio", axis=1)
    except:
        pass
    water_rat = water_rat.reset_index(name="water_ratio")
    dfr = dfr.merge(water_rat, on="event")
    dfr = dfr[dfr.water_ratio < 0.5]
    print(dfr.shape)
    # Filter out Fife NGL plant area
    bbox = [56.11, -33.3, 56.08, -3.28]
    fife = spatial_subset_dfr(dfr, bbox)
    dfr = dfr[~dfr.isin(fife)].dropna()
    print(dfr.shape)
    return dfr


if __name__ == "__main__":
    db = DataBase("VIIRS_NPP")
    cn = pd.read_parquet(Path(config_dict['OS']['data_path'], "countries_continents.parquet"))
    code = cn[cn.ISOCODE == "GBR"]["Value"].item()
    print("GBR code: ", code)
    sql_str = f"SELECT * FROM detections_extinct WHERE admin={code}"
    sql_stra = f"SELECT * FROM detections_active WHERE admin={code}"
    dfr = db.return_many_values(sql_str)
    dfr['active'] = 0
    dfra = db.return_many_values(sql_stra)
    dfra['active'] = 0
    print('data extracted')
    if (len(dfr) > 0) and (len(dfra) > 0):
        dfr = pd.concat([dfr, dfra])
    dfr = dfr.rename({"lc": "lc_m"}, axis=1)
    lc = uk_ceh_lc(dfr)
    print('uk ceh')
    dfr["lc"] = lc
    cor_lc = corine_lc(dfr)
    print('corine')
    dfr["lc_c"] = cor_lc

    dfr = get_uk_country(dfr)
    print('country')
    dfr = get_UK_climate_region(dfr)
    print('region')
    dfr = clean_nrt(dfr)
    print('clean nrt')
    """
    eps = 5
    columns = ['latitude', 'longitude', 'date']
    # uk_arc = uk_fires_archive('/home/tadas/activefire/firedata/data/VIIRS_NOAA')
    # uk_arc['type'] = 4
    # uk_arc.to_parquet('firedata/data/uk_viirs_noaa_archive.parquet')

    # uk_nrt = nrt_uk_fires_viirs()
    archive_fname_viirs_npp = 'firedata/data/uk_viirs_archive.parquet'
    archive_fname_viirs_noaa = 'firedata/data/uk_viirs_noaa_archive.parquet'
    archive_fname_modis = 'firedata/data/uk_2002_2021.parquet'
    archives = [archive_fname_viirs_npp, archive_fname_viirs_noaa]

    nrt_fname_viirs_npp = 'firedata/data/nrt_complete_viirs_npp.parquet'
    nrt_fname_viirs_noaa = 'firedata/data/nrt_complete_viirs_noaa.parquet'
    nrt_fname_modis = 'firedata/data/nrt_complete.parquet'
    nrts = [nrt_fname_viirs_npp, nrt_fname_viirs_noaa]
    # nrt_fname = 'firedata/data/nrt_complete_viirs.parquet'
    fnames = ['npp', 'noaa']
    
    for arch, nrt, fname in zip(archives, nrts, fnames):
        uk = fire_record_merged_uk(arch, nrt)
        lc = uk_ceh_lc(uk)
        uk['lc'] = lc.values
        uk['event'], uk['active'] = cluster(uk, eps)
        dfr = clean_nrt(uk)
        dfr = get_uk_country(dfr)
        dfr.to_parquet(f'firedata/data/viirs_{fname}_uk_8_26.parquet')
    
    """
    # dfr = pd.read_parquet('firedata/data/viirs_noaa_uk_8_14.parquet')
    # dfr_dt = fire_detections_to_db(dfr)

    # dfr_ev = fire_events_to_db(dfr)
    # database stuff
    # name = 'test'
    # db = DataBase(name)
    # db.insert_detections(dfr_dt.values.tolist())

    """
    lc = uk_ceh_lc(uk)
    uk['lc'] = lc.values
    uk['event'] = cluster(uk, eps)
    uk = uk[uk.lc != 0]
    uk['size'] = uk.groupby('event')['event'].transform('count')
    uk.loc[uk.event == -1, 'size'] = 1
    # uk = pd.read_parquet('firedata/data/uk_viirs_2012_2021_clean.parquet')
    uk = pd.read_parquet('uk_ros_in_hull_clean.parquet')
    dfr = clean_viirs(uk)
    dfr = pd.read_parquet('firedata/data/modis_archive_2018.parquet',
                          columns=columns)
    mo = ModisGrid()
    indx, indy = mo.modis_sinusoidal_grid_index(dfr.longitude, dfr.latitude)
    fd = FireDate()
    day_since = fd.days_since(dfr.date)
    ars = np.column_stack([day_since, indx, indy])
    sd = SplitDBSCAN(eps=eps, edge_eps=eps, split_dim=0, min_samples=2)
    sd.fit(ars)
    fname = f'firedata/event/eps_{eps}_2018.parquet'
    write_event(sd.labels_, dfr.index, fname)
    """
