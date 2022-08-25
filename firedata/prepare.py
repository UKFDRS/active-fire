import pandas as pd
from configuration import Config
from _utils import dataset_dtypes, ModisGrid, FireDate

def add_country(dfr):
    """
    Supplementary country information added to the dataset
    """
    dfr[['lon', 'lat']].to_csv('input.csv', sep = ' ', index = False, header = False)
    os.system(r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s' % ('data/gpw_v4_national_identifier_grid_rev11_30_sec.tif',
                                                                      'input.csv','output.csv'))
    dfr['cn'] = np.loadtxt('output.csv')
    dfr = dfr.fillna(-999)
    dfr['cn'] = dfr['cn'].astype(int)
    return dfr

def get_continent(dfr):
    cids = pd.read_parquet('data/countries_continents.parquet')
    cids = cids.rename({'Value': 'cn', 'Continent_Name': 'Continent'}, axis = 1)
    cids = cids.loc[(cids.Continent.notna()), :]
    cidsg = cids.groupby(['cn'])['Continent'].first().reset_index()
    dfr = pd.merge(dfr, cidsg[['cn', 'Continent']], on = 'cn', how = 'left')
    dfr.loc[((dfr.cn == 643) & (dfr.lon > 50)), 'Continent'] = 'Asia'
    dfr.loc[(dfr.cn == 398), 'Continent'] = 'Asia'
    dfr.loc[dfr.Continent==-999, 'Continent'] = 'None'
    return dfr

def travel_time(dfr):
    """
    Travel time to city/town added to the ba dataset (dfr).
    The dataset:
    D.J. Weiss, A. Nelson, H.S. Gibson, W. Temperley, S. Peedell, A. Lieber,
    M. Hancher, E. Poyart, S. Belchior, N. Fullman, B. Mappin, U. Dalrymple,
    J. Rozier, T.C.D. Lucas, R.E. Howes, L.S. Tusting, S.Y. Kang, E. Cameron,
    D. Bisanzio, K.E. Battle, S. Bhatt, and P.W. Gething. A global map of
    travel time to cities to assess inequalities in accessibility in 2015.
    (2018). Nature. doi:10.1038/nature25181.
    """
    dfr[['lon', 'lat']].to_csv('input.csv', sep = ' ', index = False, header = False)
    os.system(r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s' % ('data/2015_accessibility_to_cities_v1.0.tif',
                                                                      'input.csv','output.csv'))
    dfr['time2city'] = np.loadtxt('output.csv')
    dfr = dfr.fillna(-999)
    dfr['time2city'] = dfr['time2city'].astype(int)
    return dfr


def pop_dens(dfr):
    dfr['pop_den'] = 0
    years = [2000, 2005, 2010, 2015, 2020]
    for year in dfr.year.unique():
        print(year)
        file_year = min(years, key=lambda x:abs(x-year))
        print(file_year)
        dfr.loc[dfr.year==year, ['lon', 'lat']].to_csv('input.csv', sep = ' ', index = False, header = False)
        if file_year == 2000:
            os.system(r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s' % ('data/gpw_v4_population_density_rev11_2000_2pt5_min.tif',
                                                                      'input.csv','output.csv'))
        if file_year == 2005 :
            os.system(r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s' % ('data/gpw_v4_population_density_rev11_2005_2pt5_min.tif',
                                                                      'input.csv','output.csv'))
        if file_year == 2010 :
            os.system(r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s' % ('data/gpw_v4_population_density_rev11_2010_2pt5_min.tif',
                                                                      'input.csv','output.csv'))
        if file_year == 2015 :
            os.system(r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s' % ('data/gpw_v4_population_density_rev11_2015_2pt5_min.tif',
                                                                      'input.csv','output.csv'))
        if file_year == 2020 :
            os.system(r'gdallocationinfo -valonly -wgs84 "%s" <%s >%s' % ('data/gpw_v4_population_density_rev11_2020_2pt5_min.tif',
                                                                      'input.csv','output.csv'))
        print(len(np.loadtxt('output.csv')))
        print(dfr[dfr.year==year].shape)
        dfr.loc[dfr.year==year, 'pop_den'] = np.loadtxt('output.csv')
    return dfr

def per_fire(eps, years, group_col):
    res = []
    for year in years:
        print(year)
        dfm = pd.read_parquet(f'/mnt/data/area_burned_glob/processing/ba_grouped_{eps}_{year}.parquet')
        #dfy = fire_size_per_group(dfm, 'group')
        dfm['day_min'] = dfm.day_since - dfm.unc
        dfm['day_max'] = dfm.day_since + dfm.unc
        dfg = dfm.groupby([group_col])
        lc = group_mode(dfm, [group_col], 'lc1', 'lc')
        dfg = dfg.agg(size = ('year', 'count'),
                      year = ('year', 'median'),
                      day_since = ('day_since', 'median'),
                      day_min = ('day_min', 'min'),
                      day_max = ('day_max', 'max'),
                      x_min = ('x', 'min'),
                      x_max = ('x', 'max'),
                      y_min = ('y', 'min'),
                      y_max = ('y', 'max'),
                      lon = ('lon', 'median'),
                      lat = ('lat', 'median')).reset_index()
        dfg = pd.merge(dfg, lc[[group_col, 'lc1']], on = group_col, how = 'left')
        res.append(dfg)
    dfr = pd.concat(res)
    return dfr

def clean_nrt(dfr):
    """
    Drop fire events which are primarily stationary detections,
    water ant urban
    """
    lc_count = dfr.groupby(['event'])['lc'].value_counts().unstack(fill_value=0)
    # drop detections which are classed as static
    print(dfr.shape)
    dfr = dfr[dfr.type != 2]
    print(dfr.shape)
    # drop urban events (> 50% urban detections)
    urban_rat = (lc_count[20] + lc_count[21]) / (lc_count.sum(axis=1))
    try:
        dfr = dfr.drop('urban_ratio', axis=1)
    except:
        pass
    urban_rat = urban_rat.reset_index(name='urban_ratio')
    dfr = dfr.merge(urban_rat, on='event')
    dfr = dfr[dfr.urban_ratio < 0.5]
    print(dfr.shape)
    # drop water events (> 90% water detections)
    type_count = dfr.groupby(['event'])['type'].value_counts().unstack(fill_value=0)
    water_rat = lc_count[0] / (lc_count.sum(axis=1))
    try:
        dfr = dfr.drop('water_ratio', axis=1)
    except:
        pass
    water_rat = water_rat.reset_index(name='water_ratio')
    dfr = dfr.merge(water_rat, on='event')
    dfr = dfr[dfr.water_ratio < 0.9]
    print(dfr.shape)
    # Filter out Fife NGL plant area
    bbox = [56.11, -33.3, 56.08, -3.28]
    fife = spatial_subset_dfr(dfr, bbox)
    dfr = dfr[~dfr.isin(fife)].dropna()
    print(dfr.shape)
    return dfr

def read_hdf4(dataset_path, dataset=None):
    """
    Reads Scientific Data Set(s) stored in a HDF-EOS (HDF4) file
    defined by the file_name argument. Returns SDS(s) given
    name string provided by dataset argument. If
    no dataset is given, the function returns pyhdf
    SD instance of the HDF-EOS file open in read mode.
    """
    try:
        product = SD.SD(dataset_path)
        if dataset == 'all':
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
    except IOError as exc:
        print('Could not read dataset {0}'.format(file_name))
        raise


class PrepData(object):
    def __init__(self, sensor):
        data_path = Config.config['OS']['data_path']
        self.nrt_dataset = Config.config[sensor]['nrt_dataset']
        self.base_url = Config.config[sensor]['base_url']
        self.archive_end = Config.config[sensor]['archive_end']
        self.nrt_dataset_path = os.path.join(data_path, nrt_dataset)
        self.date_now = pd.Timestamp.utcnow()

    def frp_lulc(self, dfr):
        dfr[['tile_h', 'tile_v', 'indx', 'indy']] = ModisGrid.modis_sinusoidal_coords(dfr)
        grouped = dfr.groupby(['tile_h', 'tile_v'])
        dfrs = []
        for name, gr in grouped:
            tile_h = name[0]
            tile_v = name[1]
            landfnstr = os.path.join(land_data_path,
                'MCD12Q1.A{0}001.h{1:02}v{2:02}*'.format(year, tile_h, tile_v))
            try:
                lulc_fname = glob.glob(landfnstr)[0]
            except IndexError:
                print('tile not found: ', landfnstr)
                landfnstr = os.path.join(land_data_path,
                    'MCD12Q1.A{0}001.h{1:02}v{2:02}*'.format(year - 1, tile_h, tile_v))
                print('using: ', landfnstr)
                lulc_fname = glob.glob(landfnstr)[0]
            dslc = self.read_hdf4(lulc_fname)
            dfr['lc1'] = dslc.select('LC_Type1').get()[gr['indy'], gr['indx']]
            dfrs.append(dfr)
        dfr = pd.concat(dfrs)
        return dfr

    def prepare_nrt_dataset(self, dataset):
        """Adds required columns and sets data types of the nrt dataset"""
        # In case dataset doesn't have instrument column:
        dataset['instrument'] = self.sensor
        sensor_str = self.sensor.split('_')[0]
        dataset = dataset.astype(dataset_dtypes[f'{sensor_str}_nrt_dtypes'])
        dataset['type'] = 4
        dataset['date'] = FireDate.fire_dates(dataset)
        # Dropping original date/time columns
        dataset = dataset.drop(['acq_date', 'acq_time'], axis=1)
        dataset = dataset.sort_values(by='date').reset_index(drop=True)
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
            self.logger.info(f'Adding {rows_added} active fires to nrt record')
            # write the updated file
            nrt_updated = nrt_updated.reset_index(drop=True)
            nrt_updated.index = nrt_updated.index + index_increment
            nrt_updated.to_parquet(self.nrt_dataset_path)
            self.log_nrt_end_date(nrt_updated)
        else:
            self.logger.info(f'No new fire detections, snooze')

    def drop_in_archive_nrt(self):
        """Select nrt data starting after archive_end datetime.
        Used to reduce nrt dataset after archive update.
        WARNING: overwrites the file at nrt_dataset_path!
        """
        nrt = pd.read_parquet(self.nrt_dataset_path)
        archive_end_dt = pd.Timestamp(self.archive_end)
        nrt_selected = nrt[nrt.date.values > archive_end_dt]
        nrt_selected.to_parquet(self.nrt_dataset_path)

if __name__ == "__main__":
    pass
