"""
Example use of luigi to convert TM stored data to modeling data
for supply chain guru.

1. Pull new data from TM SQL views.
2. Check for unusable data.
3. Log unusable data.
TODO: ...


NOTE: this is not optimized. i.e. reading DfToLocal output for each process.
"""
is_main = False
if __name__ == '__main__':
    is_main = True

    import utils as u

else:
    from . import utils as u

from haversine import haversine, Unit
import pgeocode as pg
import pandas as pd
import pickle
import os

import luigi

import logging


LOG = logging.getLogger('luigi-interface')
SERVER = os.environ['SERVER'] # TODO: server-a server-b
DATABASE = os.environ['DATABASE'] # TODO: db-a db-b
ROOT = os.path.dirname(os.path.abspath(__name__))

class DfDbToLocal(luigi.Task):

    def run(self):
        db = u.get_db_connection(SERVER, DATABASE)
        LOG.info('DfDbToLocal->db: %s' % db)
        q = 'select top 10 * from vw3G_SSP_OrderMaster where BillTo like \'%hc%\''
        LOG.info('DfDbToLocal->query: %s' % db)
        df = pd.read_sql(q, con=db)
        LOG.info('DfDbToLocal->Shipments %s' % str(df.shape))
        df.to_csv(os.path.join(ROOT, 'tmp', 'tm_data.csv'), index=False)

    def output(self):
        return luigi.LocalTarget('tmp/tm_data.csv')

class Base(luigi.Task):

    @staticmethod
    def get_df():
        return pd.read_csv(os.path.join(ROOT, 'tmp', 'tm_data.csv'), 
            encoding='windows-1254')

    def norm(self, cols, df):
        LOG.info('Base->shape (pre-drop): %s' % str(df.shape))
        df.drop_duplicates(subset=cols, inplace=True)
        df.drop([col for col in df.columns if col not in cols], axis=1, inplace=True)
        LOG.info('Base->shape (post-drop): %s' % str(df.shape))
        df.reset_index(inplace=True, drop=True)
        df['%s_id' % self.name] = df.index.tolist()
        LOG.info('%s(Base)->nunique: %s' % (self.name.capitalize(), str(len(df))))
        return df

class LocationBase(luigi.Task):

    def geocode(self, df:pd.DataFrame, zip_col:str, country:str='US'):
        """returns (lats:li, lons:li)"""
        # TODO: optimize for unique zips.
        LOG.info('LocationBase->geocode->zips(%s) country(%s)' % (str(len(df)), country))
        nomi = pg.Nominatim(country)
        results = nomi.query_postal_code(df[zip_col].astype(str).tolist())
        LOG.info('%s(LocationBase)->geocode->results.lat.null(%s) results.lon.null(%s)' % \
             (self.name.capitalize(), str(results.latitude.isna().sum()), str(results.longitude.isna().sum())))
        df['latitude'] = results.latitude
        df['longitude'] = results.longitude
        return df

class Windows(Base):
    name = 'windows'
    filename = '%s.csv' % name

    def run(self):
        df = self.get_df()
        cols = [col for col in df.columns if 'date' in col.lower()]
        LOG.info('Windows->cols: %s' % str(cols))
        self.norm(cols, df).to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

    def requires(self):
        return DfDbToLocal()

    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Products(Base):
    name = 'products'
    filename = '%s.csv' % name

    def run(self):
        cols = ['CustPO']
        LOG.info('Products->cols: %s' % str(cols))
        self.norm(cols, self.get_df()).to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)  

    def requires(self): 
        return DfDbToLocal()

    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Carriers(Base):
    name = 'carriers'
    filename = '%s.csv' % name

    def run(self):
        cols = ['Mode', 'SCAC', 'CarrierName', 'EquipmentName']
        LOG.info('Carriers->cols: %s' % str(cols))
        self.norm(cols, self.get_df()).to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

    def requires(self):
        return DfDbToLocal()
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Origins(Base, LocationBase):
    name = 'origins'
    filename = '%s.csv' % name

    def run(self):
        df = self.get_df()
        cols = ['PULocId', 'PULocName', 'PUAddr', 'PUCity', 'PUState', 'PUZip']
        LOG.info('Origins->cols: %s' % str(cols))
        tmp = self.norm(cols, df)
        self.geocode(tmp, 'PUZip', 'US').to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

    def requires(self):
        return DfDbToLocal()
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Destinations(Base, LocationBase):
    name = 'destinations'
    filename = '%s.csv' % name

    def run(self):
        df = self.get_df()
        cols = ['DLLocId', 'DLLocName', 'DLAddr', 'DLCity', 'DLState', 'DLZip']
        LOG.info('Destinations->cols: %s' % str(cols))
        tmp = self.norm(cols, df)
        self.geocode(tmp, 'DLZip', 'US').to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

    def requires(self):
        return DfDbToLocal()
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class LocationMatrix(luigi.Task):
    name = 'location_matrix'
    filename = '%s.pkl' % name

    def run(self):
        origins = pd.read_csv(os.path.join(ROOT, 'tmp', Origins.filename))
        destinations = pd.read_csv(os.path.join(ROOT, 'tmp', Destinations.filename))
        LOG.info('LocationMatrix->norigins (pre-round): %s' % str(len(origins)))
        LOG.info('LocationMatrix->ndestinations (pre-round): %s' % str(len(destinations)))

        for col in ['latitude', 'longitude']: 
            origins[col] = origins[col].round(2)
            destinations[col] = destinations[col].round(2)

        origins.drop_duplicates(subset=['latitude', 'longitude'], inplace=True)
        destinations.drop_duplicates(subset=['latitude', 'longitude'], inplace=True)

        LOG.info('LocationMatrix->norigins (post-round): %s' % str(len(origins)))
        LOG.info('LocationMatrix->ndestinations (post-round): %s' % str(len(destinations)))

        locations = list(zip(origins.latitude, origins.longitude)) \
            + list(zip(destinations.latitude, destinations.longitude))
        matrix = [[(l2, haversine(l1, l2, unit=Unit.MILES)) for l2 in locations] for l1 in locations]
        LOG.info('LocationMatrix->nmatrix: %s' % str(len(matrix)))
        with open(os.path.join(ROOT, 'tmp', self.filename), 'wb') as f:
            pickle.dump(matrix, f)

    def requires(self):
        # TODO: use account as pipeline id instead of None.
        yield Destinations()
        yield Origins()
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class ProcessDbToDb(luigi.Task):
    
    def run(self):
        pass

    def requires(self):
        yield Windows()
        yield Products()
        yield Carriers()
        yield Origins()
        yield Destinations()
        yield LocationMatrix()

if is_main:
    luigi.run()