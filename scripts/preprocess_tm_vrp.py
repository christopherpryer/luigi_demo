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
SERVER = os.environ['SERVER']
ROOT = os.path.dirname(os.path.abspath(__name__))

class DfDbToLocal(luigi.Task):
    database_name = luigi.Parameter()

    def run(self):
        db = u.get_db_connection(SERVER, self.database_name)
        q = 'select top 10 * from vw3G_SSP_OrderMaster where BillTo like \'%hc%\''
        df = pd.read_sql(q, con=db)
        LOG.info('Shipments %s' % str(df.shape))
        df.to_csv(os.path.join(ROOT, 'tmp', 'tm_data.csv'), index=False)

    def output(self):
        return luigi.LocalTarget('tmp/tm_data.csv')

class Base(luigi.Task):
    database_name = luigi.Parameter()

    @staticmethod
    def get_df():
        return pd.read_csv(os.path.join(ROOT, 'tmp', 'tm_data.csv'), 
            encoding='windows-1254')

    def norm(self, cols, df):
        tmp = df[cols].drop_duplicates()
        tmp.reset_index(inplace=True, drop=True)
        tmp['%s_id' % self.name] = tmp.index.tolist()
        return tmp

class LocationBase(luigi.Task):

    @staticmethod
    def geocode(df:pd.DataFrame, zip_col:str, country:str='US'):
        """returns (lats:li, lons:li)"""
        nomi = pg.Nominatim(country)
        results = nomi.query_postal_code(df[zip_col].astype(str).tolist())
        df['latitude'] = results.latitude
        df['longitude'] = results.longitude
        return df

class Windows(Base):
    name = 'windows'
    filename = '%s.csv' % name

    def run(self):
        df = self.get_df()
        cols = [col for col in df.columns if 'date' in col.lower()]
        self.norm(cols, df).to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

    def requires(self):
        return DfDbToLocal(self.database_name)

    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Products(Base):
    name = 'products'
    filename = '%s.csv' % name

    def run(self):
        cols = ['CustPO']
        self.norm(cols, self.get_df()).to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)  

    def requires(self): 
        return DfDbToLocal(self.database_name)

    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Carriers(Base):
    name = 'carriers'
    filename = '%s.csv' % name

    def run(self):
        cols = ['Mode', 'SCAC', 'CarrierName', 'EquipmentName']
        self.norm(cols, self.get_df()).to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

    def requires(self):
        return DfDbToLocal(self.database_name)
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Origins(Base, LocationBase):
    name = 'origins'
    filename = '%s.csv' % name

    def run(self):
        df = self.get_df()
        cols = ['PULocId', 'PULocName', 'PUAddr', 'PUCity', 'PUState', 'PUZip']
        tmp = self.norm(cols, df)
        self.geocode(tmp, 'PUZip', 'US').to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

    def requires(self):
        return DfDbToLocal(self.database_name)
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Destinations(Base, LocationBase):
    name = 'destinations'
    filename = '%s.csv' % name

    def run(self):
        df = self.get_df()
        cols = ['DLLocId', 'DLLocName', 'DLAddr', 'DLCity', 'DLState', 'DLZip']
        tmp = self.norm(cols, df)
        self.geocode(tmp, 'DLZip', 'US').to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

    def requires(self):
        return DfDbToLocal(self.database_name)
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)


class LocationMatrix(luigi.Task):
    name = 'location_matrix'
    filename = '%s.pkl' % name

    def run(self):
        origins = pd.read_csv(os.path.join(ROOT, 'tmp', Origins.filename))
        destinations = pd.read_csv(os.path.join(ROOT, 'tmp', Destinations.filename))

        for col in ['latitude', 'longitude']: 
            origins[col] = origins[col].round(2)
            destinations[col] = destinations[col].round(2)

        origins.drop_duplicates(subset=['latitude', 'longitude'], inplace=True)
        destinations.drop_duplicates(subset=['latitude', 'longitude'], inplace=True)

        locations = list(zip(origins.latitude, origins.longitude)) \
            + list(zip(destinations.latitude, destinations.longitude))
        matrix = [[(l2, haversine(l1, l2, unit=Unit.MILES)) for l2 in locations] for l1 in locations]
        with open(os.path.join(ROOT, 'tmp', self.filename), 'wb') as f:
            pickle.dump(matrix, f)

    def requires(self):
        # TODO: use account as pipeline id instead of None.
        yield Destinations(None)
        yield Origins(None)
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class ProcessDbToDb(luigi.Task):
    database_name = luigi.Parameter()

    def run(self):
        pass

    def requires(self):
        yield Windows(self.database_name)
        yield Products(self.database_name)
        yield Carriers(self.database_name)
        yield Origins(self.database_name)
        yield Destinations(self.database_name)

if is_main:
    luigi.run()