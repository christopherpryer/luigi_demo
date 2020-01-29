"""
Example use of luigi to convert TM stored data to modeling data
for supply chain guru.

1. Pull new data from TM SQL views.
2. Check for unusable data.
3. Log unusable data.
TODO: ...


NOTE: this is not optimized. i.e. reading DfToLocal output for each process.
"""
if __name__ == '__main__':
    import utils as u
else:
    from . import utils as u

from haversine import haversine, Unit
import pgeocode as pg
import pandas as pd
import pickle
import os

import luigi
from luigi.contrib.mssqldb import MSSqlTarget

import logging

from dotenv import load_dotenv

ROOT = os.path.dirname(os.path.abspath(__name__))
load_dotenv(os.path.join(ROOT, '.env'))

LOG = logging.getLogger('luigi-interface')
SERVER_A = os.environ['SERVER_A']
SERVER_B = os.environ['SERVER_B']
DATABASE_A = os.environ['DB_A']
DATABASE_B = os.environ['DB_B']
SERVER_B_USR = os.environ['SERVER_B_USR']
SERVER_B_PWD = os.environ['SERVER_B_PWD']

class DfDbToLocal(luigi.Task):

    def run(self):
        db = u.get_db_connection(SERVER_A, DATABASE_A)
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

class SCGCustomers(LocationBase):
    name = 'scg_customers'
    filename = '%s.csv' % name # TODO: align this with SCG local formatting.
    sql_mapping = {
        'DLLocId': ('CustomerName', str),
        'DLCity': ('CustomerCity', str),
        'DLState': ('CustomerState', str),
        'DLZip': ('CustomerZipCode', str),
        'latitude': ('CustomerLatitude', float),
        'longitude': ('CustomerLongitude', float)
        }
    
    def run(self):
        retyper = {key: self.sql_mapping[key][1] for key in self.sql_mapping}
        customers = pd.read_csv(os.path.join(ROOT, 'tmp', Destinations.filename), dtype=retyper)
        LOG.info('SCGCustomers->customers.shape: %s' % str(customers.shape))
        # NOTE: assuming we can build models at various levels, this implementation
        # will assume the model is built at the level of a 5-digit zipcode.
        # cols = ['DLLocId', 'DLLocName', 'DLAddr', 'DLCity', 'DLState', 'DLZip']
        # below is a SCG backend-direct SQL integration.
        dropper = [col for col in customers.columns if col not in self.sql_mapping]
        customers.drop(dropper, axis=1, inplace=True)
        customers.drop_duplicates(inplace=True)
        LOG.info('SCGCustomers->drop(%s) shape(%s)' % (str(dropper), str(customers.shape)))
        renamer = {key: self.sql_mapping[key][0] for key in self.sql_mapping}
        customers.rename(columns=renamer, inplace=True)
        LOG.info('SCGCustomers->retyper(%s) renamer(%s)' % (str(retyper), str(renamer)))
        customers.to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

        # insert to live scg table TODO: point this at a staging database.
        # plus, to_sql is not efficient for large data sets (should work for weekly runs though.)
        db = u.get_db_connection(SERVER_B, DATABASE_B)
        scg_target = pd.read_sql('select * from Customers', con=db)
        LOG.info('SCGCustomers->db(%s) scg_target.shape(%s)' % (str(db), str(scg_target.shape)))
        scg_target.drop(scg_target.index, inplace=True)
        scg_target.append(customers, sort=False).to_sql('Customers', con=db, if_exists='replace', index=False)

    def requires(self): 
        return Destinations()

    def output(self):
        yield luigi.LocalTarget('tmp/%s' % self.filename)
        # TODO: ...
        #yield MSSqlTarget(host=SERVER_B, database=DATABASE_B, user=SERVER_B_USR, 
        #    password=SERVER_B_PWD, table='Customers', update_id=None)

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
        yield SCGCustomers()

if __name__ == '__main__':
    luigi.run()