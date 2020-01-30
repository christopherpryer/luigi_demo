"""report server read module"""
from . import ROOT, LOG, RPT_SERVER, RPT_DB, Utils
from . import vrp_eng as eng

import pandas as pd
import pickle
import luigi

import os

MODULE = os.path.basename(__file__)
LOG.info('%s->initialized' % MODULE)

class DbToLocal(luigi.Task):
    name = 'DbToLocal'
    filename = 'tm_data.csv'

    def run(self):
        # TODO: add account arg
        db = Utils.get_db_connection(RPT_SERVER, RPT_DB)
        LOG.info('%s->db: %s' % (self.name, db))

        q = 'select top 10 * from vw3G_SSP_OrderMaster where BillTo like \'%hc%\''
        LOG.info('%s->query: %s' % (self.name, db))

        df = pd.read_sql(q, con=db)
        LOG.info('%s->Shipments %s' % (self.name, str(df.shape)))

        df.to_csv(os.path.join(ROOT, 'tmp', 'tm_data.csv'), index=False)

    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Origins(eng.BaseProcess, eng.LocationsBase):
    name = 'origins'
    filename = '%s.csv' % name

    def run(self):
        df = self.get_df(DbToLocal.filename)
        cols = ['PULocId', 'PULocName', 'PUAddr', 'PUCity', 'PUState', 'PUZip']
        LOG.info('Origins->cols: %s' % str(cols))

        tmp = self.norm(cols, df)
        self.geocode(tmp, 'PUZip', 'US').to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

    def requires(self):
        return DbToLocal()
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Destinations(eng.BaseProcess, eng.LocationsBase):
    name = 'destinations'
    filename = '%s.csv' % name

    def run(self):
        df = self.get_df(DbToLocal.filename)
        cols = ['DLLocId', 'DLLocName', 'DLAddr', 'DLCity', 'DLState', 'DLZip']
        LOG.info('%s->cols: %s' % (self.name, str(cols)))

        tmp = self.norm(cols, df)
        self.geocode(tmp, 'DLZip', 'US').to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

    def requires(self):
        return DbToLocal()
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Carriers(eng.BaseProcess):
    name = 'carriers'
    filename = '%s.csv' % name

    def run(self):
        cols = ['Mode', 'SCAC', 'CarrierName', 'EquipmentName']
        LOG.info('%s->cols: %s' % (self.name, str(cols)))

        self.norm(cols, self.get_df(DbToLocal.filename)) \
                .to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)

    def requires(self):
        return DbToLocal()
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Products(eng.BaseProcess):
    name = 'products'
    filename = '%s.csv' % name

    def run(self):
        cols = ['CustPO']
        LOG.info('%s->cols: %s' % (str(cols), self.name))

        self.norm(cols, self.get_df(DbToLocal.filename)) \
            .to_csv(os.path.join(ROOT, 'tmp', self.filename), index=False)  

    def requires(self): 
        return DbToLocal()

    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class RptDistances(eng.LocationsMatrix):
    name = 'rpt_distance_matrix'
    filename = '%s.pkl' % name
    origins_filename = Origins.filename
    destinations_filename = Destinations.filename

    def run(self):
        matrix = self.get_matrix()
        with open(os.path.join(ROOT, 'tmp', self.filename), 'wb') as f:
            pickle.dump(matrix, f)

    def requires(self):
        # TODO: use account as pipeline id instead of None.
        yield Destinations()
        yield Origins()
    
    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)