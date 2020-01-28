"""
Example use of luigi to convert TM stored data to modeling data
for supply chain guru.

1. Pull new data from TM SQL views.
2. Check for unusable data.
3. Log unusable data.
TODO: ...


NOTE: this is not optimized. i.e. reading DfToLocal output for each process.
"""
from . import utils as u

import pandas as pd
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
        df.to_csv(os.path.join(ROOT, 'tmp', 'tm_data.csv'))

    def output(self):
        return luigi.LocalTarget('tmp/tm_data.csv')

class Windows(luigi.Task):
    name = 'windows'
    filename = '%s.csv' % name
    database_name = luigi.Parameter()

    def run(self):
        df = pd.read_csv(os.path.join(ROOT, 'tmp', 'tm_data.csv'), 
            encoding='windows-1254')
        cols = [col for col in df.columns if 'date' in col.lower()]
        df[cols].to_csv(os.path.join(ROOT, 'tmp', self.filename))

    def requires(self):
        return DfDbToLocal(self.database_name)

    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class Products(luigi.Task):
    name = 'products'
    filename = '%s.csv' % name
    database_name = luigi.Parameter()

    def run(self):
        df = pd.read_csv(os.path.join(ROOT, 'tmp', 'tm_data.csv'), 
            encoding='windows-1254')
        df[['CustPO']].to_csv(os.path.join(ROOT, 'tmp', self.filename))

    def requires(self): 
        return DfDbToLocal(self.database_name)

    def output(self):
        return luigi.LocalTarget('tmp/%s' % self.filename)

class ProcessDbToDb(luigi.Task):
    database_name = luigi.Parameter()

    def run(self):
        pass

    def requires(self):
        yield Windows(self.database_name)
        yield Products(self.database_name)

if __name__ == '__main__':
    luigi.run()