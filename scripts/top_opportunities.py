"""
Example script to manage luigi tasks for opportunity processing 
on shipment data. An opportunity is defined as a positive change
or further leverageing of element(s) in the existing operation,
as seen in the data.
"""
import luigi

import urllib
import sqlalchemy
import pyodbc

import os

import pandas as pd

import logging
logger = logging.getLogger('luigi-interface')


def get_db_connection(server, database):
    driver = '{SQL Server}'
    server = '{%s}' % server
    database = '{%s}' % database
    conn_str = ('DRIVER={};SERVER={};DATABASE={}'
                ';Trusted_Connection=yes').format(
                    driver, 
                    server, 
                    database
            )
    s = urllib.parse.quote_plus(conn_str)
    engine = sqlalchemy.create_engine(
        'mssql+pyodbc:///?odbc_connect={}'.format(s))
    return engine

SERVER = os.environ['SERVER']
CURRDIR = os.path.dirname(os.path.abspath(__name__))

class Shipments(luigi.Task):
    file_name = 'shipments.csv'
    table_name = 'shipments'
    database_name = luigi.Parameter()

    def run(self):
        db = get_db_connection(SERVER, self.database_name)
        df = pd.read_sql('select * from %s' % self.table_name, con=db)
        logger.info('Shipments %s' % str(df.shape))
        df.to_csv(os.path.join(CURRDIR, 'tmp', self.file_name),
            index=False)
            
    def output(self):
        return luigi.LocalTarget('tmp/shipments.csv')

class AggregateLTL(luigi.Task):
    database_name = luigi.Parameter() # TODO: look into luigi abstraction of db connection.
    
    def requires(self):
        return Shipments(self.database_name)


if __name__ == "__main__":
    luigi.run()