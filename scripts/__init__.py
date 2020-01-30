import logging

from dotenv import load_dotenv

import sqlalchemy
import urllib
import pyodbc

import os

ROOT = os.path.dirname(os.path.abspath(__name__))
load_dotenv(os.path.join(ROOT, '.env'))

LOG = logging.getLogger('luigi-interface')
RPT_SERVER = os.environ['SERVER_A']
SCG_SERVER = os.environ['SERVER_B']
RPT_DB = os.environ['DB_A']
SCG_DB = os.environ['DB_B']
SCG_USR = os.environ['SERVER_B_USR']
SCG_PWD = os.environ['SERVER_B_PWD']

class Utils:

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