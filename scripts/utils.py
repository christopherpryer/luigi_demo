import sqlalchemy
import urllib
import pyodbc

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