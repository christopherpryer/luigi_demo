from . import ROOT, LOG, SCG_SERVER, SCG_DB, SCG_USR, SCG_PWD, Utils
from . import vrp_eng as eng
from . import vrp_rpt as rpt

import pandas as pd
import luigi

import os

class ScgCustomers(eng.LocationsBase):
    name = 'scg_customers'
    filename = '%s.csv' % name # TODO: align this with SCG local formatting.
    sql_mapping = { # TODO: define standard eng column names to convert from.
        'DLLocId': ('CustomerName', str),
        'DLCity': ('CustomerCity', str),
        'DLState': ('CustomerState', str),
        'DLZip': ('CustomerZipCode', str),
        'latitude': ('CustomerLatitude', float),
        'longitude': ('CustomerLongitude', float)
        }
    
    def run(self):
        retyper = {key: self.sql_mapping[key][1] for key in self.sql_mapping}
        customers = pd.read_csv(os.path.join(ROOT, 'tmp', rpt.Destinations.filename), dtype=retyper)
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
        db = Utils.get_db_connection(SERVER_B, DATABASE_B)
        scg_target = pd.read_sql('select * from Customers', con=db)
        LOG.info('SCGCustomers->db(%s) scg_target.shape(%s)' % (str(db), str(scg_target.shape)))
        scg_target.drop(scg_target.index, inplace=True)
        scg_target.append(customers, sort=False).to_sql('Customers', con=db, if_exists='replace', index=False)

    def requires(self): 
        return rpt.Destinations()

    def output(self):
        yield luigi.LocalTarget('tmp/%s' % self.filename)
        # TODO: ...
        #yield MSSqlTarget(host=SERVER_B, database=DATABASE_B, user=SERVER_B_USR, 
        #    password=SERVER_B_PWD, table='Customers', update_id=None)

class RptToScg(luigi.Task):
    
    def run(self):
        pass

    def requires(self):
        yield rpt.Products()
        yield rpt.Carriers()
        yield rpt.Origins()
        yield rpt.Destinations()
        yield rpt.RptDistances()
        yield ScgCustomers()