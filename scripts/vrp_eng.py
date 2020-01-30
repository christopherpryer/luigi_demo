from . import ROOT, LOG

from haversine import haversine, Unit
import pgeocode as pg
import pandas as pd
import luigi

import os

class BaseProcess(luigi.Task):
    """
    Parent class for tasks involving initial vrp definition extracts. Implements 
    a psuedo star-method normalization technique.
    Inherits name, filename
    """

    @staticmethod
    def get_df(filename):
        return pd.read_csv(os.path.join(ROOT, 'tmp', filename), 
            encoding='windows-1254')

    def norm(self, cols, df):
        """
        drops duplicates + unwanted cols and sets new index as id field
        takes list of cols and pd.DataFrame
        returns pd.DataFrame
        """
        LOG.info('%s(BaseProcess)->shape (pre-drop): %s' % (self.name, str(df.shape)))

        df.drop_duplicates(subset=cols, inplace=True)
        df.drop([col for col in df.columns if col not in cols], axis=1, inplace=True)
        LOG.info('%s(BaseProcess)->shape (post-drop): %s' % (self.name, str(df.shape)))

        df.reset_index(inplace=True, drop=True)
        df['%s_id' % self.name] = df.index.tolist()
        LOG.info('%s(BaseProcess)->nunique: %s' % (self.name, str(len(df))))
        
        return df

class LocationsBase(luigi.Task):
    """
    Parent class for standard location data processing tasks.
    Inherits name, filename
    """

    def geocode(self, df:pd.DataFrame, zip_col:str, country:str='US'):
        """returns (lats:li, lons:li)"""
        # TODO: optimize for unique zips.
        nomi = pg.Nominatim(country)
        LOG.info('%s(LocationsBase)->geocode->zips(%s) country(%s)' % (self.name, str(len(df)), country))
        
        results = nomi.query_postal_code(df[zip_col].astype(str).tolist())
        df['latitude'] = results.latitude
        df['longitude'] = results.longitude
        LOG.info('%s(LocationsBase)->geocode->results.lat.null(%s) results.lon.null(%s)' % \
             (self.name, str(results.latitude.isna().sum()), str(results.longitude.isna().sum())))
        
        return df

class LocationsMatrix(luigi.Task):
    """
    Parent class for standard vrp distance matrix preprocessing.
    Inherits name, filename, origins filename, destinations filename
    """

    def get_matrix(self):
        origins = pd.read_csv(os.path.join(ROOT, 'tmp', self.origins_filename))
        destinations = pd.read_csv(os.path.join(ROOT, 'tmp', self.destinations_filename))
        LOG.info('%s->norigins (pre-round): %s' % (self.name, str(len(origins))))
        LOG.info('%s->ndestinations (pre-round): %s' % (self.name, str(len(destinations))))

        for col in ['latitude', 'longitude']: 
            origins[col] = origins[col].round(2)
            destinations[col] = destinations[col].round(2)

        origins.drop_duplicates(subset=['latitude', 'longitude'], inplace=True)
        destinations.drop_duplicates(subset=['latitude', 'longitude'], inplace=True)

        LOG.info('%s->norigins (post-round): %s' % (self.name, str(len(origins))))
        LOG.info('%s->ndestinations (post-round): %s' % (self.name, str(len(destinations))))

        locations = list(zip(origins.latitude, origins.longitude)) \
            + list(zip(destinations.latitude, destinations.longitude))
        matrix = [[(l2, haversine(l1, l2, unit=Unit.MILES)) for l2 in locations] for l1 in locations]
        LOG.info('%s->nmatrix: %s' % (self.name, str(len(matrix))))
        
        return matrix
