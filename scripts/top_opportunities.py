"""
Example script to manage luigi tasks for opportunity processing 
on shipment data. An opportunity is defined as a positive change
or further leverageing of element(s) in the existing operation,
as seen in the data.
"""
import luigi

class Shipments(luigi.Task):

    def run(self):
        with self.output().open('w') as output:
            output.write('testing')
            
    def output(self):
        return luigi.LocalTarget('tmp/shipments.tsv')

class AggregateLTL(luigi.Task):
    
    def requires(self):
        return Shipments()