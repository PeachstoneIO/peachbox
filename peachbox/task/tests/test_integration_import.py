import unittest

import peachbox.model
import peachbox.task

import peachbox.connector.source
import peachbox.connector.sink
import peachbox.utils

class ReviewByUserEdge(peachbox.model.MasterDataSet):
    data_unit_index = 0
    partition_key = 'true_as_of_seconds'
    partition_granularity = 3600
    output_format = peachbox.model.FileFormat.Parquet
    schema = [{'field':'user_id',    'type': 'StringType'},
              {'field':'product_id', 'type': 'StringType'}]

class ReviewByUserPipeline(object):
    def invoke(self, df):
        return df.map(lambda entry: (ReviewByUserEdge.spark_row(user_id=entry.user_id, 
                                                                product_id=entry.product_id, 
                                                                true_as_of_seconds=entry.time)))

class ImportReviews(peachbox.task.Task):
    def __init__(self):
        #super(ImportReviews, self).__init__()

        self.source = peachbox.connector.source.JSON()
        self.sink   = peachbox.connector.sink.MasterData()

    # param are passed to source and sink beforehand
    # super class has execute(): Sets params in connectors and sends out "ImportReviewsFinished"
    def _execute(self):
        df = self.source.emit()['data']
                                                       
        reviews_by_user = ReviewByUserPipeline().invoke(df) 

        #product_chain = peachbox.pipeline.Chain([ pipeline.Normalize, pipeline.ReviewForProduct ])
        #review_for_product = product_chain.invoke(df)
        #user_properties = pipeline.UserProperties.invoke(df)

        self.sink.absorb([{'data':review_by_user,     'model':Edge},
                          {'data':review_for_product, 'model':ReviewForProductEdge}, 
                           'data':user_properties,    'model':UserProperties}]
        
        self.sink.absorb({'data':reviews_by_user, 'model':ReviewByUserEdge})

class TestIntegrationImport(unittest.TestCase):

    def setUp(self):
        # Set up a data warehouse with local filesystem
        self.dwh = peachbox.DWH.Instance()
        self.dwh.fs = peachbox.fs.Local()
        self.dwh.fs.dwh_path = peachbox.utils.TestHelper().mkdir_tmp() 

        # Write mocked data
        self.json_file = peachbox.utils.TestHelper().write_json('movie_reviews.json', [{'user_id':'u1', 
            'product_id':'p1', 'time':123}])

        self.importer = ImportReviews()

    def test_execution(self):
        self.importer.execute(param={'path':self.json_file})
        #print 'dwh path: ' + peachbox.DWH.Instance().fs.dwh_path
        result = peachbox.DWH.Instance().read_data_frame(self.importer.sink.model.mart, '0/3600')
        #print result.collect()

        ## Test the output
        assert False


