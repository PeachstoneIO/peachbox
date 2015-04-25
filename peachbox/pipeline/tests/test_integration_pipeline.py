import unittest

import peachbox.pipeline
import peachbox.connector.source
import peachbox.connector.sink

from model import ReviewEdge

class ImportReviews(peachbox.pipeline.Pipeline):
    def __init__(object):
        # connector can be pre-defined or manually, must have rdd()/absorb()
        self.source = peachbox.connector.source.JSON()
        # MasterDataSet: model has target, which is determined by partition_time_range
        self.sink   = peachbox.connector.sink.MasterDataSet(model=ReviewEdge)
        # self.param is member of peachbox.pipelines.pipeline

    # param are passed to source and sink beforehand
    # super class has execute(): Sets params in connectors and sends out "ImportReviewsFinished"
    def _execute(self):
        df = self.source.data_frame()
        #reviews = df.validate(['user_id', 'product_id', 'time']).
        reviews = df.map(lambda entry: ReviewEdge(user_id=entry['user_id'], 
                    product_id=entry['product_id'], 
                    true_as_of_seconds=entry['time']))
        self.sink.absorb(reviews)

    # optional: is called by execute
    def tear_down(self):
        pass

class TestIntegrationPipeline(unittest.TestCase):

    def setUp(self):
        # Instantiate an import pipeline 
        self.importer = ImportReviews()

        # Set up a data warehouse with local filesystem
        peachbox.DWH(fs=peachbox.fs.Local())
        self.dwh_path = peachbox.utils.TestHelper.mkdir_tmp()
        peachbox.DWH.Instance().fs.dwh_path = self.dwh_path

        # Create a model 
        self.project_home = peachbox.utils.TestHelper().mkdir_tmp()
        peachbox.gen.Generator.PROJECT_HOME = self.project_home
        #peachbox.gen.model.define_node(...
        #peachbox.gen.model.define_edge...
        #imp.load_source...

    def test_execution(self):
        param = {'path':'input_path'}
        self.importer.execute(param)

        # Test the output
        assert False




