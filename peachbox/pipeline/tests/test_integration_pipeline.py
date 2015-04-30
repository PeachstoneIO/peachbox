import unittest

import peachbox.pipeline
import peachbox.connector.source
import peachbox.connector.sink
import peachbox.utils
import peachbox.gen

import imp

class ImportReviews(peachbox.pipeline.Pipeline):

    # The extra model parameter is for testing only!
    def __init__(self, mock_model):
        super(ImportReviews, self).__init__()

        # connector can be pre-defined or manually, must have rdd()/absorb()
        self.source = peachbox.connector.source.JSON()
        # MasterDataSet: model has target, which is determined by partition_time_range
        #self.sink   = peachbox.connector.sink.MasterDataSet(model=ReviewEdge)
        # self.param is member of peachbox.pipelines.pipeline

    # param are passed to source and sink beforehand
    # super class has execute(): Sets params in connectors and sends out "ImportReviewsFinished"
    def _execute(self):
        pass
        #df = self.source.data_frame()
        ##reviews = df.validate(['user_id', 'product_id', 'time']).
        #reviews = df.map(lambda entry: ReviewEdge(user_id=entry['user_id'], 
                    #product_id=entry['product_id'], 
                    #true_as_of_seconds=entry['time']))
        #self.sink.absorb(reviews)

    ## optional: is called by execute
    def tear_down(self):
        pass


class TestIntegrationPipeline(unittest.TestCase):

    def setUp(self):

        # Set up a data warehouse with local filesystem
        peachbox.DWH(fs=peachbox.fs.Local())
        self.dwh_path = peachbox.utils.TestHelper.mkdir_tmp()
        peachbox.DWH.Instance().fs.dwh_path = self.dwh_path

        # Create a model 
        self.project_home = peachbox.utils.TestHelper().mkdir_tmp()
        peachbox.gen.Generator.PROJECT_HOME = self.project_home
        peachbox.gen.app.create_app("MovieReviews", self.project_home)
        
        peachbox.gen.model.define_node("Customer", "customer_id", "string")
        peachbox.gen.model.define_node("Review", "review_id", "string")
        peachbox.gen.model.define_edge(0, "Review", "Customer", "Review")

        imp.load_source("model", self.project_home + '/model/review_edge.py')
        import model

        # Instantiate an import pipeline 
        self.importer = ImportReviews(model.ReviewEdge)


        print 'Project home: ' + self.project_home



    def test_execution(self):
        #param = {'path':'input_path'}
        #self.importer.execute(param)

        # Test the output
        assert False




