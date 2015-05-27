import unittest
from tasks.importer import ImportMovieReviews
import peachbox
import peachbox.utils
import peachbox.connector
import model.master
import sys

class TestSource(peachbox.connector.Connector):
    def __init__(self):
        self.path = ''
        self.latest_offset = 0

    def emit(self):
        rdd = peachbox.Spark.Instance().context().textFile(self.path)
        return {'data':peachbox.pipeline.JSONParser().execute(rdd)}

    def set_param(self, param):
        self.path = param['payload']['path']


class TestImportMovieReviews(unittest.TestCase):
    dwh_path = peachbox.utils.TestHelper.mkdir_tmp()

    @classmethod
    def setUpClass(cls):
        importer = ImportMovieReviews()
        importer.source = TestSource() 

        # Instantiate a data warehouse with local file system
        dwh = peachbox.DWH.Instance()
        dwh.fs = peachbox.fs.Local()
        dwh.fs.dwh_path = cls.dwh_path

        input = [{'user_id':'A37I5QIHD9UMPD',
                'product_id':'6302967538',
                'review':'review text',
                'summary':'The everyday man at war',
                'profile_name':'dmunns@yancey.main.nc.us',
                'helpfulness':'2/2',
                'time':872035200,
                'score':5.0}]

        source_dir = peachbox.utils.TestHelper.mkdir_tmp()
        json_file = peachbox.utils.TestHelper.write_json('movie_reviews.json', input, dir_name=source_dir)
        importer.source.path = json_file

        importer.execute({'payload':{'path':json_file}})
        importer.process.join()

    def setUp(self):
        self.dwh = peachbox.DWH.Instance()
        self.dwh.fs = peachbox.fs.Local()
        self.dwh.fs.dwh_path = TestImportMovieReviews.dwh_path 


    def test_user_review_edges(self):
        edges = self.dwh.query_by_key_range(model.master.UserReviewEdge, 0, sys.maxint).collect()
        self.assertEqual(u'A37I5QIHD9UMPD', edges[0].user_id)

    def test_product_review_edges(self):
        edges = self.dwh.query_by_key_range(model.master.ProductReviewEdge, 0, sys.maxint).collect()
        self.assertEqual(u'6302967538', edges[0].product_id)

    def test_review_properties(self):
        edges = self.dwh.query_by_key_range(model.master.ReviewProperties, 0, sys.maxint).collect()
        self.assertEqual(u'review text', edges[0].text)



