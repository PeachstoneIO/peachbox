import unittest
import pipelines.importer
import json
import peachbox

class TestImporter(unittest.TestCase):
    def test_user_review_edge(self):
        input = [{u'user_id':u'u1', u'product_id':u'p1', u'time':123}]

        input_rdd = peachbox.Spark.Instance().context().parallelize(input)

        pipeline = pipelines.importer.UserReviewEdge()
        edges = pipeline.execute(input_rdd)
        result = edges.collect()[0]

        review_id = unicode(hash(u'u1' + u'p1' + str(123)))

        self.assertEqual(123,  result[0])
        self.assertEqual('u1', result[1])
        self.assertEqual(review_id, result[2])


    def test_product_review_edge(self):
        input = [{u'user_id':u'u1', u'product_id':u'p1', u'time':123}]

        input_rdd = peachbox.Spark.Instance().context().parallelize(input)

        pipeline = pipelines.importer.ProductReviewEdge()
        edges = pipeline.execute(input_rdd)
        result = edges.collect()[0]

        review_id = unicode(hash(u'u1' + u'p1' + str(123)))

        self.assertEqual(123,  result[0])
        self.assertEqual(review_id, result[1])
        self.assertEqual(u'p1', result[2])


    def test_review_properties(self):
        input = [{u'user_id':u'u1', u'product_id':u'p1', u'time':123, u'helpfulness':u'5/7', 
            u'score':5.0, u'summary':u'summary of review', u'text':u'text of review'}]

        input_rdd = peachbox.Spark.Instance().context().parallelize(input)

        pipeline = pipelines.importer.ReviewProperties()
        edges = pipeline.execute(input_rdd)
        result = edges.collect()[0]

        review_id = unicode(hash(u'u1' + u'p1' + str(123)))

        self.assertEqual(123,  result[0])
        self.assertEqual(review_id, result[1])
        self.assertEqual(5, result[2])
        self.assertEqual(2, result[3])
        self.assertEqual(5, result[4])
        self.assertEqual(u'summary of review', result[5])
        self.assertEqual(u'text of review', result[6])

