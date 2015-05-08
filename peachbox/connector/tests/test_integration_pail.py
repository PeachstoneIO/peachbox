import unittest
import peachbox
import peachbox.model

from peachbox.connector.tests.fixtures import MasterDataSetModel

class TestIntegrationPail(unittest.TestCase):
    def setUp(self):
        input = [{'user':'u1', 'product':'p1', 'true_as_of_seconds':3},
                 {'user':'u2', 'product':'p2', 'true_as_of_seconds':13}]

        rdd = peachbox.Spark.Instance().context().parallelize(input)
        self.df = peachbox.Spark.Instance().sql_context().createDataFrame(rdd)

    def test_pail_creation(self):
        pails = peachbox.connector.Pail.create_pails(self.df, MasterDataSetModel)
        self.assertEqual(2, len(pails))
        self.assertEqual('u1', pails[0].data.collect()[0].user)

    def test_target(self):
        pails = peachbox.connector.Pail.create_pails(self.df, MasterDataSetModel)
        self.assertEqual('0/10', pails[0].target())
        self.assertEqual('0/20', pails[1].target())


