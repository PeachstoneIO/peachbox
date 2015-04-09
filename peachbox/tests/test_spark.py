import unittest
import peachbox.spark

class TestSpark(unittest.TestCase):
    def test_set_conf(self):
        peachbox.spark.set_conf({'app_name':'test'})
        self.assertEqual('test', peachbox.spark.get_conf()['app_name'])




        

