import unittest
import peachbox.spark

class TestSpark(unittest.TestCase):
    def test_init(self):
        with peachbox.spark.Spark() as spark:
            self.assertIsInstance(spark, peachbox.spark.Spark)

    def test_access_by_instance(self):
        with peachbox.spark.Spark() as s1:
            s2 = peachbox.spark.Instance()
            self.assertEqual(s1,s2)

    def test_instanciation_by_instance(self):
        with peachbox.spark.Instance() as spark:
            self.assertIsInstance(spark, peachbox.spark.Spark)







        

