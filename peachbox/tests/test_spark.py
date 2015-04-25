import unittest
import peachbox

class TestSpark(unittest.TestCase):
    def test_init(self):
        with peachbox.Spark() as spark:
            self.assertIsInstance(spark, peachbox.Spark)

    def test_access_by_instance(self):
        with peachbox.Spark() as s1:
            s2 = peachbox.Spark.Instance()
            self.assertEqual(s1,s2)

    def test_instanciation_by_instance(self):
        with peachbox.Spark.Instance() as spark:
            self.assertIsInstance(spark, peachbox.Spark)







        

