import unittest
import peachbox.model
import pyspark.sql.types

class TestTypes(unittest.TestCase):
    def test_spark_type(self):
        self.assertIsInstance(peachbox.model.Types.spark_type('StringType'), pyspark.sql.types.StringType)
        
    def test_python_type(self):
        self.assertEqual(peachbox.model.Types.python_type('StringType'), unicode)
