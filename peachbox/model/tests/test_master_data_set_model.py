import unittest
from peachbox.model import MasterDataSetModel
import peachbox.model

from pyspark.sql import StructField, StructType, StringType, IntegerType

class MockModel(MasterDataSetModel):
    def __init__(self):
        super(MockModel, self).__init__()
        self._schema = StructType([
                StructField("user_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("true_as_of_seconds", IntegerType(), False)]) 
        self._id = 1
        self.output_format = peachbox.model.FileFormat.JSON

    def schema(self):
        return self._schema


class TestMasterDataSet(unittest.TestCase):

    def test_schema(self):
        m = MockModel()
        assert m.schema()

    def test_output_format(self):
        m = MockModel()
        self.assertEqual(peachbox.model.FileFormat.JSON, m.output_format)


