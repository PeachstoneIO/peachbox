import unittest
import peachbox.model
#from pyspark.sql.types import *

#class JSONEdge(peachbox.model.MasterDataSet):
#    data_unit_index = 0
#    output_format = peachbox.model.FileFormat.JSON

#class ParquetModel(peachbox.model.MasterDataSet):
#    output_format = peachbox.model.FileFormat.Parquet
#    schema = [{'field':'user_id', 'type':'StringType'},
#              {'field':'product_id', 'type':'StringType'}]


class TestModel(peachbox.model.MasterDataSet):
 
    def __init__():
        print 'you successfully initialized TestModel'


class TestMasterDataSet(unittest.TestCase):
    """ Test case for MasterDataSet:
    init a model.
    """

    def test_init(self):
        model = TestModel()
        assert model
    


#    def setUp(self):
#        self.m = ParquetModel() 
#
#    def test_output_format(self):
#        self.assertEqual(peachbox.model.FileFormat.JSON, JSONEdge.output_format)
#        self.assertEqual(peachbox.model.FileFormat.Parquet, ParquetModel.output_format)
#
#    def test_target(self):
#        self.assertEqual('0', JSONEdge().target())
#
#    def test_spark_schema(self):
#        s = self.m.spark_schema()
#        self.assertIsInstance(s, StructType)
#        self.assertIsInstance(s.fields[0], StructField)
#        self.assertIsInstance(s.fields[1].dataType, StringType)
#        self.assertEqual('user_id', s.fields[1].name)
#
#    def test_spark_schema_true_as_of_seconds(self):
#        s = self.m.spark_schema()
#        self.assertIsInstance(s.fields[0].dataType, IntegerType)
#        self.assertEqual('true_as_of_seconds', s.fields[0].name)
#
#    def test_spark_row(self):
#        row = self.m.spark_row(user_id=u'u1', product_id=u'p1', true_as_of_seconds=123)
#        self.assertEqual('u1', row.user_id)
#        self.assertEqual('p1', row.product_id)
#        self.assertEqual(123,  row.true_as_of_seconds)
#
#    def test_type_check(self):
#        self.assertRaises(TypeError, self.m.spark_row, user_id='u1', product_id=7, true_as_of_seconds=123)



