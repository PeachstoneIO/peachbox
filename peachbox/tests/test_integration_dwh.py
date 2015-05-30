import unittest
import peachbox
import peachbox.fs
import peachbox.utils
import peachbox.connector
import pyspark.sql

from peachbox.connector.tests.fixtures import MasterDataSetModel

class TestIntegrationDwh(unittest.TestCase):
    def setUp(self):
        self.dwh_path = peachbox.utils.TestHelper().mkdir_tmp()
        peachbox.DWH.Instance().fs = peachbox.fs.Local()
        peachbox.DWH.Instance().fs.dwh_path = self.dwh_path

        input = [{'user':'u1', 'product':'p1', 'true_as_of_seconds':3},
                 {'user':'u2', 'product':'p2', 'true_as_of_seconds':7}]

        rdd = peachbox.Spark.Instance().context().parallelize(input)
        df = peachbox.Spark.Instance().sql_context().createDataFrame(rdd)

        self.pail = peachbox.connector.Pail()
        self.pail.data = df
        self.pail.partition_key = '10'
        self.pail.model = MasterDataSetModel

    def test_append(self):
        peachbox.DWH.Instance().append(self.pail)
        result = peachbox.DWH.Instance().read_data_frame(self.pail.model.mart, self.pail.target()).collect()
        result.sort(key=lambda e: e.true_as_of_seconds)

        self.assertEqual('u1', result[0].user)
        self.assertEqual('p1', result[0].product)
        self.assertEqual('u2', result[1].user)

    def test_unicode_in_parquet(self):
        dir = peachbox.utils.TestHelper().mkdir_tmp()+'/data'
        with peachbox.Spark.Instance() as spark:
            rdd = spark.context().parallelize([pyspark.sql.Row(my_var='Jos\xe9')])
            df = spark.sql_context().createDataFrame(rdd)
            df.saveAsParquetFile(dir)
            df = spark.sql_context().parquetFile(dir)
            assert type(df.collect()[0][0]) is unicode

    def test_append_to_existing_data(self):
        peachbox.DWH.Instance().append(self.pail)
        peachbox.DWH.Instance().append(self.pail)

        result = peachbox.DWH.Instance().read_data_frame(self.pail.model.mart, self.pail.target()).collect()
        assert len(result) is 4

        self.assertEqual('u1', result[0].user)
        self.assertEqual('p1', result[0].product)
        self.assertEqual('u2', result[1].user)
