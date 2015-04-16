import unittest                            

import peachbox.spark
import pyspark
import time
                                       
class TestIntegrationSpark(unittest.TestCase):  

    def test__context_init(self):
        with peachbox.spark.Instance() as spark:
            self.assertIsInstance(spark.context(), pyspark.context.SparkContext)

    def test_default_conf(self):
        with peachbox.spark.Instance() as spark:
            conf = dict(spark.get_spark_conf().getAll())
            self.assertEqual('peachbox', conf.get('spark.app.name'))
            self.assertEqual('local[2]', conf.get('spark.master'))

    def test_spark_conf(self):
        conf = {'spark.app.name':'test_name', 'spark.master':'local[*]'}
        with peachbox.spark.Spark(conf) as spark:
            spark_conf = dict(spark.get_spark_conf().getAll())
            self.assertEqual('test_name', spark_conf.get('spark.app.name'))
            self.assertEqual('local[*]', spark_conf.get('spark.master'))

    def test_stop_and_relaunch(self):
        with peachbox.spark.Spark() as spark:
            spark.context()
            spark.stop()
            spark.spark_conf = {'spark.app.name':'new_launch'}
            self.assertEqual('new_launch', spark.context().appName)

    def test_rdd_creation(self):
        with peachbox.spark.Spark() as spark:
            rdd = spark.context().parallelize([1,2])
            self.assertEqual([1,2], rdd.collect())

