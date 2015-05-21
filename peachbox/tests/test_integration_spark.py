import unittest                            

import peachbox
import pyspark
import time
import multiprocessing
import os

import peachbox.utils
from pyspark.sql.types import *
                                       
class TestIntegrationSpark(unittest.TestCase):  

    def test_context_init(self):
        with peachbox.Spark.Instance() as spark:
            self.assertIsInstance(spark.context(), pyspark.context.SparkContext)

    def test_default_conf(self):
        with peachbox.Spark.Instance() as spark:
            conf = dict(spark.get_spark_conf().getAll())
            self.assertEqual('peachbox', conf.get('spark.app.name'))
            self.assertEqual('local[2]', conf.get('spark.master'))

    def test_spark_conf(self):
        conf = {'spark.app.name':'test_name', 'spark.master':'local[*]'}
        with peachbox.Spark(conf) as spark:
            spark_conf = dict(spark.get_spark_conf().getAll())
            self.assertEqual('test_name', spark_conf.get('spark.app.name'))
            self.assertEqual('local[*]', spark_conf.get('spark.master'))

    def test_stop_and_relaunch(self):
        with peachbox.Spark() as spark:
            spark.context()
            spark.stop()
            spark.spark_conf = {'spark.app.name':'new_launch'}
            self.assertEqual('new_launch', spark.context().appName)

    def test_rdd_creation(self):
        with peachbox.Spark() as spark:
            rdd = spark.context().parallelize([1,2])
            self.assertEqual([1,2], rdd.collect())

    def test_sql_context(self):
        with peachbox.Spark() as spark:
            self.assertIsInstance(spark.sql_context(), pyspark.sql.SQLContext)

    def test_stop_sql_context(self):
        with peachbox.Spark() as spark:
            assert spark.sql_context()
            spark.stop()
            assert not spark._sql_context

    def test_streaming_context_initialization(self):
        with peachbox.Spark() as spark:
            assert spark.streaming_context(dstream_time_interval=2)
            spark.stop()
            assert not spark._streaming_context


    # TODO: Test streaming data properly
    def move_file(self, dir1, dir2):
        #time.sleep(10)
        os.rename(dir1+"/file.json", dir2+"/file.json")

    def test_streaming_context(self):
        dir1 = peachbox.utils.TestHelper().mkdir_tmp()
        dir2 = peachbox.utils.TestHelper().mkdir_tmp() 
        dir3 = peachbox.utils.TestHelper().mkdir_tmp() 
        
        print 'dir1: '+dir1
        print 'dir2: '+dir2
        print 'dir3: '+dir3

        j = peachbox.utils.TestHelper.write_json('file.json', [{'hello':'world'}], dir2)
        multiprocessing.Process(target=self.move_file, args=(dir2,dir1)).start()

        with peachbox.Spark() as spark:
            ssc = spark.streaming_context(1)
            rdd = ssc.textFileStream(dir1)
            rdd.map(lambda line: line)
            rdd.pprint()
            ssc.start()
            ssc.awaitTermination(1)


