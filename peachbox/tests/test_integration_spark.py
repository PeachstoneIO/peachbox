import unittest                            
#import json                                                               
#from utils import TestHelper

import peachbox.spark
import pyspark
                                       
class TestIntegrationSpark(unittest.TestCase):  
    #def setUp(self):                          
        #self.spark = Spark(app_name="sparktest")

    #def tearDown(self):
        #self.spark.stop()                
                      
    #def test_context(self):
        #sc = self.spark.context()
        #assert (self.spark is not None)             
                                   
    #def test_app_name(self):      
        #sc = self.spark.context()                  
        #self.assertEqual("sparktest",sc.appName)

    #def test_rdd_creation(self):
        #rdd = self.spark.context().parallelize([1,2])
        #self.assertEqual([1,2], rdd.collect())
                          
    #def test_rdd_readout_explicit_filename(self):
        #input_path = TestHelper.create_temp_dir()
        #filename = TestHelper.write_json('my_file', [{'foo':'bar'}])
        #rdd = self.spark.context().textFile(filename).map(lambda line: json.loads(line))
        #self.assertEqual(rdd.collect()[0]['foo'], 'bar')    

    def test_spark_initialization(self):
        self.assertIsInstance(peachbox.spark.context(), pyspark.context.SparkContext)
