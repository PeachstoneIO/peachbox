#from pyspark import SparkContext, SparkConf
import pyspark

_spark_context = None
_spark_conf    = None

def context():
    pass

def set_conf(conf):
    global _spark_conf
    _spark_conf = conf

def get_conf():
    global _spark_conf
    return _spark_conf or {}

def context():
    global _spark_context
    if not _spark_context: initialize()
    return _spark_context

def initialize():
    app_name = get_conf().get('app_name') or 'peachbox'
    master   = get_conf().get('master') or 'local'
    conf     = pyspark.SparkConf().setAppName(app_name).setMaster(master)
    if get_conf().get('spark.executor.memory'): 
        conf.set("spark.executor.memory", get_conf()['spark.executor.memory'])

    global _spark_context
    _spark_context =  pyspark.SparkContext(conf=conf)

def stop():
    global _spark_context
    if context():
        context.stop()
        _spark_context = None

