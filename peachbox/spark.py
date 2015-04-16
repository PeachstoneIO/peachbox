import pyspark
import threading

def Instance():
    if not Spark._active_instance:
        Spark._active_instance = Spark()
    return Spark._active_instance

class Spark(object):

    """
    Spark is a singleton. After initialization use peachbox.spark.Instance()
    to access the existing instance.
    """

    _active_instance = None
    _default_spark_conf = {
            'spark.app.name':'peachbox',
            'spark.master':'local[2]'
            }

    def __init__(self, spark_conf={}):
        if Spark._active_instance:
            raise ValueError(
                "Cannot run multiple Spark instances at once."
                "Use peachbox.spark.Instance() to access existing instance.")

        self.spark_conf = spark_conf
        self._spark_context = None
        Spark._active_instance = self

    def context(self):
        if not self._spark_context: self.initialize()
        return self._spark_context

    def initialize(self):
        conf = self.get_spark_conf()
        self._spark_context =  pyspark.SparkContext(conf=conf)

    def get_spark_conf(self):
        conf       = dict(Spark._default_spark_conf, **self.spark_conf)
        spark_conf = pyspark.SparkConf()

        if conf.get('spark.app.name'): spark_conf.setAppName(conf.get('spark.app.name'))
        if conf.get('spark.master'):   spark_conf.setMaster(conf.get('spark.master'))
        if conf.get('spark.executor.memory'): 
            spark_conf.set('spark.executor.memory', conf.get('spark.executor.memory'))
        return spark_conf

    def stop_context(self):
        if self._spark_context:
            self.context().stop()
            self._spark_context = None
    
    def stop(self):
        self.stop_context()
        Spark._active_instance = None

    # TODO: Context manager is only used for testing, move it to utils
    def __exit__(self, *err):
        self.stop()

    def __enter__(self):
        return self


