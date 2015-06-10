# Copyright 2015 Philipp Pahl, Sven Schubert
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pyspark

class Spark(object):
    """ Interface to Apache Spark. Singleton. After initialization use peachbox.spark.Instance()
    to access the existing instance::

        class HelloWorld()
            def __init__():

    """

    #TODO: Move Singleton functionality into base class in utils
    @staticmethod
    def Instance():
        """Spark is a singleton. Access via Instance()"""
        if not Spark._active_instance:
            Spark._active_instance = Spark()
        return Spark._active_instance

    _active_instance = None
    _default_spark_conf = {
            'spark.app.name':'peachbox',
            'spark.master':'local[2]',
            'spark.cassandra.connection.host':'localhost'
            }

    def __init__(self, spark_conf={}):
        if Spark._active_instance:
            raise ValueError(
                "Cannot run multiple Spark instances at once."
                "Use peachbox.spark.Instance() to access existing instance.")

        self.spark_conf         = spark_conf
        self._spark_context     = None
        self._sql_context       = None
        self._streaming_context = None
        Spark._active_instance  = self

    def context(self):
        """Spark context"""
        if not self._spark_context: self.initialize()
        return self._spark_context

    def sql_context(self):
        """Returns a SQLContext, which is the entry point to PySparks' data frames."""
        if not self._sql_context:
            import pyspark.sql
            self._sql_context = pyspark.sql.SQLContext(self.context())
        return self._sql_context

    def streaming_context(self, dstream_time_interval):
        if not self._streaming_context:
            import pyspark.streaming
            sc = Spark.Instance().context()
            self._streaming_context = pyspark.streaming.StreamingContext(sc, dstream_time_interval)
        return self._streaming_context

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
        if conf.get('spark.cassandra.connection.host'): 
            spark_conf.set('spark.cassandra.connection.host', conf.get('spark.cassandra.connection.host'))
        if conf.get('spark.driver.memory'): 
            spark_conf.set('spark.driver.memory', conf.get('spark.driver.memory'))
        return spark_conf

    def stop_context(self):
        if self._spark_context:
            self.context().stop()
            self._spark_context = None
            self._sql_context = None
        if self._streaming_context:
            self._streaming_context.stop()
            self._streaming_context = None
    
    def stop(self):
        self.stop_context()
        Spark._active_instance = None

    # TODO: Context manager is only used for testing, move it to utils
    def __exit__(self, *err):
        self.stop()

    def __enter__(self):
        return self


