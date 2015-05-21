from pyspark.sql.types import *


class Types(object):

    spark_types = {'StringType':StringType,
                   'IntegerType':IntegerType}

    python_types = {'StringType':unicode,
                    'IntegerType':int}

    cassandra_types = {'StringType':'text',
                       'IntegerType':'int'}

    @staticmethod
    def spark_type(peachbox_type):
        return Types.spark_types[peachbox_type]()

    @staticmethod
    def python_type(peachbox_type):
        return Types.python_types[peachbox_type]

    @staticmethod
    def cassandra_type(peachbox_type):
        return Types.cassandra_types[peachbox_type]

