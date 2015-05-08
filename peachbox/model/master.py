#from peachbox.model import Data
import pyspark.sql.types
from peachbox.model.file_format import FileFormat
import peachbox.model

class MasterDataSet():

    data_unit_index       = None
    partition_key         = None
    partition_granularity = None
    output_format         = FileFormat.Parquet
    schema                = None
    mart                  = 'master'

    _spark_schema         = None
    _spark_row            = None
    _spark_indices        = None
    _types                = None

    _initialized          = False

    @classmethod
    def target(cls):
        return str(cls.data_unit_index)

    @classmethod
    def spark_schema(cls):
        if not cls._spark_schema: cls.generate_spark_schema() 
        return cls._spark_schema

    @classmethod
    def initialize(cls):
        cls.generate_spark_schema()
        cls.generate_spark_row_definition()
        cls._initialized = True

    @classmethod
    def spark_row(cls, **kwargs):
        if not cls._initialized: cls.initialize() 

        values = [None]*len(kwargs)
        for field, value in kwargs.iteritems():
            idx = cls._spark_indices[field]
            if type(value) is not (cls._types[idx]):
                raise TypeError("%s:%s (type:%s) must be of type %s" % 
                        (field, value, type(value), cls._types[idx]))
            else:
                values[idx] = value
        return cls._spark_row(*values)


    @classmethod
    def generate_spark_schema(cls):
        fields = []
        types = [None]*(len(cls.schema)+1)
        indices = {}
        current_idx = 0

        type_int = peachbox.model.Types.spark_type('IntegerType') 
        fields.append(pyspark.sql.types.StructField('true_as_of_seconds', type_int, True))
        types[current_idx] = int
        indices['true_as_of_seconds'] = current_idx
        current_idx += 1

        for field in cls.schema:
            spark_type = peachbox.model.Types.spark_type(field['type'])
            fields.append(pyspark.sql.types.StructField(field['field'], spark_type, True))
            types[current_idx] = peachbox.model.Types.python_type(field['type'])
            indices[field['field']] = current_idx

            current_idx += 1

        cls._spark_schema = pyspark.sql.types.StructType(fields)
        cls._spark_indices = indices
        cls._types = types

    @classmethod
    def generate_spark_row_definition(cls):
        names = [field.name for field in cls.spark_schema().fields]
        cls._spark_row = pyspark.sql.Row(*names)
    


     



