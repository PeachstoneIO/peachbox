import peachbox.model

class RealTimeView():

    mart   = None
    schema = None

    key = 'true_as_of_seconds'

    _cassandra_initialized = False
    _cassandra_indices = None

    @classmethod
    def cassandra_schema(cls):
        if not cls._cassandra_schema: cls.generate_cassandra_schema()
        return cls._cassandra_schema

    @classmethod
    def cassandra_row(cls, **kwargs):
        if not cls._cassandra_initialized: cls.cassandra_initialize()

        values = [None]*len(kwargs)
        for (k,v) in kwargs.iteritems():
            #if k is not cls.key:
            values[cls._cassandra_indices[k]] = v

        #key = {cls.key: kwargs[cls.key]}
        #return (key, values)
        return tuple(values)

    @classmethod
    def cassandra_initialize(cls):
        #fields = filter(lambda entry: entry['field'] is not cls.key, cls.schema)
        fields = map(lambda entry: entry['field'], cls.schema)
        cls._cassandra_indices = {field:i for i,field in enumerate(fields)}

    @classmethod
    def name(cls):
        return cls.__name__.lower()

    @classmethod
    def cassandra_table_cql(cls):
        cql = "CREATE TABLE " + cls.name() + ' ('

        for i,entry in enumerate(cls.schema):
            cql += (entry['field'] + ' ' + peachbox.model.Types.cassandra_type(entry['type']))
            if entry['field'] is cls.key:
                cql += ' PRIMARY KEY,'
            elif i is not len(cls.schema)-1:
                cql += ','
        cql += ')'
        return cql
    
    @classmethod
    def cassandra_output_cql(cls):
        cql = "UPDATE " + cls.mart + "." + cls.name() + " SET "

        for i,entry in enumerate(cls.schema):
            if (entry['field'] is not cls.key):
                cql += entry['field'] + ' = ?'
                if (i is not len(cls.schema)-1):
                    cql += ', '
        return cql


    @classmethod
    def keyspace_name(cls):
        return cls.mart.lower()






