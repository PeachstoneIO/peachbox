from peachbox.model.view import View
import peachbox.model

class RealTimeView(View):

    keys = [] 

    _cassandra_initialized = False
    _cassandra_indices = None

    @classmethod
    def row(cls, **kwargs):
        if not cls._cassandra_initialized: cls.cassandra_initialize()
        partition_key = hash(''.join([str(kwargs[v]) for v in cls.keys])) % 10
        row = dict(kwargs)
        row.update({'partition_key':partition_key})
        return row

    @classmethod
    def cassandra_schema(cls):
        if not cls._cassandra_schema: cls.generate_cassandra_schema()
        return cls._cassandra_schema

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
        cql = "CREATE TABLE " + cls.name() + ' (partition_key int, '

        fields = [e['field'] + ' ' + peachbox.model.Types.cassandra_type(e['type']) for e in cls.schema]
        cql += ', '.join(fields)

#        for i,entry in enumerate(cls.schema):
#            cql += (entry['field'] + ' ' + peachbox.model.Types.cassandra_type(entry['type']))
#            if entry['field'] is cls.key:
#                cql += ' PRIMARY KEY,'
#            elif i is not len(cls.schema)-1:
#                cql += ','
        cql += ', PRIMARY KEY (' + ', '.join(['partition_key']+cls.keys) + ')'
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






