import peachbox
import pyspark_cassandra.streaming

class RealTimeView(object):
    def __init__(self):
        self.cassandra_driver = peachbox.CassandraDriver()

    def absorb(self, data_descriptor):
        for data_descriptor_entry in data_descriptor:
            self.update_cassandra(data_descriptor_entry['data'], data_descriptor_entry['model'])

    def update_cassandra(self, data, model):
        self.setup_keyspace(model.keyspace_name())
        self.setup_table(model)
#
#        create_table_cql = ("""CREATE TABLE %s (\
#                              user_id int PRIMARY KEY,\
#                              fname text,\
#                              lname text)""" % (cf,))
#
#        self.c.execute(create_table_cql)
#
#        conf = {"cassandra.output.thrift.address": 'localhost',
#                "cassandra.output.thrift.port": "9160",
#                "cassandra.output.keyspace": model.mart,
#                "cassandra.output.partitioner.class": "Murmur3Partitioner",
#                "cassandra.output.cql":model.cassandra_output_cql(), 
#                "mapreduce.output.basename": model.name(),
#                "mapreduce.outputformat.class": "org.apache.cassandra.hadoop.cql3.CqlOutputFormat",
#                "mapreduce.job.output.key.class": "java.util.Map",
#                "mapreduce.job.output.value.class": "java.util.List"}

        data.saveToCassandra(model.keyspace_name(), model.name())
#        data.saveAsNewAPIHadoopDataset(
#            conf=conf,
#            keyConverter="org.apache.spark.examples.pythonconverters.ToCassandraCQLKeyConverter",
#            valueConverter="org.apache.spark.examples.pythonconverters.ToCassandraCQLValueConverter")

#        key = {"user_id": (17)}
#        sc.parallelize([(key, ['john', 'burrito'])]).saveAsNewAPIHadoopDataset(
#                    conf=conf,
#                    keyConverter="org.apache.spark.examples.pythonconverters.ToCassandraCQLKeyConverter",
#                    valueConverter="org.apache.spark.examples.pythonconverters.ToCassandraCQLValueConverter")
#
    def setup_keyspace(self, mart):
        print('setting up keyspace: '+mart)
        if not self.cassandra_driver.keyspace_exists(mart):
            self.cassandra_driver.create_keyspace(mart)
        else:
            self.cassandra_driver.set_keyspace(mart)

    def setup_table(self, model):
        print 'entering setting up table'
        print self.cassandra_driver.list_tables()
        self.cassandra_driver.set_keyspace(model.keyspace_name())
        if not self.cassandra_driver.table_exists(model.name()):
            print 'setting up table: ' + model.name()
            print model.cassandra_table_cql()
            self.cassandra_driver.execute(model.cassandra_table_cql())
            self.create_cassandra_indices(model)

    def create_cassandra_indices(self, model):
        print 'creating indices'
        for key in model.keys:
            self.create_cassandra_index(key, model)

    def create_cassandra_index(self, key, model):
        index_name = model.keyspace_name()+'_'+model.name()+'_'+key + '_index'
        table = model.name()
        cql = "CREATE INDEX " + index_name + ' ON ' + table + '(' + key + ')'
        print 'creating index:'
        print cql
        self.cassandra_driver.execute(cql)

    def set_param(self, param):
        pass





