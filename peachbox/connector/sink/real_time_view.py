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
        data.saveToCassandra(model.keyspace_name(), model.name())

    def setup_keyspace(self, mart):
        if not self.cassandra_driver.keyspace_exists(mart):
            self.cassandra_driver.create_keyspace(mart)
        else:
            self.cassandra_driver.set_keyspace(mart)

    def setup_table(self, model):
        self.cassandra_driver.set_keyspace(model.keyspace_name())
        if not self.cassandra_driver.table_exists(model.name()):
            print 'setting up table: ' + model.name()
            print model.cassandra_table_cql()
            self.cassandra_driver.execute(model.cassandra_table_cql())
            self.create_cassandra_indices(model)

    def create_cassandra_indices(self, model):
        for key in model.keys:
            self.create_cassandra_index(key, model)

    def create_cassandra_index(self, key, model):
        index_name = model.keyspace_name()+'_'+model.name()+'_'+key + '_index'
        table = model.name()
        cql = "CREATE INDEX " + index_name + ' ON ' + table + '(' + key + ')'
        self.cassandra_driver.execute(cql)

    def set_param(self, param):
        pass





