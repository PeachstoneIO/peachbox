import unittest
import peachbox
#import cassandra.cluster

from pyspark import SparkContext

class TestIntegrationCassandraDriver(unittest.TestCase):
    def setUp(self):
        self.c = peachbox.CassandraDriver()

    def tearDown(self):
        self.c.shutdown()

    def test_connection(self):
        assert self.c.session() 
        self.c.shutdown()
        assert not self.c._session

    def test_keyspace_exists(self):
        assert not self.c.keyspace_exists('non-existing-ks')

    def test_create_keyspace(self):
        if(self.c.keyspace_exists('ks_test1')): self.c.drop_keyspace('ks_test1')
        self.c.create_keyspace('ks_test1')
        exists = self.c.keyspace_exists('ks_test1')
        self.c.drop_keyspace('ks_test1')
        assert exists

    def test_drop_keyspace(self):
        self.c.create_keyspace('ks_test2')
        assert self.c.keyspace_exists('ks_test2')
        self.c.drop_keyspace('ks_test2')
        assert not self.c.keyspace_exists('ks_test2')

    def test_set_keyspace(self):
        self.c.create_keyspace('ks_test3')
        self.c.create_keyspace('ks_test4')
        self.c.set_keyspace('ks_test3')
        self.assertEqual('ks_test3', self.c.session().keyspace)
        self.c.drop_keyspace('ks_test3')
        self.c.drop_keyspace('ks_test4')

    def test_write_rdd(self):
        host = 'localhost' 
        keyspace = 'test' 
        cf = 'users' 
        sc = peachbox.Spark.Instance().context() 

        if(self.c.keyspace_exists(keyspace)): self.c.drop_keyspace(keyspace)
        self.c.create_keyspace(keyspace)

        create_table_cql = ("""CREATE TABLE %s (\
                              user_id int PRIMARY KEY,\
                              fname text,\
                              lname text)""" % (cf,))

        self.c.execute(create_table_cql)

        conf = {"cassandra.output.thrift.address": host,
                "cassandra.output.thrift.port": "9160",
                "cassandra.output.keyspace": keyspace,
                "cassandra.output.partitioner.class": "Murmur3Partitioner",
                "cassandra.output.cql": "UPDATE " + keyspace + "." + cf + " SET fname = ?, lname = ?",
                "mapreduce.output.basename": cf,
                "mapreduce.outputformat.class": "org.apache.cassandra.hadoop.cql3.CqlOutputFormat",
                "mapreduce.job.output.key.class": "java.util.Map",
                "mapreduce.job.output.value.class": "java.util.List"}

        key = {"user_id": (17)}
        sc.parallelize([(key, ['john', 'burrito'])]).saveAsNewAPIHadoopDataset(
                    conf=conf,
                    keyConverter="org.apache.spark.examples.pythonconverters.ToCassandraCQLKeyConverter",
                    valueConverter="org.apache.spark.examples.pythonconverters.ToCassandraCQLValueConverter")

        sc.stop()

        resp = self.c.execute("SELECT * FROM users")
        self.assertEqual('john', resp[0].fname)
