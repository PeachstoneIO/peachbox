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

    def test_list_tables(self):
        self.c.create_keyspace('ks_test5')
        self.c.session().execute("CREATE TABLE users (f1 int PRIMARY KEY, f2 int)")
        tables = self.c.list_tables()
        self.assertEqual(['users'], tables)
        self.c.drop_keyspace('ks_test5')

    def test_table_exists(self):
        self.c.create_keyspace('ks_test6')
        self.c.session().execute("CREATE TABLE users (f1 int PRIMARY KEY, f2 int)")
        assert self.c.table_exists('users')
        self.c.drop_keyspace('ks_test6')


