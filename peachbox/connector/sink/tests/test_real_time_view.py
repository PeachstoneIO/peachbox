import unittest
import peachbox.connector.sink
import peachbox.model
import peachbox

class MyModel(peachbox.model.RealTimeView):
    mart = 'test_mart'

class TestRealTimeView(unittest.TestCase):
    def setUp(self):
        self.view = peachbox.connector.sink.RealTimeView()
        self.cassandra = peachbox.CassandraDriver()

    def test_setup_keyspace(self):
        self.view.setup_keyspace('test_mart')
        assert peachbox.CassandraDriver().keyspace_exists('test_mart')
        self.cassandra.drop_keyspace('test_mart')


