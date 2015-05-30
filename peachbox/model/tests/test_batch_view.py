import unittest
import peachbox.model

class MyView(peachbox.model.BatchView):
    mart = 'test_mart'
    keys = ['f1']
    schema = [{'field':'f1', 'type':'StringType'}]

class TestBatchView(unittest.TestCase):
    def setUp(self):
        self.view = MyView

    # TODO: Batch view is supposed to write to HBase
    def test_is_real_time_view(self):
        #self.assertIsInstance(self.view, peachbox.model.RealTimeView)
        assert issubclass(self.view, peachbox.model.RealTimeView)

    def test_keyspace_name(self):
        self.assertEqual('test_mart', self.view.keyspace_name())

    def test_keys(self):
        self.assertEqual(['f1'], self.view.keys)

    def test_table_cql(self):
        s1 = self.view.cassandra_table_cql()
        s2 = 'CREATE TABLE myview (partition_key int, f1 text, PRIMARY KEY (partition_key, f1))'
        self.assertEqual(s1, s2) 
