import unittest
import peachbox.model

class MyRealTimeView(peachbox.model.RealTimeView):
    mart = 'reviews'
    key  = 'true_as_of_seconds'

    schema = [{'field':'true_as_of_seconds', 'type':'IntegerType'},
              {'field':'user_id', 'type':'StringType'},
              {'field':'review_id', 'type':'StringType'}]

class TestRealTimeView(unittest.TestCase):

    def setUp(self):
        self.m = MyRealTimeView

    def test_cassandra_row(self):
        row = self.m.cassandra_row(user_id='user_id_1', review_id='review_id_1', true_as_of_seconds=123)
        #self.assertEqual(({'true_as_of_seconds':123}, ['user_id_1', 'review_id_1']), row)
        self.assertEqual((123,'user_id_1','review_id_1'), row)

    def test_cassandra_row_wrong_order(self):
        row = self.m.cassandra_row(review_id='review_id_1', user_id='user_id_1', true_as_of_seconds=123)
        #self.assertEqual(({'true_as_of_seconds':123}, ['user_id_1', 'review_id_1']), row)
        self.assertEqual((123,'user_id_1','review_id_1'), row)

    def test_cassandra_initialize(self):
        self.m.cassandra_initialize()
        self.assertEqual(0, self.m._cassandra_indices['true_as_of_seconds'])
        self.assertEqual(1, self.m._cassandra_indices['user_id'])
        self.assertEqual(2, self.m._cassandra_indices['review_id'])

    def test_name(self):
        self.assertEqual('MyRealTimeView', self.m.name())

#    def test_cassandra_table_cql(self):
#        print self.m.cassandra_table_cql()
#        assert False
#
#    def test_cassandra_output_cql(self):
#        print self.m.cassandra_output_cql()
#        assert False

