import unittest
import re
import peachbox.model

class MyRealTimeView(peachbox.model.RealTimeView):
    mart = 'reviews'
    keys  = ['true_as_of_seconds']

    schema = [{'field':'true_as_of_seconds', 'type':'IntegerType'},
              {'field':'user_id', 'type':'StringType'},
              {'field':'review_id', 'type':'StringType'}]

class TestRealTimeView(unittest.TestCase):

    def setUp(self):
        self.m = MyRealTimeView

    def test_row(self):
        row = self.m.row(user_id='user_id_1', review_id='review_id_1', true_as_of_seconds=123)
        partition_key = hash(str(123)) % 10
        self.assertEqual({'partition_key':partition_key, 
                          'true_as_of_seconds':123, 
                          'user_id':'user_id_1', 
                          'review_id':'review_id_1'}, row)

    def test_row_wrong_order(self):
        row = self.m.row(review_id='review_id_1', user_id='user_id_1', true_as_of_seconds=123)
        partition_key = hash(str(123)) % 10
        self.assertEqual({'partition_key':partition_key, 
                          'true_as_of_seconds':123, 
                          'user_id':'user_id_1', 
                          'review_id':'review_id_1'}, row)

    def test_create_table_cql(self):
        s1    = self.m.cassandra_table_cql()
        s2 = """CREATE TABLE myrealtimeview (partition_key int, true_as_of_seconds int, user_id text, 
                review_id text, PRIMARY KEY (partition_key, true_as_of_seconds))"""

        # Remove whitespaces
        s2 = re.sub(' +',' ', ' '.join(s2.splitlines()))
        self.assertEqual(s1, s2) 

    def test_name(self):
        self.assertEqual('MyRealTimeView'.lower(), self.m.name())

#    def test_cassandra_initialize(self):
#        self.m.cassandra_initialize()
#        self.assertEqual(0, self.m._cassandra_indices['true_as_of_seconds'])
#        self.assertEqual(1, self.m._cassandra_indices['user_id'])
#        self.assertEqual(2, self.m._cassandra_indices['review_id'])

#    def test_cassandra_table_cql(self):
#        print self.m.cassandra_table_cql()
#        assert False
#
#    def test_cassandra_output_cql(self):
#        print self.m.cassandra_output_cql()
#        assert False

