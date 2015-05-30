import unittest
import peachbox.connector.sink
import peachbox.model
import peachbox

class MyModel(peachbox.model.RealTimeView):
    mart = 'test_mart'
    keys  = ['true_as_of_seconds']
    schema = [{'field':'true_as_of_seconds', 'type':'IntegerType'},
              {'field':'user_id', 'type':'StringType'},
              {'field':'review_id', 'type':'StringType'}]

class MyModel2(peachbox.model.RealTimeView):
    mart = 'test_mart_2'
    keys  = ['true_as_of_seconds', 'score']
    schema = [{'field':'true_as_of_seconds', 'type':'IntegerType'},
              {'field':'user_id', 'type':'StringType'},
              {'field':'review_id', 'type':'StringType'},
              {'field':'score', 'type':'IntegerType'}]

class TestRealTimeView(unittest.TestCase):
    def setUp(self):
        self.view = peachbox.connector.sink.RealTimeView()
        self.c = peachbox.CassandraDriver()

    def test_setup_keyspace(self):
        self.view.setup_keyspace('test_mart')
        assert peachbox.CassandraDriver().keyspace_exists('test_mart')
        self.c.drop_keyspace('test_mart')

    def test_absorb(self):
        data = [MyModel.row(true_as_of_seconds=1, user_id='u1', review_id='r1'),
                MyModel.row(true_as_of_seconds=2, user_id='u2', review_id='r2')]
        rdd = peachbox.Spark.Instance().context().parallelize(data)
        self.view.absorb([{'data':rdd, 'model':MyModel}])
        self.c.set_keyspace(MyModel.mart)
        entry_1 = self.c.session().execute("SELECT * FROM mymodel WHERE true_as_of_seconds=1 ALLOW FILTERING")
        entry_2 = self.c.session().execute("SELECT * FROM mymodel WHERE true_as_of_seconds=2 ALLOW FILTERING")
        self.assertEqual(u'u1', entry_1[0].user_id)
        self.assertEqual(u'r1', entry_1[0].review_id)
        self.assertEqual(u'u2', entry_2[0].user_id)

        self.c.drop_keyspace(MyModel.mart)

    def test_setup_table(self):
        self.c.drop_keyspace(MyModel2.mart)

        data = [MyModel2.row(true_as_of_seconds=1, user_id='u1', review_id='r1', score=4),
                MyModel2.row(true_as_of_seconds=2, user_id='u2', review_id='r2', score=3)]
        rdd = peachbox.Spark.Instance().context().parallelize(data)

        self.view.absorb([{'data':rdd, 'model':MyModel2}])
        self.c.set_keyspace(MyModel2.keyspace_name())
        # At least one = sign is needed in queries
        entry_1 = self.c.session().execute("SELECT * FROM mymodel2 WHERE score=4 AND true_as_of_seconds<2  ALLOW FILTERING")
        entry_2 = self.c.session().execute("SELECT * FROM mymodel2 WHERE score=3 AND true_as_of_seconds>1 ALLOW FILTERING")

        self.assertEqual(entry_1[0].user_id, 'u1')
        self.assertEqual(entry_2[0].user_id, 'u2')

        self.c.drop_keyspace(MyModel2.mart)


