import unittest
import peachbox
import peachbox.task

class TestTaskChain(unittest.TestCase):
    def setUp(self):
        self.t1 = peachbox.task.JSONParser()
        self.t2 = peachbox.task.FieldExistsValidator(['k1'])
        self.rdd = peachbox.Spark.Instance().context().parallelize(['{"k1":"v1"}', 'invalid', '{"k2":"v2"}'])

    def test_chain_w_single_task(self):
        c = peachbox.task.TaskChain([self.t1])
        result = c.execute(self.rdd).collect()
        self.assertEqual('v1', result[0]['k1'])
        self.assertEqual(2, len(result))

    def test_chain_w_multiple_tasks(self):
        c = peachbox.task.TaskChain([self.t1, self.t2])
        result = c.execute(self.rdd).collect()
        self.assertEqual('v1', result[0]['k1'])
        self.assertEqual(1, len(result))

