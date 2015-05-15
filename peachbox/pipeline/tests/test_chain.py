import unittest
import peachbox
import peachbox.pipeline

class TestChain(unittest.TestCase):
    def setUp(self):
        self.p1 = peachbox.pipeline.JSONParser()
        self.p2 = peachbox.pipeline.Validator(['k1'])
        self.rdd = peachbox.Spark.Instance().context().parallelize(['{"k1":"v1"}', 'invalid', '{"k2":"v2"}'])

    def test_chain_w_single_pipeline(self):
        c = peachbox.pipeline.Chain([self.p1])
        result = c.execute(self.rdd).collect()
        self.assertEqual('v1', result[0]['k1'])
        self.assertEqual(2, len(result))

    def test_chain_w_multiple_pipelines(self):
        c = peachbox.pipeline.Chain([self.p1, self.p2])
        result = c.execute(self.rdd).collect()
        self.assertEqual('v1', result[0]['k1'])
        self.assertEqual(1, len(result))

