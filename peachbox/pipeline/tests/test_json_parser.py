import unittest
import peachbox.pipeline

class TestJSONParser(unittest.TestCase):
    def setUp(self):
        self.p = peachbox.pipeline.JSONParser()

    def test_parse(self):
        self.assertEqual(self.p.parse('{"k":"v"}'), {'k':'v'})

    def test_parse_not_valid(self):
        assert self.p.parse('') is None

    def test_execute(self):
        rdd = peachbox.Spark.Instance().context().parallelize([u'{"k":"v"}', u'invalid'])
        p = self.p.execute(rdd).collect()
        self.assertEqual(1, len(p))
        self.assertEqual('v', p[0]['k'])

