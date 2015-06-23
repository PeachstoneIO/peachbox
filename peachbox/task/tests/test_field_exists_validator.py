import unittest
import peachbox
from peachbox.task import FieldExistsValidator

""" Test FieldExistsValidator"""

class TestValidator(unittest.TestCase):
    def setUp(self):
        fields = ['f1', 'f2']
        self.v = FieldExistsValidator(fields)

    def test_validate(self):
        assert self.v.validate({'f1':'v1', 'f2':'v2'})
        assert not self.v.validate({'f1':'v1'})

    def test_execute(self):
        e1 = {'f1':'v1', 'f2':'v2'}
        e2 = {'f1':'v1'}

        rdd = peachbox.Spark.Instance().context().parallelize([e1, e2])
        p = self.v.execute(rdd).collect()
        self.assertEqual(1, len(p))
        self.assertEqual('v1', p[0]['f1'])
