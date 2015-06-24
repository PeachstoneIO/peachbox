import unittest
import peachbox
from peachbox.task import ImportProperties


class MockMasterSchema(object):
    def fill_properties(self, rdd):
        return rdd
        

class TestImportProperties(unittest.TestCase):

    def test_init(self):
        imp = ImportProperties()
        assert imp


    def test_execute(self):
        imp = ImportProperties()
        rdd = [{'bla':'blubb'}, {'b2':'bb'}]
        imp.set_master_schema(MockMasterSchema())
        result = imp.execute(rdd)
        self.assertEqual('blubb', result[0]['bla'])

#        importer = MockImportProperties()
#        sc = peachbox.Spark.Instance().context()
#        rdd = sc.parallelize(['row1', 'row2'])
#        result = importer.execute(rdd).collect()
#        self.assertEqual('row1', result[0])
#        self.assertEqual('row2', result[1])


