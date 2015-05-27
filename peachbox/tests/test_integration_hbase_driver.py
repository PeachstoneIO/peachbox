import unittest
import peachbox

class TestIntegrationHbaseDriver(unittest.TestCase):
    pass
#    def setUp(self):
#        self.hbase = peachbox.HbaseDriver()
#        self.schema = {'cf1':{}}
#
#    def test_table_exists(self):
#        t = u'my_test_table'
#        self.hbase.create_table(t, self.schema)
#        assert self.hbase.table_exists(t)
#        self.hbase.drop_table(t)
#
#    def test_create_and_drop_table(self):
#        t = u'my_test_table'
#        self.hbase.create_table(t, self.schema)
#        assert self.hbase.table_exists(t)
#        self.hbase.drop_table(t)
#        assert not self.hbase.table_exists(t)
#
#    def test_create_and_drop_table_camel_case(self):
#        t = u'MyTestTable'
#        self.hbase.create_table(t, self.schema)
#        assert self.hbase.table_exists(t)
#        self.hbase.drop_table(t)
#        assert not self.hbase.table_exists(t)
#
#
#    def test_drop_non_existing_table(self):
#        try:
#            self.hbase.drop_table('nonsense')
#        except Exception:
#            self.fail("drop_table raised Exception unexpectedly!")
#




