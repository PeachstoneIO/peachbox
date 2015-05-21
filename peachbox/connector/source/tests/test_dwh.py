import unittest
import os.path

import peachbox
import peachbox.fs
import peachbox.connector.source
import peachbox.model
import sys

class ReviewByUserEdge(peachbox.model.MasterDataSet):
    data_unit_index = 0
    partition_key = 'true_as_of_seconds'
    partition_granularity = 60*60*24*360 
    schema = [{'field':'user_id', 'type':'StringType'},
              {'field':'review_id', 'type':'StringType'}]

class TestDWH(unittest.TestCase):
    dwh = peachbox.DWH.Instance()
    dwh.fs = peachbox.fs.Local()
    dwh.fs.dwh_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'fixtures/dwh')

    def setUp(self):
        self.source = peachbox.connector.source.DWH(model=ReviewByUserEdge)

    def test_emit_all(self):
        self.source.set_param({'smallest_key':0, 'biggest_key':sys.maxint})
        assert self.source.emit()['data'].take(10)

    def test_emit_by_time_range(self):
        self.source.set_param({'smallest_key':0, 'biggest_key':904608000})
        assert self.source.emit()['data'].take(10)
        self.source.set_param({'smallest_key':904608000, 'biggest_key':sys.maxint})
        assert self.source.emit()['data'].take(10)
        self.source.set_param({'smallest_key':0, 'biggest_key':904608000-1})
        assert not self.source.emit()['data']



