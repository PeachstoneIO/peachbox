import unittest
from mock import patch
import peachbox.connector.sink
import peachbox.model

class MyModel(peachbox.model.MasterDataSet):
    schema = {}

class SomeClass(object):
    def static_method(self):
        pass

class TestMasterData(unittest.TestCase):
    def setUp(self):
        self.master_data_connector = peachbox.connector.sink.MasterData()

    @patch.object(peachbox.connector.sink.MasterData, 'data_frame')
    def test_absorb_calls_data_frame(self, mock):
        peachbox.connector.sink.MasterData().absorb([{'data':'rdd', 'model':MyModel}])
        schema = MyModel.spark_schema()
        mock.assert_called_with('rdd', schema)

    @patch.object(peachbox.connector.sink.MasterData, 'create_pails')
    @patch.object(peachbox.connector.sink.MasterData, 'data_frame')
    def test_absorb_calls_create_pails(self, mock_data_frame, mock_create_pails):
        peachbox.connector.sink.MasterData().absorb([{'data':'rdd', 'model':MyModel}])
        mock_create_pails.assert_called_with(mock_data_frame(), MyModel)

    # TODO
    def test_absorb_calls_dwh_append(self):
        assert True
