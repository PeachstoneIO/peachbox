import unittest
import peachbox.utils

import peachbox.connector.source

class TestJSON(unittest.TestCase):
    def setUp(self):
        self.json = peachbox.connector.source.JSON()

    def test_set_param(self):
        param = {'path':'/data'}
        self.json.set_param(param)
        self.assertEqual('/data', self.json.path)

    def test_set_param_raise(self):
        with self.assertRaises(ValueError):
            self.json.set_param({})

    def test_emit(self):
        json_file = peachbox.utils.TestHelper.write_json('data', [{'key':'value'}])
        self.json.set_param({'path':json_file})
        df = self.json.emit()['data'].collect()
        self.assertEqual('value', df[0].key)
