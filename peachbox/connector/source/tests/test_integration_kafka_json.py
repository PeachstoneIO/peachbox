import unittest
import peachbox.connector.source
import kafka


class TestIntegrationKafkaJSON(unittest.TestCase):
    def setUp(self):
        pass

    def test_emit(self):
        k = peachbox.connector.source.KafkaJSON('t1')
        assert k.emit()['data'].collect()
