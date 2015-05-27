import unittest
import peachbox.connector.source
import kafka
from kafka.common import OffsetRequest


class TestIntegrationKafkaJSON(unittest.TestCase):
    def setUp(self):
        self.kafka = kafka.KafkaClient('localhost:9092')
        self.topic = 't1'

    def test_emit(self):
        k = peachbox.connector.source.KafkaJSON('t1')

        reqs = [OffsetRequest(self.topic, 0, -1, 10)]
        from_offset = self.kafka.send_offset_request(reqs)[0].offsets[0] - 5
        k.set_param({'from_offset':from_offset})
        assert k.emit()['data'].collect()
