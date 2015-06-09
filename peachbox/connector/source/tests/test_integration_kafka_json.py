import unittest
import peachbox.connector.source
import kafka
from kafka.common import OffsetRequest


class TestIntegrationKafkaJSON(unittest.TestCase):
    def setUp(self):
        self.kafka = kafka.KafkaClient('localhost:9092')
        self.producer = kafka.producer.SimpleProducer(self.kafka)
        self.topic = 'test_topic_1'

    def test_emit(self):
        self.producer.send_messages(self.topic, '{"Hello":"Kafka"}')
        k = peachbox.connector.source.KafkaJSON(self.topic)

        reqs = [OffsetRequest(self.topic, 0, -1, 10)]
        from_offset = self.kafka.send_offset_request(reqs)[0].offsets[0] - 2

        k.set_param({'payload':{'latest_kafka_offset':from_offset}})
        data = k.emit()['data'].collect()
        self.assertEqual('Kafka', data[0]['Hello'])
