import peachbox.connector.source
from pyspark.streaming.kafka import OffsetRange

import pyspark
import pyspark.streaming
import pyspark.streaming.kafka
from pyspark.streaming.kafka import OffsetRange

import kafka
from kafka.common import OffsetRequest

import ujson as json

class KafkaJSON(peachbox.connector.Connector):
    def __init__(self, topic):
        self.kafka_params = {"zookeeper.connect": "localhost:2182",
                "metadata.broker.list": "localhost:9092",
                "group.id": "Group1",
                "zookeeper.connection.timeout.ms": "10000"}
        self.topic = topic

    def set_param(self, param):
        pass

    def emit(self):
        # spark = peachbox.Spark({'spark.master':'spark://eb94aa674c2d:7077'})
        # result = spark.context().parallelize([1,2,3])
        sc = peachbox.Spark.Instance().context()

        kafka_client = kafka.KafkaClient('localhost:9092')

        reqs = [OffsetRequest(self.topic, 0, -1, 10)]
        until_offset = kafka_client.send_offset_request(reqs)[0].offsets[0]

        offset_ranges = [OffsetRange(topic=self.topic, partition=0, fromOffset=0, untilOffset=until_offset)]


        result = pyspark.streaming.kafka.KafkaUtils.createRDD(sc, self.kafka_params, offset_ranges)

        result = result.map(lambda x: self.read_json(x[1]))
        return {'data':result}

    def read_json(self, line):
        parsed = {}
        try:
            parsed = json.loads(line)
        except ValueError:
            print '%s not valid JSON' % line

        return parsed

