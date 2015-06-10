#!/usr/bin/env python
import pyspark.streaming.kafka
from pyspark.streaming.kafka import OffsetRange
import pyspark.streaming
import pyspark

sc = pyspark.SparkContext(appName="kafka_testing", master='local[*]')

#dstream_time_interval = 5
#ssc = pyspark.streaming.StreamingContext(sc,dstream_time_interval)

#@staticmethod
#def createRDD(sc, kafkaParams, offsetRanges, leaders={},
             #keyDecoder=utf8_decoder, valueDecoder=utf8_decoder):

kafka_params = {"zookeeper.connect": "localhost:2182",
                "metadata.broker.list": "localhost:9092",
		"group.id": "TutorialGroup1",
		"zookeeper.connection.timeout.ms": "10000"}

tutorial1 = OffsetRange(topic='movie_reviews', partition=0, fromOffset=0, untilOffset=2)

offset_ranges = [tutorial1]

kafka = pyspark.streaming.kafka.KafkaUtils.createRDD(sc, kafka_params,offset_ranges)
kafka = kafka.map(lambda x: x[1])
print kafka.collect()
#kafka = pyspark.streaming.kafka.KafkaUtils.createStream(ssc, 'localhost:2181', 'TutorialGroup1', {'t1':1})
#kafka.pprint()

#ssc.start()
#ssc.awaitTermination()


