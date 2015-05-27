import peachbox
import pyspark.streaming.kafka

class KafkaJSONStream(peachbox.connector.Connector):
    def __init__(self, topic, dstream_time_interval):
        self.topic = topic
        self.dstream_time_interval = dstream_time_interval

    def stream_rdd(self):
        ssc = peachbox.Spark.Instance().streaming_context(self.dstream_time_interval)
        stream = pyspark.streaming.kafka.KafkaUtils.createStream(ssc, 
                'localhost:2181', 
                'KafkaGroup', 
                {self.topic:1})
        values = stream.map(lambda entry: entry[1])
        return values

    def start_stream(self):
        ssc = peachbox.Spark.Instance().streaming_context(self.dstream_time_interval)
        ssc.start()
        ssc.awaitTermination()

    def set_param(self, param):
        pass


# from pyspark import SparkConf
# import pyspark.streaming
# import pyspark.streaming.kafka
# import pyspark_cassandra
# import pyspark_cassandra.streaming
# 
# conf = SparkConf().set("spark.cassandra.connection.host", "localhost")
# #sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
# sc = pyspark.SparkContext(conf=conf)
# 
# ssc = pyspark.streaming.StreamingContext(sc, 5)
# stream = pyspark.streaming.kafka.KafkaUtils.createStream(ssc, 'localhost:2181', 'Group', {'t1':1})
# stream = stream.map(lambda entry: (123, 'fname', 'lname'))
# #stream.pprint()
# 
# stream.saveToCassandra('test', 'users')
# ssc.start()
# ssc.awaitTermination()

