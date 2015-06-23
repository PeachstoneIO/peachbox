import peachbox.task
import peachbox
import pyspark.streaming
import pyspark.streaming.kafka

import peachbox.connector.source
import peachbox.connector.sink
import peachbox.pipeline.json_parser

import pipelines.streams
import model.streams

class Reviews(peachbox.task.Task):
    def __init__(self):
        super(Reviews, self).__init__()
        self.source = peachbox.connector.source.KafkaJSONStream(topic='movie_reviews', 
                dstream_time_interval=1)
        self.sink   = peachbox.connector.sink.RealTimeView()

    def _execute(self):
        reviews = self.source.stream_rdd()

        json_parser = peachbox.pipeline.JSONParser()
        validator   = peachbox.pipeline.Validator(['user_id', 'product_id', 'time', 'score'])

        chain = peachbox.pipeline.Chain([json_parser, validator, pipelines.streams.Reviews()])
        r = chain.execute(reviews)

        self.sink.absorb([{'data':r, 'model':model.streams.Reviews}])

        self.source.start_stream()

    def tear_down(self):
        peachbox.Spark.Instance().stop()



