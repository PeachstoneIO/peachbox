import peachbox.task
import peachbox
import pyspark.streaming
import pyspark.streaming.kafka

import peachbox.connector.source
import peachbox.connector.sink
import peachbox.pipeline.json_parser

import pipelines.streams
import model.streams

class MovieReviews(peachbox.task.Task):
    def __init__(self):
        self.source = peachbox.connector.source.KafkaJSONStream(topic='movie_reviews', 
                dstream_time_interval=10)
        self.sink   = peachbox.connector.sink.RealTimeView()

    def _execute(self):
        reviews = self.source.stream_rdd()

        json_parser = peachbox.pipeline.JSONParser()
        validator   = peachbox.pipeline.Validator(['user_id', 'time', 'profile_name'])

        chain = peachbox.pipeline.Chain([json_parser, validator, pipelines.streams.ReviewByGender()])
        r = chain.execute(reviews)
        self.sink.absorb([{'data':r, 'model':model.streams.ReviewByGender}])

        self.source.start_stream()

    def tear_down(self):
        peachbox.Spark.Instance().stop()



