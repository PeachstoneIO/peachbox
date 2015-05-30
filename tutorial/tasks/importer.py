import peachbox.task
import peachbox.connector.sink
import peachbox.connector.source
import peachbox.pipeline

import pipelines.importer
import model.master
import time

from peachbox.pipeline import Chain

class ImportMovieReviews(peachbox.task.Task):
    def __init__(self):
        super(ImportMovieReviews, self).__init__()
        self.source = peachbox.connector.source.KafkaJSON(topic='movie_reviews')
        self.sink   = peachbox.connector.sink.MasterData()

    def _execute(self):
        input = self.source.emit()['data']

        # Import 'review by user edges'
        user_review_validator = peachbox.pipeline.Validator(['time', 'user_id', 'product_id'])
        user_review_chain = Chain([user_review_validator, pipelines.importer.UserReviewEdge()])
        user_review_edges = user_review_chain.execute(input)

        # Import 'product review edges'
        product_review_validator = peachbox.pipeline.Validator(['time', 'user_id', 'product_id'])
        product_review_chain = Chain([product_review_validator, pipelines.importer.ProductReviewEdge()])
        product_review_edges = product_review_chain.execute(input)


        # Import 'review properties'
        required_fields = ['time', 'user_id', 'product_id', 'helpfulness', 'score', 'summary', 'review']
        review_property_validator = peachbox.pipeline.Validator(required_fields)
        review_properties = Chain([review_property_validator, pipelines.importer.ReviewProperties()]).execute(input)

        self.sink.absorb([{'data':user_review_edges,    'model':model.master.UserReviewEdge},
                          {'data':product_review_edges, 'model':model.master.ProductReviewEdge},
                          {'data':review_properties,    'model':model.master.ReviewProperties}])

        # Payload is sent with 'Finished Event'
        self.payload = {'import_finished':int(time.time()), 'latest_kafka_offset':self.source.latest_offset}



