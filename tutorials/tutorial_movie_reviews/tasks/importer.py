# general
import time
# peachbox imports
from peachbox.task import Task
from peachbox.connector import sink, source
from peachbox.pipeline import Chain, Validator
# tutorial
from pipelines.importer import UserReviewEdge, ProductReviewEdge, ReviewProperties
import model.master

class ImportMovieReviews(Task):
    def __init__(self):
        super(ImportMovieReviews, self).__init__()
        self.source = source.KafkaJSON(topic='movie_reviews')
        self.sink   = sink.MasterData()

    def _execute(self):
        input = self.source.emit()['data']

        # Import 'review by user edges'
        user_review_validator = Validator(['time', 'user_id', 'product_id'])
        user_review_chain = Chain([user_review_validator, UserReviewEdge()])
        user_review_edges = user_review_chain.execute(input)

        # Import 'product review edges'
        product_review_validator = Validator(['time', 'user_id', 'product_id'])
        product_review_chain = Chain([product_review_validator, ProductReviewEdge()])
        product_review_edges = product_review_chain.execute(input)


        # Import 'review properties'
        required_fields = ['time', 'user_id', 'product_id', 'helpfulness', 'score', 'summary', 'review']
        review_property_validator = Validator(required_fields)
        review_properties = Chain([review_property_validator, ReviewProperties()]).execute(input)

        self.sink.absorb([{'data':user_review_edges,    'model':model.master.UserReviewEdge},
                          {'data':product_review_edges, 'model':model.master.ProductReviewEdge},
                          {'data':review_properties,    'model':model.master.ReviewProperties}])

        # Payload is sent with 'Finished Event'
        self.payload = {'import_finished':int(time.time()), 'latest_kafka_offset':self.source.latest_offset}



