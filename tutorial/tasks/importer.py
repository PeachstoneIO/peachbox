import peachbox.task
import peachbox.connector.sink
import peachbox.connector.source
import peachbox.pipeline

import pipelines.importer
import model.master

from peachbox.pipeline import Chain

class ImportMovieReviews(peachbox.task.Task):
    def __init__(self):
        self.source = peachbox.connector.source.KafkaJSON('t1')
        self.sink   = peachbox.connector.sink.MasterData()

    def _execute(self):
        df = self.source.emit()['data']

        # Available fields of the input data are: 'user_id', 'product_id', 'review', 'summary',
        # 'profile_name', 'helpfulness', 'time', 'score'

        review_by_user_validator = peachbox.pipeline.Validator(['user_id', 'product_id', 'time'])
        reviews_by_user = Chain([review_by_user_validator, pipelines.importer.ReviewByUser()]).execute(df)


        # Import the user properties 
        user_properties_validator = peachbox.pipeline.Validator(['user_id', 'time', 'profile_name', 'score'])
        properties_chain = Chain([user_properties_validator, pipelines.importer.UserProperties()])
        user_properties = properties_chain.execute(df) 
        
        self.sink.absorb([{'data':reviews_by_user, 'model':model.master.ReviewByUserEdge},
                          {'data':user_properties, 'model':model.master.UserProperties}])



