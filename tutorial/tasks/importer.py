import peachbox.task
import peachbox.connector.sink
import peachbox.connector.source
import peachbox.pipeline

import pipelines.importer
import model.master

class ImportMovieReviews(peachbox.task.Task):
    def __init__(self):
        self.source = peachbox.connector.source.KafkaJSON('t1')
        self.sink   = peachbox.connector.sink.MasterData()

    def _execute(self):
        df = self.source.emit()['data']

        validator = peachbox.pipeline.Validator(['user_id', 'product_id', 'review', 'summary', 'profile_name', 
                                                 'helpfulness', 'time', 'score'])
        chain = peachbox.pipeline.Chain([validator, pipelines.importer.ReviewByUser()])
        reviews_by_user = chain.execute(df)

#        product_review  = pipelines.import.ProductReview.transform(df)
#        self.sink.absorb([{'data':review_by_user, 'model':model.master.ReviewByUser},
#                          {'data':product_review, 'model':model.master.ProductReview])
        self.sink.absorb({'data':reviews_by_user, 'model':model.master.ReviewByUserEdge})

