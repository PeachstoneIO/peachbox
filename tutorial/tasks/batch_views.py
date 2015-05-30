import peachbox.task
import peachbox.connector.source
import peachbox.connector.sink
import peachbox.pipeline

import pipelines.batch_views
import model.master
import model.batch_views

from peachbox.pipeline import Chain

class Reviews(peachbox.task.Task):
    def __init__(self):
        super(Reviews, self).__init__()

        self.sources = [peachbox.connector.source.DWH(model.master.ReviewProperties), 
                        peachbox.connector.source.DWH(model.master.ProductReviewEdge)]

        self.sink   = peachbox.connector.sink.BatchView()

    def _execute(self):
        properties = self.sources[0].emit()['data']
        edges      = self.sources[1].emit()['data']

        if not properties: return

        input = properties.join(edges, properties.review_id==edges.review_id)\
                .select(properties.true_as_of_seconds, properties.review_id, edges.product_id, 
                        properties.score)

        reviews = pipelines.batch_views.Reviews().execute(input)

        self.sink.absorb([{'data':reviews, 'model':model.batch_views.Reviews}])

        #reviews_by_user = Chain([review_by_user_validator, pipelines.importer.ReviewByUser()]).execute(df)
        # Import the user properties 
        #self.sink.absorb({'data':reviews_by_user, 'model':model.master.ReviewByUserEdge}])

    def tear_down(self):
        peachbox.Spark.Instance().stop()
                         



