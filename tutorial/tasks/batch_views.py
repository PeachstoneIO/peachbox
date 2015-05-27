import peachbox.task
import peachbox.connector.source
import peachbox.connector.sink
import peachbox.pipeline

import pipelines.batch_views
import model.master
import model.batch_views

from peachbox.pipeline import Chain

class ReviewsByGender(peachbox.task.Task):
    def __init__(self):
        self.source = peachbox.connector.source.DWH(model.master.UserProperties)
        self.sink   = peachbox.connector.sink.BatchView()

    def _execute(self):
        df = self.source.emit()['data']
        reviews_by_gender = pipelines.batch_views.ReviewsByGender().execute(df.distinct())

        self.sink.absorb([{'data':reviews_by_gender, 'model':model.batch_views.ReviewsByGender}])

        #reviews_by_user = Chain([review_by_user_validator, pipelines.importer.ReviewByUser()]).execute(df)
        # Import the user properties 
        #self.sink.absorb({'data':reviews_by_user, 'model':model.master.ReviewByUserEdge}])

    def tear_down(self):
        peachbox.Spark.Instance().stop()
                         



