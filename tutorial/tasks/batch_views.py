import peachbox.task
import peachbox.connector.source
import peachbox.connector.sink
import peachbox.pipeline

import pipelines.batch_views
import model.master

from peachbox.pipeline import Chain

class ReviewByGender(peachbox.task.Task):
    def __init__(self):
        self.source = peachbox.connector.source.DWH(model.master.UserProperties)
        self.sink   = peachbox.connector.sink.BatchView()

    def _execute(self):
        df = self.source.emit()['data']


        review_by_gender = pipelines.batch_views.ReviewByGender().execute(df.distinct())

        print review_by_gender.take(20)
        


        #reviews_by_user = Chain([review_by_user_validator, pipelines.importer.ReviewByUser()]).execute(df)


        # Import the user properties 
        #self.sink.absorb({'data':reviews_by_user, 'model':model.master.ReviewByUserEdge}])
                         



