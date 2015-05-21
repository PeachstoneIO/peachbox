import peachbox.model

class ReviewByUserEdge(peachbox.model.RealTimeView):
    mart = 'MovieReviews'
    key = 'true_as_of_seconds'
    schema = [{'field':'true_as_of_seconds', 'type':'IntegerType'},
              {'field':'user_id', 'type':'StringType'},
              {'field':'review_id', 'type':'StringType'}]


