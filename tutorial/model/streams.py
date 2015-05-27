import peachbox.model

class ReviewByGender(peachbox.model.RealTimeView):
    mart = 'MovieReviews'
    keys  = ['true_as_of_seconds']

    schema = [{'field':'true_as_of_seconds', 'type':'IntegerType'},
              {'field':'gender', 'type':'StringType'}]


