import peachbox.model

class Reviews(peachbox.model.RealTimeView):
    mart = 'real_time_views'
    keys  = ['true_as_of_seconds', 'review_id', 'product_id', 'score']

    schema = [{'field':'true_as_of_seconds', 'type':'IntegerType'},
              {'field':'review_id', 'type':'StringType'}, 
              {'field':'product_id', 'type':'StringType'},
              {'field':'score', 'type':'IntegerType'}]


