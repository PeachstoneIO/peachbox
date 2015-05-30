#import peachbox.model
#
#class ReviewByGender(object):
#
#    @classmethod
#    def hbase_row(cls, day, gender, review_count):
#        key = gender + "{0:010d}".format(day)
#        return (key, [key, 'reviews', 'count', review_count])
#    

import peachbox.model

class Reviews(peachbox.model.BatchView):
    mart = 'batch_views'
    keys = ['true_as_of_seconds', 'review_id', 'product_id', 'score' ]
    schema = [{'field':'true_as_of_seconds', 'type':'IntegerType'},
              {'field':'review_count', 'type':'IntegerType'},
              {'field':'review_id', 'type':'StringType'},
              {'field':'product_id', 'type':'StringType'},
              {'field':'score', 'type':'IntegerType'}]
