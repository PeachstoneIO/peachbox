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

class ReviewsByGender(peachbox.model.BatchView):
    mart = 'batch_views'
    keys = ['true_as_of_seconds']
    schema = [{'field':'true_as_of_seconds', 'type':'IntegerType'},
              {'field':'gender', 'type':'StringType'},
              {'field':'review_count', 'type':'IntegerType'}]
