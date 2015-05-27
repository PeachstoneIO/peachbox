import peachbox.model

class UserReviewEdge(peachbox.model.MasterDataSet):
    data_unit_index = 0
    partition_key = 'true_as_of_seconds'
    partition_granularity = 60*60*24*360 
    schema = [{'field':'user_id', 'type':'StringType'},
              {'field':'review_id', 'type':'StringType'}]

class ProductReviewEdge(peachbox.model.MasterDataSet):
    data_unit_index = 1
    partition_key = 'true_as_of_seconds'
    partition_granularity = 60*60*24*360 
    schema = [{'field':'review_id', 'type':'StringType'},
              {'field':'product_id', 'type':'StringType'}]

class ReviewProperties(peachbox.model.MasterDataSet):
    data_unit_index = 2
    partition_key = 'true_as_of_seconds'
    partition_granularity = 60*60*24*360 
    schema = [{'field':'review_id', 'type':'StringType'},
              {'field':'helpful', 'type':'IntegerType'},
              {'field':'nothelpful', 'type':'IntegerType'},
              {'field':'score', 'type':'IntegerType'},
              {'field':'summary', 'type':'StringType'},
              {'field':'text', 'type':'StringType'}]

# Implement User properties

