import peachbox.model

class ReviewByUserEdge(peachbox.model.MasterDataSet):
    data_unit_index = 0
    partition_key = 'true_as_of_seconds'
    partition_granularity = 60*60*24*360 
    schema = [{'field':'user_id', 'type':'StringType'},
              {'field':'review_id', 'type':'StringType'}]

class UserProperties(peachbox.model.MasterDataSet):
    data_unit_index = 1
    partition_key = 'true_as_of_seconds'
    partition_granularity = 60*60*24*360 
    schema = [{'field':'user_id', 'type':'StringType'},
              {'field':'profile_name', 'type':'StringType'},
              {'field':'score', 'type':'IntegerType'}]

