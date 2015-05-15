import peachbox.model

class ReviewByUserEdge(peachbox.model.MasterDataSet):
    data_unit_index = 0
    partition_key = 'true_as_of_seconds'
    partition_granularity = 60*60*24*7 
    schema = [{'field':'user_id', 'type':'StringType'},
              {'field':'review_id', 'type':'StringType'}]

