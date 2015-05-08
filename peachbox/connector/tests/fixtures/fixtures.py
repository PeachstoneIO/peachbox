import peachbox.model

class MasterDataSetModel(peachbox.model.MasterDataSet):
    data_unit_index = 0
    partition_key = 'true_as_of_seconds'
    partition_granularity = 10
    output_format = peachbox.model.FileFormat.Parquet

