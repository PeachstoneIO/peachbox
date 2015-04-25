
class MasterDataSetModel(object):
    def __init__(self):
        self._id     = None
        self._schema = None
        
        #eg. peachbox.models.FileFormat.parquet
        self.output_format = None

    def path(self):
        raise NotImplementedError

    def schema(self):
        raise NotImplementedError

