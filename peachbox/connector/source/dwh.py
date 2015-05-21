import peachbox.connector
import peachbox
import sys

class DWH(peachbox.connector.Connector):
    def __init__(self, model):
        self.model = model
        self.smallest_key = 0
        self.biggest_key  = sys.maxint

    def set_param(self, param):
        if 'smallest_key' in param: self.smallest_key = param['smallest_key']
        if 'biggest_key'  in param: self.biggest_key  = param['biggest_key']

    def emit(self):
        dwh = peachbox.DWH.Instance()
        data = dwh.query_by_key_range(self.model, smallest_key=self.smallest_key, biggest_key=self.biggest_key)
        return {'data':data}



