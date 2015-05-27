import unittest
import peachbox.connector.sink
from peachbox.model import BatchView

class BatchModel(BatchView):
    mart = 'test_mart'
    key  = 'f1'

    schema = [{'field':'f1', 'type':'IntegerType'},
              {'field':'f2', 'type':'StringType'}]

class TestBatchView(unittest.TestCase):
    def test_absorb(self):
        input = peachbox.Spark.Instance().context().parallelize([BatchModel.row(f1=1, f2='value')])
        b = peachbox.connector.sink.BatchView()
        b.absorb([{'data':input, 'model':BatchModel}])
