import unittest
import peachbox.connector.sink

class TestBatchView(unittest.TestCase):
    def test_absorb(self):
        b = peachbox.connector.sink.BatchView()
        b.absorb({})
