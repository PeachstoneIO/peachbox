import unittest
import peachbox.fs

class TestAmazonDfs(unittest.TestCase):
    def setUp(self):
        self.dfs = peachbox.fs.AmazonDfs()

    def test_init(self):
        s3 = peachbox.fs.AmazonDfs()
        assert s3

    def test_uri(self):
        self.assertEqual('s3n://mart/filename', self.dfs.uri('mart', 'filename'))
