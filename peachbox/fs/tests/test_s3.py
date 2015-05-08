import unittest
import peachbox.fs

class TestS3(unittest.TestCase):
    def setUp(self):
        self.dfs = peachbox.fs.S3()

    def test_init(self):
        s3 = peachbox.fs.S3()
        assert s3

    def test_uri(self):
        self.assertEqual('s3n://mart/filename', self.dfs.uri('mart', 'filename'))
        self.assertEqual('s3n://mart/filename/f2', self.dfs.uri('mart', '/filename/f2'))
