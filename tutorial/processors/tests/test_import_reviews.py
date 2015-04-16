import unittest
import processors

class TestImportReviews(unittest.TestCase):
    def test_init(self):
        r = processors.ImportReviews()
        assert r
