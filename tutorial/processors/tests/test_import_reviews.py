import unittest
import processors

class TestImportReviews(unittest.TestCase):
    def test_init(self):
        r = processors.ImportReviews()
        assert r

    def test_run(self):
        r = processors.ImportReviews()
        r.datafile = "movies.json"
        r.run();

