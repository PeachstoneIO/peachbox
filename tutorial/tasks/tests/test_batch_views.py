import unittest
from tasks.batch_views import Reviews
import os.path
import os
import peachbox

class TestReviews(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dwh = peachbox.DWH.Instance()
        dwh.fs = peachbox.fs.Local()
        dwh.fs.dwh_path = os.path.join(os.getcwd(), 'dwh')
        print dwh.fs.dwh_path

    def test_execution(self):
        t = Reviews()
        t.execute()
        t.process.join()
