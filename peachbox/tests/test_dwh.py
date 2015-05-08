import unittest
import peachbox.dwh
import peachbox.fs

class TestDWH(unittest.TestCase):
    def test_init(self):
        with peachbox.DWH.Instance() as dwh:
            self.assertIsInstance(dwh, peachbox.dwh.DWH)


