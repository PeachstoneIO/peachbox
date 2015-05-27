import unittest
import peachbox.model

class TestView(unittest.TestCase):
    def test_row_raises(self):
        with self.assertRaises(NotImplementedError):
            peachbox.model.View.row(test=123)

