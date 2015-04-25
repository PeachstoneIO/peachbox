import unittest

import peachbox.fs

class TestFs(unittest.TestCase):
    class MockFs(peachbox.fs.Fs):
        def ls_d(self, mart, path=''):
            return ['/2']

    def test_dirs_of_period(self):
        fs = TestFs.MockFs()
        self.assertEqual(['/2'], fs.dirs_of_period('mart', 'path', 1, 3))

