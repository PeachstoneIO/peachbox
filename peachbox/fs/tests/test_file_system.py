import unittest

import peachbox.fs

class TestFileSystem(unittest.TestCase):
    class MockFs(peachbox.fs.FileSystem):
        def ls_d(self, mart, path=''):
            return ['/2']

    def test_dirs_of_period(self):
        fs = TestFileSystem.MockFs()
        self.assertEqual(['/2'], fs.dirs_of_period('mart', 'path', 1, 3))

