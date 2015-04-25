import unittest

import peachbox.fs
from peachbox.utils.test_helper import TestHelper
from peachbox.utils.date import Date

class TestLocal(unittest.TestCase):
    def setUp(self):
        self.fs = peachbox.fs.Local()

    def test_ls_a(self):
        mart = TestHelper.random_name()
        TestHelper.mkdir('/tmp/' + mart)
        TestHelper.touch('/tmp/' + mart + '/filename')
        self.assertEqual('/tmp/' + mart + '/filename', self.fs.ls_a('/tmp/' + mart)[1][0])

    def test_ls(self):
        mart = TestHelper.random_name()
        TestHelper.mkdir('/tmp/' + mart)
        TestHelper.touch('/tmp/' + mart + '/filename')
        self.assertEqual('/tmp/' + mart + '/filename', self.fs.ls(mart, '')[0])

    def test_dirs_in_period(self):
        mart = TestHelper.random_name()
        date1 = Date.tz_berlin(2014,4,1)
        date2 = Date.tz_berlin(2014,4,2)
        TestHelper.mkdir('/tmp/' + mart)
        TestHelper.mkdir('/tmp/' + mart + '/' + str(date1.seconds()))

        dirs = self.fs.dirs_of_period(mart, '',  date1.seconds(), date2.seconds())
        self.assertEqual(['/tmp/' + mart + '/' + str(date1.seconds())], dirs)
        
    def test_rm_r(self):
        mart = TestHelper.random_name()
        TestHelper.mkdir('/tmp/' + mart + '/1/123456')
        self.fs.rm_r(mart, '1')
        dir_exists = '/tmp/' + mart + '/1' in self.fs.ls_d(mart, '')
        assert not dir_exists

    def test_url_prefix_default(self):
        self.assertEqual('/tmp/mart/1/2/time', self.fs.uri('mart', '1/2/time'))

