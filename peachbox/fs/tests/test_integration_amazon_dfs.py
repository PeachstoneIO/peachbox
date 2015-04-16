import unittest
import boto
from boto.s3.key import Key
from boto.s3.bucket import Bucket

import peachbox.fs
import peachbox.utils
#from lib.date import Date

class TestIntegrationAmazonDfs(unittest.TestCase):
    mart = 'integrationtestppahl'

    @classmethod
    def setUpClass(cls):
        connection  = boto.connect_s3()
        connection.create_bucket(TestIntegrationAmazonDfs.mart)

    @classmethod
    def tearDownClass(cls):
        connection = boto.connect_s3()
        bucket = connection.get_bucket(TestIntegrationAmazonDfs.mart)
        for key in bucket.list():
            key.delete()
        connection.delete_bucket(TestIntegrationAmazonDfs.mart)

    def setUp(self):
        self.fs         = peachbox.fs.AmazonDfs()
        self.connection = boto.connect_s3()
        self.bucket     = self.connection.get_bucket(TestIntegrationAmazonDfs.mart)
        self.key        = Key(self.bucket)
        self.mart       = TestIntegrationAmazonDfs.mart

    def test_ls(self):
        self.key.key = 'test_ls/foo'
        self.key.set_contents_from_string('contents')
        self.assertEqual(['s3n://' + self.mart + '/test_ls/foo'], self.fs.ls(self.mart, 'test_ls'))

    def test_bucket_retrieval(self):
        mart = TestIntegrationAmazonDfs.mart
        self.assertEqual(boto.s3.bucket.Bucket, type(self.fs.bucket(mart)))
        self.assertEqual(self.bucket.name, self.fs.bucket(mart).name)

    def test_rm_r(self):
        self.key.key = 'aggregation/3/part1'
        self.key.set_contents_from_string('part1')
        self.key.key = 'aggregation/3/part2'
        self.key.set_contents_from_string('part2')

        mart = TestIntegrationAmazonDfs.mart
        files = self.fs.ls(mart, 'aggregation')
        uri_prefix = 's3n://' + TestIntegrationAmazonDfs.mart + '/'
        self.assertEqual([uri_prefix + 'aggregation/3/part1',
                          uri_prefix + 'aggregation/3/part2'], files)

        self.fs.rm_r(mart, 'aggregation/3')
        files = self.fs.ls(mart, 'aggregation')
        self.assertEqual([], files)

    def test_ls_d(self):
        self.key.key = 'dir_test/1/file1'
        self.key.set_contents_from_string('content')

        self.key.key = 'dir_test/1/file2'
        self.key.set_contents_from_string('content')

        self.key.key = 'dir_test/2/file1'
        self.key.set_contents_from_string('content')

        self.key.key = 'dir_test/2/3/file1'
        self.key.set_contents_from_string('content')

        uri_prefix = 's3n://' + TestIntegrationAmazonDfs.mart + '/'
        self.assertEqual([uri_prefix+'dir_test/1', uri_prefix+'dir_test/2'], 
                self.fs.ls_d(self.mart, 'dir_test'))

    def test_dirs_of_period(self):
        date1 = peachbox.utils.Date.tz_berlin(2014,4,1)
        date2 = peachbox.utils.Date.tz_berlin(2014,4,2)
        temp_dir = 'period_dir_test'
        self.key.key = temp_dir + '/' + str(date1.seconds()) + '/file.gz'
        self.key.set_contents_from_string('content')

        dirs = self.fs.dirs_of_period(self.mart, temp_dir, date1.seconds(), date2.seconds())
        uri_prefix = 's3n://' + TestIntegrationAmazonDfs.mart + '/'
        self.assertEqual([uri_prefix + temp_dir + '/' + str(date1.seconds())], dirs)




