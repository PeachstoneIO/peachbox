import peachbox.fs
from peachbox.fs import FileSystem
#import peachbox

import boto
import os.path
from boto.s3.key import Key

class AmazonDfs(FileSystem):
    def __init__(self):
        self.connection = boto.connect_s3()
        self.uri_scheme = 's3n://'

    def bucket(self, mart):
        return self.connection.get_bucket(mart)

    def ls(self, mart, path=''):
        return [self.uri(mart, file.name) for file in self.bucket(mart).list(path)]

    def ls_d(self, mart, path=''):
        return [self.uri(mart, file.name.strip('/')) for file in self.bucket(mart).list(path+'/', '/')]

    def rm_r(self, mart, path):
        if mart=='master' or path=="":
            raise "AmazonDfs.rm_r(): Can not delete data in master or whole bucket!"
            return
        for key in self.bucket(mart).list(prefix=path): 
            key.delete() 

    def uri(self, mart, filename):
        uri_parts = [self.uri_scheme, mart, filename]
        uri       = os.path.join(*uri_parts)
        return uri

