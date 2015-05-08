# Copyright 2015 Philipp Pahl, Sven Schubert
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import peachbox.fs
from peachbox.fs import Fs

import boto
import os.path
from boto.s3.key import Key

class S3(Fs):
    """Interface to Amazon S3. Operations work on s3n://<mart>/, where <mart> is a bucket on S3."""
    def __init__(self):
        self.uri_scheme = 's3n://'
        self._connection = None

    def connection(self):
        if not self._connection:
            self._connection = boto.connect_s3()
        return self._connection

    def bucket(self, mart):
        return self.connection().get_bucket(mart)

    def ls(self, mart, path=''):
        return [self.uri(mart, file.name) for file in self.bucket(mart).list(path)]

    def ls_d(self, mart, path=''):
        return [self.uri(mart, file.name.strip('/')) for file in self.bucket(mart).list(path+'/', '/')]

    def rm_r(self, mart, path):
        if mart=='master' or path=="":
            raise "S3.rm_r(): Can not delete data in master or whole bucket!"
            return
        for key in self.bucket(mart).list(prefix=path): 
            key.delete() 

    def path_exists(self, mart, path):
        keys = self.ls(mart)
        return any( (self.uri(mart, path) in key ) for key in keys)

    def uri(self, mart, filename):
        f = filename.strip('/')
        uri_parts = [self.uri_scheme, mart, f]
        uri       = os.path.join(*uri_parts)
        return uri

