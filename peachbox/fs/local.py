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

import os
import shutil

import peachbox.fs
from peachbox.fs.fs import Fs

class Local(Fs):
    """Interface to the local filesystem. Filesystem operations work on <self.dwh_path>/<mart>/."""

    def __init__(self):
        self.dwh_path    = '/tmp'

    def ls(self, mart, path):
        return self.ls_a(self.uri(mart, path))[1]

    def ls_d(self, mart, path):
        return self.ls_a(self.uri(mart, path))[0]

    def ls_a(self, path):
        files = []
        dirs  = []
        for dirname, dirnames, filenames in os.walk(path):
            for filename in filenames:
                files.append(os.path.join(dirname, filename))
            for subdirname in dirnames:
                dirs.append(os.path.join(dirname, subdirname))
        return dirs, files

    def rm_r(self, mart, path):
        if os.path.exists(self.uri(mart,path)):
            shutil.rmtree(self.uri(mart,path))

    def uri(self, mart, filename):
        uri_parts = [self.dwh_path, mart, filename]
        uri       = os.path.join(*uri_parts)
        return uri
    

