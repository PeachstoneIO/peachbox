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

class Fs(object):
    """Base class of the file system."""

    def ls(self, mart,  path=''):
        raise NotImplementedError

    def ls_d(self, mart, path=''):
        raise NotImplementedError

    def test_path_exists(self):
        raise NotImplementedError

    def dirs_of_period(self, mart, path, from_utime, before_utime):
        """Lists directories of a given time period."""

        dirs       = self.ls_d(mart, path)
        valid_dirs = []
        for dir in dirs:
            utime = self.dir_utime(dir)
            if utime >= from_utime and utime < before_utime:
                valid_dirs.append(dir)
        return valid_dirs

    def dir_utime(self, path):
        """Unix time of a directory. Directory name must be an integer."""

        basename = os.path.basename(path)
        try:
            utime = int(basename)
            return utime
        except ValueError:
            raise 'FileSystem.dir_utime(): Name of directory does not contain a number'

