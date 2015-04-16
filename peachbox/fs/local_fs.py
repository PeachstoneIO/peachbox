import os
import shutil

import peachbox.fs
from peachbox.fs.file_system import FileSystem

class LocalFs(FileSystem):
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
    

