#!/usr/bin/env python
#
##################################################
#
# Generate documentation for peachbox
#
##################################################

import fnmatch, os

def test_dirs():
    matches = []
    for root, dirnames, filenames in os.walk('../peachbox'):
      for dirnames in fnmatch.filter(dirnames, 'tests*'):
        matches.append(os.path.join(root, dirnames))
    return ' '.join(matches)

xcute = "rm -fr peachbox.* modules.rst _build"
print xcute
os.system(xcute)

exclude_paths = test_dirs()
os.system("sphinx-apidoc -o . ../peachbox " + exclude_paths)
os.system("make html")

