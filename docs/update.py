#!/usr/bin/env python
#
##################################################
#
# Generate documentation for peachbox
#
##################################################

import fnmatch, os
import sys

def exclude_dirs():
    matches = []
    for root, dirnames, filenames in os.walk('../peachbox'):
      for dirnames in fnmatch.filter(dirnames, 'tests*'):
        matches.append(os.path.join(root, dirnames))
    return ' '.join(matches)

xcute = "rm -fr peachbox.* modules.rst model.rst _build"
os.system(xcute)

python_path = ['$PEACHBOX/tutorial_movie_reviews', '$PYTHONPATH']

os.system("sphinx-apidoc -f -o . ../peachbox " + exclude_dirs())
os.system("PYTHONPATH="+':'.join(python_path)+"; sphinx-apidoc -f -o . ../tutorial_movie_reviews " + exclude_dirs())
os.system("PYTHONPATH="+':'.join(python_path)+"; make html")

