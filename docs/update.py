import fnmatch
import os

def test_dirs():
    matches = []
    for root, dirnames, filenames in os.walk('../peachbox'):
      for dirnames in fnmatch.filter(dirnames, 'tests*'):
        matches.append(os.path.join(root, dirnames))
    return ' '.join(matches)

print "rm -fr peachbox.* modules.rst _build"
os.system("rm -fr peachbox.* modules.rst _build")

exclude_paths = test_dirs()
os.system("sphinx-apidoc -o . ../peachbox " + exclude_paths)

os.system("make html")

