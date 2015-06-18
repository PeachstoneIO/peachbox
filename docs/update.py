#!/usr/bin/env python
#
##################################################
#
# Generate documentation for peachbox
#
##################################################

import fnmatch, os
import sys


#----------------------------------------------------------#
# --- User input
#----------------------------------------------------------#
#docdirs = [ 'peachbox', 'tutorial_movie_reviews', 'tutorial' ] 
docdirs = [ 'peachbox' ] 
#docdirs = []

#----------------------------------------------------------#
# --- useful functions
#----------------------------------------------------------#
def exclude_dirs(fdir):
    """ Collect directories and filenames ('tags'), which are not considered for documentation. """
    tags = ['tests*']
    matches = []
    for root, dirnames, filenames in os.walk(fdir):
        for tag in tags:
            for dirnames in fnmatch.filter(dirnames, tag):
                matches.append(os.path.join(root, dirnames))
    return ' '.join(matches)


def cleardoc(ddir):
    """remove doc-files generated for a single directory"""
    xcute = "rm -fr "+ddir+".*"
    os.system(xcute)
    
def clearall():
    """Remove all files generated for docdirs, and html documentation """
    for ddir in docdirs:
        cleardoc(ddir)
    xcute = "rm -fr modules.rst model.rst _build"
    os.system(xcute)


def makeapidoc(fdir):
    """ Make documentation for API, i.e. for all directories specified in docdirs"""
    xphinx = "sphinx-apidoc -f -o . "+fdir+" "+exclude_dirs(fdir)
    xpath  = "export PYTHONPATH="+fdir+":$PYTHONPATH"
    xcute  = xpath+";"+xphinx
    print "\n[update.py] Generating api-doc for directory "+fdir
    os.system(xcute)


#----------------------------------------------------------#
# --- generate doc
#----------------------------------------------------------#
# --- clean up
#clearall()

# --- api doc (each entry in docdirs)
for ddir in docdirs:
    cleardoc(ddir)
    ddir=os.path.expandvars("$PEACHBOX/"+ddir)
    makeapidoc(ddir)

# --- user manual
# todo


# --- html documentation
os.system("make html")
#os.system("PYTHONPATH="+':'.join(docdirs)+"; make html")

