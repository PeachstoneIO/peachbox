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
docdirs = [ 'peachbox', 'tutorials' ] 

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
    xphinx = "sphinx-apidoc -f -T -o . "+fdir+" "+exclude_dirs(fdir)
    xpath  = "export PYTHONPATH="+fdir+":$PYTHONPATH"
    xcute  = xpath+";"+xphinx
    print "\n[update.py] Generating api-doc for directory "+fdir
    apidir = "api"
    xcute  = "cd "+apidir+"; "+xcute
    os.system(xcute)

def askuser():
    """Request user input"""
    # raw_input returns the empty string for "enter"
    yes = set(['yes','y', 'ye', ''])
    no = set(['no','n'])

    choice = raw_input().lower()
    if choice in yes:
        return True
    elif choice in no:
        return False
    else:
        sys.stdout.write("Please respond with 'yes' or 'no'. Return false.\n")
        return False

#----------------------------------------------------------#
# --- generate doc
#----------------------------------------------------------#
# --- clean up
#clearall()

# --- api doc (each entry in docdirs)
print "Do you want to (re-)build the API doc? [y/n]: "
if askuser():
    if not os.path.exists('api'):
        os.makedirs('api')
    mymod = 'api/mymodules.txt'
    mf = open(mymod, 'w+').close()
    for ddir in docdirs:
        cleardoc(ddir)
        fdir=os.path.expandvars("$PEACHBOX/"+ddir)
        makeapidoc(fdir)
        # add module to api/mymodules.txt
        with open(mymod, "a") as myfile:
            myfile.write("   api/"+ddir+"\n")
            
# --- user manual
# todo


# --- html documentation
os.system("make html")
#os.system("PYTHONPATH="+':'.join(docdirs)+"; make html")

