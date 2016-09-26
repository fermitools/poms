import subprocess
import os

def get_version():
    codedir = os.path.abspath(os.getcwd()) 
    try:
        version = subprocess.Popen(["git", "describe", "--tags", "--abbrev=0"], cwd=codedir, stdout=subprocess.PIPE).stdout.read()
        print "version is:", version
    except Exception, e:
        print 'oops: %s' % e
        version = "unknown"
    return version
