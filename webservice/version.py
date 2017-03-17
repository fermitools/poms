import subprocess
import os

def get_version(log):
    codedir = os.path.abspath(os.getcwd())
    version = "unknown"
    try:
        version = subprocess.Popen(["git", "describe", "--tags", "--abbrev=0"], cwd=codedir, stdout=subprocess.PIPE).stdout.read()
    except Exception, e:
        pass
    log("POMS Version: %s" % version)
    return version
