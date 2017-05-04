import subprocess
import os

from . import logit

def get_version():
    codedir = os.path.abspath(os.getcwd())
    version = "unknown"
    try:
        version = subprocess.Popen(["git", "describe", "--tags", "--abbrev=0"], cwd=codedir, stdout=subprocess.PIPE).stdout.read()
    except Exception as e:
        pass
    logit.log("POMS Version: %s" % version)
    return version
