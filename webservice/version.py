import subprocess
import os

from . import logit

def get_version():
    codedir = os.path.abspath(os.getcwd())
    version = "unknown"
    try:
        devnull = open("/dev/null","w")
        version = subprocess.Popen(["git", "describe", "--campaigns", "--abbrev=0"], cwd=codedir, stdout=subprocess.PIPE, stderr = devnull).stdout.read()
        devnull.close()
        vf = open("%s/.version" % os.environ["POMS_DIR"] ,"w")
        vf.write(version)
        vf.close()
    except Exception as  e:
        pass
    if version == "unknown":
       try:
           vf = open("%s/.version" % os.environ["POMS_DIR"] ,"r")
           version = vf.read()
           vf.close()
       except Exception as e:
           pass

    logit.log("POMS Version: %s" % version)
    return str(version.strip(),'utf-8')
