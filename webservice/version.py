import subprocess
import os

from . import logit


def get_version():
    codedir = os.environ["POMS_DIR"]
    version = "unknown"
    try:
        devnull = open("/dev/null", "w")
        proc = subprocess.Popen(
            ["git", "describe", "--tags", "--abbrev=0"], cwd=codedir, stdout=subprocess.PIPE, stderr=devnull
        )
        version = proc.stdout.read()
        devnull.close()
        proc.wait()
        proc.stdout.close()
        vf = open("%s/.version" % os.environ["POMS_DIR"], "w")
        vf.write(version)
        vf.close()
    except Exception:
        pass
    if version == "unknown":
        try:
            vf = open("%s/.version" % os.environ["POMS_DIR"], "r")
            version = vf.read()
            vf.close()
        except Exception:
            pass

    logit.log("POMS Version: %s" % version)
    return str(version.strip(), "utf-8")
