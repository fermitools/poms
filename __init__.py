
import os
import sys

os.environ['POMS_DIR'] = __file__[:__file__.rfind('/')]
if not os.environ.get("WEB_CONFIG", None):
    os.environ["WEB_CONFIG"] = "/run/secrets/poms.ini"
#print "set POMS_DIR to " , os.environ['POMS_DIR']
