
import os
import sys

if "POMS_DIR" not in os.environ:
    os.environ['POMS_DIR'] = __file__[:__file__.rfind('/')]
if "WEB_CONFIG" not in os.environ:
    os.environ['WEB_CONFIG'] = "~/private/poms/config/dev.poms.toml"
#print "set POMS_DIR to " , os.environ['POMS_DIR']
