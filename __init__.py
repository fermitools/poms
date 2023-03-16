
import os
import sys

os.environ['POMS_DIR'] = __file__[:__file__.rfind('/')]
os.environ["WEB_CONFIG"] = "/home/poms/poms/webservice/poms.ini"
#print "set POMS_DIR to " , os.environ['POMS_DIR']
