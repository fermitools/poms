
import os
import sys

print "importing in " , __file__ , "..."

os.environ['POMS_DIR'] = __file__[:__file__.rfind('/')]

#print "set POMS_DIR to " , os.environ['POMS_DIR']
