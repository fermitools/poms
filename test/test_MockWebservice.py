import os
import sys
import subprocess
import time
from MockWebservice import MockWebservice

def testMockWebservice1():
     mw = MockWebservice()
     import urllib2
     ts = time.strftime("%d/%b/%Y %H:%M:%S")
     r = urllib2.urlopen("http://127.0.0.1:8888/foo/bar")
     txt = r.read()
     r.close()
     assert(txt == "Ok.\n")
     l = mw.log.readlines()
     mw.close()
     assert(l[0] == ('127.0.0.1 - - [%s] "GET /foo/bar HTTP/1.1" 200 -\n' % ts))

def testMockWebservice2():
     mw = MockWebservice()
     import urllib
     import urllib2
     import json
  
     ts = time.strftime("%d/%b/%Y %H:%M:%S")
     data = {'baz':'bleem'}
     r = urllib2.urlopen("http://127.0.0.1:8888/foo/bar", data=urllib.urlencode(data))
     txt = r.read()
     r.close()
     assert(txt == "Ok.\n")
     l = mw.log.readlines()
     mw.close()
     #print l
     assert(l[0] == "post_data = %s\n" % json.dumps(data))
     assert(l[1] == ('127.0.0.1 - - [%s] "POST /foo/bar HTTP/1.1" 200 -\n' % ts))

def testMockWebservice3():
     mw = MockWebservice()
     import urllib2
     ts = time.strftime("%d/%b/%Y %H:%M:%S")
     r = urllib2.urlopen("http://127.0.0.1:8888/poms/active_jobs")
     txt = r.read()
     r.close()
     assert(txt == "[]\n")
     l = mw.log.readlines()
     mw.close()
     assert(l[0] == ('127.0.0.1 - - [%s] "GET /poms/active_jobs HTTP/1.1" 200 -\n' % ts))
