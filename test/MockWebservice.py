import os
import sys
import subprocess
import time

class MockWebservice:
    def __init__(self):
       self.outf = "/tmp/out%d" % os.getpid()
       self.sp = subprocess.Popen('exec %s/test/mock_webservice/mock_webserver.py 2>%s' % (
            os.environ['POMS_DIR'], self.outf) , shell = True) 
       time.sleep(1)
       self.log = open(self.outf,"r")

    def close(self):
       if getattr(self,'sp',None):
           sys.stderr.write("pid is %d\n" % self.sp.pid)
           self.sp.kill()
           self.log.close()
           self.sp = None
           os.unlink(self.outf)
           self.log.close()
   
    def __del__(self):
        self.close()

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
     assert(l[0] == ('127.0.0.1 - - [%s] "GET /foo/bar HTTP/1.0" 200 -\n' % ts))

def testMockWebservice2():
     mw = MockWebservice()
     import urllib
     import urllib2
  
     ts = time.strftime("%d/%b/%Y %H:%M:%S")
     data = {'baz':'bleem'}
     r = urllib2.urlopen("http://127.0.0.1:8888/foo/bar", data=urllib.urlencode(data))
     txt = r.read()
     r.close()
     assert(txt == "Ok.\n")
     l = mw.log.readlines()
     mw.close()
     assert(l[0] == ('127.0.0.1 - - [%s] "GET /foo/bar HTTP/1.0" 200 -\n' % ts))
     assert(l[1] == "post_data = %s" % json.dumps(data))
