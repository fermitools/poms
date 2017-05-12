import os
import sys
import subprocess
import time
import poms

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
   

