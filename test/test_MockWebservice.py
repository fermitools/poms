import os
import sys
import subprocess
import time
from MockWebservice import MockWebservice
import requests

rs = requests.Session()

def testMockWebservice1():
     try:
         mw = MockWebservice()
         r = rs.get("http://127.0.0.1:8888/foo/bar")
         ts = time.strftime("%d/%b/%Y %H:%M:%S")
         txt = r.text
         r.close()
         assert(txt == "Ok.\n")
         l = mw.log.readlines()
         print( "got lines: ", l)
     finally:
         mw.close()
     assert(l[0] == ('127.0.0.1 - - [%s] "GET /foo/bar HTTP/1.1" 200 -\n' % ts))

def testMockWebservice2():
     try:
         mw = MockWebservice()
         import json
      
         ts = time.strftime("%d/%b/%Y %H:%M:%S")
         data = {'baz':'bleem'}
         r = rs.post("http://127.0.0.1:8888/foo/bar", data = data)
         txt = r.text
         r.close()
         assert(txt == "Ok.\n")
         l = mw.log.readlines()
         print(l)
     finally:
         mw.close()
     assert(l[0] == "post_data = %s\n" % json.dumps(data))
     assert(l[1] == ('127.0.0.1 - - [%s] "POST /foo/bar HTTP/1.1" 200 -\n' % ts))

def testMockWebservice3():
     try:
         mw = MockWebservice()
         ts = time.strftime("%d/%b/%Y %H:%M:%S")
         r = rs.get("http://127.0.0.1:8888/poms/active_jobs")
         txt = r.text
         r.close()
         assert(txt == "[]\n")
         l = mw.log.readlines()
     finally:
         mw.close()
     assert(l[0] == ('127.0.0.1 - - [%s] "GET /poms/active_jobs HTTP/1.1" 200 -\n' % ts))
