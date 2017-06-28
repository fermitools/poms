#!/usr/bin/env python

import sys
import os
from os import system as os_system
import threading
from poms.webservice.logit import log, logstartstop
import requests
import re

class jobsub_fetcher():

    def __init__(self, cert, key):
         self.sess = requests.Session()
         self.cert = cert
         self.key = key

    def flush(self):
         pass

    @logstartstop
    def fetch(self, jobsubjobid, group, role, force_reload = False, user = None):
         pass

    @logstartstop
    def index(self, jobsubjobid, group,  role = "Production", force_reload = False, retries = 3):

        if retries == -1:
            return []

        if group == "samdev": group = "fermilab"

        if role == "Production":
            user = "%spro" % group
        else:
            user = os.environ["USER"]     # XXX

        fifebatch = jobsubjobid[jobsubjobid.find("@")+1:]

        url = "https://%s:8443/jobsub/acctgroups/%s/sandboxes/%s/%s/" % ( fifebatch, group, user, jobsubjobid) 

        print( "trying url:", url)

        r = self.sess.get(url, cert=(self.cert,self.key),  verify=False, headers={"Accept":"text/html"})
        print ("headers:", r.request.headers)
        print ("headers:", r.headers)
        sys.stdout.flush()
        res = []
        for line in r.text.split('\n'):
            print("got line: " , line)
            # strip tags...
            line = re.sub('<[^>]*>','', line)
            fields = line.strip().split()
            if len(fields):
                fname = fields.pop(0)
                fields.append(fname)
                res.append(fields)
        r.close()

        return res

    @logstartstop
    def contents(self, filename, jobsubjobid, group,  role = "Production", retries = 3):

        if retries == -1:
            return []

        if group == "samdev": group = "fermilab"

        if role == "Production":
            user = "%spro" % group
        else:
            user = os.environ["USER"]    # XXX

        fifebatch = jobsubjobid[jobsubjobid.find("@")+1:]

        url = "https://%s:8443/jobsub/acctgroups/%s/sandboxes/%s/%s/%s/" % (fifebatch, group, user, jobsubjobid, filename) 
        print( "trying url:", url)

        r = self.sess.get(url, cert=(self.cert,self.key), stream=True, verify=False, headers={"Accept":"text/plain"})

        print ("headers:", r.request.headers)
        print ("headers:", r.headers)

        sys.stdout.flush()
        res = []
        for line in r.text.split('\n'):
            print("got line: " , line)
            res.append(line.rstrip('\n'))
        r.close()
        return res

if __name__ == "__main__":
     
    jf = jobsub_fetcher("/tmp/x509up_u%d" % os.getuid(),"/tmp/x509up_u%d"% os.getuid() )
    jobid="18533155.0@fifebatch1.fnal.gov"
    flist = jf.index(jobid, "samdev", "Analysis") 
    print("------------------")
    print(flist)
    print("------------------")
    print(jf.contents(flist[2][-1], jobid, "samdev", "Analysis"))
