#!/usr/bin/env python

import sys
import os
from os import system as os_system
import threading
from poms.webservice.logit import log, logstartstop
import requests
import re
import traceback
from collections import deque

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

        res = deque()
        if retries == -1:
            return res

        if group == "samdev": 
            group = "fermilab" 
            user = "mengel"  # kluge alert
        else:
            # XXX this will be wrong when we have real Analysis jobs...
            user = "%spro" % group


        fifebatch = jobsubjobid[jobsubjobid.find("@")+1:]


        if fifebatch == "fakebatch1.fnal.gov":
            # don't get confused by test suite...
            return

        url = "https://%s:8443/jobsub/acctgroups/%s/sandboxes/%s/%s/" % ( fifebatch, group, user, jobsubjobid)

        log( "trying url:" +  url)

        r = None
        try:
            r = self.sess.get(url, cert=(self.cert,self.key),  verify=False, headers={"Accept":"text/html"})
            log ("headers:" + repr( r.request.headers))
            log ("headers:" + repr( r.headers))
            sys.stdout.flush()
            for line in r.text.split('\n'):
                log("got line: " +  line)
                # strip tags...
                line = re.sub('<[^>]*>','', line)
                fields = line.strip().split()
                if len(fields):
                    fname = fields[0]
                    fields[0] = ""
                    fields[2] = fields[1]
                    fields.append(fname)
                    log("got fields: " + repr(fields))
                    res.append(fields)
        except:
            log(traceback.format_exc())
        finally:
            if r:
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
            user = 'mengel'    # XXX

        fifebatch = jobsubjobid[jobsubjobid.find("@")+1:]

        if fifebatch == "fakebatch1.fnal.gov":
            # don't get confused by test suite...
            return

        url = "https://%s:8443/jobsub/acctgroups/%s/sandboxes/%s/%s/%s/" % (fifebatch, group, user, jobsubjobid, filename)

        log( "trying url:" + url)

        try:
            r = self.sess.get(url, cert=(self.cert,self.key), stream=True, verify=False, headers={"Accept":"text/plain"})

            log ("headers:" + repr( r.request.headers))
            log ("headers:" + repr( r.headers))

            sys.stdout.flush()
            res = deque()
            for line in r.text.split('\n'):
                log("got line: " + line)
                res.append(line.rstrip('\n'))
        except:
            log(traceback.format_exc())
        finally:
            if r: r.close()

        return res

if __name__ == "__main__":

    jf = jobsub_fetcher("/tmp/x509up_u%d" % os.getuid(),"/tmp/x509up_u%d"% os.getuid() )
    jobid="18533155.0@fifebatch1.fnal.gov"
    flist = jf.index(jobid, "samdev", "Analysis")
    print("------------------")
    print(flist)
    print("------------------")
    print(jf.contents(flist[2][-1], jobid, "samdev", "Analysis"))
