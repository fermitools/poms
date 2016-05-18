#!/usr/bin/env python

import sys
import os
import re
import urllib2
import urllib
import json
import concurrent.futures
import Queue
import thread
import threading

class job_reporter:
    """
       class to report job status -- now runs several threads to asynchronously report queued items
       given to us, so we don't drop messages from syslog piping to us, etc.
    """
    def __init__(self, report_url, debug = 0, nthreads = 5):
        self.report_url = report_url
        self.debug = debug
        self.work = Queue.Queue()
        self.wthreads = []
        for i in range(nthreads):
            self.wthreads.append(threading.Thread(target=self.runqueue))
            #self.wthreads[i].daemon = True
            self.wthreads[i].start()

    def bail(self):
        print "thread: %d -- bailing" % thread.get_ident()
        raise KeyboardInterrupt("just quitting a thread")

    def runqueue(self):
        try:
            while 1:
                d = self.work.get(block = True)
                d['f'](*d['args'], **d['kwargs'])
        except KeyboardInterrupt:
            pass
        except:
            raise
  
    def cleanup(self):
        # first tell threads to exit
        for wth in self.wthreads:
            self.work.put({'f': job_reporter.bail, 'args': [self], 'kwargs':{}})

        # then wait for them
        for wth in self.wthreads:
            wth.join()

    def report_status(self,  jobsub_job_id = '', taskid = '', status = '' , cpu_type = '', slot='', **kwargs ):
        self.work.put({'f':job_reporter.actually_report_status, 'args': [ self, jobsub_job_id, taskid, status, cpu_type, slot], 'kwargs': kwargs})

    def actually_report_status(self, jobsub_job_id = '', taskid = '', status = '' , cpu_type = '', slot='', **kwargs ):
        data = {}
        data['task_id'] = taskid
        data['jobsub_job_id'] = jobsub_job_id
        data['cpu_type'] = cpu_type
        data['slot'] = slot
        data['status'] = status
        data.update(kwargs)

        if self.debug:
           sys.stdout.write("reporting: %s\n" %  data )
           sys.stdout.flush()
          
        uh = None
        res = None
        try:
            uh = urllib2.urlopen(self.report_url + "/update_job", data = urllib.urlencode(data))
            res = uh.read()
            sys.stderr.write("response: %s\n" % res)

        except urllib2.HTTPError, e:
            errtext = e.read()
            sys.stderr.write("Excpetion:")
            if uh:
                sys.stderr.write("HTTP fetch status %d" %  uh.getcode())
            sys.stderr.write(errtext)
            sys.stderr.write("--------")

        return res

if __name__ == '__main__':
    print "self test:"
    r = job_reporter("http://127.0.0.1:8080/poms", debug=1)
    r.report_status(jobsub_job_id="12345.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12346.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12347.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12348.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    print "started reports:"
    sys.stdout.flush()
    r.cleanup()
