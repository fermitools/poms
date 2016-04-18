#!/usr/bin/env python

import sys
import os
import re
import urllib2
import urllib
import json
import concurrent.futures

class job_reporter:
    """
       this would actually call jobsub_q, if it were efficient, and you
       could pass -format...  instead we call condor_q directly to look
       at the fifebatchhead nodes.
    """
    def __init__(self, report_url, debug = 0):
        self.report_url = report_url
        self.debug = debug
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10) 
        self.futures = []
  
    def __del__(self):
        # clean up all our futures
        for f in self.futures:
            r = f.result()
        self.futures = []

    def report_status(self,  jobsub_job_id = '', taskid = '', status = '' , cpu_type = '', slot='', **kwargs ):
        self.futures.append( self.executor.submit(self.actually_report_status,  jobsub_job_id, taskid, status, cpu_type, slot, **kwargs))
        for f in self.futures:
            if f.done():
                r = f.result()
                self.futures.remove(f)
        if len(self.futures) > 10:
            done, not_done = concurrent.futures.wait(self.futures, return_when=concurruent.futures.FIRST_COMPLETED)
            self.futures = not_done

    def actually_report_status(self, jobsub_job_id = '', taskid = '', status = '' , cpu_type = '', slot='', **kwargs ):
        data = {}
        data['task_id'] = taskid
        data['jobsub_job_id'] = jobsub_job_id
        data['cpu_type'] = cpu_type
        data['slot'] = slot
        data['status'] = status
        data.update(kwargs)

        if self.debug:
           print "reporting: ",  data 
           sys.stdout.flush()
          
        uh = None
        try:
            uh = urllib2.urlopen(self.report_url + "/update_job", data = urllib.urlencode(data))
            res = uh.read()
        except urllib2.HTTPError, e:
            errtext = e.read()
            sys.stderr.write("Excpetion:")
            if uh:
                sys.stderr.write("HTTP fetch status %d" %  uh.getcode())
            sys.stderr.write(errtext)
            sys.stderr.write("--------")

        uh.close()
        return res

if __name__ == '__main__':
    print "self test:"
    r = job_reporter("http://127.0.0.1:8080/poms", debug=1)
    r.report_status(jobsub_job_id="12345.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12346.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12347.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
    r.report_status(jobsub_job_id="12348.0@fifebatch3.fnal.gov",output_files_declared = "True",status="Located")
