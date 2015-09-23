#!/usr/bin/env python

import sys
import os
import re
import urllib2
import json
from job_reporter import job_reporter

class jobsub_q_scraper:
    """
       this would actually call jobsub_q, if it were efficient, and you
       could pass -format...  instead we call condor_q directly to look
       at the fifebatchhead nodes.
    """
    def __init__(self, job_reporter):
        self.filehandle = filehandle
        self.job_reporter = job_reporter
        self.map = {
           "0": "Unexplained",
           "1": "Idle",
           "2": "Running",
           "3": "Removed",
           "4": "Completed",
           "5": "Held",
           "6": "Submission_error",
         }


    def scan(self):
        # do a formatted output so that the Jobstatus looks 
        # like just another environment variable JOBSTATUS
        # for now we have a for loop and use condor_q, in future
        # we hope to be able to use jobsub_q with -format...
        f = os.popen("for n in 1 2; do condor_q -pool fifebatchgpvmhead$n.fnal.gov -name fifebatch$n.fnal.gov -format '%s;JOBSTATUS=' Env -format '%d\n' Jobstatus; done", "r")
        for line in f:
            if line.find('POMS_TASK_ID=') > 0:
                
                # only look at it if it has a POMS_TASK_ID in the environment

		jobenv={}
		for evv in line.split(";"):
		    name,val = evv.split("=",1)
		    jobenv[name] = val
            
                self.job_reporter.report(
                  jobid = jobenv['JOBSUBJOBID'],
                  taskid = jobenv['POMS_TASK_ID'],
                  jobstatus = jobenv['JOBSTATUS']
                )

    def poll(self):
        while(1):
            self.scan()
            time.sleep(120)

if __name__ == '__main__':
    js = jobsub_q_scraper(job_reporter("http://localhost.fnal.gov:8080/poms/"))
    js.poll()
