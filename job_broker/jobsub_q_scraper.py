#!/usr/bin/env python

import sys
import os
import re
import urllib2
import json
import time
from job_reporter import job_reporter

class jobsub_q_scraper:
    """
       this would actually call jobsub_q, if it were efficient, and you
       could pass -format...  instead we call condor_q directly to look
       at the fifebatchhead nodes.
    """
    def __init__(self, job_reporter):
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
        f = os.popen("for n in 1 2; do condor_q -pool fifebatchgpvmhead$n.fnal.gov -name fifebatch$n.fnal.gov -format '%s;JOBSTATUS=' Env -format '%d;CLUSTER=' Jobstatus -format '%d;PROCESS=' ClusterID -format \"%d;SCHEDD=fifebatch$n.fnal.gov\\n\" ProcID ; done", "r")
        for line in f:
            if line.find('POMS_TASK_ID=') > 0:
                
                # only look at it if it has a POMS_TASK_ID in the environment

		jobenv={}
		for evv in line.split(";"):
		    name,val = evv.split("=",1)
		    jobenv[name] = val
            
                print "reporting: ", jobenv

                if jobenv.has_key("JOBSUBJOBID"):
                    jobsubjobid = jobenv["JOBSUBJOBID"];
                else:
		    jobsubjobid = '%s.%s@%s' % (
			jobenv['PROCESS'],
			jobenv['CLUSTER'],
			jobenv['SCHEDD']
		      )
                self.job_reporter.report_status(
                    jobid = jobsubjobid,
                    taskid = jobenv['POMS_TASK_ID'],
                    status = self.map[jobenv['JOBSTATUS']]
                  )
            else:
                #print "skipping:" , line
                pass

    def poll(self):
        while(1):
            self.scan()
            time.sleep(120)

if __name__ == '__main__':
    js = jobsub_q_scraper(job_reporter("http://fermicloud045.fnal.gov:8080/poms"))
    js.poll()
    # for testing, just do one pass...
    #js.scan()
