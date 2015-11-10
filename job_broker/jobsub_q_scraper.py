#!/usr/bin/env python

import sys
import os
import re
import urllib2
import json
import time
import traceback
from job_reporter import job_reporter

class jobsub_q_scraper:
    """
       this would actually call jobsub_q, if it were efficient, and you
       could pass -format...  instead we call condor_q directly to look
       at the fifebatchhead nodes.
    """
    def __init__(self, job_reporter, debug = 0):
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
        self.jobmap = {}
        self.debug = debug

    def get_open_jobs(self):
	self.jobmap = {}
        try:
            conn = urllib2.urlopen(self.job_reporter.report_url + '/active_jobs')
            jobs = json.load(conn)
            conn.close()

            print "got: ", jobs
            for j in jobs:
                self.jobmap[j] = 0
        except:
            print  "Ouch!", sys.exc_info()
	    traceback.print_exc()
            pass

    def call_wrapup_tasks(self):
        try:
            conn = urllib2.urlopen(self.job_reporter.report_url + '/wrapup_tasks')
            text = conn.read()
            conn.close()

            print "got: ", text
        except:
            print  "Ouch!", sys.exc_info()
	    traceback.print_exc()
            pass

    def scan(self):

        self.get_open_jobs()

        # do a formatted output so that the Jobstatus looks 
        # like just another environment variable JOBSTATUS, etc.
        # for now we have a for loop and use condor_q, in future
        # we hope to be able to use jobsub_q with -format...

        f = os.popen("for n in 1 2; do condor_q -pool fifebatchgpvmhead$n.fnal.gov -name fifebatch$n.fnal.gov -format '%s;JOBSTATUS=' Env -format '%d;CLUSTER=' Jobstatus -format '%d;PROCESS=' ClusterID -format \"%d;SCHEDD=fifebatch$n.fnal.gov\\n\" ProcID ; done", "r")
        for line in f:
                
            if self.debug:
                print "saw line: " , line
	    jobenv={}
	    for evv in line.split(";"):
		name,val = evv.split("=",1)
		jobenv[name] = val

	    if jobenv.has_key("JOBSUBJOBID"):
		jobsub_job_id = jobenv["JOBSUBJOBID"];
	    else:
		jobsub_job_id = '%s.%s@%s' % (
		    jobenv['CLUSTER'],
		    jobenv['PROCESS'],
		    jobenv['SCHEDD']
		  )

            jobsub_job_id = jobsub_job_id.strip()

            self.jobmap[jobsub_job_id] = 1
            
            # only look at it if it has a POMS_TASK_ID in the environment

            if jobenv.has_key("POMS_TASK_ID") > 0:

                print "reporting: ", jobenv

                self.job_reporter.report_status(
                    jobsub_job_id = jobsub_job_id,
                    taskid = jobenv['POMS_TASK_ID'],
                    status = self.map[jobenv['JOBSTATUS']],
                    task_project = self.jobenv.get('SAM_PROJECT_NAME',None)
                  )
            else:
                #print "skipping:" , line
                pass

        res = f.close()

        if res == 0 or res == None:
	    for jobsub_job_id in self.jobmap.keys():
		if self.jobmap[jobsub_job_id] == 0:
		    # it is in the database, but not in our output, 
                    # so it's dead.
		    # we could get a false alarm here if condor_q fails...
		    # thats why we only do this if our close() returned 0/None.
		    self.job_reporter.report_status(
			jobsub_job_id = jobsub_job_id,
			status = "Completed")

        self.call_wrapup_tasks()

    def poll(self):
        while(1):
            try:
                self.scan()
			 
	    except KeyboardInterrupt:
	        break

	    except:
	        print "Exception!"
	        traceback.print_exc()
	        pass

            time.sleep(60)

if __name__ == '__main__':
    debug = 0
    if len(sys.argv) > 1 and sys.argv[1] == "-d":
        debug=1

    js = jobsub_q_scraper(job_reporter("http://localhost:8080/poms"), debug = debug)
    js.poll()
