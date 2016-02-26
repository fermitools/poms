#!/usr/bin/env python
import sys
import os
# make sure poms is setup...
if os.environ.get("SETUP_POMS","") == "":
    sys.path.insert(0,os.environ.get('SETUPS_DIR', os.environ.get('HOME')+'/products'))
    import setups
    print "setting up poms..."
    ups = setups.setups()
    ups.use_package("poms","","SETUP_POMS")


import re
import urllib2
import json
import time
import traceback
from job_reporter import job_reporter
from samweb_client import *

class declared_files_watcher:
    def __init__(self, job_reporter):
        self.job_reporter = job_reporter
        self.old_experiment = None

    def call_wrapup_tasks(self):
	self.jobmap = {}
        try:
            conn = urllib2.urlopen(self.job_reporter.report_url + '/wrapup_tasks'
)
            output = conn.read()
            conn.close()

            print "got: ", output
        except:
            print time.asctime(), "Ouch!", sys.exc_info()
	    traceback.print_exc()
            pass

    def get_pending_jobs(self):
	self.jobmap = {}
        try:
            conn = urllib2.urlopen(self.job_reporter.report_url + '/output_pending_jobs'
)
            jobs = json.load(conn)
            conn.close()

            #print "got: ", jobs
            sys.stdout.flush()
            return jobs
        except:
            print  time.asctime(), "Ouch!", sys.exc_info()
            sys.stdout.flush()
	    traceback.print_exc()
            pass

    def find_located_files(self, experiment, flist):
	if self.old_experiment != experiment:
	    self.samweb = SAMWebClient(experiment = experiment)
            print "got samweb handle for ", experiment
	    self.old_experiment = experiment

        res = []
        while len(flist) > 0:
            batch = flist[:500]
            flist = flist[500:]
            dims = "file_name %s" % ','.join(batch)
            #print "trying dimensions: ", dims
            sys.stdout.flush()
            found = self.samweb.listFiles(dims)
            #print "got: ", found
            sys.stdout.flush()
            res = res + found
        print "found %d located files for %s" % (len(res), experiment)
        return res

    def one_pass(self):
         jobmap = self.get_pending_jobs()
         old_experiment = ""
         samweb = None
         total_flist = {}
         present_files = {}
         for jobsub_job_id in  jobmap.keys():
             job_flist = jobmap[jobsub_job_id]["output_file_names"].split(" ")
             experiment  = jobmap[jobsub_job_id]["experiment"]
             if not total_flist.has_key(experiment):
                  total_flist[experiment] = []
	     total_flist[experiment] = total_flist[experiment] + job_flist

         print "got total file lists for experiments..."

         for experiment in total_flist.keys():
             present_files[experiment] = self.find_located_files(experiment, total_flist[experiment])
          
         for jobsub_job_id in  jobmap.keys():

             flist = jobmap[jobsub_job_id]["output_file_names"].split(" ")
             experiment  = jobmap[jobsub_job_id]["experiment"]

             all_located = 1
             for f in flist:
                  if not f in present_files[experiment]:
		      all_located = 0

             if all_located:
                 self.job_reporter.report_status(jobsub_job_id,output_files_declared = "True",status="Located")

         print "Looked through files.."

         self.call_wrapup_tasks()

    def poll(self):
        while(1):
            try:
                self.one_pass()
			 
	    except KeyboardInterrupt:
	        break

	    except:
	        print time.asctime(), "Exception!"
	        traceback.print_exc()
	        pass

            time.sleep(60)

if __name__ == "__main__":
     dfw = declared_files_watcher(job_reporter("http://localhost:8080/poms", debug=1))
     dfw.poll()
