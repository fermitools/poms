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
import requests
import json
import time
import traceback
from job_reporter import job_reporter
from samweb_client import *
import threading
import prometheus_client as prom

class declared_files_watcher:
    def __init__(self, job_reporter):
        self.rs = requests.Session()
        self.job_reporter = job_reporter
        self.old_experiment = None
        self.threadCount = prom.Gauge("Thread_count","Number of probe threads")
        self.totalTracked = 0
        self.tracked_files = prom.Gauge("Tracked_files","Number of files being processed")
        
    def report_declared_files(self,flist):
        print "entering: report_declared_files:", flist
        url = self.job_reporter.report_url + "/report_declared_files"
        try:
            conn = self.rs.post(url,data = [ ("flist",x) for x in flist])
            res = conn.text
            conn.close()
        except KeyboardInterrupt:
            raise
        except:
            print  time.asctime(), "Ouch!", sys.exc_info()
            sys.stdout.flush()
	    traceback.print_exc()
            pass

    def call_wrapup_tasks(self):
	self.jobmap = {}
        try:
            conn = self.rs.get(self.job_reporter.report_url + '/wrapup_tasks'
)
            output = conn.text
            conn.close()

            print "got: ", output
        except KeyboardInterrupt:
            raise
        except:
            print time.asctime(), "Ouch!", sys.exc_info()
	    traceback.print_exc()
            pass

    def get_pending_jobs(self):
	self.jobmap = {}
        try:
            conn = request.get(self.job_reporter.report_url + '/output_pending_jobs'
)
            jobs = conn.json()
            conn.close()

            #print "got: ", jobs
            sys.stdout.flush()
            return jobs
        except KeyboardInterrupt:
            raise
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
            batch = flist[:300]
            flist = flist[300:]
            dims = "file_name '%s'" % "','".join(batch)
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
         if jobmap == None:
             return
         samweb = None
         total_flist = {}
         present_files = {}
         for experiment in jobmap.keys():
             for jobsub_job_id in jobmap[experiment].keys():
                 job_flist = jobmap[experiment][jobsub_job_id]
                 if not total_flist.has_key(experiment):
                     total_flist[experiment] = []
	         total_flist[experiment] = total_flist[experiment] + job_flist

         print "got total file lists for experiments..."

         for experiment in total_flist.keys():
             self.totalTracked += len(total_flist[experiment])
             present_files[experiment] = self.find_located_files(experiment, total_flist[experiment])

             self.report_declared_files(total_flist[experiment])
          
         for experiment in jobmap.keys():
             for jobsub_job_id in  jobmap[experiment].keys():
                 flist = jobmap[experiment][jobsub_job_id]

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
            self.totalTracked = 0
            try:
                self.one_pass()
			 
	    except KeyboardInterrupt:
	        break

	    except:
	        print time.asctime(), "Exception!"
	        traceback.print_exc()
	        pass

            self.threadCount.set(threading.active_count())
            self.tracked_files.set(self.totalTracked)
            time.sleep(60)

if __name__ == "__main__":

    server = "http://localhost:8080/poms"
    if len(sys.argv) > 1 and sys.argv[1] == "-t":
        server = "http://localhost:8888/poms"

    ns = "profiling.apps.poms.probes.%s.declared_files_watcher" % os.uname()[1].split(".")[0]
    dfw = declared_files_watcher(job_reporter(server, debug=1, namespace = ns))
    dfw.poll()
