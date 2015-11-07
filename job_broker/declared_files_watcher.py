import sys
import os
import re
import urllib2
import json
import time
import traceback
from job_reporter import job_reporter

import samweb_client

class declared_files_watcher:
    def __init__(self, job_reporter):
        self.job_reporter = job_reporter

    def get_pending_jobs(self):
	self.jobmap = {}
        try:
            conn = urllib2.urlopen(self.job_reporter.report_url + '/output_pending_jobs'
)
            jobs = json.load(conn)
            conn.close()

            print "got: ", jobs
            return jobs
        except:
            print  "Ouch!", sys.exc_info()
	    traceback.print_exc()
            pass


    def one_pass(self):
         map = self.get_pending_jobs()
         old_experiment = ""
         samweb = None
         for jobsub_job_id in  map.keys():
             flist = map[jobsub_job_id]["output_file_names"].split(" ")
             experiment  = map[jobsub_job_id]["experiment"]
             if old_experiment != experiment:
                  samweb = samweb_client.SAMWebClient(experiment=experiment)
             all_located = 1
             for f in flist:
                  try:
                      loclist = samweb.locateFile(f)
                      print "got: ", loclist
                      if loclist == []:
                          print "file: %s not located" % f
                          all_located = 0
                      else:
                          # xxx debug
                          print "located file %s %s" % (f, repr(loclist))
                  except:
		      print "exception %s locating %s in experiment %s" % (sys.exc_info(), f, experiment)
		      all_located = 0
                      pass
                     
             if all_located:
                 self.job_reporter.report_status(jobsub_job_id,output_files_declared = "True")

if __name__ == "__main__":
     dfw = declared_files_watcher(job_reporter("http://localhost:8080/poms"))
     dfw.one_pass()
