#!/usr/bin/env python

import sys
import os
import re
import requests
import urllib.request, urllib.error, urllib.parse
import json
import time
import traceback
import resource
import gc
import pprint
import threading
from .job_reporter import job_reporter

do_memdebug = False

if do_memdebug:
    from pympler import tracker

import prometheus_client as prom

class jobsub_q_scraper:
    """
       this would actually call jobsub_q, if it were efficient, and you
       could pass -format...  instead we call condor_q directly to look
       at the fifebatchhead nodes.
    """
    def __init__(self, job_reporter, debug = 0):
        self.rs = requests.Session()
        self.job_reporter = job_reporter
        self.jobCount = prom.Gauge("jobs_in_queue","Jobs in the queue this run")
        self.threadCount = prom.Gauge("Thread_count","Number of probe threads")

        self.map = {
           "0": "Unexplained",
           "1": "Idle",
           "2": "Running",
           "3": "Removed",
           "4": "Completed",
           "5": "Held",
           "6": "Submission_error",
        }
        self.cur_report = {}
        self.prev_report = {}
        self.jobmap = {}
        self.prevjobmap = {}
        self.debug = debug
        self.passcount = 0
        if do_memdebug:
	    self.memory_tracker = tracker.SummaryTracker()
	    self.memory_tracker.print_diff()
	    self.memory_tracker.print_diff()
	    self.memory_tracker.print_diff()
        sys.stdout.flush()

    def get_open_jobs(self):
        self.prevjobmap = self.jobmap
	self.jobmap = {}
        conn = None
        try:
            conn = self.rs.get(self.job_reporter.report_url + '/active_jobs')
            jobs = conn.json()
            conn.close()
            conn = None

            #print "got: ", jobs
            print("got %d jobs" % len(jobs))
            self.jobCount.set(len(jobs))
            for j in jobs:
                self.jobmap[j] = 0
            del jobs
            jobs = None
	except KeyboardInterrupt:
	    raise
        except Exception as e:
            print("Ouch!", sys.exc_info())
	    traceback.print_exc()
            if conn: 
                conn.close()
                del conn
            del e

    def call_wrapup_tasks(self):
        conn = None
        try:
            conn = self.rs.get(self.job_reporter.report_url + '/wrapup_tasks')
            text = conn.text
            conn.close()
            conn = None

            if self.debug: print("got: ", text)
	except KeyboardInterrupt:
	    raise
        except Exception as e:
            print("Ouch!", sys.exc_info())
	    traceback.print_exc()
            if conn:
                conn.close()
                del conn
            del e

    def scan(self):
        # roll our previous/current status
        self.prev_report = self.cur_report
        self.cur_report = {}

        self.get_open_jobs()

        # do a formatted output so that the Jobstatus looks 
        # like just another environment variable JOBSTATUS, etc.
        # for now we have a for loop and use condor_q, in future
        # we hope to be able to use jobsub_q with -format...

        f = os.popen("for n in 1 2; do m=$((n+2)); condor_q -pool fifebatchhead$m.fnal.gov -global -constraint 'regexp(\".*POMS_TASK_ID=.*\",Env)' -format '%s;JOBSTATUS=' Env -format '%d;CLUSTER=' Jobstatus -format '%d;PROCESS=' ClusterID -format '%d;' ProcID -format 'GLIDEIN_SITE=%s;' MATCH_EXP_JOB_GLIDEIN_Site -format 'REMOTEHOST=%s;' RemoteHost -format 'NumRestarts=%d;' NumRestarts -format 'HoldReason=%.30s;' HoldReason -format 'RemoteUserCpu=%f;' RemoteUserCpu  -format 'EnteredCurrentStatus=%d;' EnteredCurrentStatus -format 'RemoteWallClockTime=%f;' RemoteWallClockTime -format 'Args=\"%s\";' Args -format 'JOBSUBJOBID=%s;' JobsubJobID -format 'xxx=%d\\n' ProcID && break; done", "r")
        for line in f:

            line = line.rstrip('\n')
                
            if self.debug:
                print("saw line: " , line)
	    jobenv={}
	    for evv in line.split(";"):
		name,val = evv.split("=",1)
		jobenv[name] = val

	    if "JOBSUBJOBID" in jobenv:
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

            host = jobenv.get('REMOTEHOST','')
            host = host[host.rfind('@')+1:]

            #
            # condor rarely updates wallclock time, so if they
            # didn't give us one, compute it from 
            #
            wall_time = float(jobenv.get('RemoteWallClockTime','0.0'))
            status_time =int(jobenv.get('EnteredCurrentStatus','0'))

            if float(wall_time) == 0.0 and status_time != 0 and int(jobenv.get('JOBSTATUS','0')) == 2:
                 wall_time = float(time.time() - status_time)

            if "SAM_PROJECT_NAME" not in jobenv and "Args" in jobenv  and jobenv["Args"].find("--sam_project") > 0:
                spv = jobenv["Args"][jobenv["Args"].find("--sam_project")+14:]
                spv = spv[0:spv.find(" ")]
                jobenv["SAM_PROJECT_NAME"] = spv

            if "POMS_TASK_ID" in jobenv:

                if self.debug: print("jobenv is: ", jobenv)

                args = {
                    'jobsub_job_id' : jobsub_job_id,
                    'taskid' : jobenv['POMS_TASK_ID'],
                    'status' : self.map[jobenv['JOBSTATUS']],
                    'restarts' : jobenv['NumRestarts'],
                    'node_name' : host, 
                    'host_site' : jobenv.get('GLIDEIN_SITE', ''),
                    'task_project' : jobenv.get('SAM_PROJECT_NAME',None),
                    'cpu_time' : jobenv.get('RemoteUserCpu'),
                    'wall_time' : wall_time,
                    'task_recovery_tasks_parent': jobenv.get('POMS_PARENT_TASK_ID',None),
                }

                prev = self.prev_report.get(jobsub_job_id, None)
	        self.cur_report[jobsub_job_id] = args

                #
                # only report status if its different
                #
                if not prev or prev['status'] != args['status'] or prev['node_name'] != args['node_name'] or prev['cpu_time'] != args['cpu_time'] or prev['wall_time'] != args['wall_time'] or prev['task_project'] != args['task_project']:
                    try: 
                        self.job_reporter.report_status(**args)
	            except KeyboardInterrupt:
	                raise
                    except:
	                print("Reporting Exception!")
	                traceback.print_exc()
                        pass
                else:
                    if self.debug: 
                         print("unchanged, not reporting")
                         print("prev", prev)
                         print("args", args)
                          
            else:
                #print "skipping:" , line
                pass

        res = f.close()

        if res == 0 or res == None:
	    for jobsub_job_id in list(self.jobmap.keys()):
		if self.jobmap[jobsub_job_id] == 0 and self.prevjobmap.get(jobsub_job_id,0) == 0:
		    # it is in the database, but not in our output, 
                    # nor in the previous output, we conclude it's completed.
		    # we could get a false alarm here if condor_q fails...
		    # thats why we only do this if our close() returned 0/None.
                    # and we make sure we didn't see it two runs in a row...
                    print("reporting %s as completed" % jobsub_job_id)

		    self.job_reporter.report_status(
			jobsub_job_id = jobsub_job_id,
			status = "Completed")
        else:
            print("error code: %s from condor_q" % res)

        self.call_wrapup_tasks()

    def poll(self):
        while(1):
            self.passcount = self.passcount + 1

            # just restart periodically, so we don't eat memory, etc.
            if self.passcount > 1000:
                os.execvp(sys.argv[0], sys.argv)

            self.threadCount.set(threading.active_count())

            try:
                self.scan()
			 
	    except KeyboardInterrupt:
	        raise
 
            except OSError as e:
	        print("Exception!")
	        traceback.print_exc()
                # if we're out of memory, dump core...
                if e.errno == 12:
                    resource.setrlimit(resource.RLIMIT_CORE,resource.RLIM_INFINITY)
                    os.abort()

	    except:
	        print("Exception!")
	        traceback.print_exc()
	        pass

            sys.stderr.write("%s pausing...\n" % time.asctime())
            sys.stderr.flush()
            time.sleep(30)
            sys.stderr.write("%s done...\n" % time.asctime())
            sys.stderr.flush()

            n = gc.collect()
            print("gc.collect() returns %d unreachable" % n)
            if do_memdebug:
                self.memory_tracker.print_diff()
            sys.stdout.flush()

# don't barf if we need to log utf8...
import codecs
sys.stdout = codecs.getwriter('utf8')(sys.stdout)

if __name__ == '__main__':
    debug = 0
    testing = 0

    if len(sys.argv) > 1 and sys.argv[1] == "-d":
        debug=1
        sys.argv = [sys.argv[0]] + sys.argv[2:]

    server = "http://localhost:8080/poms"
    if len(sys.argv) > 1 and sys.argv[1] == "-t":
        testing = 1
        server = "http://127.0.0.1:8888/poms"
        sys.argv = [sys.argv[0]] + sys.argv[2:]

    ns = "profiling.apps.poms.probes.%s.jobsub_q_scraper" % os.uname()[1].split(".")[0]
    jr = job_reporter(server, debug=debug, namespace=ns)
    js = jobsub_q_scraper(jr, debug = debug)
    try:
        if testing:
            print("test mode, run one scan")
            js.scan()
            print("test mode: done")
        else:
            js.poll()
    except KeyboardInterrupt:
        n = gc.collect()
        #print "gc.collect() returns %d unreachable" % n
        #print "Remaining garbage:"
        #pprint.pprint(gc.garbage)
    jr.cleanup()
    print("end of __main__")
