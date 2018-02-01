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
from job_reporter import job_reporter
from subprocess import Popen, PIPE

import prometheus_client as prom

#
# define a couple of clone classes of dict so we can see which one(s) we're using/leaking
#

class JobAttrs(dict):
     pass

class JobEnv(dict):
     pass

class JobSet(dict):
     pass

class jobsub_q_scraper:
    """
       this would actually call jobsub_q, if it were efficient, and you
       could pass -format...  instead we call condor_q directly to look
       at the fifebatchhead nodes.
    """
    def __init__(self, job_reporter, debug = 0):

        gc.enable()

        self.rs = requests.Session()
        self.job_reporter = job_reporter
        self.jobCount = prom.Gauge("jobs_in_queue","Jobs in the queue this run")
        self.threadCount = prom.Gauge("Thread_count","Number of probe threads")

        #self.jobsub_q_cmd = "for n in 1 2; do m=$((n+2)); condor_q -pool fifebatchhead$m.fnal.gov -global -constraint 'regexp(\".*POMS_TASK_ID=.*\",Env)' -format '%s;JOBSTATUS=' Env -format '%d;CLUSTER=' Jobstatus -format '%d;PROCESS=' ClusterID -format '%d;' ProcID -format 'GLIDEIN_SITE=%s;' MATCH_EXP_JOB_GLIDEIN_Site -format 'REMOTEHOST=%s;' RemoteHost -format 'NumRestarts=%d;' NumRestarts -format 'HoldReason=%.30s;' HoldReason -format 'RemoteUserCpu=%f;' RemoteUserCpu  -format 'EnteredCurrentStatus=%d;' EnteredCurrentStatus -format 'RemoteWallClockTime=%f;' RemoteWallClockTime -format 'Args=\"%s\";' Args -format 'JOBSUBJOBID=%s;' JobsubJobID -format 'xxx=%d\\n' ProcID && break; done"
        self.jobsub_q_cmd = "condor_q -pool gpcollector03.fnal.gov -global -constraint 'regexp(\".*POMS_TASK_ID=.*\",Env)' -format '%s;JOBSTATUS=' Env -format '%d;CLUSTER=' Jobstatus -format '%d;PROCESS=' ClusterID -format '%d;' ProcID -format 'GLIDEIN_SITE=%s;' MATCH_EXP_JOB_GLIDEIN_Site -format 'REMOTEHOST=%s;' RemoteHost -format 'NumRestarts=%d;' NumRestarts -format 'HoldReason=%.30s;' HoldReason -format 'RemoteUserCpu=%f;' RemoteUserCpu  -format 'EnteredCurrentStatus=%d;' EnteredCurrentStatus -format 'RemoteWallClockTime=%f;' RemoteWallClockTime -format 'Args=\"%s\";' Args -format 'JOBSUBJOBID=%s;' JobsubJobID -format 'xxx=%d\\n' ProcID"

        self.map = {
           "0": sys.intern("Unexplained"),
           "1": sys.intern("Idle"),
           "2": sys.intern("Running"),
           "3": sys.intern("Removed"),
           "4": sys.intern("Completed"),
           "5": sys.intern("Held"),
           "6": sys.intern("Submission_error"),
        }
        # intern strings we use for maps, etc. to cut down on memory
        self.k_jobsub_job_id = sys.intern('jobsub_job_id')
        self.k_task_id = sys.intern('task_id')
        self.k_status = sys.intern('status')
        self.k_restarts = sys.intern('restarts')
        self.k_node_name = sys.intern('node_name')
        self.k_host_site = sys.intern('host_site')
        self.k_task_project = sys.intern('task_project')
        self.k_cpu_time = sys.intern('cpu_time')
        self.k_reason_held = sys.intern('reason_held')
        self.k_wall_time = sys.intern('wall_time')
        self.k_task_recovery_tasks_parent = sys.intern('task_recovery_tasks_parent')
        self.k_JOBSUBJOBID = sys.intern('JOBSUBJOBID')

        self.cur_report = JobAttrs()
        self.prev_report = JobAttrs()
        self.jobmap = JobSet()
        self.prevjobmap = JobSet()
        self.tidmap = {}
        self.debug = debug
        self.passcount = 0
        self.last_known_status = {}
        sys.stdout.flush()

    def get_open_jobs(self):
        self.prevjobmap = self.jobmap
        self.jobmap = JobSet()
        self.tidmap = {}
        conn = None
        try:
            conn = self.rs.get(self.job_reporter.report_url + '/active_jobs')
            jobs = conn.json()

            print( "got: ", jobs)
            #print("got %d jobs" % len(jobs))
            self.jobCount.set(len(jobs)+0)
            for j, tid in jobs:
                self.jobmap[j] = 0
                self.tidmap[j] = tid
            del jobs
            jobs = None
        except KeyboardInterrupt:
            raise

        except Exception as e:
            sys.stderr.write("Ouch! when getting active jobs\n")
            traceback.print_exc(file=sys.stderr)
            del e
        finally:
            if conn:
               conn.close()

    def call_wrapup_tasks(self):
        return
        conn = None
        try:
            conn = self.rs.get(self.job_reporter.report_url + '/wrapup_tasks') 
            text = conn.text

            if self.debug: print("got: ", text)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            sys.stderr.write("Ouch! while calling wrapup_tasks\n")
            traceback.print_exc(file=sys.stderr)
            del e
        finally:
            if conn:
               conn.close()

    def scan(self):
        # roll our previous/current status
        del self.prev_report
        self.prev_report = self.cur_report
        self.cur_report = JobAttrs()
        jobenv = JobEnv()

        self.get_open_jobs()

        # do a formatted output so that the Jobstatus looks 
        # like just another environment variable JOBSTATUS, etc.
        # for now we have a for loop and use condor_q, in future
        # we hope to be able to use jobsub_q with -format...

        p = Popen(self.jobsub_q_cmd, shell = True, stdout = PIPE, close_fds = True, universal_newlines=True)
        f = p.stdout

        for line in f:

            line = line.rstrip('\n')
                
            # if self.debug: print("saw line: " , line)

            del jobenv
            jobenv=JobEnv()
            for evv in line.split(";"):
                name,val = evv.split("=",1)
                jobenv[sys.intern(name)] = sys.intern(val)

            if self.k_JOBSUBJOBID in jobenv:
                jobsub_job_id = jobenv[self.k_JOBSUBJOBID]
            else:
                jobsub_job_id = '%s.%s@%s' % (
                    jobenv['CLUSTER'],
                    jobenv['PROCESS'],
                    jobenv['SCHEDD']
                  )

            jobsub_job_id = sys.intern(jobsub_job_id.strip())

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

                #if self.debug: print("jobenv is: ", jobenv)

                args = {
                    self.k_jobsub_job_id : jobsub_job_id,
                    self.k_task_id : jobenv['POMS_TASK_ID'],
                    self.k_status : self.map[jobenv['JOBSTATUS']],
                    self.k_restarts : jobenv['NumRestarts'],
                    self.k_node_name : host, 
                    self.k_host_site : jobenv.get('GLIDEIN_SITE', ''),
                    self.k_task_project : jobenv.get('SAM_PROJECT_NAME',jobenv.get('SAM_PROJECT',None)),
                    self.k_cpu_time : jobenv.get('RemoteUserCpu'),
                    self.k_reason_held : jobenv.get('HoldReason'),
                    self.k_wall_time : wall_time,
                    self.k_task_recovery_tasks_parent: jobenv.get('POMS_PARENT_TASK_ID',None),
                }

                prev = self.prev_report.get(jobsub_job_id, None)
                self.cur_report[jobsub_job_id] = args
                self.last_known_status[jobsub_job_id] = args['status']

                #
                # only report status if its different
                #
                if not prev or prev['status'] != args['status'] or prev['node_name'] != args['node_name'] or prev['cpu_time'] != args['cpu_time'] or prev['wall_time'] != args['wall_time'] or prev['task_project'] != args['task_project']:
                    try: 
                        self.job_reporter.report_status(**args)
                    except KeyboardInterrupt:
                        raise
                    except Exception as e:
                        print("Reporting Exception!\n",file=sys.stderr)
                        traceback.print_exc(file=sys.stderr)
                else:
                    if self.debug: 
                         print("unchanged, not reporting\n", file=sys.stderr)
                         print("prev", prev, "\n",file=sys.stderr)
                         print("args", args, "\n",file=sys.stderr)
                          
            else:
                #print "skipping:" , line
                pass

        f.close()

        res = p.wait()

        if res == 0 or res == None:
            for jobsub_job_id in list(self.jobmap.keys()):
                if self.jobmap[jobsub_job_id] == 0 and self.prevjobmap.get(jobsub_job_id,0) == 0:
                    # it is in the database, but not in our output, 
                    # nor in the previous output, we conclude it's completed.
                    # we could get a false alarm here if condor_q fails...
                    # thats why we only do this if our p.wait() returned 0/None.
                    # and we make sure we didn't see it two runs in a row...
                    if self.last_known_status.get(jobsub_job_id,"") == 'Held':
                         report_as = "Removed"
                    else:
                         report_as = "Completed"
                    print("reporting %s as %s \n" % (jobsub_job_id,report_as), file=sys.stderr)

                    self.job_reporter.report_status(
                        jobsub_job_id = jobsub_job_id,
                        task_id = self.tidmap[jobsub_job_id],
                        status = report_as)

                    if self.last_known_status.get(jobsub_job_id,""):
                        del self.last_known_status[jobsub_job_id]

        else:
            print("error code: %s from condor_q" % res)

        #self.call_wrapup_tasks()


    def poll(self):
        while(1):
            print("top of poll", file=sys.stderr)
            sys.stderr.flush()
            
            self.passcount = self.passcount + 1

            # just restart periodically, so we don't eat memory, etc.
            #if self.passcount > 1000:
            #    os.execvp(sys.argv[0], sys.argv)

            self.threadCount.set(threading.active_count())

            try:
                self.scan()
                print("out scan", file=sys.stderr)
                sys.stderr.flush()
                         
            except KeyboardInterrupt:
                raise
 
            except OSError as e:
                print("Exception!\n", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                # if we're out of memory, dump core...
                #if e.errno == 12:
                #    resource.setrlimit(resource.RLIMIT_CORE,resource.RLIM_INFINITY)
                #    os.abort()

            except Exception as e:
                print("Exception!", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

            print("out of try", file=sys.stderr)
            sys.stderr.flush()

            gc.collect()

            sys.stderr.write("%s pausing...\n" % time.asctime())
            sys.stderr.flush()
            time.sleep(120)
            sys.stderr.write("%s done...\n" % time.asctime())
            sys.stderr.flush()

            sys.stdout.flush()

# don't barf if we need to log utf8...
#import codecs
#sys.stdout = codecs.getwriter('utf8')(sys.stdout)

if __name__ == '__main__':
    debug = 0
    testing = 0

    if len(sys.argv) > 1 and sys.argv[1] == "-d":
        debug=1
        sys.argv = [sys.argv[0]] + sys.argv[2:]

    server = "http://localhost:8080/poms"
    nthreads = 8

    if len(sys.argv) > 1 and sys.argv[1] in ["-t", "-o"]:
        testing = 1

        if sys.argv[1]  == "-t":
            nthreads = 1
            server = "http://127.0.0.1:8888/poms"

        sys.argv = [sys.argv[0]] + sys.argv[2:]

    ns = "profiling.apps.poms.probes.%s.jobsub_q_scraper" % os.uname()[1].split(".")[0]
    jr = job_reporter(server, debug=debug, namespace=ns, nthreads=nthreads)
    js = jobsub_q_scraper(jr, debug = debug)
    try:
        if testing:
            print("test mode, run one scan\n")
            js.scan()
            print("test mode: done\n")
        else:
            js.poll()

    except KeyboardInterrupt:
        from pympler import summary, muppy
        sum1 = summary.summarize(muppy.get_objects())
        summary.print_(sum1)

    jr.cleanup()
    print("end of __main__")
