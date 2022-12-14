#!/usr/bin/env python
"""
  This script is used as a last resort - if and when landscape is not functioning.
  Please see submission_agen.py for normal operations.
"""

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
from subprocess import Popen, PIPE
import logging

logit = logging.getLogger()

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

    def __init__(self, debug=0, poms_uri="http://127.0.0.1:8080/poms/"):

        self.poms_uri = poms_uri
        gc.enable()

        self.statusmap = {"0": "Unexpanded", "1": "Idle", "2": "Running", "3": "Removed", "4": "Completed", "5": "Held", "6": "Cancelled"}

        self.psess = requests.Session()
        self.known_submissions = {}
        self.jobsub_q_cmd = "condor_q -pool gpcollector03.fnal.gov -global -constraint 'regexp(\".*POMS_TASK_ID=.*\",Env)' -format '%s;JOBSTATUS=' Env -format '%d;CLUSTER=' Jobstatus -format '%d;PROCESS=' ClusterID -format '%d;' ProcID -format 'GLIDEIN_SITE=%s;' MATCH_EXP_JOB_GLIDEIN_Site -format 'REMOTEHOST=%s;' RemoteHost -format 'NumRestarts=%d;' NumRestarts -format 'HoldReason=%.30s;' HoldReason -format 'RemoteUserCpu=%f;' RemoteUserCpu  -format 'EnteredCurrentStatus=%d;' EnteredCurrentStatus -format 'RemoteWallClockTime=%f;' RemoteWallClockTime -format 'Args=\"%s\";' Args -format 'JOBSUBJOBID=%s;' JobsubJobID -format 'xxx=%d\\n' ProcID"

    def update_submission(self, submission_id, jobsub_job_id, project=None, status=None):
        logit.info(
            "update_submission: %s"
            % repr({"submission_id": submission_id, "jobsub_job_id": jobsub_job_id, "project": project, "status": status})
        )

        # for submissions, just give the cluster
        if jobsub_job_id.find(".") > 0:
            jobsub_job_id = jobsub_job_id[: jobsub_job_id.find(".")] + jobsub_job_id[jobsub_job_id.find("@") :]

        for i in range(4):
            try:
                r = self.psess.post(
                    "%s/update_submission" % self.poms_uri,
                    {"submission_id": submission_id, "jobsub_job_id": jobsub_job_id, "project": project, "status": status},
                    verify=False,
                )
                r.raise_for_status()
                break
            except requests.exceptions.ConnectionError:
                logit.error("Connection Reset!")
            except Exception(e):
                logit.error("Exception: %s" % e)
                logit.error(r.text)
            time.sleep(2 ** i)

        if r.text != "Ok.":
            logit.error("update_submission: Failed.")

    def scan(self):

        """
             Loop through jobsub output, update task info as we look at
             individual jobs.
             Then loop through tasks, report missing ones as completed
             Loop through seen tasks, report changed ones
        """

        p = Popen(self.jobsub_q_cmd, shell=True, stdout=PIPE, close_fds=True, universal_newlines=True)
        f = p.stdout

        pass_submissions = {}

        jobenv = JobEnv()

        for line in f:

            line = line.rstrip("\n")

            # if self.debug: print("saw line: " , line)

            del jobenv
            jobenv = JobEnv()
            for evv in line.split(";"):
                name, val = evv.split("=", 1)
                jobenv[sys.intern(name)] = sys.intern(val)

            if "JOBSUBJOBID" in jobenv:
                jobsub_job_id = jobenv["JOBSUBJOBID"]
            else:
                jobsub_job_id = "%s.%s@%s" % (jobenv["CLUSTER"], jobenv["PROCESS"], jobenv["SCHEDD"])

            if "SAM_PROJECT_NAME" not in jobenv and "Args" in jobenv and jobenv["Args"].find("--sam_project") > 0:
                spv = jobenv["Args"][jobenv["Args"].find("--sam_project") + 14 :]
                spv = spv[0 : spv.find(" ")]
                jobenv["SAM_PROJECT_NAME"] = spv

            d = pass_submissions.get(jobenv.get("POMS_TASK_ID", ""), {})

            # we want the min jobsub job_id for the POMS_TASK_ID...
            # either the cluster leader or the dagman...
            # turns out string compare mostly works..
            if jobsub_job_id < d.get("jobsub_job_id", "zzzzzzzzzz"):
                d["jobsub_job_id"] = jobsub_job_id

            if "SAM_PROJECT_NAME" in jobenv:
                d["project"] = jobenv["SAM_PROJECT_NAME"]

            if "JOBSTATUS" in jobenv:
                if jobenv["JOBSTATUS"] > d.get("status", "0") and d.get("status", "0") != "2" or jobenv["JOBSTATUS"] == "2":
                    d["status"] = jobenv["JOBSTATUS"]

            pass_submissions[jobenv.get("POMS_TASK_ID", "")] = d

        f.close()
        res = p.wait()

        #
        # if we don't see it anymore, mark it completed
        #
        dellist = []
        for submission_id in self.known_submissions:
            if not submission_id in pass_submissions:
                d = self.known_submissions[submission_id]
                self.update_submission(submission_id, d.get("jobsub_job_id", ""), status="Completed")
                dellist.append(submission_id)

        for submission_id in dellist:
            del self.known_submissions[submission_id]

        #
        # report it if changed
        #
        for submission_id in pass_submissions:
            if pass_submissions[submission_id] != self.known_submissions.get(submission_id, None):
                d = pass_submissions[submission_id]
                self.update_submission(
                    submission_id,
                    d.get("jobsub_job_id", ""),
                    status=self.statusmap[d.get("status", "0")],
                    project=d.get("project", ""),
                )

            self.known_submissions[submission_id] = d

    def poll(self):
        while 1:
            print("top of poll", file=sys.stderr)
            sys.stderr.flush()

            try:
                self.scan()

            except KeyboardInterrupt:
                raise

            except OSError as e:
                print("Exception!\n", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)
                # if we're out of memory, dump core...
                # if e.errno == 12:
                #    resource.setrlimit(resource.RLIMIT_CORE,resource.RLIM_INFINITY)
                #    os.abort()

            except Exception as e:
                print("Exception!", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

            sys.stderr.flush()

            gc.collect()

            sys.stderr.write("%s pausing...\n" % time.asctime())
            sys.stderr.flush()
            time.sleep(30)
            sys.stderr.write("%s done...\n" % time.asctime())
            sys.stderr.flush()

            sys.stdout.flush()


if __name__ == "__main__":
    debug = 0
    testing = 0

    requests.packages.urllib3.disable_warnings()

    if len(sys.argv) > 1 and sys.argv[1] == "-d":
        debug = 1
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        logging.basicConfig(level=logging.DEBUG)
        sys.argv = [sys.argv[0]] + sys.argv[2:]
    else:
        logging.basicConfig(level=logging.INFO)

    server = "http://localhost:8080/poms"
    nthreads = 8

    if len(sys.argv) > 1 and sys.argv[1] in ["-t", "-o"]:
        testing = 1

        if sys.argv[1] == "-t":
            nthreads = 1
            server = "http://127.0.0.1:8888/poms"

        sys.argv = [sys.argv[0]] + sys.argv[2:]

    js = jobsub_q_scraper(debug=debug)
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
