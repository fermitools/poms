#!/usr/bin/env python
"""
POMS agent to collect submission info from the Landscape "lens" service
and report it into POMS
"""

import logging
import sys
import os
import time
from http.client import HTTPConnection
import requests
import re
import configparser
import argparse
from datetime import datetime

HTTPConnection.debuglevel = 1

LOGIT = logging.getLogger()

def get_status(entry):
    """
        given a dictionary from the Landscape service,
        return the status for our submission
    """
    if entry["done"] and entry["failed"] * 2 > entry["completed"]:
        return "Failed"
    # Only consider a submission as cancelled if every job within it is cancelled, or if user marks it as cancelled when killing it.
    if entry["done"] and entry["cancelled"] > 1 and (entry["running"] + entry["idle"] + entry["held"] +  entry["failed"] + entry["completed"]) == 0:
        return "Cancelled"
    if entry["done"]:
        return "Completed"
    if entry["held"] > 0:
        return "Held"
    if entry["running"] == 0 and entry["idle"] != 0:
        return "Idle"
    if entry["running"] > 0:
        return "Running"
    return "Unknown"


class Agent:
    """
        Class for the submission reporting agent -- uses the service
        at submission_uri in the __init__() below and get info on
        recent submissions and reports them to POMS
    """

    def __init__(self, config, poms_uri=None, submission_uri=None):

        """
            Setup webservice http session objects, uri's to reach things,
            headers we'll reuse, and status/info dictionaries, and fetch
            our experiment list from POMS
        """
        self.cfg = configparser.ConfigParser()
        self.cfg.read(config)
        
        self.poms_uri = poms_uri if poms_uri else self.cfg.get("submission_agent", "poms_uri")
        self.submission_uri = submission_uri if submission_uri else self.cfg.get("submission_agent", "submission_uri")
        # biggest time window we should ask LENS for
        self.maxtimedelta = 3600
        self.known = {}
        self.known["status"] = {}
        self.known["project"] = {}
        self.known["pct"] = {}
        self.known["maxjobs"] = {}
        self.known["poms_task_id"] = {}
        self.known["jobsub_job_id"] = {}
        self.known["dd_project_id"] = {}
        self.known["not_on_server"] = {}

        self.psess = requests.Session()
        self.ssess = requests.Session()
        self.headers = {
            self.cfg.get("submission_agent", "poms_user_header", fallback='X-Shib-Userid'):
            self.cfg.get("submission_agent", "poms_user", fallback='poms')
        }
        self.psess.headers.update(self.headers)
        self.submission_headers = {
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Connection": "keep-alive",
            "DNT": "1",
            "Origin": self.cfg.get("submission_agent", "lens_server"),
        }
        self.ssess.headers.update(self.submission_headers)

        # last_seen[group] is set of poms task ids seen last time
        self.last_seen = {}
        self.timeouts = (300, 300)
        self.strikes = {}
        self.poll_interval = 120

        htr = self.psess.get("%s/experiment_list"%self.poms_uri)
        self.elist = htr.json()
        self.elist.sort()
        htr.close()
        self.lastconn = {}
        dd_status_map = {
            "Submitted Pending Start":"Idle" ,
            "Completed with failures": "Completed" ,
            "Failed to Launch":"LaunchFailed",
            "Cancelled":"Cancelled",
        }
        
        
    def update_submissions(self, submissions):

        """
            actually report information on a submission to POMS
        """
        LOGIT.info("Update Submissions: %s" % submissions)
        try:
            sess = requests.session()
            htr = sess.post(
                url="%s/update_submission" % self.poms_uri, 
                data=submissions,
                headers=self.headers,
                timeout=self.timeouts,
                verify=False,
            )

        except requests.exceptions.ConnectionError:
            LOGIT.exception("Connection Reset! NOT Retrying once...")

        if htr.text != "Ok.":
            LOGIT.error("update_submission: Failed.")
            LOGIT.error(htr.text)
            
        not_on_server = htr.json()
        for submission_id in not_on_server:
            self.known["not_on_server"][submission_id] = True
        htr.close()

    def update_submission(self, submission_id, jobsub_job_id, pct_complete=None, project=None, status=None):
        """
            actually report information on a submission to POMS
        """
        LOGIT.info(
            "update_submission: %s",
            repr(
                {
                    "submission_id": submission_id,
                    "jobsub_job_id": jobsub_job_id,
                    "project": project,
                    "pct_complete": pct_complete,
                    "status": status,
                }
            ),
        )
        try:
            sess = requests.session()
            htr = sess.post(
                "%s/update_submission" % self.poms_uri,
                {
                    "submission_id": submission_id,
                    "jobsub_job_id": jobsub_job_id,
                    "project": project,
                    "status": status,
                    "pct_complete": pct_complete,
                },
                headers=self.headers,
                timeout=self.timeouts,
                verify=False,
            )

        except requests.exceptions.ConnectionError:
            LOGIT.exception("Connection Reset! NOT Retrying once...")
            # -- *not* retrying, as it seems these errors are generally
            #    spurious
            # htr = self.psess.post("%s/update_submission" % self.poms_uri,
            #                      {
            #                          'submission_id': submission_id,
            #                          'jobsub_job_id': jobsub_job_id,
            #                          'project': project,
            #                          'status': status,
            #                          'pct_complete': pct_complete
            #                      },
            #                      timeout=self.timeouts,
            #                      verify=False)

        if htr.text != "Ok.":
            LOGIT.error("update_submission: Failed.")
            LOGIT.error(htr.text)

        htr.close()

    def get_individual_submission(self, jobsubjobid):
        """
           get submission info from service if the submissions call had
           an error.
        """

        if jobsubjobid == None:
            return None

        postresult = self.ssess.post(
            self.submission_uri,
            data=self.cfg.get("submission_agent", "submission_info_query") % jobsubjobid,
            timeout=self.timeouts,
        )
        ddict = postresult.json()
        LOGIT.info("individual submission %s data: %s", jobsubjobid, repr(ddict))
        postresult.close()

        if ddict.get("errors", None) != None:
            return None

        ddict = ddict.get("data", {}).get("submission", None)

        return ddict

    def get_project(self, entry):

        """
           get project info from service if we don't have it
           -- it could be in environment information or command line args
        """

        # check if we already know it...

        res = self.known["project"].get(entry["pomsTaskID"], None)
        if res:
            LOGIT.info("already knew project for %s: %s", entry["pomsTaskID"], res)
            return res

        # otherwise look it up... in the submission info

        postresult = self.ssess.post(
            self.submission_uri,
            data=self.cfg.get("submission_agent", "submission_project_query") % entry["id"],
            timeout=self.timeouts,
        )
        ddict = postresult.json()
        ddict = ddict["data"]["submission"]
        LOGIT.info("data: %s", repr(ddict))
        postresult.close()
        
         # it looks like we should do this, to update our cache, *but* we
        # need to defer it for the logic in check_submissions() below,
        # otherwise we'll never report it...
        # self.known['project'][entry['pomsTaskID']] = res
           
        return self.check_entry_for_project(ddict, True)

       
    
    def check_entry_for_project(self, entry):
        if self.known["project"].get(entry["pomsTaskID"], None):
            return self.known["project"].get(entry["pomsTaskID"])
        project = None
        if entry.get("args", None):
            pos1 = entry["args"].find("--sam_project")
            if pos1 > 0:
                LOGIT.info("saw --sam_project in args")
                pos2 = entry["args"].find(" ", pos1 + 15)
                project = entry["args"][pos1 + 14 : pos2]
                LOGIT.info("got: %s", project)
            pos1 = entry["args"].find("--project_name")
            if pos1 > 0:
                LOGIT.info("saw --project_name in args")
                pos2 = entry["args"].find(" ", pos1 + 15)
                project = entry["args"][pos1 + 15 : pos2]
                LOGIT.info("got: %s", project)
        if not project and entry.get("SAM_PROJECT_NAME", None):
            project = entry["SAM_PROJECT_NAME"]
        if not project and entry.get("SAM_PROJECT", None):
            project = entry["SAM_PROJECT"]
        if project:
            LOGIT.info("found project for %s: %s", entry["pomsTaskID"], project)
            return project
        return project
            

    def maybe_report(self, entry):

        if entry == None or entry['pomsTaskID'] == None:
            return

        if entry["done"] == self.known["status"].get(entry["pomsTaskID"], None):
            report_status_flag = False
        else:
            report_status_flag = True

        report_status = get_status(entry)

        self.known["jobsub_job_id"][entry["pomsTaskID"]] = entry["id"]
        
        ntot = (int(entry["running"]) + int(entry["idle"]) + 
                int(entry["held"]) + int(entry["completed"]) + 
                int(entry["failed"]) + int(entry["cancelled"]))

        if ntot >= self.known["maxjobs"].get(entry["pomsTaskID"], 0):
            self.known["maxjobs"][entry["pomsTaskID"]] = ntot
        else:
            ntot = self.known["maxjobs"][entry["pomsTaskID"]]

        ncomp = int(entry["completed"]) + int(entry["failed"]) + int(entry["cancelled"])

        if ntot > 0:
            report_pct_complete = ncomp * 100.0 / ntot
        else:
            report_pct_complete = None

        if report_pct_complete == self.known["pct"].get(entry["pomsTaskID"], None):
            report_pct_complete_flag = False
        else:
            report_pct_complete_flag = True

        report_project = self.check_entry_for_project(entry)
        if report_project == self.known["project"].get(entry["pomsTaskID"], None):
            report_project_flag = False
        else:
            report_project_flag = True

        #
        # actually report it if there's anything changed...
        #
        if report_status_flag or report_project_flag or report_pct_complete_flag:
            submission_update = {
                "submission_id":entry["pomsTaskID"],
                "jobsub_job_id":entry["id"],
                "pct_complete":report_pct_complete,
                "project":report_project,
                "status":report_status,
            }
        else:
            submission_update = None

        #
        # now update our known status if available
        #
        self.known["poms_task_id"][entry["pomsTaskID"]] = entry["id"]
        if entry["pomsTaskID"] not in self.known["status"] or report_status:
            self.known["status"][entry["pomsTaskID"]] = entry["done"]

        if entry["pomsTaskID"] not in self.known["project"] or report_project:
            self.known["project"][entry["pomsTaskID"]] = report_project

        if entry["pomsTaskID"] not in self.known["pct"] or report_pct_complete:
            self.known["pct"][entry["pomsTaskID"]] = report_pct_complete
        
        return submission_update
    
    
    def maybe_report_data_dispatcher(self, entry, key, val):
        
        dd_status, report_status = self.get_dd_status(entry, val)
        
        submission_update = {
                "submission_id": entry["pomsTaskID"],
                "jobsub_job_id":entry["id"],
                "dd_project_id": key,
            }
        update = False
        
        if val != self.known["dd_project_id"].get(entry["pomsTaskID"], None):
            self.known["dd_project_id"][entry["pomsTaskID"]] = key
            update = True
        
        if entry("id", None) and entry["id"] != self.known["jobsub_job_id"].get(entry["pomsTaskID"], None):
            self.known["jobsub_job_id"][entry["pomsTaskID"]] = entry["id"]
            update = True
            
        self.known["poms_task_id"][entry["pomsTaskID"]] = entry["id"]
        
        if dd_status != self.known["status"].get(entry["pomsTaskID"], None):
            self.known["status"][entry["pomsTaskID"]] = dd_status
            submission_update["dd_status"] = dd_status
            submission_update["status"] = report_status
            update = True

        if val != self.known["pct"].get(entry["pomsTaskID"], None):
            self.known["pct"][entry["pomsTaskID"]] = val
            submission_update["pct_complete"] = val
            update = True
        
        if update:
            return submission_update
        else:
            return None

    def get_running_submissions_LENS(self, group, since):

        ddict = {}
        if time.time() - self.lastconn.get(group, 0) > self.maxtimedelta:
            # last info was too long ago, just clear it
            if self.lastconn.get(group, None):
                del self.lastconn[group]

        if since:
            LOGIT.info("check_submissions: since %s", since)
            since = ', from: \\"%s\\", to:\\"now\\"' % since
        elif self.lastconn.get(group, None):
            since = ', from: \\"%s\\", to:\\"now\\"' % time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(self.lastconn[group] - 2*self.poll_interval))
        else:
            since = ''

        if group == "samdev":
            group = "fermilab"
        try:
            # keep track of when we started
            start = time.time()
            htr = self.ssess.post(
                self.submission_uri,
                data=self.cfg.get("submission_agent", "running_query") % (group, since),
                timeout=self.timeouts,
            )
            ddict = htr.json()
            htr.close()
            # only remember it if we succeed...
            self.lastconn[group] = start
        except requests.exceptions.RequestException as r:
            LOGIT.info("connection error for group %s: %s", group, r)
            ddict = {}
            pass

        print( "ddict: %s" % repr(ddict))
        if ddict:
            return ddict.get("data",{}).get("submissions",[])
        else:
            return []

    def get_running_submissions_POMS(self, group):
        url = self.cfg.get("submission_agent", "poms_running_query")
        try:
            htr = self.psess.get(url)
            flist = htr.json()
            print("poms running_submissions: ", repr(flist))
            ddict = [ {'pomsTaskID': x[0], 'id': x[1], "POMS_DATA_DISPATCHER_PROJECT_ID": x[3]} for x in flist if x[2] == group]
            print("poms running_submissions for " , group,  ": ", repr(ddict))
            htr.close()
            return ddict
        except:
            logging.exception("running_submissons_POMS")
            return {}
        
    def get_all_running_submissions_POMS(self, exp_list):
        url = self.cfg.get("submission_agent", "poms_running_query")
        try:
            htr = self.psess.get(url)
            flist = htr.json()
            print("poms running_submissions: ", repr(flist))
            ddict = [ {'pomsTaskID': x[0], 'id': x[1], "POMS_DATA_DISPATCHER_PROJECT_ID": x[3]} for x in flist if x[2] in exp_list]
            print("poms running_submissions for all experiments: %s" % repr(ddict))
            htr.close()
            return ddict
        except:
            logging.exception("running_submissons_POMS")
            return {}
    
    def get_dd_project_statuses(self, project_ids):
        start = datetime.now()
        url = self.cfg.get("submission_agent", "poms_dd_complete_query_all") % ",".join(project_ids)
        try:
            LOGIT.info("getting poms data dispatcher project statuses for projects: %s", project_ids)
            htr = self.psess.get(url)
            dd_statuses = htr.json()
            elapsed_time = datetime.now() - start
            LOGIT.info("got poms data dispatcher project statuses for projects: %s | Elapsed Time: %s.%s seconds" % (repr(dd_statuses), elapsed_time.seconds, elapsed_time.microseconds))
            htr.close()
            return dd_statuses
        except:
            logging.exception("get_dd_project_statuses")
            return {}
        

    def check_submissions(self, group=None, full_list=None, since=""):
        """
            get submission info from Landscape for a given group
            update various known bits of info
        """

        LOGIT.info("check_submissions: %s", group)

        thispass = set()
        all_submissions = []
        sublist = []
        all_task_ids = {}
        no_task_id = []
        jid_sub_id = 1
        if full_list:
            for exp in full_list:
                all_submissions.extend(self.get_running_submissions_LENS(exp, since))
            all_submissions.extend(self.get_all_running_submissions_POMS(full_list))
            for x in all_submissions:
                if not x.get("pomsTaskID", None) and x.get("id", None):
                    all_task_ids[jid_sub_id * -1] = x.get("id")
                    jid_sub_id += 1
                elif x.get("pomsTaskID", None):
                  all_task_ids[x.get("pomsTaskID")] = x.get("id", None)
            LOGIT.info("All task ids: %s" % all_task_ids)
        elif group:
            sublist = self.get_running_submissions_LENS(group, since)
            sublist.extend(self.get_running_submissions_POMS(group))
            LOGIT.info("%s data: %s", group, repr(sublist))
            sublist.sort(key=(lambda x: x.get("pomsTaskID", "")))
            all_task_ids = {x.get("pomsTaskID"):x.get("id", None) for x in sublist if x.get("pomsTaskID", None)}
        
        start = datetime.now()
        LOGIT.info("Attempting to get data on all running jobs")
        
        if full_list:
            submissions, jobs, job_index = self.get_all_submissions(all_task_ids)
            elapsed_time = datetime.now() - start
            LOGIT.info("Got data on all running jobs | elapsed time: %s.%s seconds" % (elapsed_time.seconds, elapsed_time.microseconds))
            submissions_to_update = {}
            dd_project_entries_to_check = {}
            for entry in submissions.values():
                if not entry:
                    continue
                if "pomsTaskID" not in entry:
                    continue
                
                pomsTaskID = entry.get('pomsTaskID')
                job = jobs.get(job_index.get(pomsTaskID), None) if pomsTaskID in job_index else None
                if job:
                    entry["job"] = job
                    entry["id"] = job["id"]
                    entry["status"] = job["status"]
                    id = entry["id"]
                    LOGIT.info("Found job for submission: %d | details: %s" % (pomsTaskID, job))
                else:
                    entry["id"] = self.fix_job_id_if_valid(entry.get('id'))
                    id = entry["id"]
                    
                thispass.add(pomsTaskID)
                
                    
                if self.known["status"].get(id, None) == "Completed":
                    continue
                
                do_sam = entry.get("POMS_DATA_DISPATCHER_PROJECT_ID",'') == ''
                    
                if do_sam:
                    update_submission = self.maybe_report(entry)
                    if update_submission:
                        submissions_to_update[pomsTaskID] = update_submission
                else:
                    LOGIT.info("Added dd_project: %s" % entry["POMS_DATA_DISPATCHER_PROJECT_ID"])
                    dd_project_entries_to_check[entry["POMS_DATA_DISPATCHER_PROJECT_ID"]] = entry
                    
            if len(dd_project_entries_to_check) > 0:
                project_ids = list(dd_project_entries_to_check.keys())
                dd_project_statuses = self.get_dd_project_statuses(project_ids)
                for key, val in dd_project_statuses.items():
                    if key == "project_ids":
                        continue
                    entry = dd_project_entries_to_check.get(key)
                    update_submission = self.maybe_report_data_dispatcher(entry, key, val)
                    if update_submission:
                        submissions_to_update[pomsTaskID] = update_submission
                
            if len(submissions_to_update) > 0:
                self.update_submissions(submissions_to_update)
    
    
                
        
        #for entry in sublist:
#
        #    # skip if we don't have a pomsTaskID...
        #    if not entry.get("pomsTaskID", None):
        #        continue
#
        #    # don't get confused by duplicate listings
        #    if entry.get("pomsTaskID") in thispass:
        #        continue
#
        #    thispass.add(entry.get("pomsTaskID"))
#
        #    id = entry.get('id')
        #    pomsTaskID = entry.get('pomsTaskID')
#
        #    if self.known["status"].get(id,None) == "Completed":
        #        continue
#
        #    fullentry = self.get_individual_submission(id)
#
        #    if fullentry and 'data' in fullentry:
        #         fullentry = fullentry['data'] 
#
        #    self.maybe_report(fullentry)
#
        #    if fullentry and fullentry["done"]:
        #        self.update_submission(pomsTaskID, jobsub_job_id=id, status=get_status(fullentry))
#
        #self.last_seen[group] = thispass
                
    def get_dd_status(self, entry, dd_pct):
        
        status = entry.get("status", None)
        if entry["done"]:
            if entry["error"] or dd_pct < 80:
                return "Completed with failures", self.dd_status_map.get("Completed with failures")
            if entry["cancelled"] > 1 and (entry["running"] + entry["idle"] + entry["held"] +  entry["failed"] + entry["completed"]) == 0:
                return "Cancelled", "Cancelled"
            return "Completed", "Completed"
        else:
            if entry["id"] or self.known["jobsub_job_id"][entry["pomsTaskId"]]:
                if entry["held"] > 0:
                    return "Held", "Held"
                if entry["idle"]:
                    return "Idle", "Idle"
                if dd_pct > 0 and dd_pct < 100:
                    return "Running", "Running"
                if dd_pct == 0:
                    return "Submitted Pending Start", self.dd_status_map.get("Submitted Pending Start")
                if dd_pct == 100:
                    return "Completed", "Completed"
            else:
                unknown_job = self.known["unknown_jobs"].get(entry["pomsTaskId"], 0)
                if unknown_job < 5:
                    unknown_job += 1
                    self.known["unknown_jobs"][entry["pomsTaskId"]] = unknown_job
                    return "Attempting to get Job Id (Attempt: %d of 5)" % unknown_job, "Unknown"
                else:
                    return "Failed to Launch", self.dd_status_map.get("Failed to Launch")
                    
        
        
        
    def fix_job_id_if_valid(self, job_id):
        if ".0@" in job_id:
            return job_id
        return ".0@".join(job_id.split("@"))
    
    def get_all_submissions(self, task_jobs):
        job_query_list = []
        submission_query_list = []
        submission_index = {}
        job_index = {}
        
        i = 0
        for task_id, job_id in task_jobs.items():
            job_id = task_jobs.get(task_id, None)
            if job_id:
                job_id = self.fix_job_id_if_valid(job_id)
                job_query_list.append("j%d:%s" % (i, self.cfg.get("submission_agent", "append_job") % job_id))
                job_index[task_id] = "j%d" % i
            
            if task_id < 0:
                submission_query_list.append("s%d:%s" % (i, self.cfg.get("submission_agent", "append_submission_jid") % self.fix_job_id_if_valid(job_id)))
            else:
                submission_query_list.append("s%d:%s" % (i, self.cfg.get("submission_agent", "append_submission_sid") % task_id))
            submission_index["s%d" % i] = task_id
            i += 1
        
        submissions_query = self.cfg.get("submission_agent", "all_jobs_query_base") % ",".join(submission_query_list)
        submissions_results = self.ssess.post(
            self.submission_uri,
            data=submissions_query,
            timeout=self.timeouts,
        )
        submissions_dict = submissions_results.json()
        submissions_results.close()
        
        
        if submissions_dict.get("errors", None) != None:
            LOGIT.info("All Submissions Response yielded some errors: %s" % submissions_dict.get("errors"))
        
        if not submissions_dict.get("data"):
            LOGIT.info("All Submissions Response yielded no results")
            return None
        
        for entry in submissions_dict.get("data").values():
            if "jobs" not in entry and "id" in entry and ".0@" not in entry.get("id"):
                job_id = job_index.get(entry.get("pomsTaskId"), None)
                if not job_id:
                    job_id = self.fix_job_id_if_valid(entry.get("id"))
                    job_query_list.append("j%d:%s" % (i, self.cfg.get("submission_agent", "append_job") % job_id))
                    job_index[entry.get("pomsTaskId")] = "j%d" % i
                    
        jobs_query = self.cfg.get("submission_agent", "all_jobs_query_base") % ",".join(job_query_list)
        jobs_results = self.ssess.post(
            self.submission_uri,
            data=jobs_query,
            timeout=self.timeouts,
        )
        jobs_dict = jobs_results.json()
        jobs_results.close()
        
        if jobs_dict.get("errors", None) != None:
            LOGIT.info("All Jobs Response yielded some errors: %s" % jobs_dict.get("errors"))
            
        if not jobs_dict.get("data"):
            LOGIT.info("All Jobs Response yielded no results")
            return None

        return submissions_dict["data"], jobs_dict["data"], job_index

    def poll(self, since=""):
        """
           Operate as a daemon, poll service and update every 30 sec or so
        """
        while 1:
            try:
                self.check_submissions(full_list=self.elist, since=since)
                #for exp in self.elist:
                #    self.check_submissions(exp, since=since)
            except:
                LOGIT.exception("Exception in check_submissions")
            time.sleep(self.poll_interval)
            since = ""


def main():
    """
       mainline --handle command line parameters and
           instantiate an Agent object.
    """

    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", default="./submission_agent.cfg")
    ap.add_argument("-d", "--debug", action="store_true")
    ap.add_argument("--since", type=str)
    ap.add_argument("-t", "--test", action="store_true", default=False)
    ap.add_argument("-T", "--one-time", action="store_true", default=False)

    args = ap.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(filename)s:%(lineno)s:%(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(filename)s:%(lineno)s:%(message)s")

    if args.test:
        agent = Agent(poms_uri="http://127.0.0.1:8080", submission_uri=os.environ["SUBMISSION_INFO"], config=args.config)
        for exp in agent.elist:
            agent.check_submissions(exp, since=args.since)
    elif args.one_time:
        agent = Agent(config=args.config)
        for exp in agent.elist:
            agent.check_submissions(exp, since=args.since)
    else:
        agent = Agent(config=args.config)
        agent.poll(since=args.since)


main()
