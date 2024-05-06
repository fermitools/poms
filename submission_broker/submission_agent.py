#!/usr/bin/env python
"""
POMS agent to collect submission info from the Landscape "lens" service
and report it into POMS
"""

import logging
import os
import time
import configparser
import json
import argparse
import uuid

import requests

from datetime import datetime
from http.client import HTTPConnection
from helper_functions import *
from local_queue import SubmissionQueue


HTTPConnection.debuglevel = 1




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
        self.known["dd_status"] = {}
        self.known["project"] = {}
        self.known["pct"] = {}
        self.known["maxjobs"] = {}
        self.known["poms_task_id"] = {}
        self.known["jobsub_job_id"] = {}
        self.known["dd_task_id"] = {}
        self.known["dd_project_id"] = {}
        
        self.queue = SubmissionQueue(self.cfg, str(uuid.uuid4()))
        self.psess = requests.Session()
        self.ssess = requests.Session()
        self.headers = {
            self.cfg.get("submission_agent", "poms_user_header", fallback='X-Shib-Userid'):
            self.cfg.get("submission_agent", "poms_user", fallback='poms'),
            "X-Poms-Agent": self.queue.agent_header
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
        
        self.dd_status_map = {
            "Submitted Pending Start":"Idle" ,
            "Completed with failures": "Completed" ,
            "Failed to Launch":"LaunchFailed",
            "Cancelled":"Cancelled",
        }
        self.queued_submissions = None
        self.running_in_queue = None
        self.job_to_sub_index = None
        self.sub_to_job_index = None
        self.query_results = None
        self.completed_submissions = None 
        self.ignored = None
        self.queue.set_session(self.psess, self.elist)
       
        
        
    def update_submissions(self, submissions):

        """
            actually report information on a submission to POMS
        """
        record_queue_log("Attempting to update submissions: %s" % list(submissions.keys()))
        record_queue_log("Values: %s" % list(submissions.values()))
        record_queue_log("Begin call")
        start = datetime.now()
        try:
            sess = requests.session()
            htr = sess.post(
                url="%s/update_submissions" % (self.poms_uri), 
                data={
                        "submission_updates": json.dumps(submissions)
                    },
                headers=self.headers,
                timeout=self.timeouts,
                verify=False,
            )

        except requests.exceptions.ConnectionError:
            record_queue_log("Connection Reset! NOT Retrying once...", level="exception")

            
        response = htr.json()
        elapsed_time = datetime.now() - start
        record_queue_log("Recieved response from poms", {"status": response.get("status", "Failed"), "request_duration": f"{elapsed_time.seconds}.{elapsed_time.microseconds} seconds"})
        if response and response.get("status") == "Success":
            statuses = response.get("response")
            success = []
            print("Statuses: %s" % statuses )
            for submission_id, is_on_server_server in statuses.items():
                if not is_on_server_server:
                    jobid = submissions.get(int(submission_id)).get("jobsub_job_id", None)
                    if str(submission_id) not in self.ignored["submissions"]:
                        self.ignored["submissions"].add(submission_id)
                    if jobid and jobid not in self.ignored["jobs"]:
                        self.ignored["jobs"].add(jobid)
                        
                else:
                    success.append(submission_id)
            
            record_queue_log("Updated %s %s" % (len(success), "submission" if len(success) == 1 else "submissions"), updated=success, not_found=len(submissions) - len(success))
            record_queue_log("Not found on server: %s" % (len(submissions) - len(success)))
                
            if len(self.ignored["submissions"]) > 0 or len(self.ignored["jobs"]) > 0:
                record_queue_log("Ignoring %d submission(s) & %s job_id(s)" % (len(self.ignored["submissions"]), len(self.ignored["jobs"])))
                
                
            self.queue.store_ignored(self.ignored)
        htr.close()

    def update_submission(self, submission_id, jobsub_job_id, pct_complete=None, project=None, status=None):
        """
            actually report information on a submission to POMS
        """
        record_queue_log(
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
            record_queue_log("Connection Reset! NOT Retrying once...", level="exception")

        if htr.text != "Ok.":
            record_queue_log("Failed: %s" % htr.text, level="error")
            
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
        record_queue_log("Individual Submission Response: %s" % repr(ddict), job_id=jobsubjobid)
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
            record_queue_log("Project already being tracked: %s" % res, task_id=entry["pomsTaskID"])
            return res

        # otherwise look it up... in the submission info

        postresult = self.ssess.post(
            self.submission_uri,
            data=self.cfg.get("submission_agent", "submission_project_query") % entry["id"],
            timeout=self.timeouts,
        )
        ddict = postresult.json()
        ddict = ddict["data"]["submission"]
        record_queue_log("Received Data", **ddict)
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
                record_queue_log("saw --sam_project in args")
                pos2 = entry["args"].find(" ", pos1 + 15)
                project = entry["args"][pos1 + 14 : pos2]
                record_queue_log("got: %s" % project)
            pos1 = entry["args"].find("--project_name")
            if pos1 > 0:
                record_queue_log("saw --project_name in args")
                pos2 = entry["args"].find(" ", pos1 + 15)
                project = entry["args"][pos1 + 15 : pos2]
                record_queue_log("got: %s" % project)
        if not project and entry.get("SAM_PROJECT_NAME", None):
            project = entry["SAM_PROJECT_NAME"]
        if not project and entry.get("SAM_PROJECT", None):
            project = entry["SAM_PROJECT"]
        if project:
            record_queue_log("found sam project: %s" % project, task_id=entry["pomsTaskID"])
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
        
        ntot = int(entry.get("njobs", (int(entry["running"]) + int(entry["idle"]) + 
                int(entry["held"]) + int(entry["completed"]) + 
                int(entry["failed"]) + int(entry["cancelled"]))))

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
        
        dd_status, report_status, val = self.get_dd_status(entry, val)
        report_status = get_status(entry)
        submission_update = {
                "submission_id": entry["pomsTaskID"],
                "jobsub_job_id":entry["id"],
                "dd_task_id": key,
            }
        update = False
        
        if val != self.known["dd_task_id"].get(entry["pomsTaskID"], None):
            self.known["dd_task_id"][entry["pomsTaskID"]] = key
            update = True
        
        if entry.get("id", None) and entry["id"] != self.known["jobsub_job_id"].get(entry["pomsTaskID"], None):
            self.known["jobsub_job_id"][entry["pomsTaskID"]] = entry["id"]
            update = True
            
        self.known["poms_task_id"][entry["pomsTaskID"]] = entry["id"]
        
        if dd_status and dd_status != self.known["dd_status"].get(entry["pomsTaskID"], None):
            self.known["dd_status"][entry["pomsTaskID"]] = dd_status
            submission_update["dd_status"] = dd_status
            submission_update["status"] = report_status
            update = True

        if val != self.known["pct"].get(entry["pomsTaskID"], None):
            self.known["pct"][entry["pomsTaskID"]] = val
            submission_update["pct_complete"] = val
            update = True
            
        if entry.get("POMS_DATA_DISPATCHER_PROJECT_ID", None) and entry["POMS_DATA_DISPATCHER_PROJECT_ID"] != self.known["dd_project_id"].get(entry["pomsTaskID"], None):
            self.known["dd_project_id"][entry["pomsTaskID"]] = entry["POMS_DATA_DISPATCHER_PROJECT_ID"]
            submission_update["dd_project_id"] = entry["POMS_DATA_DISPATCHER_PROJECT_ID"]
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
            record_queue_log("checking submissions since: %s" % since)
            since = ', from: \\"%s\\", to:\\"now\\"' % since
        elif self.lastconn.get(group, None):
            since = ', from: \\"%s\\", to:\\"now\\"' % time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(self.lastconn[group] - 2*self.poll_interval))
        else:
            since = ''

        if group == "samdev":
            group = "fermilab"
        try:
            # keep track of when we started
            start = datetime.now(utc)
            htr = self.ssess.post(
                self.submission_uri,
                data=self.cfg.get("submission_agent", "running_query") % (group, since),
                timeout=self.timeouts,
            )
            ddict = htr.json()
            ddict = ddict.get("data",{}).get("submissions",[])
            htr.close()
            # only remember it if we succeed...
            start_time = start.time()
            start_time = time.struct_time((0, start_time.hour, start_time.minute, start_time.second, 0, 0, 0, 0, -1))
            start_time = time.mktime(start_time)
            self.lastconn[group] = start_time
            
            elapsed = datetime.now(utc) - start
            record_queue_log(f"Found {len(ddict)} Submissions since {since}", group=group, request_duration=f"{elapsed.seconds}.{elapsed.microseconds} seconds")
        except requests.exceptions.RequestException as r:
            record_queue_log("Connection error for group %s: %s" % (group, r), level="exception")
            ddict = {}
            pass

        if ddict:
            return [submission for submission in ddict 
                    if str(submission.get("pomsTaskID")) not in self.ignored["submissions"] 
                    and self.format_jobid(submission.get("id")) not in self.ignored["jobs"]]
        else:
            return []

    def get_running_submissions_POMS(self, group):
        start = datetime.now(utc)
        record_queue_log("Begin Request", group=group)
        url = self.cfg.get("submission_agent", "poms_running_query")
        try:
            htr = self.psess.get(url)
            flist = htr.json()
            elapsed = datetime.now(utc) - start
            record_queue_log("Response: %s" % flist, group=group, request_duration=f"{elapsed.seconds}.{elapsed.microseconds} seconds")
            ddict = [ {'pomsTaskID': x[0], "group": x[2], 'id': x[1], "POMS_DATA_DISPATCHER_TASK_ID": x[3], "queued_at": str(datetime.now(utc))} for x in flist if x[2] == group]
            queue_items = []
            for item in ddict:
                if item["pomsTaskID"] not in self.completed_submissions:
                    queue_items.append(item)
            if queue_items:
                self.queue.save_queue_data({"queued": queue_items})
                for item in queue_items:
                    record_queue_log("Queued Submission", **item)
            htr.close()
            return ddict
        except:
            logging.exception("running_submissons_POMS")
            return {}
        
    def get_all_running_submissions_POMS(self, exp_list):
        start = datetime.now(utc)
        record_queue_log("Begin Request", group="all")
        url = self.cfg.get("submission_agent", "poms_running_query")
        try:
            htr = self.psess.get(url)
            flist = htr.json()
            "Response: %s" % flist
            elapsed = datetime.now(utc) - start
            record_queue_log("Response: %s" % flist, group=exp_list, request_duration=f"{elapsed.seconds}.{elapsed.microseconds} seconds")
            ddict = [ {'pomsTaskID': x[0],  "group": x[2], 'id': x[1], "POMS_DATA_DISPATCHER_TASK_ID": x[3],  "queued_at": str(datetime.now(utc))} for x in flist if x[2] in exp_list]
            queue_items = []
            for item in ddict:
                if item["pomsTaskID"] not in self.completed_submissions:
                    queue_items.append(item)
            if queue_items:
                self.queue.save_queue_data({"queued": queue_items})
                for item in queue_items:
                    record_queue_log("Queued Submission", **item)
            htr.close()
            return [submission for submission in ddict 
                    if str(submission.get("pomsTaskID")) not in self.ignored["submissions"] 
                    and submission.get("id") not in self.ignored["jobs"]]
        except:
            logging.exception("running_submissons_POMS")
            return {}
    
    def get_dd_task_statuses(self, dd_task_ids):
        record_queue_log("Data Dispatcher Project Status | Begin")
        start = datetime.now()
        parsed_ids = []
        for task_id in dd_task_ids:
            if isinstance(task_id, str):
                task_id = int(task_id)
            if task_id != None:
                parsed_ids.append(task_id)
                
        url = self.cfg.get("submission_agent", "poms_dd_complete_query_all") % ",".join(dd_task_ids)
        try:
            record_queue_log("Fetching Projects: %s" % dd_task_ids)
            htr = self.psess.get(url)
            dd_statuses = htr.json()
            elapsed_time = datetime.now() - start
            record_queue_log("Response Received", request_duration=f"{elapsed_time.seconds}.{elapsed_time.microseconds} seconds", statuses=dd_statuses)
            htr.close()
            return dd_statuses
        except Exception as e:
            record_queue_log(e, "Data Dispatcher Project Status", level="exception", statuses=dd_statuses)
            return {}
        

    def get_job_id_formats(self, job_id):
        job_id_no_schedd = job_id.replace(".0@", "@") if job_id else None
        job_id_with_schedd = job_id_no_schedd.replace("@", ".0@") if job_id_no_schedd else None
        return job_id_no_schedd, job_id_with_schedd
    
    def validate_entry(self, entry):
        task_id = entry.get("pomsTaskID", None)
        task_id = int(task_id) if task_id else None
        jobId = entry.get("id", None)
        jobId = self.format_jobid(jobId) if jobId else None
        
        if task_id and not jobId:
            jobId = self.sub_to_job_index.get(str(task_id), None)
        elif jobId and not task_id: 
            task_id = self.job_to_sub_index.get(jobId, None)
            task_id = int(task_id) if task_id else None
        
        if task_id:
            if str(task_id) in self.completed_submissions or str(task_id) in self.ignored["submissions"]:
                if jobId and jobId not in self.ignored["jobs"]:
                    self.ignored["jobs"].add(jobId)
                return False, None, None
        
        if jobId and jobId in self.ignored["jobs"]:
            if task_id and str(task_id) not in self.ignored["submissions"]:
                self.ignored["submissions"].add(str(task_id))
            return False, None, None
        
        return True, task_id, jobId
                
    def check_submissions(self, group=None, full_list=None, since=""):
        """
            get submission info from Landscape for a given group
            update various known bits of info
        """
        if group:
            record_queue_log("Begin", group= group)
        else:
            record_queue_log("Begin", groups= full_list)

        thispass = set()
        all_submissions_pre = {}
        sublist = []
        all_task_ids = {}
        
        if isinstance(self.ignored["jobs"], list):
            self.ignored["jobs"] = set(self.ignored["jobs"])
        if isinstance(self.ignored["submissions"], list):
            self.ignored["submissions"] = set(self.ignored["submissions"])
        
        if full_list:
            for entry in self.queued_submissions:
                valid, task_id, jobId = self.validate_entry(entry)
                if not valid:
                    continue
                if task_id and jobId:
                    entry["id"] = jobId
                    all_task_ids[task_id] = jobId
                    self.sub_to_job_index[str(task_id)] = jobId
                    self.job_to_sub_index[jobId] = str(task_id)
                elif task_id:
                    all_submissions_pre[task_id] = entry
                else:
                    # Can't track it without a task_id
                    self.ignored["jobs"].add(jobId)

            for exp in full_list:
                lens_submissions = self.get_running_submissions_LENS(exp, since)
                for entry in lens_submissions:
                    valid, task_id, jobId = self.validate_entry(entry)
                    if not valid:
                        continue
                    if not task_id:
                         # Can't track it without a task_id
                        self.ignored["jobs"].add(jobId)
                        continue
                    if task_id and jobId:
                        if task_id in all_task_ids:
                            # already have this one
                            continue
                        elif task_id in all_submissions_pre:
                            # This task was missing a job_id
                            all_task_ids[task_id] = jobId
                            self.job_to_sub_index[jobId] = str(task_id)
                            del all_submissions_pre[task_id]
                            
                    else:
                        # we'll try to get the job_id with a separate query
                        all_task_ids[task_id] = None
            
            record_queue_log("Run statistics | Known tasks: %s, Unknown taks: %s" % (len(all_task_ids), len(all_submissions_pre)))
            del all_submissions_pre
        elif group:
            sublist = self.get_running_submissions_LENS(group, since)
            sublist.extend(self.get_running_submissions_POMS(group))
            record_queue_log("%s data: %s" % (group, sublist))
            sublist.sort(key=(lambda x: x.get("pomsTaskID", "")))
            
            all_task_ids = {
                x.get("pomsTaskID"): x.get("id", None) 
                for x in sublist 
                if "pomsTaskID" in x
                and str(x["pomsTaskID"]) not in self.ignored["submissions"]
                and x.get("id", None) not in self.ignored["jobs"]
            }

        start = datetime.now()
        record_queue_log("Attempting to get data on all running jobs")
        
        if full_list and all_task_ids:
            submissions = self.get_all_submissions(all_task_ids)
            elapsed_time = datetime.now() - start
            record_queue_log("Got data on all running jobs | elapsed time: %s.%s seconds" % (elapsed_time.seconds, elapsed_time.microseconds))
            submissions_to_update = {}
            dd_task_entries_to_check = {}
            for entry in submissions.values() if submissions else []:
                if type(entry) != dict:
                    continue
                
                pomsTaskID = entry.get('pomsTaskID')
                if isinstance(pomsTaskID, str):
                    pomsTaskID = int(pomsTaskID)
                record_queue_log("Found job for submission_id: %d | job_id: %s" % (pomsTaskID, entry.get("id")))
                thispass.add(pomsTaskID)
                
                    
                if self.known["status"].get(entry["id"], None) == "Completed":
                    continue
                
                do_sam = entry.get("POMS_DATA_DISPATCHER_TASK_ID",'') == ''
                    
                if do_sam:
                    update_submission = self.maybe_report(entry)
                    if update_submission:
                        submissions_to_update[pomsTaskID] = update_submission
                else:
                    record_queue_log("Queued dd_task_id: %s for update" % entry["POMS_DATA_DISPATCHER_TASK_ID"])
                    dd_task_entries_to_check[entry["POMS_DATA_DISPATCHER_TASK_ID"]] = entry
                    
            
                    
            if len(dd_task_entries_to_check) > 0:
                dd_task_ids = list(dd_task_entries_to_check.keys())
                dd_task_statuses = self.get_dd_task_statuses(dd_task_ids)
                for key, val in dd_task_statuses.items():
                    if key == "dd_submissions":
                        continue
                    entry_to_check = dd_task_entries_to_check.get(key)
                    record_queue_log("checking entry: %s" % entry_to_check)
                    update_submission = self.maybe_report_data_dispatcher(entry_to_check, key, val)
                    if update_submission:
                        submissions_to_update[update_submission['submission_id']] = update_submission
                        
            if len(submissions_to_update) > 0:
                self.update_submissions(submissions_to_update)
            
            
            record_queue_log("Checking queued items for updates")
            for sub_id, entry in  submissions_to_update.items():
                if "status" in entry and entry["status"] in ["Completed", "Completed with failures",  "Located", "Cancelled", "Failed", "Failed to Launch"]:
                    if sub_id in self.query_results:
                        record_queue_log("Finished processing submission_id: %s | Final Status: %s" % (sub_id, entry["status"]))
                        self.completed_submissions[sub_id] = dict(self.query_results[sub_id])
                        self.completed_submissions[sub_id]["final_status"] = entry["status"]
                        self.queue.update_results = True
                            
                elif "status" in entry and sub_id in self.query_results:
                    record_queue_log("Status change detected for submission_id: %s | Status: %s" % (sub_id, entry["status"]))
                    self.query_results[sub_id]["status"] = entry["status"]
                    
            if self.queue.update_results:
                record_queue_log("Updating Results")
                self.queue.store_results(self.completed_submissions)
            
            
            record_queue_log("No queued items to update")
            return
    
        else:
            record_queue_log("check_submissions | Nothing to check")
                
        
    def get_dd_status(self, entry, dd_pct):
        
        if type(dd_pct) == str:
            ntot = (int(entry["running"]) + int(entry["idle"]) + 
                int(entry["held"]) + int(entry["completed"]) + 
                int(entry["failed"]) + int(entry["cancelled"]))

            if ntot >= self.known["maxjobs"].get(entry["pomsTaskID"], 0):
                self.known["maxjobs"][entry["pomsTaskID"]] = ntot
            else:
                ntot = self.known["maxjobs"][entry["pomsTaskID"]]

            ncomp = int(entry["completed"]) + int(entry["failed"]) + int(entry["cancelled"])

            if ntot > 0:
                dd_pct = ncomp * 100.0 / ntot
            else:
                dd_pct = 0
                
        if entry["done"]:
            if dd_pct == 100:
                return "Completed", "Completed", dd_pct
            if entry["error"] or dd_pct < 80:
                return "Completed with failures", self.dd_status_map.get("Completed with failures"), dd_pct
            if entry["cancelled"] > 1 and (entry["running"] + entry["idle"] + entry["held"] +  entry["failed"] + entry["completed"]) == 0:
                return "Cancelled", "Cancelled", dd_pct
            return "Completed", "Completed", dd_pct
        else:
            if entry["id"] or self.known["jobsub_job_id"][entry["pomsTaskID"]]:
                if entry["held"] > 0:
                    return "Held", "Held", dd_pct
                if "submitTime" in entry and entry["running"] == 0:
                    return "Idle", "Idle", dd_pct
                if (dd_pct > 0 and dd_pct < 100) or (ncomp > 0 and not entry["idle"]) or entry["running"] > 0:
                    return "Running", "Running", dd_pct
                if dd_pct == 0:
                    return "Submitted Pending Start", self.dd_status_map.get("Submitted Pending Start"), dd_pct
                
            else:
                unknown_job = self.known["unknown_jobs"].get(entry["pomsTaskID"], 0)
                if unknown_job < 5:
                    unknown_job += 1
                    self.known["unknown_jobs"][entry["pomsTaskID"]] = unknown_job
                    return "Attempting to get Job Id (Attempt: %d of 5)" % unknown_job, "Unknown", dd_pct
                else:
                    return "Failed to Launch", self.dd_status_map.get("Failed to Launch"), dd_pct
                    
        
        
        
    def format_jobid(self, job_id):
        parts = job_id.split("@")
        if len(parts) == 2:
            id = parts[0].replace(".000000.0.000000", "").replace(".0","")
            schedd = parts[1]
            job_id = "%s.0@%s" % (id, schedd)
        return job_id
    
    def get_all_submissions(self, task_jobs):
        job_query_list = []
        submission_query_list = []
        submission_index = {}
        job_index = {}
        
        s = 0
        j = 0
        
        
        for task_id, job_query in self.running_in_queue.items():
            if task_id not in self.completed_submissions:
                job_query_list.append("j%d:%s" % (j, job_query))
                job_index[task_id] = "j%d" % j
                j+=1
        
        # Insert our existing queries for jobs in progress
        for task_id, job_id in task_jobs.items():
            if job_id and str(task_id) not in self.running_in_queue:
                lens_jobsub_query = self.cfg.get("submission_agent", "append_submission_jid") % job_id
                job_query_list.append("j%d:%s" % (j, lens_jobsub_query))
                job_index[task_id] = "j%d" % j
                self.running_in_queue[str(task_id)] = lens_jobsub_query
                j+=1
            elif task_id > 0 and not job_id:
                submission_query_list.append("s%d:%s" % (s, self.cfg.get("submission_agent", "append_submission_sid") % task_id))
                submission_index["s%d" % s] = task_id
                s+=1
        
        # Fetch submission queries data from lens
        if submission_query_list:
            submissions_query = self.cfg.get("submission_agent", "all_jobs_query_base") % ",".join(submission_query_list)
            submissions_results = self.ssess.post(
                self.submission_uri,
                data=submissions_query,
                timeout=self.timeouts,
            )
            submissions_dict = submissions_results.json()
            submissions_results.close()
            
            if "errors" in submissions_dict and submissions_dict["errors"]:
                for error in submissions_dict["errors"]:
                    if "path" in error and "message" in error and  "no submission found with the given pomsTaskId" in error["message"]:
                        for index in error["path"]:
                            if index in submission_index:
                                task_id = submission_index[index]
                                self.ignored["submissions"].add(str(submission_index[index]))
                                record_queue_log("Submission %s not found in POMS, ignoring." % submission_index[index])
                

            record_queue_log("All Submissions Query returned %d results" % len(submissions_dict.get("data", {})))
        else:
            submissions_dict = {}
            record_queue_log("No submissions to query for.")

        
        
        
        # Now generate the lens queries for the rest of the known job ids
        for entry in submissions_dict.get("data", {}).values():
            valid, task_id, jobId = self.validate_entry(entry)
            if not valid or not jobId:
                continue
            if (task_id and jobId) or jobId in self.job_to_sub_index:
                job_query_list.append("j%d:%s" % (j, self.cfg.get("submission_agent", "append_job") % jobId))
                job_index[entry.get("pomsTaskID")] = "j%d" % j
                j+=1
                if str(task_id) not in self.running_in_queue:
                    self.running_in_queue[str(task_id)] = lens_jobsub_query
                
        # Fetch jobs queries data from lens    
        jobs_query = self.cfg.get("submission_agent", "all_jobs_query_base") % ",".join(job_query_list)
        jobs_results = self.ssess.post(
            self.submission_uri,
            data=jobs_query,
            timeout=self.timeouts,
        )
        jobs_dict = jobs_results.json()
        jobs_results.close()
        
        if jobs_dict.get("errors", None) != None:
            record_queue_log("Ferry request yielded some errors: %s" % jobs_dict.get("errors"))
            
        if not jobs_dict.get("data"):
            record_queue_log("Ferry request yielded no results")
            return None

        entries = {
            s["pomsTaskID"]:s 
            for s in submissions_dict.get("data", {}).values() 
            if s and  "pomsTaskID" in s
            and s["pomsTaskID"] not in self.ignored["submissions"]
        }
        jobs = {
            j["pomsTaskID"]:j 
            for j in jobs_dict["data"].values() 
            if j and "pomsTaskID" in j
            and j["pomsTaskID"] not in self.ignored["submissions"]
            }
        
        for item in jobs.values():
            if "id" not in item:
                continue
            job_id = self.format_jobid(item["id"])
            key = item.get("pomsTaskID", None)
            if not key or key not in self.running_in_queue:
                key = self.job_to_sub_index.get(job_id, None)
                if not key:
                    continue
                item["pomsTaskID"] = key
                self.query_results[key] = item
                    
            
        for key in jobs.keys():
            if key in entries:
                entries.update(jobs[key])
            
        entries.update(self.query_results)
        
        return entries

    def poll(self, since=""):
        """
           Operate as a daemon, poll service and update every 30 sec or so
        """
        while 1:
            try:
                (   
                    run_number, run_start,
                    self.queued_submissions, 
                    self.running_in_queue, 
                    self.job_to_sub_index, 
                    self.sub_to_job_index, 
                    self.query_results, 
                    self.completed_submissions, 
                    self.ignored
                ) = self.queue.begin_run()
                set_run_number(run_number)
                set_run_start(run_start)
                
                _ = self.check_submissions(full_list=self.elist, since=since)
                
                self.queue.save_queue_data({
                    "indexes": {
                        "j_to_s": self.sub_to_job_index,
                        "s_to_j": self.job_to_sub_index
                    },
                    "processing": self.running_in_queue,
                    "query_results": self.query_results
                })
                
                self.queue.post_run_cleanup()
                
                next_run_timestamp, next_run_friendly = calculate_next_run(self.poll_interval)
                record_queue_log("Run Complete | Next Run Begins in %s" % next_run_friendly, complete=True, next_run=next_run_timestamp)
            except Exception as e:
                record_queue_log(e, level="exception")
            
            time.sleep(self.poll_interval)
            since = ""


def main():
    """
       mainline --handle command line parameters and
           instantiate an Agent object.
    """

    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", default=os.environ.get("WEB_CONFIG", "/home/poms/poms/submission_broker/submission_agent.cfg"))
    ap.add_argument("-d", "--debug", action="store_true")
    ap.add_argument("--since", type=str)
    ap.add_argument("-t", "--test", action="store_true", default=False)
    ap.add_argument("-T", "--one-time", action="store_true", default=False)

    args = ap.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(filename)s:%(lineno)s:%(message)s")
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(filename)s:%(lineno)s:%(message)s")
    
    config = args.config
    if not config:
        config = os.environ.get("WEB_CONFIG", "/home/poms/poms/submission_broker/submission_agent.cfg")
    
    record_queue_log("init | Config: %s" % config)
    if args.test:
        agent = Agent(poms_uri="http://127.0.0.1:8080", submission_uri=os.environ["SUBMISSION_INFO"], config=config)
        for exp in agent.elist:
            agent.check_submissions(exp, since=args.since)
    elif args.one_time:
        agent = Agent(config=config)
        for exp in agent.elist:
            agent.check_submissions(exp, since=args.since)
    else:
        agent = Agent(config=config)
        agent.poll(since=args.since)


main()
