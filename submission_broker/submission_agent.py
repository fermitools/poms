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

HTTPConnection.debuglevel = 1
HTTPConnection.port = os.environ['SA_PORT']
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

        if ddict.get("args", None):
            pos1 = ddict["args"].find("--sam_project")
            if pos1 > 0:
                LOGIT.info("saw --sam_project in args")
                pos2 = ddict["args"].find(" ", pos1 + 15)
                res = ddict["args"][pos1 + 14 : pos2]
                LOGIT.info("got: %s", res)
            pos1 = ddict["args"].find("--project_name")
            if pos1 > 0:
                LOGIT.info("saw --project_name in args")
                pos2 = ddict["args"].find(" ", pos1 + 15)
                res = ddict["args"][pos1 + 15 : pos2]
                LOGIT.info("got: %s", res)
        if not res and ddict.get("SAM_PROJECT_NAME", None):
            res = ddict["SAM_PROJECT_NAME"]
        if not res and ddict.get("SAM_PROJECT", None):
            res = ddict["SAM_PROJECT"]

        # it looks like we should do this, to update our cache, *but* we
        # need to defer it for the logic in check_submissions() below,
        # otherwise we'll never report it...
        # self.known['project'][entry['pomsTaskID']] = res
        LOGIT.info("found project for %s: %s", entry["pomsTaskID"], res)
        return res

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

        if self.get_project(entry) == self.known["project"].get(entry["pomsTaskID"], None):
            report_project_flag = False
        else:
            report_project_flag = True

        report_project = self.get_project(entry)

        #
        # actually report it if there's anything changed...
        #
        if report_status_flag or report_project_flag or report_pct_complete_flag:
            self.update_submission(
                entry["pomsTaskID"],
                jobsub_job_id=entry["id"],
                pct_complete=report_pct_complete,
                project=report_project,
                status=report_status,
            )

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

        LOGIT.info("ddict: %s", repr(ddict))
        if ddict:
            return ddict.get("data",{}).get("submissions",[])
        else:
            return []

    def get_running_submissions_POMS(self, group):
        url = self.cfg.get("submission_agent", "poms_running_query")
        try:
            htr = self.psess.get(url)
            flist = htr.json()
            LOGIT.info("poms running_submissions: ", repr(flist))
            ddict = [ {'pomsTaskID': x[0], 'id': x[1]} for x in flist if x[2] == group]
            LOGIT.info("poms running_submissions for " , group,  ": ", repr(ddict))
            htr.close()
            return ddict
        except:
            logging.exception("running_submissons_POMS")
            return {}
        

    def check_submissions(self, group, since=""):
        """
            get submission info from Landscape for a given group
            update various known bits of info
        """

        LOGIT.info("check_submissions: %s", group)

        thispass = set()
        sublist = self.get_running_submissions_LENS( group, since)
        sublist.extend( self.get_running_submissions_POMS(group))

        LOGIT.info("%s data: %s", group, repr(sublist))

        sublist.sort(key=(lambda x: x.get("pomsTaskID", "")))

        for entry in sublist:

            # skip if we don't have a pomsTaskID...
            if not entry.get("pomsTaskID", None):
                continue

            # don't get confused by duplicate listings
            if entry.get("pomsTaskID") in thispass:
                continue

            thispass.add(entry.get("pomsTaskID"))

            id = entry.get('id')
            pomsTaskID = entry.get('pomsTaskID')

            if self.known["status"].get(id,None) == "Completed":
                continue

            fullentry = self.get_individual_submission(id)

            if fullentry and 'data' in fullentry:
                 fullentry = fullentry['data'] 

            self.maybe_report(fullentry)

            if fullentry and fullentry["done"]:
                self.update_submission(pomsTaskID, jobsub_job_id=id, status=get_status(fullentry))

        self.last_seen[group] = thispass

    def poll(self, since=""):
        """
           Operate as a daemon, poll service and update every 30 sec or so
        """
        while 1:
            try:
                for exp in self.elist:
                    self.check_submissions(exp, since=since)
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
    ap.add_argument("-c", "--config", default="/home/poms/private/config/uwsgi-poms-submission-agent.ini")
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
        agent = Agent(poms_uri="http://poms_webservice:8080", submission_uri=os.environ["SUBMISSION_INFO"], config=args.config)
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
