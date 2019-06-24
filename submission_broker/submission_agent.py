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

HTTPConnection.debuglevel = 1

LOGIT = logging.getLogger()


def get_status(entry):
    """
        given a dictionary from the Landscape service,
        return the status for our submission
    """
    if entry["done"] and entry["failed"] * 2 > entry["completed"]:
        return "Failed"
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

    full_query = """
         {"query":"{submissions(group: \\"%s\\" %s){id pomsTaskID done running idle held failed completed } }","operationName":null}
         """

    submission_project_query = """
          {"query":"{submission(id:\\"%s\\"){id SAM_PROJECT:env(name:\\"SAM_PROJECT\\")  SAM_PROJECT_NAME:env(name:\\"SAM_PROJECT_NAME\\")  args}}","operationName":null}
        """

    submission_info_query = """
          {"query":"{submission(id:\\"%s\\"){id pomsTaskID done running idle held failed completed } }","operationName":null}
        """

    def __init__(
        self,
        poms_uri="http://127.0.0.1:8080/poms/",
        submission_uri="https://landscape.fnal.gov/lens/query",
        submission_uri_dev="https://landscape.fnal.gov/lens/query",
        #submission_uri_dev="https://landscapeitb.fnal.gov/lens/query",
    ):

        """
            Setup webservice http session objects, uri's to reach things,
            headers we'll reuse, and status/info dictionaries, and fetch
            our experiment list from POMS
        """

        self.psess = requests.Session()
        self.ssess = requests.Session()
        self.poms_uri = poms_uri
        if os.environ.get("HOSTNAME", "").find("pomsgpvm") == 0:
            self.submission_uri = submission_uri
        else:
            self.submission_uri = submission_uri_dev
        # biggest time window we should ask LENS for
        self.maxtimedelta = 3600
        self.known = {}
        self.known["status"] = {}
        self.known["project"] = {}
        self.known["pct"] = {}
        self.known["maxjobs"] = {}
        self.known["poms_task_id"] = {}
        self.known["jobsub_job_id"] = {}
        self.submission_headers = {
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Connection": "keep-alive",
            "DNT": "1",
            "Origin": "https://landscape.fnal.gov",
        }
        # last_seen[group] is set of poms task ids seen last time
        self.last_seen = {}
        self.timeouts = (90, 90)
        self.strikes = {}

        htr = self.psess.get("http://127.0.0.1:8080/poms/experiment_list")
        self.elist = htr.json()
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

        postresult = self.ssess.post(
            self.submission_uri,
            data=Agent.submission_info_query % jobsubjobid,
            timeout=self.timeouts,
            headers=self.submission_headers,
        )
        ddict = postresult.json()
        LOGIT.info("submission %s data: %s", jobsubjobid, repr(ddict))
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
            data=Agent.submission_project_query % entry["id"],
            timeout=self.timeouts,
            headers=self.submission_headers,
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

    def check_submissions(self, group, since=""):
        """
            get submission info from Landscape for a given group
            update various known bits of info
        """

        LOGIT.info("check_submissions: %s", group)

        if time.time() - self.lastconn.get(group, 0) > self.maxtimedelta:
            # last info was too long ago, just clear it
            if self.lastconn.get(group, None):
                del self.lastconn[group]

        if since:
            LOGIT.info("check_submissions: since %s", since)
            since = ', from: \\"%s\\"' % since
        elif self.lastconn.get(group, None):
            since = ', from: \\"%s\\"' % time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(self.lastconn[group] - 120))

        if group == "samdev":
            group = "fermilab"
        try:
            # keep track of when we started
            start = time.time()
            htr = self.ssess.post(
                self.submission_uri,
                data=Agent.full_query % (group, since),
                timeout=self.timeouts,
                headers=self.submission_headers,
            )
            ddict = htr.json()
            htr.close()
            # only remember it if we succeed...
            self.lastconn[group] = start
        except requests.exceptions.RequestException as r:
            LOGIT.info("connection error for group %s: %s", group, r)
            ddict = {}
            pass

        LOGIT.info("%s data: %s", group, repr(ddict))

        if not ddict.get("data", None) or not ddict["data"].get("submissions", None):
            LOGIT.info("No data?")
            return

        thispass = set()

        # some come up with errors, look them up individually...
        # shove in front of zeroed out entry in 'submissions' list.

        haveerrors = ddict.get("errors", None) != None
        count = 0
        ignore = set()
        while count < 2 and haveerrors:
            count = count + 1
            haveerrors = False
            for entry in ddict.get("errors", []):
                LOGIT.info("checking error: %s", entry)
                m = re.match("unable to find info for (.*)", entry["message"])
                if m:
                    jobid = m.group(1)
                    ignore.add(jobid)

        for entry in ddict["data"]["submissions"]:

            if entry.get("id") in ignore:
                LOGIT.info("ignoring: %s due to error", entry)
                continue

            # skip if we don't have a pomsTaskID...
            if not entry.get("pomsTaskID", None):
                continue

            # don't get confused by duplicate listings
            if entry.get("pomsTaskID") in thispass:
                continue

            thispass.add(entry.get("pomsTaskID"))
            # if we see it, reset our strikes count
            self.strikes[entry.get("pomsTaskID")] = 0

            if entry["done"] == self.known["status"].get(entry["pomsTaskID"], None):
                report_status_flag = False
            else:
                report_status_flag = True

            report_status = get_status(entry)

            self.known["jobsub_job_id"][entry["pomsTaskID"]] = entry["id"]

            ntot = int(entry["running"]) + int(entry["idle"]) + int(entry["held"])
            if ntot >= self.known["maxjobs"].get(entry["pomsTaskID"], 0):
                self.known["maxjobs"][entry["pomsTaskID"]] = ntot
            else:
                ntot = self.known["maxjobs"][entry["pomsTaskID"]]

            ncomp = ntot - (entry["running"] + entry["held"] + entry["idle"])

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

        LOGIT.debug("thispass: %s\nlast_seen: %s" % (repr(thispass), repr(self.last_seen.get(group,None))))

        if self.last_seen.get(group, None):
            missing = self.last_seen[group] - thispass
            # submissions we used to see, but don't anymore...
            for id in missing:

                if self.strikes.get(id,0) < 3:
                   # must have 3 strikes in a row
                   self.strikes[id] = self.strikes.get(id,0) + 1
                   # put it  thispass so we get here next time
                   thispass.add(id)
                   continue

                # if our last status was Completed, this is expected,
                # just go on
                if self.known["status"][id] == "Completed":
                    continue

                LOGIT.info("reporting no longer seen submission id %s completed." % id)
                self.update_submission(id, jobsub_job_id=self.known["jobsub_job_id"][id], status="Completed")
                self.known["status"][id] = "Completed"

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
            time.sleep(120)
            since = ""


def main():
    """
       mainline --handle command line parameters and
           instantiate an Agent object.
    """

    if len(sys.argv) > 1 and sys.argv[1] == "--since":
        since = sys.argv[2]
        sys.argv = [sys.argv[0]] + sys.argv[3:]
    else:
        since = ""

    if len(sys.argv) > 1 and sys.argv[1] == "-d":
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(filename)s:%(lineno)s:%(message)s")
        sys.argv = [sys.argv[0]] + sys.argv[2:]
    else:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(filename)s:%(lineno)s:%(message)s")

    if len(sys.argv) > 1 and sys.argv[1] == "-t":
        agent = Agent(poms_uri="http://127.0.0.1:8080", submission_uri=os.environ["SUBMISSION_INFO"])
        for exp in agent.elist:
            agent.check_submissions(exp, since=since)
    elif len(sys.argv) > 1 and sys.argv[1] == "-T":
        agent = Agent()
        for exp in agent.elist:
            agent.check_submissions(exp, since=since)
    else:
        agent = Agent()
        agent.poll(since)


main()
