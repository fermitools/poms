#!/usr/bin/env python

import requests
import logging
import sys
import time
from http.client import HTTPConnection
HTTPConnection.debuglevel = 1

logit = logging.getLogger()

class Agent:
    def __init__(self, poms_uri = "http://127.0.0.1:80/poms/", submission_uri = 'https://landscapeitb.fnal.gov/api/query'):
        self.psess = requests.Session()
        self.ssess = requests.Session()
        self.poms_uri = poms_uri
        self.submission_uri = submission_uri
        self.known_status = {}
        self.known_project = {}
        self.known_pct = {}
        self.submission_headers = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json',
            'Accept': '*/*','Connection': 'keep-alive',
            'DNT': '1',
            'Origin': 'https://landscapeitb.fnal.gov' 
        }

        self.full_query = """{"query":"{ submissions(query: \\"POMS_TASK_ID: gt 1\\") { id jobs { id } done } }","operationName":null}"""

    def update_submission(self, submission_id, jobsub_job_id, project = None, status = None):
        r = self.psess.post(self.poms_uri, {'submission_id': submission_id, 'jobsub_job_id': jobsub_job_id, 'project': project, 'status': status}, verify=False)
        if (r.text != "Ok."):
            logit.error("update_submission: Failed.");

    def get_project(self, e):
        if e.get('Args',None):
           p1 = e['Args'].find('--sam_project')
           if p1 > 0:
               p2 = e['Args'].find(' ',p1+15)
               return e['Args'][p1+14:p2]
        if e.get('Env',None):
           if e['Env'].get('SAM_PROJECT_NAME'):
               return e['Env']['SAM_PROJECT_NAME']
           if e['Env'].get('SAM_PROJECT'):
               return e['Env']['SAM_PROJECT']
        return None

    def check_submissions(self):
        logit.info("check_submissions:")
        r = self.ssess.post(self.submission_uri, data=self.full_query, headers=self.submission_headers)
        d = r.json()
        logit.info("data: %s" % repr(d))
        if not d.get('data',None) or not d['data'].get('submissions',None):
            return

        for e in d['data']['submissions']:

            # skip if we don't have a POMS_TASK_ID...
            if not e.get('POMS_TASK_ID',None):
                continue

            if e['done'] == self.known_status.get(e['POMS_TASK_ID'],None):
                report_status = None
            else:
                report_status = ('Completed' if e['done'] else 'Running')

            if e.get('jobs',None):
                ntot = int(e['jobs']['Running']) + int(e['jobs']['Completed']) + int(e['jobs']['Idle']) + int(e['jobs']['Held']) + int(e['jobs']['Removed']) 
                ncomp = int(e['jobs']['Completed']) 
                if ntot > 0:
                    report_pct_completd = ncomp * 100.0 / ntot
                else:
                    report_pct_completed = None

                if report_pct_completed == self.known_pct[e['POMS_TASK_ID']]:
                    report_pct_completed = None

            else:
                report_pct_completed = None

            if self.get_project(e) == self.known_project.get(e['POMS_TASK_ID'],None):
                report_project = None
            else:
                report_project = self.get_project(e)

            #
            # actually report it if there's anything changed...
            #
            if report_status or report_project or report_pct_completed:
                self.update_submission(e['POMS_TASK_ID'], jobsub_job_id = e['id'], pct_completed = report_pct_completed, project = report_project, status = report_status)

            #
            # now update our known status if available
            #
            if e['POMS_TASK_ID'] not in self.known_status or report_status:
                self.known_status[e['POMS_TASK_ID']] = e['done']

            if e['POMS_TASK_ID'] not in self.known_project or report_project:
                self.known_project[e['POMS_TASK_ID']] = report_project

            if e['POMS_TASK_ID'] not in self.known_pct or report_pct:
                self.known_pct[e['POMS_TASK_ID']] = report_pct

    def poll(self):
        while( 1 ):
           try:
               self.check_submissions()
           except Exception as e:
               logit.error('Exception: %s" % e')
           time.sleep(120)

if len(sys.argv) > 1 and sys.argv[1] == '-t':
   logging.basicConfig(level=Debug)
   a = Agent(poms_uri="http://127.0.0.1:8080",submission_uri=getenv("SUBMISSION_INFO") )
   a.check_submissions()
elif len(sys.argv) > 1 and sys.argv[1] == '-T':
   logging.basicConfig(level=logging.DEBUG)
   a = Agent()
   a.check_submissions()
else:
   a = Agent()
   a.poll()
