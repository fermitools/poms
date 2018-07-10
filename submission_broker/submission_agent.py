#!/usr/bin/env python

import requests
import logging
import sys
import time
from http.client import HTTPConnection
HTTPConnection.debuglevel = 1

logit = logging.getLogger()

class Agent:
    def __init__(self, poms_uri = "http://127.0.0.1:8080/poms/", submission_uri = 'https://landscapeitb.fnal.gov/api/query'):
        self.psess = requests.Session()
        self.ssess = requests.Session()
        self.poms_uri = poms_uri
        self.submission_uri = submission_uri
        self.known_status = {}
        self.known_project = {}
        self.known_pct = {}
        self.max_njobs = {}
        self.submission_headers = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json',
            'Accept': '*/*','Connection': 'keep-alive',
            'DNT': '1',
            'Origin': 'https://landscapeitb.fnal.gov' 
        }

        self.full_query = """{"query":"{ submissions(user: "*pro") { id pomsTaskID done running idle held } }","operationName":null}"""

    def update_submission(self, submission_id, jobsub_job_id, project = None, status = None):
        logit.info('update_submission: %s' % repr({'submission_id': submission_id, 'jobsub_job_id': jobsub_job_id, 'project': project, 'status': status}))
        try:
            r = self.psess.post("%s/update_submission"%self.poms_uri, {'submission_id': submission_id, 'jobsub_job_id': jobsub_job_id, 'project': project, 'status': status}, verify=False)
        except requests.exceptions.ConnectionError:
            logit.error("Connection Reset! Retrying once...")
            time.sleep(1)
            r = self.psess.post("%s/update_submission"%self.poms_uri, {'submission_id': submission_id, 'jobsub_job_id': jobsub_job_id, 'project': project, 'status': status}, verify=False)

        if (r.text != "Ok."):
            logit.error("update_submission: Failed.");
            logit.error(r.text)


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

    def sub_status(self, e) {
        if e['done']:
            return "Completed"
        if e['held'] > 0: 
            return "Held"
        if e['running'] == 0 and e['idle'] != 0:
            return "Idle"
        if e['running'] > 0:
            reurn "Running"
        return "Unknown"
    }

    def check_submissions(self):
        logit.info("check_submissions:")
        r = self.ssess.post(self.submission_uri, data=self.full_query, headers=self.submission_headers)
        d = r.json()
        logit.info("data: %s" % repr(d))
        if not d.get('data',None) or not d['data'].get('submissions',None):
            return

        for e in d['data']['submissions']:

            # skip if we don't have a pomsTaskID...
            if not e.get('pomsTaskID',None):
                continue

            if e['done'] == self.known_status.get(e['pomsTaskID'],None):
                report_status = None
            else:
                report_status = self.get_status(e)

            ntot = int(e['running']) + int(e['idle']) + int(e['held']) 
            if ntot >= maxjobs.get(e['pomsTaskID'], 0):
               maxjobs[e['pomsTaskID'] = ntot
            else:
               ntot = maxjobs[e['pomsTaskID']]

            ncomp = ntot - (e['running'] + e['held'] + e['idle'])
          
            if ntot > 0:
                report_pct_completd = ncomp * 100.0 / ntot
            else:
                report_pct_completed = None

            if report_pct_completed == self.known_pct[e['pomsTaskID']]:
                report_pct_completed = None

            if self.get_project(e) == self.known_project.get(e['pomsTaskID'],None):
                report_project = None
            else:
                report_project = self.get_project(e)

            #
            # actually report it if there's anything changed...
            #
            if report_status or report_project or report_pct_completed:
                self.update_submission(e['pomsTaskID'], jobsub_job_id = e['id'], pct_completed = report_pct_completed, project = report_project, status = report_status)

            #
            # now update our known status if available
            #
            if e['pomsTaskID'] not in self.known_status or report_status:
                self.known_status[e['pomsTaskID']] = e['done']

            if e['pomsTaskID'] not in self.known_project or report_project:
                self.known_project[e['pomsTaskID']] = report_project

            if e['pomsTaskID'] not in self.known_pct or report_pct:
                self.known_pct[e['pomsTaskID']] = report_pct

    def poll(self):
        while( 1 ):
           try:
               self.check_submissions()
           except Exception as e:
               logit.error("Exception: %s" % e)
           time.sleep(30)

if len(sys.argv) > 1 and sys.argv[1] == '-d':
   logging.basicConfig(level=logging.DEBUG)
   sys.argv = [sys.argv[0]] + sys.argv[2:]
else:
   logging.basicConfig(level=logging.INFO)

if len(sys.argv) > 1 and sys.argv[1] == '-t':
   a = Agent(poms_uri="http://127.0.0.1:8080",submission_uri=getenv("SUBMISSION_INFO") )
   a.check_submissions()
elif len(sys.argv) > 1 and sys.argv[1] == '-T':
   a = Agent()
   a.check_submissions()
else:
   a = Agent()
   a.poll()
