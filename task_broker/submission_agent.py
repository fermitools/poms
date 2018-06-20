#!/usr/bin/env python

import requests
import logging

logit = logging.getLogger()


class Agent:
    def __init__(self, poms_uri = "http://127.0.0.1:80/poms/", submission_uri = "http://fifemon.fnal.gov/submission_status/"):
        self.psess = requests.Session()
        self.ssess = requests.Session()
        self.poms_uri = poms_uri
        self.submission_uri = submission_uri
        self.known_status = {}
        self.known_project = {}
        self.known_pct = {}

    def update_submission(self, submission_id, jobsub_job_id, project = None, status = None):
        r = self.psess.post(self.poms_uri, {'submission_id': submission_id, 'jobsub_job_id': jobsub_job_id, 'project': project, 'status': status})
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
        r = self.ssess.get(self.submission_uri)
        d = r.json()
        for e in d['running']:
            if e['status'] == known_status.get(e['POMS_TASK_ID'],None):
                report_status = None
            else:
                report_status = e['status']

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

            if self.get_project(e) == known_project.get(e['POMS_TASK_ID'],None):
                report_project = None
            else:
                report_project = self.get_project(e)

            if report_status or report_project or report_pct_completed:
                self.update_submission(e['POMS_TASK_ID'], jobsub_job_id = e['jobsub_job_id'],pct_completed = report_pct_completed, project = report_project, status = report_status)

            if e['POMS_TASK_ID'] not in known_status or report_status:
                known_status[e['POMS_TASK_ID']] = e['status']

            if e['POMS_TASK_ID'] not in known_project or report_project:
                known_project[e['POMS_TASK_ID']] = report_project

            if e['POMS_TASK_ID'] not in known_pct or report_pct:
                known_pct[e['POMS_TASK_ID']] = report_pct

        for e in d['completed']:
            self.update_submission(e['POMS_TASK_ID'
    def poll(self):
        while( 1 ):
           try:
               self.check_submissions()
           except Exception as e:
               logit.error('Exception: %s" % e')

if sys.argv[1] == '-t':
   a = Agent(poms_uri="http://127.0.0.1:8080",submission_uri=getenv("SUBMISSION_INFO") )
   a.check_submissions()
else:
   a = Agent()
   a.poll()
