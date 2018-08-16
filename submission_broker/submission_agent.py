#!/usr/bin/env python

import logging
import sys
import time
import requests
from http.client import HTTPConnection
HTTPConnection.debuglevel = 1

logit = logging.getLogger()

class Agent:
    '''
        Class for the submission reporting agent -- uses the service
        at submission_uri in the __init__() below and get info on 
        recent submissions and reports them to POMS
    '''
    def __init__(self, poms_uri="http://127.0.0.1:8080/poms/",
                 submission_uri='https://landscapeitb.fnal.gov/lens/query'):

        '''
            Setup webservice http session objects, uri's to reach things,
            headers we'll reuse, and status/info dictionaries, and fetch
            our experiment list from POMS
        '''

        self.psess = requests.Session()
        self.ssess = requests.Session()
        self.poms_uri = poms_uri
        self.submission_uri = submission_uri
        self.known_status = {}
        self.known_project = {}
        self.known_pct = {}
        self.maxjobs = {}
        self.submission_headers = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Origin': 'https://landscapeitb.fnal.gov'
        }

        self.full_query = '{"query":"{submissions(group: \\"%s\\"){  id   pomsTaskID   done   running   idle   held } }","operationName":null}'

        self.submission_query = '{"query":"{submission(id:\\"%s\\"){  pomsTaskID  SAM_PROJECT:env(name:\\"SAM_PROJECT\\")  SAM_PROJECT_NAME:env(name:\\"SAM_PROJECT_NAME\\")  args}}","operationName":null}'

        r = self.psess.get("http://127.0.0.1:8080/poms/experiment_list")
        self.elist = r.json()
        r.close()

    def update_submission(self, submission_id, jobsub_job_id,
                          pct_complete=None,
                          project=None,
                          status=None):

        '''
            actually report information on a submission to POMS
        '''

        logit.info('update_submission: %s' %
                   repr({
                       'submission_id': submission_id,
                       'jobsub_job_id': jobsub_job_id,
                       'project': project,
                       'status': status}))
        try:
            r = self.psess.post("%s/update_submission"%self.poms_uri,
                                {
                                    'submission_id': submission_id,
                                    'jobsub_job_id': jobsub_job_id,
                                    'project': project,
                                    'status': status,
                                    'pct_complete': pct_complete
                                },
                                verify=False)

        except requests.exceptions.ConnectionError:
            logit.error("Connection Reset! Retrying once...")
            time.sleep(1)
            r = self.psess.post("%s/update_submission"%self.poms_uri, {'submission_id': submission_id, 'jobsub_job_id': jobsub_job_id, 'project': project, 'status': status}, verify=False)

        if r.text != "Ok.":
            logit.error("update_submission: Failed.")
            logit.error(r.text)

        r.close()


    def get_project(self, e):

        '''
           get project info from service if we don't have it
           -- it could be in environment information or command line args
        '''

        # check if we already know it...

        res = self.known_project.get(e['pomsTaskID'], None)
        if res:
            logit.info("already knew project for %s: %s" % (e['pomsTaskID'], res))
            return res

        # otherwise look it up... in the submission info

        postresult = self.ssess.post(self.submission_uri,
                            data=self.submission_query % e['id'],
                            headers=self.submission_headers)
        ddict = postresult.json()
        ddict = d['data']['submission']
        logit.info("data: %s" % repr(ddict))
        postresult.close()

        if ddict.get('args', None):
            p1 = ddict['args'].find('--sam_project')
            if p1 > 0:
                logit.info("saw --sam_project in args")
                p2 = ddict['args'].find(' ', p1+15)
                res =  ddict['args'][p1+14:p2]
                logit.info("got: %s" % res)
        if not res and ddict.get('SAM_PROJECT_NAME', None):
            res = ddict['SAM_PROJECT_NAME']
        if not res and ddict.get('SAM_PROJECT', None):
            res = ddict['SAM_PROJECT']

        # it looks like we should do this, to update our cache, *but* we
        # need to defer it for the logic in check_submissions() below,
        # otherwise we'll never report it...
        # self.known_project[e['pomsTaskID']] = res
        logit.info("found project for %s: %s" % (e['pomsTaskID'], res))
        return res

    def get_status(self, e):
        '''
            given a dictionary from the Landscape service, 
            return the status for our submission
        '''
        if e['done']:
            return "Completed"
        if e['held'] > 0:
            return "Held"
        if e['running'] == 0 and e['idle'] != 0:
            return "Idle"
        if e['running'] > 0:
            return "Running"
        return "Unknown"

    def check_submissions(self, group):
        '''
            get submission info from Landscape for a given group
            update various known bits of info
        '''
        logit.info("check_submissions: %s" % group)
        if group == 'samdev':
            group = 'fermilab'
        r = self.ssess.post(self.submission_uri, 
                            data=self.full_query % group, 
                            headers=self.submission_headers)
        d = r.json()
        r.close()
        logit.info("data: %s" % repr(d))
        if not d.get('data', None) or not d['data'].get('submissions', None):
            return

        for e in d['data']['submissions']:

            # skip if we don't have a pomsTaskID...
            if not e.get('pomsTaskID', None):
                continue

            if e['done'] == self.known_status.get(e['pomsTaskID'], None):
                report_status = None
            else:
                report_status = self.get_status(e)

            ntot = int(e['running']) + int(e['idle']) + int(e['held'])
            if ntot >= self.maxjobs.get(e['pomsTaskID'], 0):
                self.maxjobs[e['pomsTaskID']] = ntot
            else:
                ntot = self.maxjobs[e['pomsTaskID']]

            ncomp = ntot - (e['running'] + e['held'] + e['idle'])

            if ntot > 0:
                report_pct_complete = ncomp * 100.0 / ntot
            else:
                report_pct_complete = None

            if report_pct_complete == self.known_pct.get(e['pomsTaskID'], None):
                report_pct_complete = None

            if self.get_project(e) == self.known_project.get(e['pomsTaskID'], None):
                report_project = None
            else:
                report_project = self.get_project(e)

            #
            # actually report it if there's anything changed...
            #
            if report_status or report_project or report_pct_complete:
                self.update_submission(e['pomsTaskID'], 
                                       jobsub_job_id=e['id'],
                                       pct_complete=report_pct_complete,
                                       project = report_project,
                                       status = report_status)

            #
            # now update our known status if available
            #
            if e['pomsTaskID'] not in self.known_status or report_status:
                self.known_status[e['pomsTaskID']] = e['done']

            if e['pomsTaskID'] not in self.known_project or report_project:
                self.known_project[e['pomsTaskID']] = report_project

            if e['pomsTaskID'] not in self.known_pct or report_pct_complete:
                self.known_pct[e['pomsTaskID']] = report_pct_complete

    def poll(self):
        while( 1 ):
           try:
               for exp in self.elist:
                   self.check_submissions(exp)
           except Exception as e:
               raise
               logit.error("Exception: %s" % e)
           time.sleep(30)

def main():
    if len(sys.argv) > 1 and sys.argv[1] == '-d':
       logging.basicConfig(level=logging.DEBUG)
       sys.argv = [sys.argv[0]] + sys.argv[2:]
    else:
       logging.basicConfig(level=logging.INFO)

    if len(sys.argv) > 1 and sys.argv[1] == '-t':
       a = Agent(poms_uri="http://127.0.0.1:8080",submission_uri=getenv("SUBMISSION_INFO") )
       for exp in a.elist:
           a.check_submissions(exp)
    elif len(sys.argv) > 1 and sys.argv[1] == '-T':
       a = Agent()
       for exp in a.elist:
           a.check_submissions(exp)
    else:
       a = Agent()
       a.poll()

main()
