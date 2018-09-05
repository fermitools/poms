#!/usr/bin/env python
'''
POMS agent to collect submission info from the Landscape "lens" service
and report it into POMS
'''

import logging
import sys
import os
import time
from http.client import HTTPConnection
import requests
HTTPConnection.debuglevel = 1

LOGIT = logging.getLogger()

def get_status(entry):
    '''
        given a dictionary from the Landscape service,
        return the status for our submission
    '''
    if entry['done']:
        return "Completed"
    if entry['held'] > 0:
        return "Held"
    if entry['running'] == 0 and entry['idle'] != 0:
        return "Idle"
    if entry['running'] > 0:
        return "Running"
    return "Unknown"

class Agent:
    '''
        Class for the submission reporting agent -- uses the service
        at submission_uri in the __init__() below and get info on
        recent submissions and reports them to POMS
    '''
    full_query = '''
         {"query":"{submissions(group: \\"%s\\" %s){id pomsTaskID done running idle   held } }","operationName":null}
         '''

    submission_query = '''
          {"query":"{submission(id:\\"%s\\"){  pomsTaskID  SAM_PROJECT:env(name:\\"SAM_PROJECT\\")  SAM_PROJECT_NAME:env(name:\\"SAM_PROJECT_NAME\\")  args}}","operationName":null}
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
        self.known = {}
        self.known['status'] = {}
        self.known['project'] = {}
        self.known['pct'] = {}
        self.known['maxjobs'] = {}
        self.submission_headers = {
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json',
            'Accept': '*/*',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Origin': 'https://landscapeitb.fnal.gov'
        }


        htr = self.psess.get("http://127.0.0.1:8080/poms/experiment_list")
        self.elist = htr.json()
        htr.close()

    def update_submission(self, submission_id, jobsub_job_id,
                          pct_complete=None,
                          project=None,
                          status=None):

        '''
            actually report information on a submission to POMS
        '''

        LOGIT.info('update_submission: %s',
                   repr({
                       'submission_id': submission_id,
                       'jobsub_job_id': jobsub_job_id,
                       'project': project,
                       'status': status}))
        try:
            htr = self.psess.post("%s/update_submission"%self.poms_uri,
                                  {
                                      'submission_id': submission_id,
                                      'jobsub_job_id': jobsub_job_id,
                                      'project': project,
                                      'status': status,
                                      'pct_complete': pct_complete
                                  },
                                  verify=False)

        except requests.exceptions.ConnectionError:
            LOGIT.error("Connection Reset! Retrying once...")
            time.sleep(1)
            htr = self.psess.post("%s/update_submission" % self.poms_uri,
                                  {
                                      'submission_id': submission_id,
                                      'jobsub_job_id': jobsub_job_id,
                                      'project': project,
                                      'status': status
                                  },
                                  verify=False)

        if htr.text != "Ok.":
            LOGIT.error("update_submission: Failed.")
            LOGIT.error(htr.text)

        htr.close()


    def get_project(self, entry):

        '''
           get project info from service if we don't have it
           -- it could be in environment information or command line args
        '''

        # check if we already know it...

        res = self.known['project'].get(entry['pomsTaskID'], None)
        if res:
            LOGIT.info("already knew project for %s: %s", entry['pomsTaskID'], res)
            return res

        # otherwise look it up... in the submission info

        postresult = self.ssess.post(self.submission_uri,
                                     data=Agent.submission_query % entry['id'],
                                     headers=self.submission_headers)
        ddict = postresult.json()
        ddict = ddict['data']['submission']
        LOGIT.info("data: %s", repr(ddict))
        postresult.close()

        if ddict.get('args', None):
            pos1 = ddict['args'].find('--sam_project')
            if pos1 > 0:
                LOGIT.info("saw --sam_project in args")
                pos2 = ddict['args'].find(' ', pos1+15)
                res = ddict['args'][pos1+14:pos2]
                LOGIT.info("got: %s", res)
        if not res and ddict.get('SAM_PROJECT_NAME', None):
            res = ddict['SAM_PROJECT_NAME']
        if not res and ddict.get('SAM_PROJECT', None):
            res = ddict['SAM_PROJECT']

        # it looks like we should do this, to update our cache, *but* we
        # need to defer it for the logic in check_submissions() below,
        # otherwise we'll never report it...
        # self.known['project'][entry['pomsTaskID']] = res
        LOGIT.info("found project for %s: %s", entry['pomsTaskID'], res)
        return res


    def check_submissions(self, group, since = ''):
        '''
            get submission info from Landscape for a given group
            update various known bits of info
        '''

        LOGIT.info("check_submissions: %s", group)

        if since:
           LOGIT.info("check_submissions: since %s", since)
           since = ', from: "%s%"' % since

        if group == 'samdev':
            group = 'fermilab'
        try:
            htr = self.ssess.post(self.submission_uri,
                                  data=Agent.full_query % (group, since),
                                  headers=self.submission_headers)
            ddict = htr.json()
            htr.close()
        except requests.exceptions.RequestException as r:
            LOGIT.info("connection error for group %s: %s" , group, r)
            ddict={}
            pass

        LOGIT.info("data: %s", repr(ddict))
        if not ddict.get('data', None) or not ddict['data'].get('submissions', None):
            return

        thispass = set()
        for entry in ddict['data']['submissions']:

            # skip if we don't have a pomsTaskID...
            if not entry.get('pomsTaskID', None):
                continue

            # don't get confused by duplicate listings
            if entry.get('id', None) in thispass:
                continue
            thispass.add(entry.get('id',None))

            if entry['done'] == self.known['status'].get(entry['pomsTaskID'], None):
                report_status = None
            else:
                report_status = get_status(entry)

            ntot = int(entry['running']) + int(entry['idle']) + int(entry['held'])
            if ntot >= self.known['maxjobs'].get(entry['pomsTaskID'], 0):
                self.known['maxjobs'][entry['pomsTaskID']] = ntot
            else:
                ntot = self.known['maxjobs'][entry['pomsTaskID']]

            ncomp = ntot - (entry['running'] + entry['held'] + entry['idle'])

            if ntot > 0:
                report_pct_complete = ncomp * 100.0 / ntot
            else:
                report_pct_complete = None

            if report_pct_complete == self.known['pct'].get(entry['pomsTaskID'], None):
                report_pct_complete = None

            if self.get_project(entry) == self.known['project'].get(entry['pomsTaskID'], None):
                report_project = None
            else:
                report_project = self.get_project(entry)

            #
            # actually report it if there's anything changed...
            #
            if report_status or report_project or report_pct_complete:
                self.update_submission(entry['pomsTaskID'],
                                       jobsub_job_id=entry['id'],
                                       pct_complete=report_pct_complete,
                                       project=report_project,
                                       status=report_status)

            #
            # now update our known status if available
            #
            if entry['pomsTaskID'] not in self.known['status'] or report_status:
                self.known['status'][entry['pomsTaskID']] = entry['done']

            if entry['pomsTaskID'] not in self.known['project'] or report_project:
                self.known['project'][entry['pomsTaskID']] = report_project

            if entry['pomsTaskID'] not in self.known['pct'] or report_pct_complete:
                self.known['pct'][entry['pomsTaskID']] = report_pct_complete

    def poll(self, since = ''):
        '''
           Operate as a daemon, poll service and update every 30 sec or so
        '''
        while 1:
            try:
                for exp in self.elist:
                    self.check_submissions(exp, since = since)
            except:
                LOGIT.exception("Exception in check_submissions")
            time.sleep(30)
            since = '' 

def main():
    '''
       mainline --handle command line parameters and
           instantiate an Agent object.
    '''

    if len(sys.argv) > 1 and sys.argv[1] == '--since':
        since = sys.argv[2]
        sys.argv = [sys.argv[0]] + sys.argv[3:]
    else:
        since = ''

    if len(sys.argv) > 1 and sys.argv[1] == '-d':
        logging.basicConfig(level=logging.DEBUG)
        sys.argv = [sys.argv[0]] + sys.argv[2:]
    else:
        logging.basicConfig(level=logging.INFO)


    if len(sys.argv) > 1 and sys.argv[1] == '-t':
        agent = Agent(poms_uri="http://127.0.0.1:8080",
                      submission_uri=os.environ["SUBMISSION_INFO"])
        for exp in agent.elist:
            agent.check_submissions(exp, since=since)
    elif len(sys.argv) > 1 and sys.argv[1] == '-T':
        agent = Agent()
        for exp in agent.elist:
            agent.check_submissions(exp,since = since)
    else:
        agent = Agent()
        agent.poll(since)

main()
