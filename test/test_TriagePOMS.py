import DBHandle
import datetime
import time
import os
import socket
from poms.webservice.utc import utc
from poms.webservice.samweb_lite import samweb_lite
from poms.model.poms_model import Campaign, Job

from mock_stubs import gethead, launch_seshandle, camp_seshandle, err_res, getconfig

dbh = DBHandle.DBHandle()
dbhandle = dbh.get()

from mock_poms_service import mock_poms_service
from mock_redirect import mock_redirect_exception
import logging
from webservice.jobsub_fetcher import jobsub_fetcher
logger = logging.getLogger('cherrypy.error')
# when I get one...

from mock_job import mock_job

mps = mock_poms_service()

fetcher = jobsub_fetcher()

mj = mock_job()

def test_triage_job():
    c = dbhandle.query(Campaign).filter(Campaign.name == 'mwm_test_1').first()
    mj.launch(str(c.campaign_id), n_jobs=3, exit_code = -1)
    mj.close()
    job = dbhandle.query(Job).filter(Job.jobsub_job_id == mj.jids[0]).first()

    res = mps.triagePOMS.triage_job(dbhandle, fetcher, dbh.cf, job.job_id)

    # we should get ( [], (job,task,camp) , [JobHistory,...], ...)
    # so check a bit.. 
    # - we got the job we asked for
    # - we got a JobHistory for that job
    # - the first one is "Idle"
    assert(res[1][0].jobsub_job_id == mj.jids[0])
    assert(res[2][0].job_id == res[1][0].job_id)
    assert(res[2][0].status == 'Idle')

    # we could add more stuff here, its a lot of data

def test_job_table():

    res = mps.triagePOMS.job_table(dbhandle, jobsub_job_id = mj.jids[0])

    # we should get a ( [(job,task,camp),...] , ['columns'], ['columns'], ...)
    # where we have one (job,task,camp) tuple (the one we asked for)
    # and the column sets should be for jobs, tasks, and campaigns...
    assert(len(res[0]) == 1)
    assert(res[0][0][0].jobsub_job_id == mj.jids[0])
    assert(res[1][0] == 'job_id')
    assert(res[2][0] == 'task_id')
    assert(res[3][0] == 'campaign_id')

def test_failed_jobs():

    print "jids:", mj.jids

    res = mps.triagePOMS.failed_jobs_by_whatever(dbhandle,  f = ['jobsub_job_id'])

    # our jobs from test_triage_job should have jobs .0, .1, and .2 exit
    # code 0, 1, and 2 respectively, so .1 and .2 should be found
    # in the failed_jobs by whatever, and the .0 should *not* be in there
    found0 = False
    found1 = False
    found2 = False
    tlist = res[0]
    for row in tlist:
        if row[0] == mj.jids[0]:
            found0 = True
        if row[0] == mj.jids[1]:
            found1 = True
        if row[0] == mj.jids[2]:
            found2 = True

    assert(not found0)
    assert(found1)
    assert(found2)
