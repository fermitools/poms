import DBHandle
import datetime
import time
import os
import socket
from webservice.utc import utc
from webservice.samweb_lite import samweb_lite
from model.poms_model import Campaign, Job

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

def test_1():
    mj = mock_job()
    c = dbhandle.query(Campaign).filter(Campaign.name == 'mwm_test_1').first()
    mj.launch(str(c.campaign_id), n_jobs=1)
    jids = mj.jids
    mj.close()
    job = dbhandle.query(Job).filter(Job.jobsub_job_id == jids[0]).first()

    tuple = mps.triagePOMS.triage_job(dbhandle, fetcher, dbh.cf, job.job_id)
    print tuple
    #assert(False)
