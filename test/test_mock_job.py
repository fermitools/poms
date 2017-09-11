from mock_job import mock_job
import DBHandle
import datetime
from poms.webservice.utc import utc
from poms.webservice.poms_model import Job
from mock_poms_service import mock_poms_service
import time
from utils import beverbose

dbh = DBHandle.DBHandle()
mps = mock_poms_service()

#beverbose()

import logging
logger = logging.getLogger('cherrypy.error')

def test_mock_job_1():
    m = mock_job()
    njobs = 3
    m.launch("14", njobs)
    print("mock_job_ids == ", m.jids)
    
    time.sleep(3)

    # make sure all jobs showed up...

    for j in m.jids:
        dj = dbh.get().query(Job).filter(Job.jobsub_job_id == j ).first()
        assert(dj != None)

    assert(len(m.jids) == njobs)
    m.close()

def test_mock_job_2():
    m = mock_job()
    njobs = 3
    m.launch("14", njobs, fileflag=1, dataset="gen_cfg")
    print("mock_job_ids == ", m.jids)
    
    m.close()

    # make sure all jobs showed up...

    print("jobids: ", repr(m.jids))
    for j in m.jids:
        dj = dbh.get().query(Job).filter(Job.jobsub_job_id == j ).first()
        if not dj:
            print( j , " -- not found!")
        assert(dj != None )
        print("jobid: " , j , "found")

    m.close()

    # should have 3 extra job ids, leader and 2 trailers...
    assert(len(m.jids) == njobs + 3)
    m.close()
