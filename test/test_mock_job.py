from mock_job import mock_job
import DBHandle
import datetime
from webservice.utc import utc
from mock_poms_service import mock_poms_service

dbh = DBHandle.DBHandle()
mps = mock_poms_service()

def test_mock_job_1():
    m = mock_job()
    m.launch("14",3)
    print "mock_job_ids == ", m.jids
    
    sleep(3)
    # make sure all jobs showed up...
    for j in m.jids:
        dj = dbh.query(Job).filter(Job.jobsub_job_id == j ).first()
        assert(dj != None)
    m.close()

test_mock_job_1()
