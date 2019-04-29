import DBHandle
from poms.webservice.condor_log_parser import get_joblogs
from mock_stubs import getconfig

dbh = DBHandle.DBHandle()

cert = (getconfig("elasticsearch_cert"),)
key = (getconfig("elasticsearch_key"),)


def test_condor_log_parser():
    jobsub_job_id = "18771837.0@jobsub02.fnal.gov"
    experiment = "samdev"
    role = "Analysis"
    get_joblogs(dbh.get(), jobsub_job_id, cert, key, experiment, role)
