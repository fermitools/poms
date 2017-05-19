import DBHandle
from poms.webservice.condor_log_parser import get_joblogs

dbh = DBHandle.DBHandle()

def test_condor_log_parser():
    jobsub_job_id = "16948234.0@fifebatch1.fnal.gov"
    experiment = "nova"
    role = "Production"
    get_joblogs(dbh.get(), jobsub_job_id, experiment, role)

