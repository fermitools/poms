import DBHandle
from poms.webservice.condor_log_parser import get_joblogs
from poms.webservice.logit import  setlevel, log, DEBUG, INFO, CRITICAL
import logging.config
from poms.webservice import logging_conf


dbh = DBHandle.DBHandle()

def test_condor_log_parser():
    logging.config.dictConfig(logging_conf.LOG_CONF)
    setlevel(level=DEBUG)
    log(CRITICAL, "testing 1 2 3")
    log(INFO, "testing 4 5 6")
    log(DEBUG, "testing 7 8 9")
    jobsub_job_id = "16948234.0@fifebatch1.fnal.gov"
    experiment = "nova"
    role = "Production"
    get_joblogs(dbh.get(), jobsub_job_id, experiment, role)
    assert(False)

