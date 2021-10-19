import DBHandle
from poms.webservice.condor_log_parser import get_joblogs
from utils import get_config
import logging

logger = logging.getLogger("cherrypy.error")

dbh = DBHandle.DBHandle()

config = get_config()
cert = eval(config.get("elasticsearch_cert"))
key = eval(config.get("elasticsearch_key"))

logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


def test_condor_log_parser():
    jobsub_job_id = "49654860@jobsub02.fnal.gov"
    experiment = "samdev"
    role = "Analysis"
    print("cert: %s, key %s" % (cert, key))
    res = get_joblogs(dbh.get(), jobsub_job_id, cert, key, experiment, role)
    print("res: %s" % repr(res))
    if isinstance(res, dict):
        print(
            "n_idle %d, n_running %d, n_completed %d"
            % (len(res.get("idle", [])), len(res.get("running", [])), len(res.get("completed", [])))
        )
    # assert(False)
