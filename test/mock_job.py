
import logging
import mock_poms_service
import DBHandle
from webservice.samweb_lite import samweb_lite
import time
import os

class mock_job:
    def __init__(self):
        self.pids = []
        self.jids = []

    def launch(self, campaign_id, n):
	mps = mock_poms_service.mock_poms_service()
	dbh = DBHandle.DBHandle()
        task_id = mps.taskPOMS.get_task_id_for(dbh.get(), campaign_id, experiment = "samdev", command_executed = 'fake_task')
        for i in range(n):
           self.run(task_id)

    def close(self):
        for p in self.pids:
            os.waitpid(p,0)
        self.pids = []
         
    def run(self, task_id):

	jid = str(time.time())+"@fakebatch1.fnal.gov"
        self.jids.append(jid)

        n = os.fork()

        if n < 0:
            print "Ouch!"
        elif n > 0:
            self.pids.append(n)
        else:
	    mps = mock_poms_service.mock_poms_service()
            self.jp = mps.jobsPOMS
	    dbh = DBHandle.DBHandle()

	    logger = logging.getLogger('cherrypy.error')
            rpstatus = "hmm..."
            samhandle = samweb_lite()

            self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')
            time.sleep(1)
            self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Running')
            time.sleep(1)
            self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')
            time.sleep(1)
            exit(0)
