
import logging
import mock_poms_service
import DBHandle
from webservice.samweb_lite import samweb_lite
import time
import os

logger = logging.getLogger('cherrypy.error')


class mock_job:
    def __init__(self):
        self.pids = []
        self.jids = []

    def launch(self, campaign_id, n):
	mps = mock_poms_service.mock_poms_service()
	dbh = DBHandle.DBHandle()
        task_id = mps.taskPOMS.get_task_id_for(dbh.get(), campaign_id, experiment = "samdev", command_executed = 'fake_task')
        for i in range(n):
           self.run(task_id, i)

    def close(self):
        for p in self.pids:
            logger.info("waiting for: %d" % p)
            os.waitpid(p,0)
        self.pids = []
         
    def run(self, task_id, i):

	jid = str(int(time.time()) + i/10.0)+"@fakebatch1.fnal.gov"
        self.jids.append(jid)

        logger.info("launching fake job id %s" % jid)

        n = os.fork()

        if n < 0:
            print "Ouch!"
        elif n > 0:
            self.pids.append(n)
        else:
	    mps = mock_poms_service.mock_poms_service()
            self.jp = mps.jobsPOMS
	    dbh = DBHandle.DBHandle()

            rpstatus = "hmm..."
            samhandle = samweb_lite()

            self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')
            time.sleep(1)
            self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Running')
            time.sleep(1)
            self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')
            time.sleep(1)
            os._exit(0)

if __name__ == '__main__':
    import sys
    njobs = 1
    campaign_id = "14"
    logger.setLevel(50)
    while len(sys.argv) > 1:
        print "d:" , len(sys.argv), sys.argv
        if sys.argv[1] == "-v":
           logger.setLevel(10)
           sys.argv = sys.argv[1:]
           continue
        if sys.argv[1] == "-N":
           njobs = int(sys.argv[2])
           sys.argv = sys.argv[2:]
           continue
        if sys.argv[1] == "--campaign_id":
           campaign_id = sys.argv[2]
           sys.argv = sys.argv[2:]
           continue

    print "njobs:", njobs, "campaign_id", campaign_id

    m = mock_job()
    m.launch(campaign_id, njobs)

    print "Fake jobids:", m.jids

