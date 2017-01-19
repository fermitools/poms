
import mock_poms_service
import DBHandle

mps = mock_poms_service()
dbh = DBHandle.DBHandle()
logger = logging.getLogger('cherrypy.error')


class mock_job:
    def __init__(self):
        self.pids = []
        self.jp = mps.jobsPOMS
        self.jid = str(time.time())+"@fakebatch1.fnal.gov"

    def fake_jobsub_jobid(self):
        self.return jid

    def fake_launch(self, n)
        task_id = mps.taskPOMS.get_task_id(dbh.get, 14, experiment = 'samdev', command_executed = 'fake_task')
        for i in range(n):
           self.run(task_id)
         
    def run(self, task_id):
        jjid = self.fake_jobsub_jobid()
        n = fork()
        if n < 0:
            print "Ouch!"
        elif n > 0:
            self.pids.append(n)
        else:
            self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, host_site = "fake_host", status = 'Idle')
            time.sleep(1)
            self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, host_site = "fake_host", status = 'Running')
            time.sleep(1)
            self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, host_site = "fake_host", status = 'Completd')
            time.sleep(1)
            exit(0)
