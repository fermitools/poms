
import logging
import mock_poms_service
import DBHandle
from webservice.samweb_lite import samweb_lite
import time
import os

logger = logging.getLogger('cherrypy.error')

rpstatus = "200 Ok."

class mock_job:
    def __init__(self):
        self.pids = []
        self.jids = []

    def launch(self, campaign_id, n_jobs, fileflag = False,dataset = None):

        
        if dataset:
           njobs += 3
           projname = "mock_jobset_%d" % time.time()
        else:
           projname = None
           
	mps = mock_poms_service.mock_poms_service()
	dbh = DBHandle.DBHandle()
        task_id = mps.taskPOMS.get_task_id_for(dbh.get(), campaign_id, experiment = "samdev", command_executed = 'fake_task')
        for i in range(n_jobs):
           self.run(task_id, i, n_jobs, fileflag, dataset, projname)

    def close(self):
        for p in self.pids:
            logger.info("waiting for: %d" % p)
            os.waitpid(p,0)
        self.pids = []
         
    def run(self, task_id, i, njobs, fileflag = False, dataset = None, projname = None):

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
            samhandle = samweb_lite()

            if dataset and i == 0:
                # pretend to be a dagman... wake up right away, wait for everyone else then exit.  We just sleep to wait for them.
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')
                time.sleep(0.5)
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Running')
                time.sleep(10)
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')
                os._exit(0)

            # handle start/end project jobs
            if dataset and i in [1,njobs - 1]:
                # if we're a startProject job, we appear/go idle 
                #  just after dagman...
                # otherwise (endProject) we wake up after others finish
                if i == 1:
                    time.sleep(1)
                else:
                    time.sleep(8)
                # pretend to be a startproject/endproject job
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')

                time.sleep(0.5)
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Running')

                os.environ['EXPERIMENT'] = 'samdev'

                import ifdh
                ih = ifdh.ifdh()
                if i == 1:
                   u = ih.startProject(name, )
                   print "started project", u
                else:
                   u = ih.findProject(projname, 'samdev')
                   ih.endProject(u)
                   
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')
                os._exit(0)
      
            else:
                # normal boring job...
                time.sleep(2)

		self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')
		time.sleep(0.5)
		self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Running')
		self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: user code', user_script = '/fake/job/script', node_name='fakenode', vendor_id = 'FakeCPU')
                 
                if dataset:
		    u = ih.findProject(projname, 'samdev')
		    cid = ih.establishProcess( u, 'demo', version, hostname, os.environ['USER'], 'demo', jid, 1)
		    f = ih.getNextFile(u, cid)
		    self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: copying files in', input_file_names = os.path.basename(f))
                    time.sleep(0.5)
		    ih.updateFileStatus(u, cid, f, 'transferred')
		    self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running')
                    time.sleep(0.5)
		    ih.updateFileStatus(u, cid, f, 'consumed')
                    ih.endProcess(u, cid)
                else:
                    if fileflag:
		        self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: copying files in', input_file_names = 'fake_input_%s' % jid)
                    time.sleep(2)
                
		if fileflag:
                    self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: copying files out', output_file_names = 'fake_output_%s' % jid )
		self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: user code succeeded', user_exe_exit_code = 0)
		self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')
            time.sleep(1)
            os._exit(0)

if __name__ == '__main__':
    import sys
    njobs = 1
    campaign_id = "14"
    logger.setLevel(50)
    dataset = None
    fileflag = False
    while len(sys.argv) > 1:
        print "d:" , len(sys.argv), sys.argv
        if sys.argv[1] == "-v":
           logger.setLevel(10)
           sys.argv = sys.argv[1:]
           continue
        if sys.argv[1] == "-f":
           fileflag = True
           sys.argv = sys.argv[1:]
           continue
        if sys.argv[1] == "-N":
           njobs = int(sys.argv[2])
           sys.argv = sys.argv[2:]
           continue
        if sys.argv[1] == "-D":
           dataset = int(sys.argv[2])
           sys.argv = sys.argv[2:]
           continue
        if sys.argv[1] == "--campaign_id":
           campaign_id = sys.argv[2]
           sys.argv = sys.argv[2:]
           continue

    print "njobs:", njobs, "campaign_id", campaign_id

    m = mock_job()
    m.launch(campaign_id, njobs, fileflag, dataset)

    print "Fake jobids:", m.jids

