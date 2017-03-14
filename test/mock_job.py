
import logging
import mock_poms_service
import DBHandle
from webservice.samweb_lite import samweb_lite
import time
import os
import os.path
import socket

logger = logging.getLogger('cherrypy.error')

rpstatus = "200 Ok."

os.environ['EXPERIMENT'] = 'samdev'

version = "v1_0"

class mock_job:
    def __init__(self):
        self.pids = []
        self.jids = []

    def launch(self, campaign_id, n_jobs, fileflag = False,dataset = None, exit_code = 0):

        
        if dataset:
           n_jobs += 3
           projname = "mock_jobset_%d" % time.time()
        else:
           projname = None
           
	mps = mock_poms_service.mock_poms_service()
	dbh = DBHandle.DBHandle()
        task_id = mps.taskPOMS.get_task_id_for(dbh.get(), campaign_id, experiment = "samdev", command_executed = 'fake_task')
        for i in range(n_jobs):
           self.run(task_id, i, n_jobs, fileflag, dataset, projname, exit_code)

    def close(self):
        for p in self.pids:
            logger.info("waiting for: %d" % p)
            os.waitpid(p,0)
        self.pids = []
         
    def run(self, task_id, i, n_jobs, fileflag = False, dataset = None, projname = None, exit_code = 0):

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
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, task_project = projname)

                time.sleep(10)
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')
                os._exit(0)

            # handle start/end project jobs
            if dataset and i in [1,n_jobs - 1]:
                # if we're a startProject job, we appear/go idle 
                #  just after dagman...
                # otherwise (endProject) we wake up after others finish
                if i == 1:
                    time.sleep(1)
                else:
                    time.sleep(9)
                # pretend to be a startproject/endproject job
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')

                time.sleep(0.5)
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Running')
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, task_project = projname)

                os.environ['EXPERIMENT'] = 'samdev'

                import ifdh
                ih = ifdh.ifdh()
                try:
                   if i == 1:
                      u = ih.startProject(projname, 'samdev', dataset, os.environ['USER'],'samdev')
	   
                      print "started project", u
                   else:
                      u = ih.findProject(projname, 'samdev')
                      ih.endProject(u)
                except:
                    print "exception in start/end project"
                    print sys.exc_info()
                   
                self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')
                os._exit(0)
      
            else:
                # normal boring job...
                if dataset:
                    # wait for startproject...
                    while os.kill(self.pids[1],0):
                        sleep(1)
		time.sleep(2)

		self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')
		time.sleep(0.5)
		self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Running')

		self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: user code', user_script = '/fake/job/script', node_name='fakenode', vendor_id = 'FakeCPU')
                 
                if dataset:
                    self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, task_project = projname)

                    import ifdh
                    ih = ifdh.ifdh()
		    u = ih.findProject(projname, 'samdev')
                    #hostname = socket.gethostname()
                    hostname = 'fnpc3000.fnal.gov'

		    cid = ih.establishProcess( u, 'demo', version, hostname, os.environ['USER'], 'demo', jid, 1)
		    f = ih.getNextFile(u, cid)
                    inpf = os.path.basename(f)
		    self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: copying files in', input_file_names = os.path.basename(f))
                    time.sleep(0.5)
		    ih.updateFileStatus(u, cid, f, 'transferred')
		    self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running')
                    time.sleep(0.5)
		    ih.updateFileStatus(u, cid, f, 'consumed')
                    ih.endProcess(u, cid)
                else:
                    inpf = 'fake_input_%s' % jid
                    if fileflag:
		        self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: copying files in', input_file_names = inpf )
                    time.sleep(2)
                
		if fileflag:
                    ofn = ('fake_output_%s' % jid).replace('@','_')
                    self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: copying files out', output_file_names = ofn )

                if exit_code == 0:
		    self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: user code succeeded', user_exe_exit_code = 0)
                elif exit_code == -1:
		    self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: user code failed', user_exe_exit_code = str(i))
                else:
		    self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running: user code failed', user_exe_exit_code = exit_code)
		self.jp.update_job(dbh.get(), logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')
            time.sleep(1)

            # have file appear with location...

	    if fileflag:
                vers = "v1_0"
                # stats for a file containing "hello\n"
                checksum = '"enstore:138740254"'
                fs = 6
                now = time.strftime("%Y-%m-%d %H:%M:%S")
                meta_json = """
{
  "file_name":   "%s",
  "create_date": "%s",
  "update_date": "%s",
  "file_type":   "test",
  "file_size":   %s,
  "checksum":    %s,
  "application": {
    "family":      "demo",
    "name":        "fake_eventgen",
    "version":      "%s"
  },
  "parents": [ {"file_name": "%s" } ]
}
""" % (ofn, now, now, fs, checksum, vers, inpf)
                # declare metadata
                # add location
                mf = "/tmp/foo.json"
                fp = open(mf, "w")
                fp.write(meta_json)
                fp.close()
                os.system("samweb -e samdev declare-file %s" % mf)
                #os.unlink(mf)
                os.system("samweb -e samdev add-file-location %s 'dcache:/pnfs/fermilab/volatile/fake/'" % ofn )
           
            os._exit(0)

if __name__ == '__main__':
    import sys
    n_jobs = 1
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
           n_jobs = int(sys.argv[2])
           sys.argv = sys.argv[2:]
           continue
        if sys.argv[1] == "-D":
           dataset = sys.argv[2]
           sys.argv = sys.argv[2:]
           continue
        if sys.argv[1] == "--campaign_id":
           campaign_id = sys.argv[2]
           sys.argv = sys.argv[2:]
           continue

    print "n_jobs:", n_jobs, "campaign_id", campaign_id

    m = mock_job()
    m.launch(campaign_id, n_jobs, fileflag, dataset)

    print "Fake jobids:", m.jids

