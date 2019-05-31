import mock_poms_service
import DBHandle
from webservice.samweb_lite import samweb_lite
import time
import os
import os.path
import socket
import subprocess
import sys
from utils import setup_ifdhc

setup_ifdhc()

import logging

logger = logging.getLogger("cherrypy.error")


def do_ifdh(*args):
    args = ["ifdh"] + [str(x) for x in args]
    logger.debug("running: " + repr(args))

    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    so, se = p.communicate()
    print("\nstdout:\n", so, "\nstderr:\n", se)
    p.wait()
    return so[:-1]


class job_updater:
    """
       since we don't really track jobs anymore, this instead tracks
       submissions and updates them...
    """

    def __init__(self):
        self.submissions = {}
        self.mps = mock_poms_service.mock_poms_service()

    def update_job(
        self,
        dbh,
        rpstatus,
        samhandle,
        submission_id=None,
        jobsub_job_id=None,
        host_site=None,
        status=None,
        task_project=None,
        user_script=None,
        node_name=None,
        vendor_id=None,
        user_exe_exit_code=None,
    ):
        if self.submissions.get(submission_id, None) == None:
            self.submissions[submission_id] = {}

        self.submissions[submission_id][jobsub_job_id] = status

        min_jobsub_job_id = "zzzz"
        rcount = 0
        icount = 0
        ccount = 0
        hcount = 0
        total = 0
        for jjid, status in self.submissions[submission_id].items():
            if jjid < min_jobsub_job_id:
                min_jobsub_job_id = jjid

            total = total + 1
            if status == "Running":
                rcount = rcount + 1
            if status == "Completed":
                ccount = ccount + 1
            if status == "Idle":
                icount = icount + 1

            if ccount == total:
                sub_status = "Completed"
            elif rcount > 0:
                sub_status = "Running"
            else:
                sub_status = "Idle"

        self.mps.submissionsPOMS.update_submission(dbh, submission_id, min_jobsub_job_id, ccount / total, sub_status, task_project)


rpstatus = "200 Ok."

os.environ["EXPERIMENT"] = "samdev"

version = "v1_0"


class mock_job:
    """ simulate jobs running *and* agents reporting on them
        to allow testing of reporting pages and of workflow code """

    def __init__(self):
        self.pids = []
        self.jids = []

    def launch(self, campaign_stage_id, n_jobs, fileflag=False, dataset=None, exit_code=0):

        if dataset:
            n_jobs += 3
            projname = "mock_jobset_%d" % time.time()
        else:
            projname = None

        mps = mock_poms_service.mock_poms_service()
        dbh = DBHandle.DBHandle()
        submission_id = mps.submissionsPOMS.get_task_id_for(
            dbh.get(), campaign_stage_id, experiment="samdev", command_executed="fake_task"
        )

        print("got POMS_TASK_ID=%s" % submission_id)

        for i in range(n_jobs):
            self.run(submission_id, i, n_jobs, fileflag, dataset, projname, exit_code)

    def close(self):
        for p in self.pids:
            logger.info("waiting for: %d" % p)
            os.waitpid(p, 0)
        self.pids = []

    def run(self, submission_id, i, n_jobs, fileflag=False, dataset=None, projname=None, exit_code=0):

        jid = str(int(time.time()) + i / 10.0) + "@fakebatch1.fnal.gov"
        self.jids.append(jid)

        logger.info("launching fake job id %s" % jid)

        if exit_code == -1:
            exit_code = i

        n = os.fork()

        if n < 0:
            print("Ouch!")
        elif n > 0:
            self.pids.append(n)
        else:
            mps = mock_poms_service.mock_poms_service()
            self.jp = job_updater()
            dbh = DBHandle.DBHandle()
            samhandle = samweb_lite()

            if dataset and i == 0:
                # pretend to be a dagman... wake up right away, wait for everyone else then exit.  We just sleep to wait for them.
                self.jp.update_job(
                    dbh.get(),
                    rpstatus,
                    samhandle,
                    submission_id=submission_id,
                    jobsub_job_id=jid,
                    host_site="fake_host",
                    status="Idle",
                )
                time.sleep(0.5)
                self.jp.update_job(
                    dbh.get(),
                    rpstatus,
                    samhandle,
                    submission_id=submission_id,
                    jobsub_job_id=jid,
                    host_site="fake_host",
                    status="Running",
                )
                self.jp.update_job(
                    dbh.get(), rpstatus, samhandle, submission_id=submission_id, jobsub_job_id=jid, task_project=projname
                )

                time.sleep(20)
                self.jp.update_job(
                    dbh.get(),
                    rpstatus,
                    samhandle,
                    submission_id=submission_id,
                    jobsub_job_id=jid,
                    host_site="fake_host",
                    status="Completed",
                )
                os._exit(0)

            # handle start/end project jobs
            if dataset and i in [1, n_jobs - 1]:
                # if we're a startProject job, we appear/go idle
                #  just after dagman...
                # otherwise (endProject) we wake up after others finish
                if i == 1:
                    time.sleep(1)
                else:
                    for k in range(2, n_jobs - 1):
                        while 1:
                            try:
                                os.kill(self.pids[k], 0)
                            except:
                                break
                # pretend to be a startproject/endproject job
                print("start/end job: %d" % i)
                self.jp.update_job(
                    dbh.get(),
                    rpstatus,
                    samhandle,
                    submission_id=submission_id,
                    jobsub_job_id=jid,
                    host_site="fake_host",
                    status="Idle",
                )

                time.sleep(0.5)
                self.jp.update_job(
                    dbh.get(),
                    rpstatus,
                    samhandle,
                    submission_id=submission_id,
                    jobsub_job_id=jid,
                    host_site="fake_host",
                    status="Running",
                )
                self.jp.update_job(
                    dbh.get(), rpstatus, samhandle, submission_id=submission_id, jobsub_job_id=jid, task_project=projname
                )

                os.environ["EXPERIMENT"] = "samdev"

                try:
                    if i == 1:
                        print("Trying to start project...", projname, str(time.asctime()))
                        u = do_ifdh("startProject", projname, "samdev", dataset, os.environ["USER"], "samdev")
                        time.sleep(7)  # wait for project to actually start

                        print("started project", u, time.asctime())
                    else:
                        u = do_ifdh("findProject", projname, "samdev")
                        do_ifdh("endProject", u)
                except:
                    print("exception in start/end project")
                    print(sys.exc_info())

                self.jp.update_job(
                    dbh.get(),
                    rpstatus,
                    samhandle,
                    submission_id=submission_id,
                    jobsub_job_id=jid,
                    host_site="fake_host",
                    status="Completed",
                )
                os._exit(0)

            else:
                # normal boring job...
                if dataset:
                    # wait for startproject...
                    while 1:
                        try:
                            time.sleep(1)
                            os.kill(self.pids[1], 0)
                        except:
                            break
                    print("finished waiting for startproject ", time.asctime())
                else:
                    time.sleep(2)

                self.jp.update_job(
                    dbh.get(),
                    rpstatus,
                    samhandle,
                    submission_id=submission_id,
                    jobsub_job_id=jid,
                    host_site="fake_host",
                    status="Idle",
                )
                time.sleep(0.5)
                self.jp.update_job(
                    dbh.get(),
                    rpstatus,
                    samhandle,
                    submission_id=submission_id,
                    jobsub_job_id=jid,
                    host_site="fake_host",
                    status="Running",
                )

                self.jp.update_job(
                    dbh.get(),
                    rpstatus,
                    samhandle,
                    submission_id=submission_id,
                    jobsub_job_id=jid,
                    host_site="fake_host",
                    status="running: user code",
                    user_script="/fake/job/script",
                    node_name="fakenode",
                    vendor_id="FakeCPU",
                )

                if dataset:
                    self.jp.update_job(
                        dbh.get(), rpstatus, samhandle, submission_id=submission_id, jobsub_job_id=jid, task_project=projname
                    )

                    print("Trying to find project...", time.asctime())
                    u = do_ifdh("findProject", projname, "samdev")
                    # hostname = socket.gethostname()
                    hostname = "fnpc3000.fnal.gov"

                    # ifdh establishProcess  projecturi  appname  appversion  location  user  appfamily   description   filelimit   schemas

                    print(
                        "trying to establishProcess(%s, 'demo', %s, %s, %s, %s, %s %s) %s \n"
                        % (u, version, hostname, os.environ["USER"], "demo", jid, 1, time.asctime())
                    )

                    cid = do_ifdh("establishProcess", u, "demo", version, hostname, os.environ["USER"], "demo", jid, 1)
                    f = do_ifdh("getNextFile", u, cid)
                    inpf = os.path.basename(f)
                    self.jp.update_job(
                        dbh.get(),
                        rpstatus,
                        samhandle,
                        submission_id=submission_id,
                        jobsub_job_id=jid,
                        host_site="fake_host",
                        status="running: copying files in",
                        input_file_names=os.path.basename(f),
                    )
                    time.sleep(0.5)
                    do_ifdh("updateFileStatus", u, cid, f, "transferred")
                    self.jp.update_job(
                        dbh.get(),
                        rpstatus,
                        samhandle,
                        submission_id=submission_id,
                        jobsub_job_id=jid,
                        host_site="fake_host",
                        status="running",
                    )
                    time.sleep(0.5)
                    do_ifdh("updateFileStatus", u, cid, f, "consumed")
                    do_ifdh("endProcess", u, cid)
                else:
                    inpf = "fake_input_%s" % jid
                    if fileflag:
                        self.jp.update_job(
                            dbh.get(),
                            rpstatus,
                            samhandle,
                            submission_id=submission_id,
                            jobsub_job_id=jid,
                            host_site="fake_host",
                            status="running: copying files in",
                            input_file_names=inpf,
                        )
                    time.sleep(2)

                if fileflag:
                    ofn = ("fake_output_%s" % jid).replace("@", "_")
                    self.jp.update_job(
                        dbh.get(),
                        rpstatus,
                        samhandle,
                        submission_id=submission_id,
                        jobsub_job_id=jid,
                        host_site="fake_host",
                        status="running: copying files out",
                        output_file_names=ofn,
                    )
                    self.jp.update_job(
                        dbh.get(),
                        rpstatus,
                        samhandle,
                        submission_id=submission_id,
                        jobsub_job_id=jid,
                        host_site="fake_host",
                        status="running: user code completed",
                        user_exe_exit_code=str(exit_code),
                    )
                else:
                    self.jp.update_job(
                        dbh.get(),
                        rpstatus,
                        samhandle,
                        submission_id=submission_id,
                        jobsub_job_id=jid,
                        host_site="fake_host",
                        status="running: user code failed",
                        user_exe_exit_code=str(exit_code),
                    )
                self.jp.update_job(
                    dbh.get(),
                    rpstatus,
                    samhandle,
                    submission_id=submission_id,
                    jobsub_job_id=jid,
                    host_site="fake_host",
                    status="Completed",
                )
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
""" % (
                    ofn,
                    now,
                    now,
                    fs,
                    checksum,
                    vers,
                    inpf,
                )
                # declare metadata
                # add location
                mf = "/tmp/foo.json"
                fp = open(mf, "w")
                fp.write(meta_json)
                fp.close()
                os.system("samweb -e samdev declare-file %s" % mf)
                # os.unlink(mf)
                os.system("samweb -e samdev add-file-location %s 'dcache:/pnfs/fermilab/volatile/fake/'" % ofn)

            os._exit(0)


if __name__ == "__main__":
    import sys

    n_jobs = 1
    campaign_stage_id = "18"  # _joe...
    logger.setLevel(50)
    dataset = None
    fileflag = False
    waitflag = False
    while len(sys.argv) > 1:
        print("d:", len(sys.argv), sys.argv)
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
            if dataset == "None":
                # if they gave us "None", ignore it...
                dataset = None
            sys.argv = sys.argv[2:]
            continue
        if sys.argv[1] == "--campaign_stage_id":
            campaign_stage_id = sys.argv[2]
            sys.argv = sys.argv[2:]
            continue
        if sys.argv[1] == "--wait":
            waitflag = True
            sys.argv = sys.argv[1:]
            continue
        print("unknown argument:", sys.argv[1])
        break

    print("n_jobs:", n_jobs, "campaign_stage_id", campaign_stage_id)

    m = mock_job()
    m.launch(campaign_stage_id, n_jobs, fileflag, dataset)

    print("Fake jobids:", m.jids)

    if waitflag:
        m.close()
