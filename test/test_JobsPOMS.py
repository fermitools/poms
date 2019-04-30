import DBHandle
import datetime
import time
import os
import json
import socket
from poms.webservice.utc import utc
from poms.webservice.samweb_lite import samweb_lite
from poms.webservice.poms_model import CampaignStage, JobType, LoginSetup, Submission
from mock_Ctx import Ctx

from mock_stubs import gethead, launch_seshandle, camp_seshandle, err_res, getconfig

from mock_poms_service import mock_poms_service
from mock_redirect import mock_redirect_exception
import Mock_jobsub_rm
import logging

logger = logging.getLogger("cherrypy.error")
# when I get one...

mps = mock_poms_service()
mock_rm = Mock_jobsub_rm.Mock_jobsub_rm()

rpstatus = "200"
########


def test_kill_jobs():
    ctx = Ctx(sam=samweb_lite())
    mock_rm = Mock_jobsub_rm.Mock_jobsub_rm()

    # Two submission_id for the same campaign
    # Provide a submission_id for the fake campaign
    submission_id = mps.taskPOMS.get_task_id_for(ctx, campaign="14")
    # Provide a submission_id for the second task
    task_id2 = mps.taskPOMS.get_task_id_for(ctx, campaign="14")
    jid_n = time.time()

    # 1 Job in the first submission_id
    jid1 = "%d.0@fakebatch1.fnal.gov" % jid_n
    mps.taskPOMS.update_submission(ctx, submission_id=submission_id, jobsub_job_id=jid1, status="Running")

    # 3Job in a new submission_id but same campaign
    jid3 = "%d.0@fakebatch1.fnal.gov" % (jid_n + 2)
    mps.taskPOMS.update_submission(ctx, submission_id=task_id2, jobsub_job_id=jid3, status="Running")

    # Control arguments
    c_arg = "-G fermilab --role Analysis --jobid "
    c_output_killjob = jid1  # Control output
    c_output_killTask = [jid1.replace(".0", "")]
    # Control output
    # it is going to kill the task just killing the first job without .0, cluster.
    c_output_killCampaign = [jid1.replace(".0", ""), jid3.replace(".0", "")]
    # Control output it is going to kill the CampaignStage
    # just killing the first job without of each submission_id

    # Calls to the rutine under test.
    # all task

    output_killTask, c_obje_T, c_idr_T, task_idr_T, job_idr_T = mps.jobsPOMS.kill_jobs(
        ctx, submission_id=submission_id, confirm="yes", act="kill"
    )

    # all campaign
    output_killCampaign, c_obje_C, c_idr_C, task_idr_C, job_idr_C = mps.jobsPOMS.kill_jobs(
        ctx, campaign_stage_id="14", confirm="yes", act="kill"
    )

    # Check kill jobs in one task
    print("got output:", output_killTask)
    assert output_killTask.rfind("--jobid=") > 0
    assert output_killTask.rfind("=%s" % jid1) > 0

    # we may have jobs besides the ones we just added in the task , just do ours..

    # Check kill all jobs in one CampaignStage,  that also prof that the job market as completed is not killed.
    print("got output:", output_killCampaign)
    assert output_killCampaign.rfind("--constraint=POMS4_CAMPAIGN_STAGE_ID") > 0
    assert output_killCampaign.rfind("=14") > 0

    # there may be *other* jobs in this campaign than the ones we added in this test

    # Closing the mock
    mock_rm.close()


def test_jobtype_list():
    ctx = Ctx(sam=samweb_lite())
    res = mps.jobsPOMS.jobtype_list(ctx, name=None, full=None)
    print("got: ", res)
    found = False
    for d in res:
        if d.get("name", "") == "generic":
            found = True
    assert found


"""
Not yet implemented:

def test_update_SAM_project():
    res = mps.jobsPOMS.update_SAM_project(ctx, j, projname)

"""
