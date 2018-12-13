import DBHandle
import datetime
import time
import os
import json
import socket
from poms.webservice.utc import utc
from poms.webservice.samweb_lite import samweb_lite
from poms.webservice.poms_model import CampaignStage, JobType, LoginSetup, Submission

from mock_stubs import gethead, launch_seshandle, camp_seshandle, err_res, getconfig

from mock_poms_service import mock_poms_service
from mock_redirect import mock_redirect_exception
import Mock_jobsub_rm
import logging
logger = logging.getLogger('cherrypy.error')
# when I get one...

mps = mock_poms_service()
mock_rm=Mock_jobsub_rm.Mock_jobsub_rm()

rpstatus = "200"
########



def test_kill_jobs():
    ##Calling the DB and SAM handles methods.
    dbhandle = DBHandle.DBHandle().get()
    samhandle = samweb_lite()
    #mock_rm=Mock_jobsub_rm.Mock_jobsub_rm()
    #Two submission_id for the same campaign
    submission_id = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14') #Provide a submission_id for the fake campaign
    task_id2 = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14') #Provide a submission_id for the second task
    jid_n = time.time()

    jid1 = "%d.0@fakebatch1.fnal.gov" % jid_n  #1 Job in the first submission_id
    mps.taskPOMS.update_submission(dbhandle, submission_id = submission_id, jobsub_job_id = jid1, status = 'Running')
    jid3 = "%d.0@fakebatch1.fnal.gov" % (jid_n + 2) #3Job in a new submission_id but same campaign
    mps.taskPOMS.update_submission(dbhandle, submission_id = task_id2, jobsub_job_id = jid3,  status = 'Running')

    #Control arguments
    c_arg="-G fermilab --role Analysis --jobid "
    c_output_killjob = jid1 #Control output
    c_output_killTask = [jid1.replace('.0','')] #Control output #it is going to kill the task just killing the first job without .0, cluster.
    c_output_killCampaign =[jid1.replace('.0',''),jid3.replace('.0','')] #Control output it is going to kill the CampaignStage just killing the first job without of each submission_id


    #Calls to the rutine under test.
    output_killTask, c_obje_T, c_idr_T, task_idr_T, job_idr_T = mps.jobsPOMS.kill_jobs(dbhandle, submission_id=submission_id, confirm = "yes", act="kill") #all task
    output_killCampaign, c_obje_C, c_idr_C, task_idr_C, job_idr_C = mps.jobsPOMS.kill_jobs(dbhandle, campaign_stage_id='14', confirm = "yes", act="kill") #all campaign

    #Check kill jobs in one task

    sep=output_killTask.rfind('--jobid ')
    assert(sep!=-1) #--jobid option was in called in the command
    print("got output:", output_killTask)
    jrm_idtl=output_killTask.split('--jobid ')[1].split(",")
    jrm_idtl[-1]=jrm_idtl[-1].rstrip('\n')

    # we may have jobs besides the ones we just added in the task , just do ours..
    for jid in c_output_killTask:
        assert(jid in jrm_idtl)

    #Check kill all jobs in one CampaignStage,  that also prof that the job market as completed is not killed.
    sep=output_killCampaign.rfind('--jobid ')
    assert(sep!=-1) #--jobid option was in called in the command
    print("got output:", output_killCampaign)
    jrm_idcl=output_killCampaign.split('--jobid ')[1].split(",")
    jrm_idcl[-1]=jrm_idcl[-1].rstrip('\n')

    # there may be *other* jobs in this campaign than the ones we added in this test

    # just make sure the ones we have are in there.
    #
    for jid in c_output_killCampaign:
        assert(jid in jrm_idcl)

    #Closing the mock
    mock_rm.close()

    '''
    Verbosity not necessary
    Checking variables, no necessary for the test.
    print "$$"*20
    print "The output_killjob is: ", output_killjob
    print "c_obje", c_obje
    print "c_idr", task_idr
    print "job_idr", job_idr
    print "The output_killjob2 is: ", output_killjob2
    print "c_obje2", c_obje2
    print "c_idr2", task_idr2
    print "job_idr2", job_idr2
    print "The output_killTask is: ", output_killTask
    print "c_obje", c_obje_T
    print "c_idr", task_idr_T
    print "job_idr", job_idr_T
    print "The output_killCampaign is: ", output_killCampaign
    print "c_obje", c_obje_C
    print "c_idr", task_idr_C
    print "job_idr", job_idr_C
    print "$$"*20

    t_obj1=dbhandle.query(Submission).filter(Submission.submission_id == submission_id).first()
    t_obj2=dbhandle.query(Submission).filter(Submission.submission_id == task_id2).first()
    job_obj_db = dbhandle.query(Job).filter(Job.submission_id== submission_id).all()  ####this is thing
    print "*"*10
    print "task_id1", submission_id
    print "task_id2", task_id2
    print "Id of this test"
    print "job_id1 = ", job_obj1.job_id
    print jid1
    print "job_id2 = ", job_obj2.job_id
    print jid2
    print "job_id3 = ", job_obj3.job_id
    print jid3
    print "job_id4 = ", job_obj4.job_id
    print jid4
    print "jobs with task id", submission_id
    for x in job_obj_db:
        print x.jobsub_job_id
    print "#"*10
    '''

