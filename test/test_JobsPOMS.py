import DBHandle
import datetime
import time
import os
import json
import socket
from poms.webservice.utc import utc
from poms.webservice.samweb_lite import samweb_lite
from poms.webservice.poms_model import Campaign, CampaignDefinition, LaunchTemplate, Job, Task

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

#
# ---------------------------------------
# utilities to set up tests
# test update_job with some set of fields
#
def do_update_job(fielddict):
    dbhandle = DBHandle.DBHandle().get()
    samhandle = samweb_lite()
    #fielddict['status']
    task_id = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14')
    jid = "%d@fakebatch1.fnal.gov" % time.time()

    mps.jobsPOMS.update_job(dbhandle, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')

    mps.jobsPOMS.update_job(dbhandle, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid,  **fielddict )

    j = dbhandle.query(Job).filter(Job.jobsub_job_id == jid).first()

    assert(j != None)

    for f,v in list(fielddict.items()):
        if f.startswith('task_'):
            f = f[5:]
            assert(getattr(j.task_obj, f) == v)
        elif f.endswith('file_names'):
            # should look it up on JobFiles but...
            pass
        else:
            assert(str(getattr(j,f)) == str(v))

    mps.jobsPOMS.update_job(dbhandle, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')


def test_active_update_jobs():
    dbhandle = DBHandle.DBHandle().get()
    samhandle = samweb_lite()

    l1 = mps.jobsPOMS.active_jobs(dbhandle)

    jid = "%f@fakebatch1.fnal.gov" % time.time()

    # start up a fake job
    task_id = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14')

    mps.jobsPOMS.update_job(dbhandle, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')

    l2 = mps.jobsPOMS.active_jobs(dbhandle)

    # end fake job
    mps.jobsPOMS.update_job(dbhandle, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')

    l3 = mps.jobsPOMS.active_jobs(dbhandle)

    # so job list should have gotten longer and shorter
    # and jobid should be only in the middle list...
    assert(len(l2)  > len(l1))
    assert(len(l2)  > len(l3))
    jpair = (jid,task_id)
    assert(jpair in l2)
    assert(not jpair in l3)

def test_update_SAM_project():
    # stubbed out for now, until production SAM implements the call.
    assert(True)

def test_update_job_1():
    # try updating lots of parameters
    fielddict = {
        'cpu_type': 'Athalon',
        'node_name': 'fake.some.domain',
        'host_site': 'FAKESITE',
        'status' : 'running: on empty',
        'user_exe_exit_code': '17',
        'task_project': 'mwm_demo_project',
        # needs a reail task id...
        # 'task_recovery_tasks_parent': '1234',
        'cpu_time': '100.3',
        'wall_time': '200.4',
        'output_file_names': 'outfile1.root outfile2.root',
        'input_file_names': 'infile1.root',
    }
    do_update_job(fielddict)

def test_bulk_update_job():
    dbhandle = DBHandle.DBHandle().get()
    samhandle = samweb_lite()
    l1 = mps.jobsPOMS.active_jobs(dbhandle)

    ft = float(int(time.time()))
    jids = []
    jids.append("%.1f@fakebatch1.fnal.gov" % ft)
    jids.append("%.1f@fakebatch1.fnal.gov" % (ft + 0.1))
    jids.append("%.1f@fakebatch1.fnal.gov" % (ft + 0.2))
    task_id = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14')

    data = []
    for jid in jids:
        data.append({'jobsub_job_id': jid, 'task_id': task_id, 'status': 'Idle'})

    mps.jobsPOMS.bulk_update_job(dbhandle, rpstatus, samhandle, json.dumps(data) )

    for jid in jids:
        j = dbhandle.query(Job).filter(Job.jobsub_job_id == jid).first()
        assert(jid != None)


def test_update_job_2():
    # try updating lots of parameters
    fielddict = {
        'status' : 'running: on full',
    }
    do_update_job(fielddict)

    print("testing all the info that the jobscraper pass the job_log_scraper")
    fielddict = {
                'status': 'test_status' ,
                 # 'slot':'finally_something_in_this_field', # -- ??? mengel
                'output_file_names':'test_file_test_joblogscraper.txt' ,
                'node_name': 'fake_node_test_joblogscraper',
                'user_exe_exit_code':'10',
                'cpu_type': 'Athalon',
                }
    do_update_job(fielddict)


def test_update_job_q_scraper():

    print("check this from the jobsub_q scrapper")
    fielddict = {
                }


def test_kill_jobs():
    ##Calling the DB and SAM handles methods.
    dbhandle = DBHandle.DBHandle().get()
    samhandle = samweb_lite()
    #mock_rm=Mock_jobsub_rm.Mock_jobsub_rm()
    #Two task_id for the same campaign
    task_id = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14') #Provide a task_id for the fake campaign
    task_id2 = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14') #Provide a task_id for the second task

    jid_n = time.time() * 10
    #Create jobs in the same campaign, 2 in one task_id, one in another task_id but same campaign, and on job in the same task_id, campaign but market as completed.
    jid1 = "%d.0@fakebatch1.fnal.gov" % jid_n  #1 Job in the first task_id
    mps.jobsPOMS.update_job(dbhandle, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid1, host_site = "fake_host", status = 'running')
    jid2 = "%d.0@fakebatch1.fnal.gov" % (jid_n + 1) #2Job in the first task_id
    mps.jobsPOMS.update_job(dbhandle, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid2, host_site = "fake_host", status = 'running')
    jid3 = "%d.0@fakebatch1.fnal.gov" % (jid_n + 2) #3Job in a new task_id but same campaign
    mps.jobsPOMS.update_job(dbhandle, rpstatus, samhandle, task_id = task_id2, jobsub_job_id = jid3, host_site = "fake_host", status = 'running')
    jid4 = "%d.0@fakebatch1.fnal.gov" % (jid_n + 3) 
    mps.jobsPOMS.update_job(dbhandle, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid4, host_site = "fake_host", status = 'Completed')

    #Control arguments
    c_arg="-G fermilab --role Analysis --jobid "
    c_output_killjob = jid1 #Control output
    c_output_killTask = [jid1.replace('.0','')] #Control output #it is going to kill the task just killing the first job without .0, cluster.
    c_output_killCampaign =[jid1.replace('.0',''),jid3.replace('.0','')] #Control output it is going to kill the Campaign just killing the first job without of each task_id

    #Guetting the jid (key in database) that belong to the jobid in jobsub. They are different. The key db is used in the next code block
    job_obj1 = dbhandle.query(Job).filter(Job.jobsub_job_id == jid1).first()
    job_obj2 = dbhandle.query(Job).filter(Job.jobsub_job_id == jid2).first()
    #job_obj3 = dbhandle.query(Job).filter(Job.jobsub_job_id == jid3).first()
    #job_obj4 = dbhandle.query(Job).filter(Job.jobsub_job_id == jid4).first()

    #Calls to the rutine under test.
    output_killjob, c_obje, c_idr, task_idr, job_idr = mps.jobsPOMS.kill_jobs(dbhandle, job_id=job_obj1.job_id, confirm = "yes") #single job
    output_killTask, c_obje_T, c_idr_T, task_idr_T, job_idr_T = mps.jobsPOMS.kill_jobs(dbhandle, task_id=task_id, confirm = "yes") #all task
    output_killCampaign, c_obje_C, c_idr_C, task_idr_C, job_idr_C = mps.jobsPOMS.kill_jobs(dbhandle, campaign_id='14', confirm = "yes") #all campaign
    #output_killjob2, c_obje2, c_idr2, task_idr2, job_idr2 = mps.jobsPOMS.kill_jobs(dbhandle, job_id=job_obj2.job_id, confirm = "yes")

    #Now the check the outputs, they need a bit of pre-processing.
    #Arguments

    print("output_killjob: %s" % output_killjob)
    sep=output_killjob.rfind('--jobid ')
    assert(sep!=-1) #--jobid option was in called in the command

    jrm_args=output_killjob[0:sep+8] #the arguments are correct
    assert(jrm_args==c_arg) #compare the arguments are in place

    #Check single job kill
    jrm_id = output_killjob.split('--jobid ')[1].rstrip('\n')
    assert(jrm_id==c_output_killjob)

    #Check kill jobs in one task

    sep=output_killTask.rfind('--jobid ')
    assert(sep!=-1) #--jobid option was in called in the command
    print("got output:", output_killTask)
    jrm_idtl=output_killTask.split('--jobid ')[1].split(",")
    jrm_idtl[-1]=jrm_idtl[-1].rstrip('\n')

    # we may have jobs besides the ones we just added in the task , just do ours..
    for jid in c_output_killTask:
        assert(jid in jrm_idtl)

    #Check kill all jobs in one Campaign,  that also prof that the job market as completed is not killed.
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

    t_obj1=dbhandle.query(Task).filter(Task.task_id == task_id).first()
    t_obj2=dbhandle.query(Task).filter(Task.task_id == task_id2).first()
    job_obj_db = dbhandle.query(Job).filter(Job.task_id== task_id).all()  ####this is thing
    print "*"*10
    print "task_id1", task_id
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
    print "jobs with task id", task_id
    for x in job_obj_db:
        print x.jobsub_job_id
    print "#"*10
    '''


############Do not pay attention to the info below
def test_output_pending():
    dbhandle = DBHandle.DBHandle().get()

    res = mps.jobsPOMS.output_pending_jobs(dbhandle)
    assert(res != None)
