import DBHandle
import datetime
import time
import os
import socket
from webservice.utc import utc
from webservice.samweb_lite import samweb_lite
from model.poms_model import Campaign, CampaignDefinition, LaunchTemplate, Job

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

    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')

    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid,  **fielddict )

    j = dbhandle.query(Job).filter(Job.jobsub_job_id == jid).first()

    assert(j != None)

    for f,v in fielddict.items():
        if f.startswith('task_'):
            f = f[5:]
            assert(getattr(j.task_obj, f) == v)
        elif f.endswith('file_names'):
            # should look it up on JobFiles but...
            pass
        else:
            assert(str(getattr(j,f)) == str(v))

    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')


def test_active_update_jobs():
    dbhandle = DBHandle.DBHandle().get()
    samhandle = samweb_lite()

    l1 = mps.jobsPOMS.active_jobs(dbhandle)

    jid = "%f@fakebatch1.fnal.gov" % time.time()

    # start up a fake job
    task_id = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14')

    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')

    l2 = mps.jobsPOMS.active_jobs(dbhandle)

    # end fake job
    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')

    l3 = mps.jobsPOMS.active_jobs(dbhandle)

    # so job list should have gotten longer and shorter
    # and jobid should be only in the middle list...
    assert(len(l2)  > len(l1))
    assert(len(l2)  > len(l3))
    assert(jid in l2)
    assert(not jid in l3)

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

def test_update_job_2():
    # try updating lots of parameters
    fielddict = {
        'status' : 'running: on full',
    }
    do_update_job(fielddict)


def test_update_job_log_scraper():
    print "testing all the info that the jobscraper pass the job_log_scraper"
    fielddict = {
                'status': 'test_status' ,
                'slot':'finally_something_in_this_field'
                'output_file_names':'test_file_test_joblogscraper.txt' ,
                'node_name': 'fake_node_test_joblogscraper',
                'user_exe_exit_code':'10',
                'cpu_type': 'Athalon',
                }
    do_update_job(fielddict)


def test_update_job_q_scraper():

    print "check this from the jobsub_q scrapper"
    fielddict = {
                }


def test_kill_jobs():
    task_id = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14') #Provide a task_id for the fake campaign
    task_id2 = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14') #Provide a task_id for the second task

    ##Create two jobs with fakes job_id
    jid = "%d@fakebatch1.fnal.gov" % time.time() #1 Job in the first task_id
    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running')
    jid2 = "%d@fakebatch1.fnal.gov" % time.time()#2Job in the first task_id
    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid2, host_site = "fake_host", status = 'running')
    jid3 = "%d@fakebatch1.fnal.gov" % time.time() #3Job in a new task_id but same campaign
    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id2, jobsub_job_id = jid3, host_site = "fake_host", status = 'running')
    output, c_obje, c_idr, task_idr, job_idr = mps.jobsPOMS.kill_jobs(dbhandle, logger.info, campaign_id='14', task_id=task_id, jobsub_job_id=jid, confirm = "yes")
    c_output_killjob = "-G Analysis --role fermilab --jobid "+jid #Control output
    c_output_killTask = "-G Analysis --role fermilab --jobid "+jid+","+jid2 #Control output
    c_output_killCampaign ="-G Analysis --role fermilab --jobid "+jid+","+jid2+","+jid3
    assert(output == c_output)
    output, c_obje, c_idr, task_idr, job_idr = mps.jobsPOMS.kill_jobs(dbhandle, logger.info, campaign_id='14', task_id=task_id, confirm = "yes")
    assert(output == c_output_killTask)
    output, c_obje, c_idr, task_idr, job_idr = mps.jobsPOMS.kill_jobs(dbhandle, logger.info, campaign_id='14', confirm = "yes")
    assert(output == c_output_killCampaign)
    jid4 = "%d@fakebatch1.fnal.gov" % time.time()
    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid4, host_site = "fake_host", status = 'Completed')
    assert(output == c_output_killCampaign)


    mock_rm.close()



    '''
    fielddict = {
                'status': 'pipe_test' ,
                'output_file_names':'test_file.txt' , ##it is up
                'jobsub_job_id': 'fake_jobsub_id@fakebatch2' ,
                'node_name': 'fake_node_pipe' ,         ###it is up
                'taskid': '1234',
                'user_exe_exit_code':'10',  #it is up
                'cpu_type': 'Athalon',      #it is up
                'slot':'finally_something_in_this_field'
                }
    '''



############Do not pay attention to the info below
