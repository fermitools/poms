import DBHandle
import datetime
import time
import os
import json
import socket
from poms.webservice.utc import utc
from poms.webservice.samweb_lite import samweb_lite
from poms.model.poms_model import Campaign, CampaignDefinition, LaunchTemplate, Job

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
    
    mps.jobsPOMS.bulk_update_job(dbhandle, logger.info, rpstatus, samhandle, json.dumps(data) )
    
    for jid in jids:
        j = dbhandle.query(Job).filter(Job.jobsub_job_id == jid).first()
        assert(jid != None)


def test_update_job_2():
    # try updating lots of parameters
    fielddict = {
        'status' : 'running: on full',
    }
    do_update_job(fielddict)

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
    #Two task_id for the same campaign
    task_id = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14') #Provide a task_id for the fake campaign
    task_id2 = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14') #Provide a task_id for the second task
    #Create jobs in the same campaign, 2 in one task_id, one in another task_id but same campaign, and on job in the same task_id, campaign but market as completed.
    jid = "%d@fakebatch1.fnal.gov" % time.time() #1 Job in the first task_id
    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'running')
    jid2 = "%d@fakebatch1.fnal.gov" % time.time()#2Job in the first task_id
    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid2, host_site = "fake_host", status = 'running')
    jid3 = "%d@fakebatch1.fnal.gov" % time.time() #3Job in a new task_id but same campaign
    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id2, jobsub_job_id = jid3, host_site = "fake_host", status = 'running')
    jid4 = "%d@fakebatch1.fnal.gov" % time.time()
    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid4, host_site = "fake_host", status = 'Completed')
    #Control arguments
    c_arg="-G Analysis --role fermilab --jobid "
    c_output_killjob = jid #Control output
    c_output_killTask = jid+jid2 #Control output
    c_output_killCampaign =jid+jid2+jid3 #Control output
    #Calls to the test rutine
    output_killjob, c_obje, c_idr, task_idr, job_idr = mps.jobsPOMS.kill_jobs(dbhandle, logger.info, campaign_id='14', task_id=task_id, jobsub_job_id=jid, confirm = "yes")
    output_killTask, c_obje, c_idr, task_idr, job_idr = mps.jobsPOMS.kill_jobs(dbhandle, logger.info, campaign_id='14', task_id=task_id, confirm = "yes")
    output_killCampaign, c_obje, c_idr, task_idr, job_idr = mps.jobsPOMS.kill_jobs(dbhandle, logger.info, campaign_id='14', confirm = "yes")
    #Now the check the outputs, they need a bit of pre-processing.
    #Arguments
    sep=output.rfind('--jobid ')
    assert(sep!=-1) #--jobid argument was in called in the command
    jrm_args=output_killjob[0:sep+8] #the arguments are correct
    assert(jrm_args==c_arg) #compare the arguments are in place

    #Check single job kill
    jrm_id=output_killjob.split('--jobid ')[1].split(",")
    assert(jrm_id==c_output_killjob)

    #Check kill jobs in one task
    jrm_idtl=output_killTask.split('--jobid ')[1].split(",")
    jrm_idtl.sort()
    assert(c_output_killTask==jrm_idl)

    #Check kill all jobs in one Campaign,  that also prof that the job market as completed is not killed.
    jrm_idcl=output_killCampaign.split('--jobid ')[1].split(",")
    jrm_idcl.sort()
    assert(c_output_killCampaign == jrm_idcl)

    #Closing the mock
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
def test_output_pending():
    dbhandle = DBHandle.DBHandle().get()

    res = mps.jobsPOMS.output_pending_jobs(dbhandle)
    assert(res != None)
    
