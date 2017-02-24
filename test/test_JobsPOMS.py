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
import logging
logger = logging.getLogger('cherrypy.error')
# when I get one...

mps = mock_poms_service()
rpstatus = "200"

#
# ---------------------------------------
# utilities to set up tests
#


#
# test update_job with some set of fields
#
def do_update_job(fielddict):
    dbhandle = DBHandle.DBHandle().get()
    samhandle = samweb_lite()
    
    
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

    data = {}
    for jid in jids:
        data[jid] = {'jobsub_job_id': jid, 'task_id': task_id, 'status': 'Idle'}
    
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


def test_output_pending():
    dbhandle = DBHandle.DBHandle().get()

    res = mps.jobsPOMS.output_pending_jobs(dbhandle)
    assert(res != None)
    
