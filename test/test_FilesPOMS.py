import DBHandle
import datetime
import time
#from utc import utc
import os
import socket
from mock.mock import MagicMock
from poms.webservice.utc import utc
from poms.webservice.samweb_lite import samweb_lite
from poms.model.poms_model import Campaign, CampaignDefinition, LaunchTemplate, Job, JobFile, Task

from mock_stubs import gethead, launch_seshandle, camp_seshandle, err_res, getconfig

from mock_poms_service import mock_poms_service
from mock_redirect import mock_redirect_exception
import logging
logger = logging.getLogger('cherrypy.error')
# when I get one...

mps = mock_poms_service()
rpstatus = "200"

dbhandle = DBHandle.DBHandle().get()

#
# use Mock to make a fake samweb_lite so we can just verify if we
# would have called SAM and not really do it...
#
class fake_samweb_lite:

     have_cache = MagicMock(return_value = True)
     fetch_info = MagicMock(return_value = {'foo':'bar'})
     fetch_info_list = MagicMock(return_value = [{'foo':'bar'},{'foo':'bar2'},{'foo':'bar3'}])
     do_totals = MagicMock(return_value = [10] )
     update_project_description = MagicMock(return_value = True)
     list_files = MagicMock(return_value = 'fake_list_files' )
     count_files = MagicMock(return_value = 10 )
     count_files_list = MagicMock(return_value = [10,20,30])
     create_definition = MagicMock(return_value = True )


def test_show_dimension_files():
    samhandle = fake_samweb_lite()

    experiment = 'samdev'
    dims = 'fake dimension string'
    res = mps.filesPOMS.show_dimension_files(samhandle, experiment,dims)
    assert(res == 'fake_list_files')
    samhandle.list_files.assert_called_with(experiment,dims, dbhandle = None)

def test_list_task_logged_files():
    samhandle = fake_samweb_lite()
    flist = ['file1.root', 'file2.root', 'file3.root']

    # setup a task, log some files...
    #
    task_id = mps.taskPOMS.get_task_id_for(dbhandle,campaign='14')
    jid = "%d@fakebatch1.fnal.gov" % time.time()

    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Idle')
    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Running', output_file_names = ' '.join(flist))
    mps.jobsPOMS.update_job(dbhandle, logger.info, rpstatus, samhandle, task_id = task_id, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')
    # ... now verify we got those files

    fl, t, jobsub_job_id = mps.filesPOMS.list_task_logged_files(dbhandle,task_id)
    for f in fl:
       assert(f.file_name in flist)


def test_get_inflight():
    dbhandle = DBHandle.DBHandle().get()
    samhandle = samweb_lite()
    t = time.time()
    tUTC=time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t)) #time stamp for file creation
    campaign_id_test = '14' #test campaign
    jid = "%d@fakebatch_test.fnal.gov" % time.time() #fake jobsub_job_id
    task_id_test = mps.taskPOMS.get_task_id_for(dbhandle,campaign=campaign_id_test)
    mps.jobsPOMS.update_job(dbhandle, rpstatus, samhandle, task_id = task_id_test, jobsub_job_id = jid, host_site = "fake_host", status = 'Completed')
    jobj = dbhandle.query(Job).filter(Job.jobsub_job_id==jid).first() #taking a job object from the job just included in the previous stage
    #db_job_id=jobj.job_id #this is the job id in the database different from the jobsub_job_id
    fname="testFile_Felipe_%s_%s.root" % (tUTC,task_id_test)
    jf=JobFile(file_name = fname, file_type = "output", created = tUTC , job_obj = jobj) #including the file into the JobFile Table.
    dbhandle.add(jf)
    dbhandle.commit()

    #Verbosity
    print "t", t
    print "UTC", tUTC
    print "jid", jid
    print "task_id", task_id_test
    print "job object id in the db", jobj.job_id
    print "the jobsub_job_id", jobj.jobsub_job_id
    print jf
    #jf.job_files.append(jf) extracted from poms files ....


    print "I want to emulate the same query done inside the method because is not working"
    fobj=dbhandle.query(JobFile).join(Job).join(Task).join(Campaign)
    q = dbhandle.query(JobFile).join(Job).join(Task).join(Campaign)
    q = q.filter(Task.campaign_id == Campaign.campaign_id)
    q = q.filter(Task.task_id == Job.task_id)
    q = q.filter(Job.job_id == JobFile.job_id)
    q = q.filter(Job.job_id == job_obj.job_id)
    print "q", q.all()
    print "q.output_files_declared", q.output_files_declared()

    outlist = mps.filesPOMS.get_inflight(dbhandle, task_id=task_id_test)
    print "the outlist is", outlist
    assert(outlist == fname)
    #update_job_common
    #commit
    #job_object=dbhandle.query(Job, JobFile).with_for_update(of=Job).filter(JobFile.job_id == jobj.job_id).first()
