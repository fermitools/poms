import DBHandle
import datetime
import time
import os
import socket
from webservice.utc import utc
from webservice.samweb_lite import samweb_lite
from model.poms_model import Campaign, CampaignDefinition, LaunchTemplate, Task

from mock_stubs import gethead, launch_seshandle, camp_seshandle, err_res, getconfig

dbh = DBHandle.DBHandle()
dbhandle = dbh.get()
from mock_poms_service import mock_poms_service
from mock_redirect import mock_redirect_exception
import logging

logger = logging.getLogger('cherrypy.error')
# when I get one...


mps = mock_poms_service()

def fake_out_kerberos():
    # put a copy our current credentials where the launch code is
    # going to try to put creds from the POMS keytab; that way it
    # works even if that kinit fails..
    kcache = os.environ['KRB5CCNAME'].replace('FILE:','')
    os.system("cp %s /tmp/krb5cc_poms_submit_fermilab" % kcache)
#
# ---------------------------------------
# utilities to set up tests
#

def add_mock_job_launcher():

    campaign_definition = dbh.get().query(CampaignDefinition).filter(CampaignDefinition.name=='test_launch_mock_job_generic').first()
    launch = dbh.get().query(LaunchTemplate).filter(LaunchTemplate.name == 'test_launch_local_generic').first()

    if campaign_definition or launch:
        print "Hm.. some already exist?"

    fqdn = socket.gethostname()
    # add launch template
    if launch == None:
	mps.campaignsPOMS.launch_template_edit(
	   dbh.get(), 
	   logger.info, 
	   camp_seshandle, 
	   action = 'add',
	   ae_launch_name= 'test_launch_local_generic',
	   ae_launch_id ='',
	   ae_launch_host = fqdn,
	   ae_launch_account =  os.environ['USER'],      
	   ae_launch_setup = "source /grid/fermiapp/products/common/etc/setups; set -x; klist; cd %s;   setup -. poms" % os.environ['POMS_DIR'],
	   experiment = 'samdev',
	   experimenter_id = '4'
	)
    # add job type
    if campaign_definition == None:
	mps.campaignsPOMS.campaign_definition_edit(
	   dbh.get(), 
	   logger.info, 
	   camp_seshandle, 
	   action = 'add',
	   ae_campaign_definition_id = '',
	   ae_definition_name = 'test_launch_mock_job_generic',
	   ae_input_files_per_job = '0',
	   ae_output_files_per_job = '0',
	   ae_output_file_patterns = '%',
	   ae_launch_script = 'python $POMS_DIR/test/mock_job.py --campaign_id $POMS_CAMPAIGN_ID -N 3',
	   ae_definition_parameters = '[]',
	   ae_definition_recovery = '[]',
	   experiment = 'samdev',
	   experimenter_id = '4'
	)

def del_mock_job_launcher():
    campaign_definition = dbh.get().query(CampaignDefinition).filter(CampaignDefinition.name=='test_launch_mock_job_generic').first()
    launch = dbh.get().query(LaunchTemplate).filter(LaunchTemplate.name == 'test_launch_local_generic').first()
    
    if launch:
        launch_id = launch.launch_id
	mps.campaignsPOMS.launch_template_edit(
	    dbh.get(), 
	    logger.info, 
	    camp_seshandle, 
	    action = 'delete',
	    name= 'test_launch_local_generic',
	    ae_launch_id = launch_id
	)
    if campaign_definition:
        campaign_definition_id = campaign_definition.campaign_definition_id
	mps.campaignsPOMS.campaign_definition_edit(
	    dbh.get(), 
	    logger.info, 
	    camp_seshandle, 
	    action = 'delete',
	    name = 'test_launch_mock_job_generic',
	    campaign_definition_id = campaign_definition_id,
	)

def add_campaign(name, deps, dataset = None, split = 'None'):
    campaign = dbh.get().query(Campaign).filter(Campaign.name=='name').first()
    campaign_definition = dbh.get().query(CampaignDefinition).filter(CampaignDefinition.name=='test_launch_mock_job_generic').first()
    launch = dbh.get().query(LaunchTemplate).filter(LaunchTemplate.name == 'test_launch_local_generic').first()

    if campaign:
        print "campaign %s already exists..." % name
        return
    if not campaign_definition or not launch:
        print "Ouch! adding campaign definition or launch didn't work"
        return

    if dataset == None:
        dataset = "None"

    mps.campaignsPOMS.campaign_edit(
        dbh.get(), 
        logger.info, 
        launch_seshandle, 
        action='add',
        ae_campaign_id = '',
        ae_campaign_name = name,
        ae_active = True,
        ae_split_type = 'None',
        ae_dataset = dataset,
        ae_vo_role = 'Analysis',
        ae_software_version = 'v1_0',
        ae_param_overrides = '[]',
        ae_campaign_definition_id = campaign_definition.campaign_definition_id,
        ae_launch_id = launch.launch_id,
        ae_completion_type = "located",
        ae_completion_pct = "95",
        ae_depends = deps,
        experiment = 'samdev',
        experimenter_id = '4',
    )
    campaign_id = dbh.get().query(Campaign).filter(Campaign.name==name).first().campaign_id
    return campaign_id

def del_campaign(name):
    campaign_id = dbh.get().query(Campaign).filter(Campaign.name==name).first().campaign_id
    mps.campaignsPOMS.campaign_edit(
        dbh.get(), 
        logger.info, 
        camp_seshandle, 
        action = 'delete',
        name = name,
        campaign_id = campaign_id,
    )

#
# ---------------------------------------
# Actual tests
#

def setup():
    fake_out_kerberos()
    add_mock_job_launcher()

def test_workflow_1():
     # setup workflow bits for _joe depending on _fred,
     # launch it

     cid_fred = add_campaign('_fred','{"campaigns":[],"file_patterns":[]}')
     cid_joe = add_campaign('_joe','{"campaigns":["_fred"],"file_patterns":["%"]}')

     before_fred = mps.triagePOMS.job_counts(dbh.get(), campaign_id = cid_fred , tdays=1)
     before_joe = mps.triagePOMS.job_counts(dbh.get(), campaign_id = cid_joe , tdays=1)

     mps.taskPOMS.launch_jobs(dbh.get(), logger.info, getconfig, gethead, launch_seshandle, samweb_lite(), err_res, cid_fred)


     time.sleep(10)
     mps.taskPOMS.wrapup_tasks(dbh.get(), logger.info, samweb_lite(), getconfig, gethead, launch_seshandle, err_res )
     time.sleep(10)

     after_fred = mps.triagePOMS.job_counts(dbh.get(), campaign_id = cid_fred , tdays=1)
     after_joe = mps.triagePOMS.job_counts(dbh.get(), campaign_id = cid_joe , tdays=1)
     
     #
     # check here that the jobs actually ran etc.
     # 

     #del_campaign('_fred')
     #del_campaign('_joe')
     #del_mock_job_launcher()

     print "before:", before_fred, before_joe
     print "after:" , after_fred, after_joe

     assert(after_fred['All'] > before_fred['All'])
     assert(after_joe['All'] > before_joe['All'])

     #assert(False)

def test_show_campaigns():
     # NOTE: this assumes someone has run the test_workflow_1 test sometime
     #       recently. Otherwise we might not have the _fred campaign
     items = mps.campaignsPOMS.show_campaigns(dbhandle, logger.info, samweb_lite(), experiment = 'samdev' )
     found = False

     for c in items[2]:
         if c.name == '_fred':
             found=True
     assert(found)

def test_campaign_info():
     # NOTE: this assumes someone has run the test_workflow_1 test sometime
     #       recently. Otherwise we might not have the _fred campaign
     c = dbh.get().query(Campaign).filter(Campaign.name=='_fred').first()

     items = mps.campaignsPOMS.campaign_info(dbhandle, logger.info, samweb_lite(), err_res, campaign_id = c.campaign_id )

     assert(items[0][0].name == '_fred')

def test_campaign_time_bars():
     # NOTE: this assumes someone has run the test_workflow_1 test sometime
     #       recently. Otherwise we might not the most recent task id
     #       for the _fred campaign in the time bars...
     campaign_id = dbh.get().query(Campaign).filter(Campaign.name=='_fred').first().campaign_id
     items = mps.campaignsPOMS.campaign_time_bars(dbhandle, campaign_id = campaign_id )

     task = dbh.get().query(Task).filter(Task.campaign_id == campaign_id).order_by(Task.created.desc()).first()

     l = []
     for j in task.jobs:
         l.append(j.jobsub_job_id)
     l.sort()
     assert(str(items).find(l[0].replace('.fnal.gov','')) > 0)

def test_register_existing_campaign():
     
    mps.campaignsPOMS.register_poms_campaign(
        dbh.get(), 
        logger.info, 
        'samdev',
        '_fred',
        version = 'v1_0',
        role = 'Analysis'
     )

def test_register_new_campaign():
    # Note: this assumes we have a *generic* launch type and campaign def
    mps.campaignsPOMS.register_poms_campaign(
        dbh.get(), 
        logger.info, 
        'samdev',
        'test_%d' % time.time() ,
        version = 'v1_0',
        role = 'Analysis'
     )

def test_register_new_campaign_2():
    # Note: this assumes we have a *generic* launch type and campaign def
    mps.campaignsPOMS.register_poms_campaign(
        dbh.get(), 
        logger.info, 
        'samdev',
        'test_%d' % time.time() ,
        version = 'v1_0',
        role = 'Analysis',
        dataset = 'foobie'
     )

def test_update_launch_schedule():
    campaign_id = dbh.get().query(Campaign).filter(Campaign.name=='_fred').first().campaign_id

    mps.campaignsPOMS.update_launch_schedule(logger.info, campaign_id, dowlist = '*', domlist = '1', monthly = '' , month = '1', hourlist = '1', submit = 'submit' , minlist = '1', delete = '')

    comment = "POMS_CAMPAIGN_ID=%d" % campaign_id

    #
    # verify its in crontab
    #
    f = os.popen('crontab -l', 'r')
    found = False
    for line in f:
       if line.find(comment) > 0:
           found = True
    f.close()

    assert(found)

    #
    # verify schedule_launch finds it
    #
    res = mps.campaignsPOMS.schedule_launch(dbh.get(), campaign_id)

    assert(str(res[1]).find(comment) > 0)

    #
    # try to delete it
    #
    mps.campaignsPOMS.update_launch_schedule(logger.info,  campaign_id,  minlist = '', hourlist = '', delete = 'True')

    #
    # verify its NOT in crontab
    #
    f = os.popen('crontab -l', 'r')
    found = False
    for line in f:
       if line.find(comment) > 0:
           found = True
    f.close()

    assert(not found)

# Still needed
# campaigns that use each split type, run repeatedly
# campaigns that declare output files
# get_recovery_list_for_campaign_def?
# make_stale_campaigns_inactive?
