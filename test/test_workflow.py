import DBHandle
import datetime
import time
import os
import socket
from webservice.utc import utc
from webservice.samweb_lite import samweb_lite
from model.poms_model import Campaign, CampaignDefinition, LaunchTemplate

from mock_stubs import gethead, launch_seshandle, camp_seshandle, err_res, getconfig

dbh = DBHandle.DBHandle()
from mock_poms_service import mock_poms_service
from mock_redirect import mock_redirect_exception
import logging
logger = logging.getLogger('cherrypy.error')
# when I get one...


mps = mock_poms_service()

#
# ---------------------------------------
# utilities to set up tests
#
def add_mock_job_launcher():

    fqdn = socket.gethostname()
    # add launch template
    mps.campaignsPOMS.launch_template_edit(
       dbh.get(), 
       logger.info, 
       camp_seshandle, 
       action = 'add',
       ae_launch_name= 'test_launch_local',
       ae_launch_id ='',
       ae_launch_host = fqdn,
       ae_launch_account =  os.environ['USER'],      
       ae_launch_setup = "export POMS_DIR='%s'" % os.environ['POMS_DIR'],
       experiment = 'samdev',
       experimenter_id = '4'
    )
    # add job type
    mps.campaignsPOMS.campaign_definition_edit(
       dbh.get(), 
       logger.info, 
       camp_seshandle, 
       action = 'add',
       ae_campaign_definition_id = '',
       ae_definition_name = 'test_launch_mock_job',
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
    campaign_definition_id = dbh.get().query(CampaignDefinition).filter(CampaignDefinition.name=='test_launch_mock_job').first().campaign_definition_id
    launch_id = dbh.get().query(LaunchTemplate).filter(LaunchTemplate.name == 'test_launch_local').first().launch_id
    
    mps.campaignsPOMS.launch_template_edit(
        dbh.get(), 
        logger.info, 
        camp_seshandle, 
        action = 'delete',
        name= 'test_launch_local',
        ae_launch_id = launch_id
    )
    mps.campaignsPOMS.campaign_definition_edit(
        dbh.get(), 
        logger.info, 
        camp_seshandle, 
        action = 'delete',
        name = 'test_launch_mock_job',
        campaign_definition_id = campaign_definition_id,
    )

def add_campaign(name, deps):
    campaign_definition_id = dbh.get().query(CampaignDefinition).filter(CampaignDefinition.name=='test_launch_mock_job').first().campaign_definition_id
    launch_id = dbh.get().query(LaunchTemplate).filter(LaunchTemplate.name == 'test_launch_local').first().launch_id
    mps.campaignsPOMS.campaign_edit(
        dbh.get(), 
        logger.info, 
        launch_seshandle, 
        action='add',
        ae_campaign_id = '',
        ae_campaign_name = name,
        ae_active = True,
        ae_split_type = 'None',
        ae_dataset = 'test_dataset',
        ae_vo_role = 'Analysis',
        ae_software_version = 'v1_0',
        ae_param_overrides = '[]',
        ae_campaign_definition_id = campaign_definition_id,
        ae_launch_id = launch_id,
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

def test_workflow_1():
     # setup workflow bits for _joe depending on _fred,
     # launch it
     # add_mock_job_launcher()

     cid_fred = add_campaign('_fred','{"campaigns":[],"file_patterns":[]}')
     cid_joe = add_campaign('_joe','{"campaigns":["_fred"],"file_patterns":["%"]}')

     mps.taskPOMS.launch_jobs(dbh.get(), logger.info, getconfig, gethead, launch_seshandle, samweb_lite(), err_res, cid_fred)

     time.sleep(5)

     #
     # check here that the jobs actually ran etc.
     # 

     del_campaign('_fred')
     del_campaign('_joe')
     del_mock_job_launcher()


