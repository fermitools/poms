import DBHandle
import datetime
import os
import socket
from webservice.utc import utc

dbh = DBHandle.DBHandle()
from mock_poms_service import mock_poms_service
from mock_redirect import mock_redirect_exception
logger = logging.getLogger('cherrypy.error')
# when I get one...
#ms = mock_session.mock_session()
ms = None


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
       ms, 
       action = 'add',
       ae_launch_name= 'test_launch_local',
       ae_launch_id ='',
       ae_launch_host = fqdn,
       ae_launch_account =  os.environ['USER'],      
       ae_launch_setup = "export POMS_DIR='%s'" % os.environ['POMS_DIR']
    )
    # add job type
    mps.campaignsPOMS.campaign_definition_edit(
       dbh.get(), 
       logger.info, 
       ms, 
       action = 'add',
       ae_campaign_definition_id = '',
       ae_definiton_name = 'test_launch_mock_job',
       ae_input_files_per_job = '0',
       ae_output_files_per_job = '0',
       ae_output_file_patterns = '%',
       ae_launch_script = 'python $POMS_DIR/test/mock_job.py --campaign_id $POMS_CAMPAIGN_ID -N 3',
       ae_definition_parameters = '[]',
       ae_definition_recoveries = '[]',
       experimenter_id = '4'
    )

def del_mock_job_launcher():
    campaign_definition_id = dbh.get().query(CampaignDefinition).filter(CampaignDefinition.name=='test_mock_launch_job').first()
    launch_id = dbh.get().query(LaunchTemplate).filter(LaunchTemplate.name == 'test_launch_local')
    
    mps.campaignsPOMS.launch_template_edit(
       dbh.get(), 
       logger.info, 
       ms, 
       action = 'delete',
       ae_launch_name= 'test_launch_local',
       ae_launch_id = launch_id
    )
    mps.campaignsPOMS.campaign_definition_edit(
       dbh.get(), 
       logger.info, 
       ms, 
       action = 'add',
       ae_definiton_name = 'test_launch_mock_job'
       ae_campaign_definition_id = campaign_definition_id,
   )

def add_campaign_def(name, deps):
    mps.campaignsPOMS.campaign_edit(

def del_campaign_def(name)


#
# ---------------------------------------
# Actual tests
#

def test_workflow_1():
     # setup workflow bits for _joe depending on _fred,
     # launch it
     add_mock_job_launcher()

     cid_fred = add_campaign_def('_fred',[])
     cid_joe = add_campaign_def('_joe',[['_fred','%'])

     mps.taskPOMS.launch_jobs(dbh.get(), logger.info, getconfig, gethead, ms, samweb, err_res, cid_fred)

     del_campaign_def('_fred',[])
     del_campaign_def('_joe',[['_fred','%'])
     del_mock_job_launcher()


