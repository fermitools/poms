import DBHandle
import datetime
import time
#from utc import utc
import os
import socket
from mock.mock import MagicMock
from poms.webservice.utc import utc
from poms.webservice.samweb_lite import samweb_lite
from poms.webservice.poms_model import CampaignStage, JobType, LoginSetup, Submission

from mock_stubs import gethead, launch_seshandle, camp_seshandle, err_res, getconfig

from mock_poms_service import mock_poms_service
from mock_redirect import mock_redirect_exception
import logging
logger = logging.getLogger('cherrypy.error')
# when I get one...

mps = mock_poms_service()
rpstatus = "200"
fn_list=[]
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


