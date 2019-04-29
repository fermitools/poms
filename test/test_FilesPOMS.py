import DBHandle
from mock_Ctx import Ctx
import datetime
import time

# from utc import utc
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

logger = logging.getLogger("cherrypy.error")
# when I get one...

mps = mock_poms_service()
rpstatus = "200"
fn_list = []
dbhandle = DBHandle.DBHandle().get()


#
# use Mock to make a fake samweb_lite so we can just verify if we
# would have called SAM and not really do it...
#
class fake_samweb_lite:

    have_cache = MagicMock(return_value=True)
    fetch_info = MagicMock(return_value={"foo": "bar"})
    fetch_info_list = MagicMock(return_value=[{"foo": "bar"}, {"foo": "bar2"}, {"foo": "bar3"}])
    do_totals = MagicMock(return_value=[10])
    update_project_description = MagicMock(return_value=True)
    list_files = MagicMock(return_value="fake_list_files")
    count_files = MagicMock(return_value=10)
    count_files_list = MagicMock(return_value=[10, 20, 30])
    create_definition = MagicMock(return_value=True)


samhandle = fake_samweb_lite()
experiment = "samdev"
ctx = Ctx(sam=samhandle, experiment=experiment, db="None")
dims = "fake dimension string"


def test_show_dimension_files():

    res = mps.filesPOMS.show_dimension_files(ctx, dims)
    ctx.sam.list_files.assert_called_with(experiment, dims, dbhandle="None")
    assert res == "fake_list_files"


def test_show_dimension_files():
    res = mps.filesPOMS.show_dimension_files(ctx, dims)
    ctx.sam.list_files.assert_called_with(experiment, dims, dbhandle="None")
    assert res.find("fake_list_files") >= 0


"""
Not implemented yet:
----
def test_get_pending_dict_for_campaigns():
    res = mps.filesPOMS.get_pending_dict_for_campaigns(ctx, campaign_id_list)
def test_get_pending_for_campaigns():
    res = mps.filesPOMS.get_pending_for_campaigns(ctx, campaign_id_list)

def test_get_pending_dims_for_task_lists():
    res = mps.filesPOMS.get_pending_dims_for_task_lists(ctx, task_list_list)

def test_get_pending_for_task_lists():
    res = mps.filesPOMS.get_pending_for_task_lists(ctx, task_list_list)

def test_get_file_upload_path():
    res = mps.filesPOMS.get_file_upload_path(ctx, filename)

def test_file_uploads():
    res = mps.filesPOMS.file_uploads(ctx, checkuser=None)

def test_upload_file():
    res = mps.filesPOMS.upload_file(ctx, quota, filename)

def test_remove_uploaded_files():
    res = mps.filesPOMS.remove_uploaded_files(ctx, filename, action=None)

def test_get_launch_sandbox():
    res = mps.filesPOMS.get_launch_sandbox(ctx)
"""
