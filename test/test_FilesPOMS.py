import DBHandle
from mock_Ctx import Ctx
import datetime
import time
import io

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
dims = "fake dimension string"


class mock_cherrypy_file:
    def __init__(self, name="foo", contents=b"This is a test"):
        self.filename=name
        self.file = io.BytesIO(contents)

def test_show_dimension_files():

    ctx = Ctx(sam=samhandle, experiment=experiment, db="None")
    res = mps.filesPOMS.show_dimension_files(ctx, dims)
    ctx.sam.list_files.assert_called_with(experiment, dims, dbhandle="None")
    assert res == "fake_list_files"


def test_show_dimension_files():
    ctx = Ctx(sam=samhandle, experiment=experiment, db="None")
    res = mps.filesPOMS.show_dimension_files(ctx, dims)
    ctx.sam.list_files.assert_called_with(experiment, dims, dbhandle="None")
    assert res.find("fake_list_files") >= 0


def test_uploads():
    ctx = Ctx()
    fname = "foo"
    testdata = b"test data\n"
    mcf = mock_cherrypy_file(name=fname, contents=testdata)
    path = mps.filesPOMS.get_file_upload_path(ctx, fname)
    # make sure the dir is there but not the file..
    try:
        os.unlink(path)
    except:
        pass
    try:
        os.makedirs(os.path.dirname(path))
    except:
        pass
    res = mps.filesPOMS.upload_file(ctx, 1024, mcf)
    assert(os.access(path,os.R_OK))
    with open(path,"rb") as f:
        assert(f.read() == testdata)
    fl = mps.filesPOMS.file_uploads(ctx, checkuser=None)
    print(repr(fl))
    # fl should have an entry for our file of the right size
    k = 0
    for i in range(len(fl[0])):
        if fl[0][i][0] == fname:
            k = i
    assert(fl[0][k][0]=='foo')
    assert(fl[0][k][1]== len(testdata))
 
    # clean up
    res = mps.filesPOMS.remove_uploaded_files(ctx, fname, action=None)
    assert(not os.access(path,os.R_OK))

def test_get_launch_sandbox():
    ctx = Ctx()
    path = mps.filesPOMS.get_launch_sandbox(ctx)
    print("got path:" , path)
    assert(os.access(path,os.R_OK))
    
