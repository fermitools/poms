from mock.mock import MagicMock
from mock_poms_service import mock_poms_service
mps = mock_poms_service()

import logging
logger = logging.getLogger('cherrypy.error')

gethead = MagicMock()

class exp_is_root:
    is_root = MagicMock(return_value = True)

class exp_not_root:
    is_root = MagicMock(return_value = False)

sesshandle_root = MagicMock(return_value = exp_is_root)
sesshandle_plain = MagicMock(return_value = exp_not_root)

def test_can_report_data1_root_user():
    gethead.side_effect = ['123.45.6.7', '123.45.5.5', 'mengel']
    res = mps.accessPOMS.can_report_data(gethead, logger.info, sesshandle_root)
    assert(res == True)
    
def test_can_report_data_plain_user():
    gethead.side_effect = ['123.45.6.7', '123.45.5.5','nobody']
    res = mps.accessPOMS.can_report_data(gethead, logger.info, sesshandle_plain)
    assert(res == False)
    
def test_can_report_local_service():
    gethead.side_effect = [None,'127.0.0.1','']
    res = mps.accessPOMS.can_report_data(gethead, logger.info, sesshandle_plain)
    assert(res == True)

# need can_db_admin tests...

