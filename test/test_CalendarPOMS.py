from mock.mock import MagicMock
from mock_poms_service import mock_poms_service
from DBHandle import DBHandle
from webservice.utc import utc
import time
from datetime import datetime, timedelta

mps = mock_poms_service()
dbhandle = DBHandle().get()

import logging
logger = logging.getLogger('cherrypy.error')

def test_calendar_page_1():
    stime = time.time() - 3600
    etime = stime + 3600
    mps.calendarPOMS.add_event(dbhandle, "fifemon_mu2e_samweb", stime + 1800, stime + 2400)
    res = mps.calendarPOMS.calendar_json(dbhandle, datetime.fromtimestamp(stime, utc),datetime.fromtimestamp(etime, utc), utc, '')
    print "got res:" , res
    assert(str(res).find('fifemon_mu2e_samweb') > 0)

# need tests for downtimes...
