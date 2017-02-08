from mock.mock import MagicMock
from mock_poms_service import mock_poms_service
from DBHandle import DBHandle
from webservice.utc import utc
import time
from datetime import datetime, timedelta
from model.poms_model import Campaign

mps = mock_poms_service()
dbhandle = DBHandle().get()

import logging
logger = logging.getLogger('cherrypy.error')


class authorized:
    is_authorized = MagicMock(return_value = True)

sesshandle = MagicMock(return_value = authorized)

def test_tags_1():
    # test tagging a campaign, and then looking up campaigns with that tag...
    tag_name = 'test_tag_%d' % time.time()
    
    c = dbhandle.query(Campaign).filter(Campaign.name == 'mwm_test_1').first()

    mps.tagsPOMS.link_tags(sesshandle, dbhandle, c.campaign_id, tag_name, c.experiment)

    clist = mps.tagsPOMS.search_tags(dbhandle, tag_name)

    cs = clist[0][0]

    assert(cs.campaign_id == c.campaign_id)

