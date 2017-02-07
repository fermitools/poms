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

print ' testings tags'

def test_tags_1():
    # test tagging a campaign, and then looking up campaigns with that tag...
    tag_name = 'test_tag_%d' % time.time()
    tag_name = 'mvi4'
    cname = 'test_mvi1'
    #cname = 'mwm_test_1'
    print ' testing for campaign %s' %cname
    #c = dbhandle.query(Campaign).filter(Campaign.name == 'mwm_test_1').first()
    c = dbhandle.query(Campaign).filter(Campaign.name == cname).first()

    mps.tagsPOMS.link_tags(sesshandle, dbhandle, c.campaign_id, tag_name, c.experiment)

    clist = mps.tagsPOMS.search_tags(dbhandle, tag_name)
    print ' campaign %s, ntags= %s' %(cname,len(clist)) 
    for aval in clist: print ' v=%s' %aval
    cs = clist[0][0]

    assert(cs.campaign_id == c.campaign_id)
    assert (1==2)
