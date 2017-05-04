from mock.mock import MagicMock
from mock_poms_service import mock_poms_service
from mock_stubs import gethead, launch_seshandle, camp_seshandle, err_res, getconfig
from DBHandle import DBHandle
from poms.webservice.utc import utc
import time
from datetime import datetime, timedelta
from poms.model.poms_model import Campaign, Tag

mps = mock_poms_service()
dbhandle = DBHandle().get()

import logging
logger = logging.getLogger('cherrypy.error')


class authorized:
    is_authorized = MagicMock(return_value = True)

sesshandle = MagicMock(return_value = authorized)

def test_tags_create():
    # test tagging a campaign, and then looking up campaigns with that tag...
    tag_name = 'test_tag_%d' % time.time()
    tag_name = 'mvi_%d' %time.time()
    cname = 'mwm_test_1'
    #print ' testing for campaign %s' %cname
    c = dbhandle.query(Campaign).filter(Campaign.name == 'mwm_test_1').first()

    mps.tagsPOMS.link_tags(dbhandle, sesshandle, c.campaign_id, tag_name, c.experiment)

    clist = mps.tagsPOMS.search_tags(dbhandle, tag_name)
    #print ' campaign %s, ntags= %s' %(cname,len(clist)) 
    #for aval in clist: print ' v=%s' %aval
    cs = clist[0][0]

    assert(cs.campaign_id == c.campaign_id)
    #assert(1==2)        # Just to force printing on screen.

# Create and delete a tag

def test_tags_delete():

    # test tagging a campaign, and then looking up campaigns with that tag...
    tag_name = 'mvi_tag_for_delete'
    cname = 'mwm_test_1'
    c = dbhandle.query(Campaign).filter(Campaign.name == 'mwm_test_1').first()
    exp = c.experiment
    #print ' testing for campaign %s, experiemnt = %s ' %(cname,exp)
    
    #This creates the tag
    mps.tagsPOMS.link_tags(dbhandle, sesshandle, c.campaign_id, tag_name, c.experiment)

    clist = mps.tagsPOMS.search_tags(dbhandle, tag_name)
    
    results=clist[0]            #It contains the list of campaigns ID with the tag.
    qq = clist[1]               #It contains the list of the initial tag(s)

    #print ' len(results)=%s, len(qq)= %s' %(len(results),len(qq))
    cids=[]
    if results: 
        for val in results: 
            #print ' found tag %s for campaign id =%s' %(tag_name,val.campaign_id)
            cids.append(val.campaign_id)
        
    else:
        pass #print ' tag %s not found for campaign %s' %(tag_name,cname)

    if results: 
        #finding tag id from tag name
        allTags = dbhandle.query(Tag).filter(Tag.tag_name==tag_name, Tag.experiment == exp).all()
        for atag in allTags:
            #print ' tag = %s, id = %s' %(atag.tag_name,atag.tag_id) 
            for cid in cids:
              #print ' deleting tag %s id =%s for campaign %s' %(atag.tag_name,atag.tag_id,cid)
              mps.tagsPOMS.delete_campaigns_tags(dbhandle, sesshandle, cid, atag.tag_id, c.experiment)
    
    clist = mps.tagsPOMS.search_tags(dbhandle, tag_name)
    results=clist[0]
    assert(len(results)==0)
    #assert (1==2)   # Just to force printing on screen..


def test_tags_create_multi():

    print(' Testings : create a tag to multiple campaigns')
    tag_name = 'mvi_tag_%d' %time.time()
    #cname = 'mwm_test_1'
    campaigns = dbhandle.query(Campaign).filter(Campaign.name.like('%mwm%')).all()
    print(' found %s campaigns ' %len(campaigns))
    for ac in campaigns:
        print(' %s %s %s' %(ac.campaign_id,ac.name, ac.experiment))
        mps.tagsPOMS.link_tags(dbhandle, sesshandle, ac.campaign_id, tag_name, ac.experiment)

    # now verify
    clist = mps.tagsPOMS.search_tags(dbhandle, tag_name)
    results=clist[0]
    if results: 
        for val in results: 
            print(' found tag %s for campaign id =%s' %(tag_name,val.campaign_id))
    assert(len(results)>0)

    #assert (1==2)   # Just to force printing on screen..
