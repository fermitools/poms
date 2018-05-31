from mock.mock import MagicMock
from mock_poms_service import mock_poms_service
from mock_stubs import gethead, launch_seshandle, camp_seshandle, err_res, getconfig
from DBHandle import DBHandle
from poms.webservice.utc import utc
import time
from datetime import datetime, timedelta
from poms.webservice.poms_model import CampaignStage, Campaign

mps = mock_poms_service()
dbhandle = DBHandle().get()

import logging
logger = logging.getLogger('cherrypy.error')


class authorized:
    is_authorized = MagicMock(return_value = True)

#sesshandle = MagicMock(return_value = authorized)

def test_tags_create():
    # test tagging a campaign, and then looking up campaign_stages with that tag...
    tag_name = 'test_tag_%d' % time.time()
    tag_name = 'mvi_%d' %time.time()
    cname = 'mwm_test_splits'
    cs = dbhandle.query(CampaignStage).filter(CampaignStage.name == 'mwm_test_splits').first()

    res = mps.tagsPOMS.link_tags(dbhandle, camp_seshandle, cs.campaign_stage_id, tag_name, cs.experiment)
    print("link_tags returns: %s" % res)

    clist = mps.tagsPOMS.search_tags(dbhandle, tag_name)
    print("search_tags returns: %s" % repr(clist))

    print(' testing for campaign %s' %cname)
    print(' campaign %s, ntags= %s' %(cname,len(clist)) )
    for aval in clist: print(' v=%s' %aval)
    cs = clist[0][0]

    assert(cs.campaign_stage_id == cs.campaign_stage_id)
    #assert(1==2)        # Just to force printing on screen.

# Create and delete a tag

def test_tags_delete():

    # test tagging a campaign, and then looking up campaign_stages with that tag...
    tag_name = 'mvi_tag_for_delete'
    cname = 'mwm_test_splits'
    cs = dbhandle.query(CampaignStage).filter(CampaignStage.name == 'mwm_test_splits').first()
    exp = cs.experiment
    #print ' testing for campaign %s, experiemnt = %s ' %(cname,exp)
    
    #This creates the tag
    mps.tagsPOMS.link_tags(dbhandle, camp_seshandle, cs.campaign_stage_id, tag_name, cs.experiment)

    clist = mps.tagsPOMS.search_tags(dbhandle, tag_name)
    
    results=clist[0]            #It contains the list of campaign_stages ID with the tag.
    qq = clist[1]               #It contains the list of the initial tag(s)

    #print ' len(results)=%s, len(qq)= %s' %(len(results),len(qq))
    cids=[]
    if results: 
        for val in results: 
            #print ' found tag %s for campaign id =%s' %(tag_name,val.campaign_stage_id)
            cids.append(val.campaign_stage_id)
        
    else:
        pass #print ' tag %s not found for campaign %s' %(tag_name,cname)

    if results: 
        #finding tag id from tag name
        allTags = dbhandle.query(Campaign).filter(Campaign.tag_name==tag_name, Campaign.experiment == exp).all()
        for atag in allTags:
            #print ' tag = %s, id = %s' %(atag.tag_name,atag.campaign_id) 
            for cid in cids:
              #print ' deleting tag %s id =%s for campaign %s' %(atag.tag_name,atag.campaign_id,cid)
              mps.tagsPOMS.delete_campaigns_tags(dbhandle, camp_seshandle, cid, atag.campaign_id, cs.experiment)
    
    clist = mps.tagsPOMS.search_tags(dbhandle, tag_name)
    results=clist[0]
    assert(len(results)==0)
    #assert (1==2)   # Just to force printing on screen..


def test_tags_create_multi():

    print(' Testings : create a tag to multiple campaign_stages')
    tag_name = 'mvi_tag_%d' %time.time()
    #cname = 'mwm_test_splits'
    campaign_stages = dbhandle.query(CampaignStage).filter(CampaignStage.name.like('%mwm%')).all()
    print(' found %s campaign_stages ' %len(campaign_stages))
    for ac in campaign_stages:
        print(' %s %s %s' %(ac.campaign_stage_id,ac.name, ac.experiment))
        mps.tagsPOMS.link_tags(dbhandle, camp_seshandle, ac.campaign_stage_id, tag_name, ac.experiment)

    # now verify
    clist = mps.tagsPOMS.search_tags(dbhandle, tag_name)
    results=clist[0]
    if results: 
        for val in results: 
            print(' found tag %s for campaign id =%s' %(tag_name,val.campaign_stage_id))
    assert(len(results)>0)

    #assert (1==2)   # Just to force printing on screen..
