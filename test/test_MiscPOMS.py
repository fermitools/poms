from mock_Ctx import Ctx
from mock_poms_service import mock_poms_service
from poms.webservice.samweb_lite import samweb_lite
from poms.webservice.poms_model import Campaign, CampaignStage, JobType

samhandle = samweb_lite()
fp = mock_poms_service()
test_campaign_id = 700
test_campaign_stage_id = 33
test_campaign_name = "mwm_test_9"

import DBHandle
dbhandle = DBHandle.DBHandle().get()

def test_get_jobtype_id():
    ctx = Ctx(sam=samhandle)
    res = fp.miscPOMS.get_jobtype_id(ctx, "fakesim")
    print(res)
    assert res != None


def test_get_loginsetup_id():
    ctx = Ctx(sam=samhandle)
    res = fp.miscPOMS.get_loginsetup_id(ctx, "samdev-o-rama")
    print(res)
    assert res != None

def test_get_recovery_list_for_campaign_def():
    ctx = Ctx(sam=samhandle)
    campaign_def = dbhandle.query(JobType).filter(JobType.job_type_id == 20).first()
    res = fp.miscPOMS.get_recovery_list_for_campaign_def(ctx, campaign_def)

def test_get_recoveries():
    ctx = Ctx(sam=samhandle)
    cid = test_campaign_stage_id
    res = fp.miscPOMS.get_recoveries(ctx, cid)
    print(res)
    print(isinstance(res, list))


