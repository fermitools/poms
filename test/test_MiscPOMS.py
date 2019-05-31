from mock_Ctx import Ctx
from mock_poms_service import mock_poms_service
from poms.webservice.samweb_lite import samweb_lite
from poms.webservice.poms_model import Campaign, CampaignStage

samhandle = samweb_lite()
fp = mock_poms_service()
test_campaign_id = 700
test_campaign_stage_id = 33
test_campaign_name = "mwm_test_9"


def test_get_jobtype_id():
    ctx = Ctx(sam=samhandle)
    res = fp.miscPOMS.get_jobtype_id(ctx, "fakesim")
    print(res)
    assert(res != None)

def test_get_loginsetup_id():
    ctx = Ctx(sam=samhandle)
    res = fp.miscPOMS.get_loginsetup_id(ctx, "samdev-o-rama")
    print(res)
    assert(res != None)
