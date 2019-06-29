from mock_Ctx import Ctx
from mock_poms_service import mock_poms_service
from poms.webservice.samweb_lite import samweb_lite
from poms.webservice.poms_model import Campaign, CampaignStage

samhandle = samweb_lite()
fp = mock_poms_service()
test_campaign_id = 700
test_campaign_stage_id = 33
test_campaign_name = "mwm_test_9"


def test_get_campaign_stage_name():
    ctx = Ctx(sam=samhandle)
    campaign_stage_id = test_campaign_stage_id
    res = fp.stagesPOMS.get_campaign_stage_name(ctx, campaign_stage_id)
    assert res == " mwm_test_9"



def test_show_campaign_stages():
    ctx = Ctx(sam=samhandle)
    res = fp.stagesPOMS.show_campaign_stages(ctx)
    print(res)
    assert isinstance(res[0][0], CampaignStage)


def test_campaign_stage_info():
    ctx = Ctx(sam=samhandle)
    res = fp.stagesPOMS.campaign_stage_info(ctx, test_campaign_stage_id)
    print(res)
    assert isinstance(res[0][0], CampaignStage)


def test_reset_campaign_split():
    ctx = Ctx(sam=samhandle)
    res = fp.stagesPOMS.reset_campaign_split(ctx, test_campaign_stage_id)
    assert True


def test_campaign_stage_submissions():
    ctx = Ctx(sam=samhandle)
    # res = fp.stagesPOMS.campaign_stage_submissions(ctx, campaign_name="fake%20demo%20v1.0%20%20w/chars", stage_name="f_sim_v1",campaign_stage_id=2)
    res = fp.stagesPOMS.campaign_stage_submissions(ctx, campaign_stage_id=2)
    assert "submission_id" in res["submissions"][0]
    res = fp.stagesPOMS.campaign_stage_submissions(ctx, campaign_id=890)
    print(res)
    assert "submission_id" in res["submissions"][0]
