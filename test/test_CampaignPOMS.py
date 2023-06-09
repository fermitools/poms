from mock_Ctx import Ctx
from mock_poms_service import mock_poms_service
from poms.webservice.samweb_lite import samweb_lite
from poms.webservice.poms_model import Campaign, CampaignStage

samhandle = samweb_lite()
fp = mock_poms_service()
test_campaign_id = 700
test_campaign_stage_id = 33
test_campaign_name = "mwm_test_9"


def test_get_campaign_id():
    ctx = Ctx(sam=samhandle)
    campaign_name = test_campaign_name
    res = fp.campaignsPOMS.get_campaign_id(ctx, campaign_name)
    assert res != None


def test_get_campaign_name():
    ctx = Ctx(sam=samhandle)
    campaign_id = test_campaign_id
    res = fp.campaignsPOMS.get_campaign_name(ctx, campaign_id)
    assert res != None


def test_campaign_list():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.campaign_list(ctx)
    print(res)
    assert res
    assert "campaign_stage_id" in res[0] and "name" in res[0]


def test_campaign_add_del_name():
    ctx = Ctx(sam=samhandle)
    name = ("test_add_this",)
    res = fp.campaignsPOMS.campaign_add_name(ctx, campaign_name=name)
    res = fp.campaignsPOMS.get_campaign_id(ctx, campaign_name=name)
    assert res != None
    res = fp.campaignsPOMS.show_campaigns(ctx, action="delete", del_campaign_name=name, pcl_call="1")
    res = fp.campaignsPOMS.get_campaign_id(ctx, campaign_name=name)
    assert res == None


def test_campaign_deps_svg():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.campaign_deps_svg(ctx, campaign_name="mwm_test_9")
    print(res)
    assert res.find("vis.DataSet") >= 0


def test_campaign_deps_ini():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.campaign_deps_ini(ctx, name="mwm_test_9")
    print(res)
    assert res.find("[campaign]") >= 0


def test_mark_campaign_active():
    campaign_id = str(test_campaign_id)
    ctx = Ctx(sam=samhandle)
    is_active = False
    res = fp.campaignsPOMS.mark_campaign_active(ctx, campaign_id, is_active, "")
    is_active = True
    res = fp.campaignsPOMS.mark_campaign_active(ctx, campaign_id, is_active, "")


def test_show_campaigns():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.show_campaigns(ctx)
    print(res)
    assert isinstance(res[0][0], Campaign)


# skip this, we do it in test_split_types...
# def test_get_dataset_for():
#    ctx = Ctx(sam=samhandle)
#    res = fp.campaignsPOMS.get_dataset_for(ctx, camp)

"""
Not yet implemented

def test_launch_campaign():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.launch_campaign(
        ctx,
        campaign_id,
        launcher,
        dataset_override=None,
        parent_submission_id=None,
        param_overrides=None,
        test_login_setup=None,
        test_launch=False,
        output_commands=False
    )

def test_fixup_recoveries():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.fixup_recoveries(ctx, job_type_id, recoveries)

def test_make_test_campaign_for():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.make_test_campaign_for(ctx, campaign_def_id, campaign_def_name)

def test_session_status_history():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.session_status_history(ctx, submission_id)


def test_list_launch_file():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.list_launch_file(ctx, campaign_stage_id, fname, login_setup_id=None)
def test_schedule_launch():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.schedule_launch(ctx, campaign_stage_id)

def test_update_launch_schedule():
    res = fp.campaignsPOMS.update_launch_schedule(...)
    pass

def test_make_stale_campaigns_inactive():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.make_stale_campaigns_inactive(ctx)

def test_split_type_javascript():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.split_type_javascript(ctx)

def test_save_campaign():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.save_campaign(ctx, replace=False, pcl_call=0, *args, **kwargs)

======== skip these, we're planning to drop them anyway...
def test_job_type_edit():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.job_type_edit(ctx, **kwargs)

def test_login_setup_edit_1():
    ctx = Ctx(sam=samhandle)
    kwargs = {
    }
    res = fp.campaignsPOMS.login_setup_edit(ctx, **kwargs)
    print(res)
    assert('templates' in res)

def test_campaign_stage_edit():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.campaign_stage_edit(ctx, **kwargs)

def test_campaign_stage_edit_query():
    ctx = Ctx(sam=samhandle)
    res = fp.campaignsPOMS.campaign_stage_edit_query(ctx, **kwargs)

"""
