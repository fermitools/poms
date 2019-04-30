import DBHandle
import poms.webservice.Permissions as Permissions
from poms.webservice.poms_model import CampaignStage, Submission, Experimenter
from mock_Ctx import Ctx

p = Permissions.Permissions()
dbh = DBHandle.DBHandle()
dbhandle = dbh.get()
victim_cs = (
    dbhandle.query(CampaignStage)
    .join(Submission, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
    .filter(CampaignStage.experiment == "samdev", CampaignStage.creator == 4)
    .first()
)
victim_sub = dbhandle.query(Submission).filter(Submission.campaign_stage_id == victim_cs.campaign_stage_id).first()

ctx = Ctx(db=dbhandle)


def test_is_superuser():
    ctx.username="mengel"
    assert p.is_superuser(ctx)
    ctx.username="pbuitrag"
    assert not p.is_superuser(ctx)
    ctx.username="mengel"


def item_role(t, id):

    ctx.username="mengel"
    p.can_view(ctx, t, item_id=id)

    # these should raise PermissionError and not get to the assert
    # we pick on Paola 'cause shes not here anymore, but is still in
    # the database ;-)
    try:
        ctx.username="pbuitrag"
        p.can_view(ctx, t, item_id=id)
        assert False
    except PermissionError:
        pass
    except:
        raise

    try:
        ctx.username="pbuitrag"
        p.can_view(ctx, t, item_id=id)
        assert False
    except PermissionError:
        pass
    except:
        raise

    assert True


def test_sub():
    item_role("Submission", victim_sub.submission_id)


def test_cs():
    item_role("CampaignStage", victim_cs.campaign_stage_id)


def test_c():
    item_role("Campaign", victim_cs.campaign_id)


def test_ls():
    item_role("LoginSetup", victim_cs.login_setup_id)


def test_jt():
    item_role("JobType", victim_cs.job_type_id)
