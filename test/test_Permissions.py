import DBHandle
import poms.webservice.Permissions as Permissions
from poms.webservice.poms_model import CampaignStage, Submission, Experimenter

p = Permissions.Permissions()
dbh = DBHandle.DBHandle()
dbhandle = dbh.get()
victim_cs = dbhandle.query(CampaignStage).join(Submission, Submission.campaign_stage_id == CampaignStage.campaign_stage_id).filter(CampaignStage.experiment=='samdev', CampaignStage.creator == 4).first()
victim_sub = dbhandle.query(Submission).filter(Submission.campaign_stage_id == victim_cs.campaign_stage_id).first()

def test_is_superuser():
    assert(p.is_superuser(dbhandle, 'mengel'))
    assert(not p.is_superuser(dbhandle, 'pbuitrag'))

def item_role(t, id):

    p.can_view(dbhandle, 'mengel', 'samdev', 'superuser', t, item_id = id)

    # these should raise PermissionError and not get to the assert
    # we pick on Paola 'cause shes not here anymore, but is still in
    # the database ;-)
    try:
        p.can_view(dbhandle,  'pbuitrag', 'nova', 'production', t, item_id = id)
        assert(False)
    except PermissionError:
        pass
    except:
        raise

    try:
        p.can_view(dbhandle, 'pbuitrag', 'nova', 'analysis', t, item_id = id)
        assert(False)
    except PermissionError:
        pass
    except:
        raise

    assert(True)

def test_sub():
     item_role('Submission', victim_sub.submission_id)

def test_cs():
     item_role('CampaignStage', victim_cs.campaign_stage_id)

def test_c():
     item_role('Campaign', victim_cs.campaign_id)

def test_ls():
     item_role('LoginSetup', victim_cs.login_setup_id)

def test_jt():
     item_role('JobType', victim_cs.job_type_id)

