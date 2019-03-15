'''
Module to centralize and cache permissions checks
Since we don't allow changing ownership of entities,
we can remember who they belong to and make permission
checks fast, and put the calls in the poms_service layer.
'''

import poms.webservice.logit as logit
from poms.webservice.poms_model import JobType, LoginSetup, Campaign, CampaignStage, Submission, Experimenter

class Permissions:
    def __init__(self):
        self.icache = {}
        self.sucache = {}

    def is_superuser(self, dbhandle, user):
        if not user in self.sucache:
            rows = dbhandle.query(Experimenter.root).filter(Experimenter.username == user).all()
        return rows[0].root
            
    def get_exp_owner_role(self, dbhandle, t, item_id = None, name = None, experiment = None):
        if not name and not item_id:
            raise AssertionError("need either item_id or name")
        if t == "Submission":
            k = "sub:%s" % item_id
            q = (dbhandle.query(CampaignStage.experiment, 
                                CampaignStage.creator, 
                                CampaignStage.creator_role)
                .join(Submission,Submission.campaign_stage_id == CampaignStage.campaign_stage_id))
            if item_id:
                q = q.filter(Submission.submission_id == item_id)
            if name:
                q = q.filter(Submission.jobsub_job_id == name)
        elif t == "CampaignStage":
            k = "cs:%s" % item_id
            q = (dbhandle.query(CampaignStage.experiment, 
                                CampaignStage.creator, 
                                CampaignStage.creator_role))
            if item_id:
                q = q.filter(CampaignStage.campaign_stage_id == item_id)
            if name:
                raise AssertionError("cannot lookup campaign stages by name as they are not unique")
        elif t == "Campaign":
            k = "c:%s" % item_id
            q = (dbhandle.query(Campaign.experiment, 
                                Campaign.creator, 
                                Campaign.creator_role))
            if item_id:
                q = q.filter(Campaign.campaign_id == item_id)
            if name:
                q = q.filter(Campaign.name == item_id, Campaign.experiment == experiment)
        elif t == "LoginSetup":
            k = "ls:%s" % item_id
            q = (dbhandle.query(LoginSetup.experiment, 
                                LoginSetup.creator, 
                                LoginSetup.creator_role))
            if item_id:
                q = q.filter(LoginSetup.login_setup_id == item_id)
            if name:
                q = q.filter(LoginSetup.name == name , LoginSetup.experiment == experiment)
        elif t == "JobType":
            k = "c:%s" % item_id
            q = (dbhandle.query(JobType.experiment, 
                                JobType.creator, 
                                JobType.creator_role))
            if item_id:
                q = q.filter(JobType.job_type_id == item_id)
            if name:
                q = q.filter(JobType.name == name , JobType.experiment == experiment)
        else:
            raise Exception("unknown item type '%s'" % t)

        if not k in self.icache:
            rows = q.all()
            logit.log("permissions: got data: %s" % repr(list(rows)))
            if rows:
                self.icache[k] = rows[0]
            else:
                # return None for nonexistent items -- means we're editing
                # a new thing, etc. trying to view a nonexistent item --
                # it may fail , but not for permissions.
                return (None, None, None)
               
        return self.icache[k]

    def can_view(self, dbhandle, username, cur_exp, cur_role, t, item_id = None, name = None, experiment = None):
        if self.is_superuser(dbhandle, username):
            return
        if not item_id and not name:
            return
        exp, owner, role = self.get_exp_owner_role(dbhandle, t, item_id = item_id, name = name, experiment = experiment)
        if exp and exp != cur_exp:
            raise PermissionError("Must be acting as experiment %s to see this" % exp)

    def can_modify(self, dbhandle, username, cur_exp, cur_role, t, item_id = None, name = None, experiment = None):
        if self.is_superuser(dbhandle, username):
            return None
        if not item_id and not name:
            return

        exp, owner, role = self.get_exp_owner_role(dbhandle, t, item_id = item_id, name = name, experiment = experiment)

        if exp and exp != cur_exp:
            raise PermissionError("Must be acting as experiment %s to change this" % exp)
        if role and cur_role not in ('coordinator','superuser') and role != cur_role:
            raise PermissionError("Must be role %s to change this" % role)

        if cur_role == 'analysis' and owner != username:
            raise PermissionError("Must be user %s to change this" % exp)

    def can_do(self, dbhandle, username, cur_exp, cur_role, t, item_id = None, name = None, experiment = None ):
        if self.is_superuser(dbhandle, username):
            return
        if not item_id and not name:
            return
        exp, owner, role = self.get_exp_owner_role(dbhandle, t, item_id = item_id, name = name, experiment = experiment)

        if exp and exp != cur_exp:
            raise PermissionError("Must be acting as experiment %s to do this" % exp)
        if role and cur_role not in ('coordinator','superuser') and role != cur_role:
            raise PermissionError("Must be role %s to do this" % role)

        if role and cur_role == 'analysis' and owner != username:
            raise PermissionError("Must be user %s to do this" % exp)
