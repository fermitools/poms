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
            
    def get_exp_owner_role(self, dbhandle, t, item_id):
        if t == "Submission":
            k = "sub:%s" % item_id
            q = (dbhandle.query(CampaignStage.experiment, 
                                CampaignStage.creator, 
                                CampaignStage.creator_role)
                .join(Submission,Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
                .filter(Submission.submission_id == item_id))
        elif t == "CampaignStage":
            k = "cs:%s" % item_id
            q = (dbhandle.query(CampaignStage.experiment, 
                                CampaignStage.creator, 
                                CampaignStage.creator_role)
                .filter(CampaignStage.campaign_stage_id == item_id))
        elif t == "Campaign":
            k = "c:%s" % item_id
            q = (dbhandle.query(Campaign.experiment, 
                                Campaign.creator, 
                                Campaign.creator_role)
                .filter(Campaign.campaign_id == item_id))
        elif t == "LoginSetup":
            k = "ls:%s" % item_id
            q = (dbhandle.query(LoginSetup.experiment, 
                                LoginSetup.creator, 
                                LoginSetup.creator_role)
                .filter(LoginSetup.login_setup_id == item_id))
        elif t == "JobType":
            k = "c:%s" % item_id
            q = (dbhandle.query(JobType.experiment, 
                                JobType.creator, 
                                JobType.creator_role)
                .filter(JobType.job_type_id == item_id))
        else:
            raise Exception("unknown item type '%s'" % t)

        if not k in self.icache:
            rows = q.all()
            logit.log("permissions: got data: %s" % repr(list(rows)))
            if rows:
                self.icache[k] = rows[0]
            else:
                raise Exception("unknown item %s %s" % (t, item_id))
               
        return self.icache[k]

    def can_view(self, dbhandle,  t, item_id, username, cur_exp, cur_role):
        if self.is_superuser(dbhandle, username):
            return
        exp, owner, role = self.get_exp_owner_role(dbhandle, t, item_id)
        if exp != cur_exp:
            raise PermissionError("Must be acting as experiment %s to see this" % exp)

    def can_modify(self, dbhandle, t, item_id, username, cur_exp, cur_role):
        if self.is_superuser(dbhandle, username):
            return None

        exp, owner, role = self.get_exp_owner_role(dbhandle, t, item_id)

        if exp != cur_exp:
            raise PermissionError("Must be acting as experiment %s to change this" % exp)
        if cur_role not in ('coordinator','superuser') and role != cur_role:
            raise PermissionError("Must be role %s to change this" % role)

        if cur_role == 'analysis' and owner != username:
            raise PermissionError("Must be user %s to change this" % exp)

    def can_do(self, dbhandle, t, item_id, username, cur_exp, cur_role):
        if self.is_superuser(dbhandle, username):
            return
        exp, owner, role = self.get_exp_owner_role(dbhandle, t, item_id)

        if exp != cur_exp:
            raise PermissionError("Must be acting as experiment %s to do this" % exp)
        if cur_role not in ('coordinator','superuser') and role != cur_role:
            raise PermissionError("Must be role %s to do this" % role)

        if cur_role == 'analysis' and owner != username:
            raise PermissionError("Must be user %s to do this" % exp)
