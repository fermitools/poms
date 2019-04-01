'''
Module to centralize and cache permissions checks
Since we don't allow changing ownership of entities,
we can remember who they belong to and make permission
checks fast, and put the calls in the poms_service layer.
'''

import poms.webservice.logit as logit
from poms.webservice.poms_model import JobType, LoginSetup, Campaign, CampaignStage, Submission, Experimenter, ExperimentsExperimenters

class Permissions:
    def __init__(self):
        self.icache = {}
        self.sucache = {}
        self.excache = {}

    def is_superuser(self, dbhandle, user):
        if not user in self.sucache:
            if str(user)[0] in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
                rows = dbhandle.query(Experimenter.root).filter(Experimenter.experimenter_id == user).all()
            else:
                rows = dbhandle.query(Experimenter.root).filter(Experimenter.username == user).all()
            self.sucache[user] = rows[0].root
        logit.log("is_superuser(%s) returning %s" %( user, self.sucache[user]))
        return self.sucache[user]

    def check_experiment_role(self, dbhandle, user, experiment, desired_role):
        key = "%s:%s" %(user, experiment)
        if not key in self.excache:
            rows = (dbhandle.query(ExperimentsExperimenters.role)
                    .join(Experimenter, Experimenter.experimenter_id == ExperimentsExperimenters.experimenter_id)
                    .filter(Experimenter.username == user, ExperimentsExperimenters.experiment == experiment)).all()
            if rows:
                self.excache[key] = rows[0]
            else:
                self.excache[key] = None
        logit.log("experiment_role(%s,%s) returning: %s" % (user, experiment, self.excache[key]))
        if not self.excache[key]:
            raise PermissionError("user %s is not in experiment %s" % (user, experiment))
        if self.excache[key] == "analysis" and desired_role != self.excache[key]:
            raise PermissionError("user %s cannot have role %s in experiment %s" % (user, desired_role, experiment))
        if self.excache[key] == "production" and desired_role == 'superuser':
            raise PermissionError("user %s cannot have role %s in experiment %s" % (user, desired_role, experiment))

        return self.excache[key]

    def get_exp_owner_role(self, dbhandle, t, item_id = None, name = None, experiment = None):
        if not name and not item_id:
            raise AssertionError("need either item_id or name")

        if t == "Experiment":
            return experiment, None, None

        if t == "Submission":
            k = "sub:%s" % item_id
            q = (dbhandle.query(CampaignStage.experiment,
                                CampaignStage.creator,
                                CampaignStage.creator_role)
                 .join(Submission, Submission.campaign_stage_id == CampaignStage.campaign_stage_id))
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

        # special case for Experimenter checkss
        if t == "Experimenter":
            if item_id != username:
                raise PermissionError("Only user %s can view this" % item_id)
            return

        self.check_experiment_role(dbhandle, user, cur_exp, cur_role)

        if not item_id and not name:
            return

        exp, owner, role = self.get_exp_owner_role(dbhandle, t, item_id=item_id, name=name, experiment=experiment)
        logit.log("can_view: cur: %s, %s, %s; item: %s, %s, %s" % (username, cur_exp, cur_role, owner, exp, role))
        if exp and exp != cur_exp:
            raise PermissionError("Must be acting as experiment %s to see this" % exp)

    def can_modify(self, dbhandle, username, cur_exp, cur_role, t, item_id=None, name=None, experiment=None):
        if self.is_superuser(dbhandle, username):
            return None
        if not item_id and not name:
            return

        # special case for experimenter check
        if t == "Experimenter":
            if item_id != username:
                raise PermissionError("Only user %s can change this" % item_id)
            return


        exp, owner, role = self.get_exp_owner_role(dbhandle, t, item_id=item_id, name=name, experiment=experiment)

        # if no owner, role passed in from url, default to one in item
        if cur_exp is None:
            cur_exp = exp
        if cur_role is None:
            cur_role = role

        self.check_experiment_role(dbhandle, user, cur_exp, cur_role)


        logit.log("can_modify: cur: %s, %s, %s; item: %s, %s, %s" % (username, cur_exp, cur_role, owner, exp, role))
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

        # special case for experimenter
        if t == "Experimenter":
            if item_id != username:
                raise PermissionError("Only user %s can view this" % item_id)
            return

        self.check_experiment_role(dbhandle, user, cur_exp, cur_role)

        exp, owner, role = self.get_exp_owner_role(dbhandle, t, item_id=item_id, name=name, experiment=experiment)

        logit.log("can_do: cur: %s, %s, %s; item: %s, %s, %s" % (username, cur_exp, cur_role, owner, exp, role))
        if exp and exp != cur_exp:
            raise PermissionError("Must be acting as experiment %s to do this" % exp)
        if role and cur_role not in ('coordinator','superuser') and role != cur_role:
            raise PermissionError("Must be role %s to do this" % role)

        if role and cur_role == 'analysis' and owner != username:
            raise PermissionError("Must be user %s to do this" % exp)
