"""
Module to centralize and cache permissions checks
Since we don't allow changing ownership of entities,
we can remember who they belong to and make permission
checks fast, and put the calls in the poms_service layer.
"""

from poms.webservice.poms_model import (
    JobType,
    LoginSetup,
    Campaign,
    CampaignStage,
    Submission,
    Experimenter,
    ExperimentsExperimenters,
)
from . import logit


class Permissions:
    def __init__(self):
        self.clear_cache()

    def clear_cache(self):
        self.icache = {}
        self.sucache = {}
        self.excache = {}

    def is_superuser(self, ctx):
        if not ctx.username in self.sucache:
            # Is this a username or user_id?
            if str(ctx.username).isdecimal():
                rows = ctx.db.query(Experimenter.root).filter(Experimenter.experimenter_id == ctx.username).all()
            else:
                rows = ctx.db.query(Experimenter.root).filter(Experimenter.username == ctx.username).all()
            self.sucache[ctx.username] = rows[0].root
        logit.log("is_superuser(%s) returning %s" % (ctx.username, self.sucache[ctx.username]))
        return self.sucache[ctx.username]

    def check_experiment_role(self, ctx):
        if ctx.experiment == "fermilab":
            ctx.experiment = "samdev"
        key = "%s:%s" % (ctx.username, ctx.experiment)
        if not key in self.excache:
            rows = (
                ctx.db.query(ExperimentsExperimenters.role)  #
                .join(Experimenter, Experimenter.experimenter_id == ExperimentsExperimenters.experimenter_id)
                .filter(Experimenter.username == ctx.username, ExperimentsExperimenters.experiment == ctx.experiment)
            ).all()
            if rows:
                self.excache[key] = rows[0][0]
            else:
                self.excache[key] = None
        logit.log("experiment_role(%s,%s,%s) returning: %s" % (ctx.username, ctx.experiment, ctx.role, self.excache[key]))
        if not self.excache[key]:
            raise PermissionError("username %s is not in experiment %s" % (ctx.username, ctx.experiment))
        if not ctx.role in ("analysis", "production-shifter", "production", "superuser"):
            raise PermissionError("invalid role %s" % ctx.role)
        if self.excache[key] == "analysis" and ctx.role != self.excache[key]:
            raise PermissionError("username %s cannot have role %s in experiment %s" % (ctx.username, ctx.role, ctx.experiment))
        if self.excache[key] == "production" and ctx.role == "superuser":
            raise PermissionError("username %s cannot have role %s in experiment %s" % (ctx.username, ctx.role, ctx.experiment))

        return self.excache[key]

    def get_exp_owner_role(self, ctx, t, item_id=None, name=None, experiment=None, campaign_id=None, campaign_name=None):
        if not name and not item_id:
            raise AssertionError("need either item_id or name")

        k = None
        q = None
        if t == "Experiment":
            return experiment, None, None

        if t == "Submission":
            k = "sub:%s" % (item_id or name)
            q = (
                ctx.db.query(CampaignStage.experiment, Experimenter.username, CampaignStage.creator_role)
                .join(Submission, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)  #
                .join(Experimenter, CampaignStage.creator == Experimenter.experimenter_id)  #
            )
            if item_id and str(item_id).isdigit():
                q = q.filter(Submission.submission_id == item_id)
            if name:
                q = q.filter(Submission.jobsub_job_id == name)
        elif t == "CampaignStage":
            if isinstance(name, list):
                campaign_name, name = name
            q = ctx.db.query(CampaignStage.experiment, Experimenter.username, CampaignStage.creator_role).join(  #
                Experimenter, CampaignStage.creator == Experimenter.experimenter_id
            )
            if item_id and str(item_id).isdigit():
                k = "cs:%s" % item_id
                q = q.filter(CampaignStage.campaign_stage_id == item_id)
            if name and campaign_id:
                k = "cs:%s_%s_%d" % (experiment, name, campaign_id)
                q = q.filter(
                    CampaignStage.name == name, CampaignStage.campaign_id == campaign_id, CampaignStage.experiment == experiment
                )
            elif name and campaign_name:
                k = "cs:%s_%s_%s" % (experiment, name, campaign_name)
                q = q.join(Campaign, CampaignStage.campaign_id == Campaign.campaign_id).filter(
                    CampaignStage.name == name, Campaign.name == campaign_name, CampaignStage.experiment == experiment
                )
            elif name and name.find(",") > 0:
                campaign_name, stage_name = name.split(",", 2)
                q = q.join(Campaign, CampaignStage.campaign_id == Campaign.campaign_id).filter(
                    CampaignStage.name == stage_name, Campaign.name == campaign_name, CampaignStage.experiment == experiment
                )
        elif t == "Campaign":
            q = ctx.db.query(Campaign.experiment, Experimenter.username, Campaign.creator_role).join(  #
                Experimenter, Campaign.creator == Experimenter.experimenter_id
            )
            if item_id and str(item_id).isdigit():
                k = "c:%s" % item_id
                q = q.filter(Campaign.campaign_id == item_id)
            if name:
                k = "c:%s_%s" % (experiment, name)
                q = q.filter(Campaign.name == name, Campaign.experiment == experiment)
        elif t == "LoginSetup":
            q = ctx.db.query(LoginSetup.experiment, Experimenter.username, LoginSetup.creator_role).join(
                Experimenter, LoginSetup.creator == Experimenter.experimenter_id
            )
            if item_id:
                k = "ls:%s" % item_id
                q = q.filter(LoginSetup.login_setup_id == item_id)
            if name:
                k = "ls:%s_%s" % (experiment, name)
                q = q.filter(LoginSetup.name == name, LoginSetup.experiment == experiment)
        elif t == "JobType":
            q = ctx.db.query(JobType.experiment, Experimenter.username, JobType.creator_role).join(
                Experimenter, Experimenter.experimenter_id == JobType.creator
            )
            if item_id:
                k = "c:%s" % item_id
                q = q.filter(JobType.job_type_id == item_id)
            if name:
                k = "c:%s_%s" % (experiment, name)
                q = q.filter(JobType.name == name, JobType.experiment == experiment)
        else:
            raise Exception("unknown item type '%s'" % t)

        if not k or not q:
            return (None, None, None)

        if k not in self.icache:
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

    def can_view(self, ctx, t, item_id=None, name=None, experiment=None, campaign_id=None, campaign_name=None):

        if self.is_superuser(ctx) and ctx.role == 'superuser':
            return

        if item_id or name:
            exp, owner, role = self.get_exp_owner_role(
                ctx, t, item_id=item_id, name=name, experiment=experiment, campaign_id=campaign_id, campaign_name=campaign_name
            )
        else:
            exp, owner, role = None, None, None

        # if no owner, role passed in from url, default to one in item
        if ctx.experiment is None:
            ctx.experiment = exp
        if ctx.role is None:
            ctx.role = role

        logit.log(
            "can_view: %s: cur: %s, %s, %s; item: %s, %s, %s" % (t, ctx.username, ctx.experiment, ctx.role, owner, exp, role)
        )

        self.check_experiment_role(ctx)

        # special case for Experimenter checkss
        if t == "Experimenter":
            if item_id != ctx.username:
                raise PermissionError("Only user %s can view this" % item_id)
            return

        if not item_id and not name:
            return

        if exp and exp != ctx.experiment:
            logit.log("can_view: resp: fail")
            raise PermissionError("Must be acting as experiment %s to see this" % exp)
        logit.log("can_view: resp: ok")

    def nonexistent(self, ctx, t, item_id=None, name=None, experiment=None, campaign_id=None, campaign_name=None):
        logit.log(
            "nonexistent: %s: cur: %s, %s, %s; item: %s, %s, %s" % (t, ctx.username, ctx.experiment, ctx.role, owner, exp, role)
        )
        owner = None
        role = None
        if item_id or name:
            exp, owner, role = self.get_exp_owner_role(
                ctx, t, item_id=item_id, name=name, experiment=experiment, campaign_id=campaign_id, campaign_name=campaign_name
            )
        if owner != None or role != None:
            logit.log("nonexsitent: resp: fail")
            raise PermissionError("%s named %s already exists." % (name, item_id))
        logit.log("nonexistent: resp: ok")

    def can_modify(self, ctx, t, item_id=None, name=None, experiment=None, campaign_id=None, campaign_name=None):

        if self.is_superuser(ctx) and ctx.role == 'superuser':
            return None

        if item_id or name:
            exp, owner, role = self.get_exp_owner_role(
                ctx, t, item_id=item_id, name=name, experiment=experiment, campaign_id=campaign_id, campaign_name=campaign_name
            )
        else:
            exp, owner, role = None, None, None

        # if no owner, role passed in from url, default to one in item
        if ctx.experiment is None:
            ctx.experiment = exp
        if ctx.role is None:
            ctx.role = role

        logit.log(
            "can_modify: %s cur: %s, %s, %s; item: %s, %s, %s" % (t, ctx.username, ctx.experiment, ctx.role, owner, exp, role)
        )
        self.check_experiment_role(ctx)

        if not item_id and not name:
            return

        # special case for experimenter check
        if t == "Experimenter":
            if item_id != ctx.username:
                logit.log("can_view: resp: fail")
                raise PermissionError("Only user %s can change this" % item_id)
            logit.log("can_view: resp: ok")
            return

        if exp and exp != ctx.experiment:
            logit.log("can_modify: resp: fail")
            raise PermissionError("Must be acting as experiment %s to change this" % exp)
        if role and ctx.role not in ("coordinator", "superuser") and role != ctx.role:
            logit.log("can_modify: resp: fail")
            raise PermissionError("Must be role %s to change this" % role)

        if ctx.role == "analysis" and owner and owner != ctx.username:
            logit.log("can_modify: resp: fail")
            raise PermissionError("Must be user %s to change this" % owner)
        logit.log("can_modify: resp: ok")

    def can_do(self, ctx, t, item_id=None, name=None, experiment=None, campaign_id=None, campaign_name=None):

        if self.is_superuser(ctx) and ctx.role == 'superuser':
            return

        if item_id or name:
            exp, owner, role = self.get_exp_owner_role(
                ctx, t, item_id=item_id, name=name, experiment=experiment, campaign_id=campaign_id, campaign_name=campaign_name
            )
        else:
            exp, owner, role = None, None, None

        # if there was no role, experiment in the url, get the one
        # from the item

        if ctx.role == None or ctx.role == "None" or ctx.role == "":
            ctx.role = role

        if ctx.experiment == None or ctx.experiment == "None" or ctx.experiment == "":
            ctx.experiment = experiment

        logit.log("can_do: %s cur:  %s, %s, %s; item: %s, %s, %s" % (t, ctx.username, ctx.experiment, ctx.role, owner, exp, role))

        self.check_experiment_role(ctx)

        if not item_id and not name:
            return

        # special case for experimenter
        if t == "Experimenter":
            if item_id != ctx.username:
                raise PermissionError("Only user %s can do this" % item_id)
            return

        if exp and exp != ctx.experiment:
            logit.log("can_do: resp: fail")
            raise PermissionError("Must be acting as experiment %s to do this" % exp)

        if role and ctx.role == "analysis" and owner and owner != ctx.username:
            logit.log("can_do: resp: fail")
            raise PermissionError("Must be user %s to do this" % owner)

        if ctx.role == "production" and role == "analysis":
            # bandaid for get_task_id_for stuff...
            logit.log("letting it slide...")
            return

        # make production-shifter work -- you can *do* things production
        # users can do, so if the user is a production-shifter (ctx.role) 
        # the action requires production, rewrite it to production-shifter
        # so it matches below
        if role == 'production' and ctx.role == 'production-shifter':
            role = 'production-shifter'

        if role and ctx.role not in ("coordinator", "superuser") and role != ctx.role:
            logit.log("can_do: resp: fail")
            raise PermissionError("Must be role %s, not %s to do this" % (role, ctx.role))

        logit.log("can_do: resp: ok")
