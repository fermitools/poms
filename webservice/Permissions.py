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
import os
import subprocess
import json
from typing import Union, Optional, List
#import htcondor  # type: ignore
import sys
import time
import glob
import uuid
from datetime import datetime
from datetime import timedelta
#VAULT_OPTS = htcondor.param.get("SEC_CREDENTIAL_GETTOKEN_OPTS", "")
DEFAULT_ROLE = "analysis"

class Permissions:
    
    def __init__(self):
        self.clear_cache()
    0
    def get_tmp(self) -> str:
        """return temp directory path"""
        return os.environ.get("TMPDIR", "/tmp")
    def get_file_upload_path(self, ctx, filename):
        return "%s/uploads/%s/%s/%s" % (ctx.config_get("base_uploads_dir"), ctx.experiment, ctx.username, filename)

    
    def get_launch_sandbox(self, ctx):

        uploads = self.get_file_upload_path(ctx, "")
        uu = uuid.uuid4()  # random uuid -- shouldn't be guessable.
        sandbox = "%s/sandboxes/%s" % (ctx.config_get("base_uploads_dir"), str(uu))
        os.makedirs(sandbox, exist_ok=False)
        upload_path = self.get_file_upload_path(ctx, "*")
        logit.log("get_launch_sandbox linking items from upload_path %s into %s" % (upload_path, sandbox))
        flist = glob.glob(upload_path)
        for f in flist:
            os.link(f, "%s/%s" % (sandbox, os.path.basename(f)))
        return sandbox

    def get_token(self, ctx, debug: int = 0) -> str:
        """get path to token file"""
        pid = os.getuid()
        tmp = self.get_tmp()
        exp = ctx.experiment
        role = ctx.role if ctx.role == "production" else DEFAULT_ROLE
        if exp == "samdev":
            issuer: Optional[str] = "fermilab"
        else:
            issuer = exp

        tokenfile = f"{tmp}/bt_token_{issuer}_{role}_{pid}"
        os.environ["BEARER_TOKEN_FILE"] = tokenfile

        if not self.check_token(ctx, tokenfile):
            if ctx.role == "analysis":
                sandbox = self.get_launch_sandbox(ctx)
                proxyfile = "%s/x509up_voms_%s_Analysis_%s" % (sandbox, exp, ctx.username)
                htgettokenopts = "--vaulttokeninfile=%s/bt_%s_Analysis_%s" % (sandbox, exp, ctx.username)
            else:
                sandbox = "$HOME"
                proxyfile = "/opt/%spro/%spro.Production.proxy" % (exp, exp)
                htgettokenopts = "-r %s --credkey=%spro/managedtokens/fifeutilgpvm01.fnal.gov" % (ctx.role, exp)
                # samdev doesn't really have a managed token...
                if exp == "samdev":
                    htgettokenopts = "-r default"

            cmd = f"htgettoken {htgettokenopts} -i {issuer} -a htvaultprod.fnal.gov "

            if role != DEFAULT_ROLE:
                cmd = f"{cmd} -r {role.lower()}  -a htvaultprod.fnal.gov "  # Token-world wants all-lower

            if debug > 0:
                sys.stderr.write(f"Running: {cmd}")

            res = os.system(cmd)
            if res != 0:
                raise PermissionError(f"Failed acquiring token. Please enter the following input in your terminal and authenticate at the link it provides: 'export BEARER_TOKEN_FILE={tokenfile} {cmd}'")
            if self.check_token(ctx, tokenfile):
                return tokenfile
            raise PermissionError(f"Failed acquiring token. Please enter the following input in your terminal and authenticate at the link it provides: 'export BEARER_TOKEN_FILE={tokenfile} {cmd}'")
        return tokenfile
    
    def check_token(self, ctx) -> bool:
        """check if token is (almost) expired"""
       
        pid = os.getuid()
        #tmp = self.get_tmp()
        role = ctx.role if ctx.role == "production" else DEFAULT_ROLE
        
        """if ctx.experiment == "samdev":
            issuer: Optional[str] = "fermilab"
        else:
            issuer = ctx.experiment
        """
        path = f"/run/user/{pid}"
        vaultpath = "/home/poms/uploads/%s/%s" % (ctx.experiment, ctx.username)
        if role == "analysis":
            tokenfile = f"{path}/bt_{ctx.experiment}_analysis_{ctx.username}"
            vaultfile = f"vt_{ctx.experiment}_analysis_{ctx.username}"
        else:
            tokenfile = f"{path}/bt_{ctx.experiment}_production_{ctx.username}"
            vaultfile = f"vt_{ctx.experiment}_production_{ctx.username}"
        try:
            if (role == "analysis" or ctx.experiment == "samdev") and not os.path.exists(tokenfile):
                if os.path.exists("%s/%s" % (vaultpath, vaultfile)):
                    # Bearer token does not exist, but user has uploaded a vault token, we will trust that it is valid.
                    return True
                return False
            elif role == "production" and ctx.experiment != "samdev":
                logit.log("No need to check if analysis or samdev user bearer token exists, is production user: %s - %s" % (role, ctx.experiment))
                return True
            else:
                return True
        except Exception as e:
            logit.log("An error occured while checking for tokens for user=%s, role=%s, exp=%s. Assuming token info is in launch script: %s" % (ctx.username,role, ctx.experiment. repr(e)))
            return True
        
        """if os.path.exists("/tmp/%s" % vaultfile):
            vaultfile = "/tmp/%s" % vaultfile
        
        try:
            os.environ["BEARER_TOKEN_FILE"] = tokenfile
            p = subprocess.Popen(f"httokendecode", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            so, se = p.communicate()
            p.wait()
            data = json.loads(so.decode('utf8'))
            in_experiment = f"/{issuer}" in data["wlcg.groups"]
            
            if not in_experiment:
                raise PermissionError("User not in experiment")
            exp_ticks = data["exp"]
            #converted_ticks = datetime.datetime.now() + datetime.timedelta(microseconds = exp_ticks/10)
            #tok_scope = data["scope"]
            # logit.log("Time Left Real: %s" % (datetime.utcfromtimestamp(exp_ticks) - datetime.utcnow()))
            if datetime.utcfromtimestamp(exp_ticks) - datetime.utcnow() > timedelta(minutes=60):
                return True
            else:
                logit.log("Removing vault file")
                os.remove(vaultfile)
                logit.log("Removing token file")
                os.remove(tokenfile)
                logit.log("Files Removed")
                return False
        except ValueError as e:
            logit.log("Removing vault file")
            os.remove(vaultfile)
            logit.log("Removing token file")
            os.remove(tokenfile)
            logit.log("Files Removed")
            logit.log("Error authorizing token: %s" % (str(e)))
            print(
                "decode_token.sh could not successfully extract the "
                f"expiration time from token file {tokenfile}. Please open "
                "a ticket to Distributed Computing Support if you need further "
                "assistance."
            )
            return False"""
        return False

  

    def clear_cache(self):
        self.icache = {}
        self.sucache = {}
        self.excache = {}
        logit.log("Cleared Cache")

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
        if self.is_superuser(ctx):
            return "superuser"

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
        if t == "Experiment" or t == "Experimenter":
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
        if self.is_superuser(ctx) and ctx.role == "superuser":
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

        if exp and exp != ctx.experiment and not self.is_superuser(ctx):
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
        if self.is_superuser(ctx) and ctx.role == "superuser":
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

        if exp and exp != ctx.experiment and not self.is_superuser(ctx):
            logit.log("can_modify: resp: fail")
            raise PermissionError("Must be acting as experiment %s to change this" % exp)

        logit.log("can_modify_test: %s cur: %s, %s, %s; item: %s, %s, %s" % (role, ctx.username, ctx.experiment, ctx.role, owner, exp, role))

        if role and ctx.role not in ("coordinator", "superuser") and role != ctx.role:
            logit.log("can_modify: resp: fail")
            raise PermissionError("Must be role %s to change this" % role)

        if ctx.role == "analysis" and owner and owner != ctx.username:
            logit.log("can_modify: resp: fail")
            raise PermissionError("Must be user %s to change this" % owner)
        logit.log("can_modify: resp: ok")

    def can_do(self, ctx, t, item_id=None, name=None, experiment=None, campaign_id=None, campaign_name=None):
        if self.is_superuser(ctx) and ctx.role == "superuser":
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

        if exp and exp != ctx.experiment and not self.is_superuser(ctx):
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
        if role == "production" and ctx.role == "production-shifter":
            role = "production-shifter"

        if role and ctx.role not in ("coordinator", "superuser") and role != ctx.role:
            logit.log("can_do: resp: fail")
            raise PermissionError("Must be role %s, not %s to do this" % (role, ctx.role))

        logit.log("can_do: resp: ok")

