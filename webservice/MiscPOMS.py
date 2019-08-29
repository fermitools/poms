#!/usr/bin/env python
"""
This module contain the methods that allow to create campaign_stages, definitions and templates.
List of methods:

Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in
poms_service.py written by Marc Mengel, Michael Gueith and Stephen White.
Date: April 28th, 2017. (changes for the POMS_client)
"""

import glob
import importlib
import json
import os
import subprocess
import time
import traceback
from collections import OrderedDict, deque
from datetime import datetime, timedelta
import re

import cherrypy
from crontab import CronTab
from sqlalchemy import and_, distinct, func, or_, text, Integer
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import joinedload, attributes, aliased

from . import logit
from .poms_model import (
    Campaign,
    CampaignStage,
    CampaignsTag,
    JobType,
    JobTypeSnapshot,
    CampaignDependency,
    CampaignRecovery,
    CampaignStageSnapshot,
    Experiment,
    Experimenter,
    HeldLaunch,
    LoginSetup,
    LoginSetupSnapshot,
    RecoveryType,
    Submission,
    SubmissionHistory,
    SubmissionStatus,
)
from .utc import utc


class MiscPOMS:
    """
       Business logic for CampaignStage related items
    """

    # h3. __init__
    def __init__(self, ps):
        """
            initialize ourself with a reference back to the overall poms_service
        """
        self.poms_service = ps

    # h3. login_setup_edit
    def login_setup_edit(self, ctx, **kwargs):
        """
            callback to actually change launch templates from edit screen
        """

        experimenter = ctx.get_experimenter()
        data = {}
        template = None
        message = None
        ae_launch_id = None
        data["exp_selections"] = (
            ctx.db.query(Experiment).filter(~Experiment.experiment.in_(["root", "public"])).order_by(Experiment.experiment)
        )
        action = kwargs.pop("action", None)
        logit.log("login_setup_edit: action is: %s , args %s" % (action, repr(kwargs)))
        pcl_call = int(kwargs.pop("pcl_call", 0))
        pc_username = kwargs.pop("pc_username", None)
        if isinstance(pc_username, str):
            pc_username = pc_username.strip()

        ae_launch_name = kwargs.get("ae_launch_name", "")
        ae_launch_name = ae_launch_name.strip()
        ls = (
            ctx.db.query(LoginSetup)
            .filter(LoginSetup.experiment == ctx.experiment)
            .filter(LoginSetup.name == ae_launch_name)
            .first()
        )

        if ls and not ctx.experiment == ls.experiment:
            raise PermissionError("You are not acting as the right experiment")

        if action == "delete":
            self.poms_service.permissions.can_modify(ctx, "LoginSetup", name=username, experiment=ctx.experiment)
            name = ae_launch_name
            try:
                ctx.db.query(LoginSetup).filter(
                    LoginSetup.experiment == experiment,
                    LoginSetup.name == name,
                    LoginSetup.creator == experimenter.experimenter_id,
                ).delete(synchronize_session=False)
                ctx.db.commit()
                message = "The login setup '%s' has been deleted." % name
            except SQLAlchemyError as exc:
                ctx.db.rollback()
                message = "The login setup '%s' has been used and may not be deleted." % name
                logit.log(message)
                logit.log(" ".join(exc.args))
            finally:
                return {"message": message}

        elif action in ("add", "edit"):
            logit.log("login_setup_edit: add,edit case")
            if pcl_call == 1:
                if isinstance(ae_launch_name, str):
                    ae_launch_name = ae_launch_name.strip()
                name = ae_launch_name
                experimenter = ctx.get_experimenter()
                if experimenter:
                    experimenter_id = experimenter.experimenter_id
                else:
                    experimenter_id = 0

                if action == "edit":
                    ae_launch_id = (
                        ctx.db.query(LoginSetup)
                        .filter(LoginSetup.experiment == experiment)
                        .filter(LoginSetup.name == name)
                        .first()
                    ).login_setup_id
                ae_launch_host = kwargs.pop("ae_launch_host", None)
                ae_launch_account = kwargs.pop("ae_launch_account", None)
                ae_launch_setup = kwargs.pop("ae_launch_setup", None)
            else:
                if isinstance(ae_launch_name, str):
                    ae_launch_name = ae_launch_name.strip()
                ae_launch_id = kwargs.pop("ae_launch_id")
                experimenter_id = kwargs.pop("experimenter_id")
                ae_launch_host = kwargs.pop("ae_launch_host")
                ae_launch_account = kwargs.pop("ae_launch_account")
                ae_launch_setup = kwargs.pop("ae_launch_setup")

            try:
                if action == "add":
                    logit.log("adding new LoginSetup...")
                    role = ctx.role
                    if role in ("root", "superctx.username"):
                        raise cherrypy.HTTPError(status=401, message="You are not authorized to add launch template.")
                    exists = (
                        ctx.db.query(LoginSetup)
                        .filter(LoginSetup.experiment == ctx.experiment)
                        .filter(LoginSetup.name == ae_launch_name)
                    ).first()
                    if exists:
                        message = "A login setup named %s already exists." % ae_launch_name
                    else:
                        template = LoginSetup(
                            experiment=ctx.experiment,
                            name=ae_launch_name,
                            launch_host=ae_launch_host,
                            launch_account=ae_launch_account,
                            launch_setup=ae_launch_setup,
                            creator=experimenter_id,
                            created=datetime.now(utc),
                            creator_role=role,
                        )
                        ctx.db.add(template)
                        ctx.db.commit()
                        data["login_setup_id"] = template.login_setup_id
                else:
                    logit.log("editing existing LoginSetup...")
                    columns = {
                        "name": ae_launch_name,
                        "launch_host": ae_launch_host,
                        "launch_account": ae_launch_account,
                        "launch_setup": ae_launch_setup,
                        "updated": datetime.now(utc),
                        "updater": experimenter_id,
                    }
                    template = ctx.db.query(LoginSetup).filter(LoginSetup.login_setup_id == ae_launch_id).update(columns)
                    ctx.db.commit()
                    data["login_setup_id"] = ae_launch_id

            except IntegrityError as exc:
                message = "Integrity error: " "you are most likely using a name which " "already exists in database."
                logit.log(" ".join(exc.args))
                ctx.db.rollback()
                raise
            except SQLAlchemyError as exc:
                message = "SQLAlchemyError: " "Please report this to the administrator. " "Message: %s" % " ".join(exc.args)
                logit.log(" ".join(exc.args))
                ctx.db.rollback()
                raise
            except BaseException:
                message = "unexpected error ! \n" + traceback.format_exc(4)
                logit.log(" ".join(message))
                ctx.db.rollback()
                raise

        # Find templates
        if ctx.experiment:  # cuz the default is find
            if kwargs.get("update_view", None) is None:
                # view flags not specified, use defaults
                data["view_active"] = "view_active"
                data["view_inactive"] = None
                data["view_mine"] = experimenter.experimenter_id
                data["view_others"] = experimenter.experimenter_id
                data["view_analysis"] = "view_analysis" if ctx.role in ("analysis", "superctx.username") else None
                data["view_production"] = "view_production" if ctx.role in ("production", "superctx.username") else None
            else:
                data["view_active"] = kwargs.get("view_active", None)
                data["view_inactive"] = kwargs.get("view_inactive", None)
                data["view_mine"] = kwargs.get("view_mine", None)
                data["view_others"] = kwargs.get("view_others", None)
                data["view_analysis"] = kwargs.get("view_analysis", None)
                data["view_production"] = kwargs.get("view_production", None)
            data["curr_experiment"] = ctx.experiment
            data["authorized"] = []

            q = (
                ctx.db.query(LoginSetup, Experiment)
                .join(Experiment)
                .filter(LoginSetup.experiment == ctx.experiment)
                .order_by(LoginSetup.name)
            )

            if data["view_analysis"] and data["view_production"]:
                pass
            elif data["view_analysis"]:
                q = q.filter(LoginSetup.creator_role == "analysis")
            elif data["view_production"]:
                q = q.filter(LoginSetup.creator_role == "production")

            if data["view_mine"] and data["view_others"]:
                pass
            elif data["view_mine"]:
                q = q.filter(LoginSetup.creator == data["view_mine"])
            elif data["view_others"]:
                q = q.filter(LoginSetup.creator != data["view_others"])

            if data["view_active"] and data["view_inactive"]:
                pass
            elif data["view_active"]:
                q = q.filter(LoginSetup.active.is_(True))
            elif data["view_inactive"]:
                q = q.filter(LoginSetup.active.is_(False))

            data["templates"] = q.all()

            for l_t in data["templates"]:
                if ctx.role in ("root", "superctx.username"):
                    data["authorized"].append(True)
                elif ctx.role == "production":
                    data["authorized"].append(True)
                elif ctx.role == "analysis" and l_t.LoginSetup.creator == experimenter.experimenter_id:
                    data["authorized"].append(True)
                else:
                    data["authorized"].append(False)
        data["message"] = message
        return data

    # h3. job_type_edit
    def job_type_edit(self, ctx, **kwargs):
        """
            callback from edit screen/client.
        """
        data = {}
        message = None
        data["exp_selections"] = (
            ctx.db.query(Experiment).filter(~Experiment.experiment.in_(["root", "public"])).order_by(Experiment.experiment)
        )
        action = kwargs.pop("action", None)
        experimenter = ctx.db.query(Experimenter).filter(Experimenter.username == ctx.username).scalar()
        data["curr_experiment"] = ctx.experiment
        pcl_call = int(kwargs.pop("pcl_call", 0))
        # email is the info we know about the ctx.username in POMS DB.
        pc_username = kwargs.pop("pc_username", None)

        if action == "delete":
            name = kwargs.pop("ae_definition_name")
            self.poms_service.permissions.can_modify(ctx, "JobType", name=name, experiment=ctx.experiment)
            if isinstance(name, str):
                name = name.strip()
            if pcl_call == 1:  # Enter here if the access was from the poms_client
                cid = (
                    ctx.db.query(JobType)
                    .filter(
                        JobType.experiment == ctx.experiment,
                        JobType.name == name,
                        JobType.creator == experimenter.experimenter_id,
                    )
                    .scalar()
                    .job_type_id
                )
            else:
                cid = kwargs.pop("job_type_id")
            try:
                (ctx.db.query(CampaignRecovery).filter(CampaignRecovery.job_type_id == cid).delete(synchronize_session=False))
                (ctx.db.query(JobType).filter(JobType.job_type_id == cid).delete(synchronize_session=False))
                ctx.db.commit()
                message = "The job type '%s' has been deleted." % name
            except SQLAlchemyError as exc:
                ctx.db.rollback()
                message = ("The job type, %s, " "has been used and may not be deleted.") % name  # Was: campaign definition
                logit.log(message)
                logit.log(" ".join(exc.args))
            finally:
                return {"message": message}

        elif action in ("add", "edit"):
            logit.log("job_type_edit: add or exit case")
            job_type_id = None
            definition_parameters = kwargs.pop("ae_definition_parameters")
            if definition_parameters:
                definition_parameters = json.loads(definition_parameters)
            if pcl_call == 1:  # Enter here if the access was from the poms_client
                name = kwargs.pop("ae_definition_name")
                if isinstance(name, str):
                    name = name.strip()

                experimenter = ctx.get_experimenter()
                if experimenter:
                    experimenter_id = experimenter.experimenter_id
                else:
                    experimenter_id = 0

                if action == "edit":
                    job_type_id = ctx.db.query(JobType).filter(JobType.name == name).one().job_type_id  # Check here!
                else:
                    pass
                input_files_per_job = kwargs.pop("ae_input_files_per_job", 0)
                output_files_per_job = kwargs.pop("ae_output_files_per_job", 0)
                output_file_patterns = kwargs.pop("ae_output_file_patterns")
                launch_script = kwargs.pop("ae_launch_script")
                recoveries = kwargs.pop("ae_definition_recovery", "[]")

                # Getting the info that was not passed by the poms_client
                # arguments

                if output_file_patterns in (None, ""):
                    output_file_patterns = "%"

                if launch_script in (None, ""):
                    raise AssertionError("launch_script is required")

                if definition_parameters in (None, ""):
                    definition_parameters = []
            else:
                experimenter_id = kwargs.pop("experimenter_id")
                job_type_id = kwargs.pop("ae_campaign_definition_id")
                name = kwargs.pop("ae_definition_name")
                if isinstance(name, str):
                    name = name.strip()
                input_files_per_job = kwargs.pop("ae_input_files_per_job", 0)
                output_files_per_job = kwargs.pop("ae_output_files_per_job", 0)
                output_file_patterns = kwargs.pop("ae_output_file_patterns")
                launch_script = kwargs.pop("ae_launch_script")
                recoveries = kwargs.pop("ae_definition_recovery")
            try:
                if action == "add":
                    role = ctx.role
                    if role in ("root", "superctx.username"):
                        raise cherrypy.HTTPError(status=401, message=("You are not authorized " "to add campaign definition."))
                    else:
                        j_t = JobType(
                            name=name,
                            experiment=ctx.experiment,
                            input_files_per_job=input_files_per_job,
                            output_files_per_job=output_files_per_job,
                            output_file_patterns=output_file_patterns,
                            launch_script=launch_script,
                            definition_parameters=definition_parameters,
                            creator=experimenter_id,
                            created=datetime.now(utc),
                            creator_role=role,
                        )

                    ctx.db.add(j_t)
                    ctx.db.flush()
                    job_type_id = j_t.job_type_id
                else:
                    columns = {
                        "name": name,
                        "input_files_per_job": input_files_per_job,
                        "output_files_per_job": output_files_per_job,
                        "output_file_patterns": output_file_patterns,
                        "launch_script": launch_script,
                        "definition_parameters": definition_parameters,
                        "updated": datetime.now(utc),
                        "updater": experimenter_id,
                    }
                    j_t = ctx.db.query(JobType).filter(JobType.job_type_id == job_type_id).update(columns)

                if recoveries:
                    self.fixup_recoveries(ctx, job_type_id, recoveries)
                    ctx.db.commit()

            except IntegrityError as exc:
                message = "Integrity error: " "you are most likely using a name which " "already exists in database."
                logit.log(" ".join(exc.args))
                ctx.db.rollback()
            except SQLAlchemyError as exc:
                message = "SQLAlchemyError: " "Please report this to the administrator. " "Message: %s" % " ".join(exc.args)
                logit.log(" ".join(exc.args))
                ctx.db.rollback()
            else:
                ctx.db.commit()

        # Find definitions
        if ctx.experiment:  # cuz the default is find

            if kwargs.get("update_view", None) is None:
                # view flags not specified, use defaults
                data["view_active"] = "view_active"
                data["view_inactive"] = None
                data["view_mine"] = experimenter.experimenter_id
                data["view_others"] = experimenter.experimenter_id
                data["view_analysis"] = "view_analysis" if ctx.role in ("analysis", "superctx.username") else None
                data["view_production"] = "view_production" if ctx.role in ("production", "superctx.username") else None
            else:
                data["view_active"] = kwargs.get("view_active", None)
                data["view_inactive"] = kwargs.get("view_inactive", None)
                data["view_mine"] = kwargs.get("view_mine", None)
                data["view_others"] = kwargs.get("view_others", None)
                data["view_analysis"] = kwargs.get("view_analysis", None)
                data["view_production"] = kwargs.get("view_production", None)

            data["authorized"] = []
            # for testing ui...
            # data['authorized'] = True
            q = (
                ctx.db.query(JobType, Experiment)
                .join(Experiment)
                .filter(JobType.experiment == ctx.experiment)
                .order_by(JobType.name)
            )

            if data["view_analysis"] and data["view_production"]:
                pass
            elif data["view_analysis"]:
                q = q.filter(JobType.creator_role == "analysis")
            elif data["view_production"]:
                q = q.filter(JobType.creator_role == "production")

            if data["view_mine"] and data["view_others"]:
                pass
            elif data["view_mine"]:
                q = q.filter(JobType.creator == data["view_mine"])
            elif data["view_others"]:
                q = q.filter(JobType.creator != data["view_others"])

            # JobTypes don't have an active field(yet?)
            if data["view_active"] and data["view_inactive"]:
                pass
            elif data["view_active"]:
                q = q.filter(JobType.active.is_(True))
            elif data["view_inactive"]:
                q = q.filter(JobType.active.is_(False))

            data["definitions"] = q.all()
            cids = []
            for df in data["definitions"]:
                cids.append(df.JobType.job_type_id)
                if ctx.role in ["root", "superctx.username"]:
                    data["authorized"].append(True)
                elif df.JobType.creator_role == "production" and ctx.role == "production":
                    data["authorized"].append(True)
                elif df.JobType.creator_role == ctx.role and df.JobType.creator == experimenter.experimenter_id:
                    data["authorized"].append(True)
                else:
                    data["authorized"].append(False)

            recs_dict = {}
            for cid in cids:
                recs_dict[cid] = json.dumps(self.get_recoveries(ctx, cid))
            data["recoveries"] = recs_dict
            data["rtypes"] = ctx.db.query(RecoveryType.name, RecoveryType.description).order_by(RecoveryType.name).all()

        data["message"] = message
        return data

    # h3. get_jobtype_id
    def get_jobtype_id(self, ctx, name):
        """
           lookup job type id for name in current experiment
        """
        # experimenter = ctx.db.query(Experimenter).filter(Experimenter.username == ctx.username).scalar()
        # user_id = experimenter.experimenter_id

        return ctx.db.query(JobType.job_type_id).filter(JobType.experiment == ctx.experiment, JobType.name == name).scalar()

    # h3. get_loginsetup_id
    def get_loginsetup_id(self, ctx, name):
        """
           lookup login setup id by name for current experiment
        """

        return (
            ctx.db.query(LoginSetup.login_setup_id)
            .filter(LoginSetup.experiment == ctx.experiment, LoginSetup.name == name)
            .scalar()
        )

    # h3. split_type_javascript
    def split_type_javascript(self, ctx):
        class fake_campaign_stage:
            # h3. __init__
            def __init__(self, dataset="", cs_split_type=""):
                self.dataset = dataset
                self.cs_split_type = cs_split_type

        modmap = {}
        docmap = {}
        parammap = {}
        rlist = []

        modmap["None"] = None
        docmap["None"] = "No splitting is done"
        parammap["None"] = []

        # make the import set POMS_DIR..
        importlib.import_module("poms.webservice")

        gpath = "%s/webservice/split_types/*.py" % os.environ["POMS_DIR"]
        rlist.append("/* checking: %s */ " % gpath)

        split_list = glob.glob(gpath)

        modnames = [os.path.basename(x).replace(".py", "") for x in split_list]

        fake_cs = fake_campaign_stage(dataset="null", cs_split_type="")

        for modname in modnames:

            if modname == "__init__":
                continue

            fake_cs.cs_split_type = "%s(2)" % modname

            mod = importlib.import_module("poms.webservice.split_types." + modname)
            split_class = getattr(mod, modname)
            inst = split_class(fake_cs, ctx.sam, ctx.db)
            poptxt = inst.edit_popup()

            if poptxt != "null":
                modmap[modname] = "%s_edit_popup" % modname
                rlist.append(poptxt)
            else:
                modmap[modname] = None

            description = split_class.__doc__
            docmap[modname] = description
            parammap[modname] = inst.params()

        rlist.append("split_type_edit_map =")
        rlist.append(
            json.dumps(modmap)
            .replace('",', '",\n')
            .replace("null,", "null,\n")
            .replace('": "', '": ')
            .replace('",', ",")
            .replace('"}', "}")
        )
        rlist.append(";")
        rlist.append("split_type_doc_map =")
        rlist.append(json.dumps(docmap).replace('",', '",\n'))
        rlist.append(";")
        rlist.append("split_type_param_map =")
        rlist.append(json.dumps(parammap).replace(",", ",\n"))
        rlist.append(";")
        return "\n".join(rlist)

    # h3. get_recovery_list_for_campaign_def
    def get_recovery_list_for_campaign_def(self, ctx, campaign_def):
        """
            return the recovery list for a given campaign_def
        """
        rlist = (
            ctx.db.query(CampaignRecovery)
            .options(joinedload(CampaignRecovery.recovery_type))
            .filter(CampaignRecovery.job_type_id == campaign_def.job_type_id)
            .order_by(CampaignRecovery.recovery_order)
        )

        # convert to a real list...
        l = deque()
        for r in rlist:
            l.append(r)
        rlist = l

        return rlist

    # h3. snapshot_parts
    def snapshot_parts(self, ctx, s, campaign_stage_id):

        cs = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).one()
        for table, snaptable, field, sfield, sid, tfield in [
            [
                CampaignStage,
                CampaignStageSnapshot,
                CampaignStage.campaign_stage_id,
                CampaignStageSnapshot.campaign_stage_id,
                cs.campaign_stage_id,
                "campaign_stage_snapshot_obj",
            ],
            [JobType, JobTypeSnapshot, JobType.job_type_id, JobTypeSnapshot.job_type_id, cs.job_type_id, "job_type_snapshot_obj"],
            [
                LoginSetup,
                LoginSetupSnapshot,
                LoginSetup.login_setup_id,
                LoginSetupSnapshot.login_setup_id,
                cs.login_setup_id,
                "login_setup_snapshot_obj",
            ],
        ]:

            i = ctx.db.query(func.max(snaptable.updated)).filter(sfield == sid).first()
            j = ctx.db.query(table).filter(field == sid).first()
            if i[0] is None or j is None or j.updated is None or i[0] < j.updated:
                newsnap = snaptable()
                columns = j._sa_instance_state.class_.__table__.columns
                for fieldname in list(columns.keys()):
                    setattr(newsnap, fieldname, getattr(j, fieldname))
                ctx.db.add(newsnap)
            else:
                newsnap = ctx.db.query(snaptable).filter(snaptable.updated == i[0]).first()
            setattr(s, tfield, newsnap)
        ctx.db.add(s)
        ctx.db.commit()

    # h3. get_recoveries
    def get_recoveries(self, ctx, cid):
        """
        Build the recoveries dict for job_types cids
        """
        recs = (
            ctx.db.query(CampaignRecovery)
            .filter(CampaignRecovery.job_type_id == cid)
            .order_by(CampaignRecovery.job_type_id, CampaignRecovery.recovery_order)
            .all()
        )

        logit.log("get_recoveries(%d) got %d items" % (cid, len(recs)))
        rec_list = []
        for rec in recs:
            logit.log("get_recoveries(%d) -- rec %s" % (cid, repr(rec)))
            if isinstance(rec.param_overrides, str):
                logit.log("get_recoveries(%d) -- saw string param_overrides" % cid)
                if rec.param_overrides in ("", "{}", "[]"):
                    rec.param_overrides = []
                rec_vals = [rec.recovery_type.name, json.loads(rec.param_overrides)]
            else:
                rec_vals = [rec.recovery_type.name, rec.param_overrides]

            rec_list.append(rec_vals)

        logit.log("get_recoveries(%d) returning %s" % (cid, repr(rec_list)))
        return rec_list

    # h3. fixup_recoveries
    def fixup_recoveries(self, ctx, job_type_id, recoveries):
        """
         fixup_recoveries -- factored out so we can use it
            from either edit endpoint.
         Given a JSON dump of the recoveries, clean out old
         recoveriy entries, add new ones.  It probably should
         check if they're actually different before doing this..
        """
        (ctx.db.query(CampaignRecovery).filter(CampaignRecovery.job_type_id == job_type_id).delete(synchronize_session=False))  #
        i = 0
        for rtn in json.loads(recoveries):
            rect = rtn[0]
            recpar = rtn[1]
            rt = ctx.db.query(RecoveryType).filter(RecoveryType.name == rect).first()
            cr = CampaignRecovery(job_type_id=job_type_id, recovery_order=i, recovery_type=rt, param_overrides=recpar)
            i = i + 1
            ctx.db.add(cr)

    # h3. held_launches
    def held_launches(self, ctx):
        
        if ctx.role == 'analysis':
            # analysis users see their own held jobs
            eid = ctx.get_experimenter().experimenter_id
            hjl = ctx.db.query(HeldLaunch).filter(HeldLaunch.launcher == eid).all()
        if ctx.role == 'production':
            # production users see their experiment production
            hjl = ctx.db.query(HeldLaunch).join(CampaignStage,HeldLaunch.campaign_stage_id == CampaignStage.campaign_stage_id).filter(CampaignStage.experiment == ctx.experiment,CampaignStage.creator_role == ctx.role).all()

        if ctx.role == 'superuser':
            # superusers see all their experiment's jobs
            hjl = ctx.db.query(HeldLaunch).join(CampaignStage,HeldLaunch.campaign_stage_id == CampaignStage.campaign_stage_id).filter(CampaignStage.experiment == ctx.experiment).all()

        return {"hjl": hjl}

    # h3. held_launches_remove
    def held_launches_remove(self, ctx, createds, delete):
        eid = ctx.get_experimenter().experimenter_id
        if isinstance(createds, str):
            createds = [createds]
        ctx.db.query(HeldLaunch).filter(HeldLaunch.launcher == eid, HeldLaunch.created.in_(createds)).delete(synchronize_session=False)
        ctx.db.commit()
