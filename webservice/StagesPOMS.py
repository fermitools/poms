#!/usr/bin/env python
"""
This module contain the methods that allow to create campaign_stages, definitions and templates.
List of methods:
campaign_stage_edit, campaign_stage_edit_query.
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in
poms_service.py written by Marc Mengel, Michael Gueith and Stephen White.
Date: April 28th, 2017. (changes for the POMS_client)
"""
import configparser
import ast
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
from sqlalchemy import and_, distinct, desc, func, or_, text, Integer
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import joinedload, attributes, aliased

from . import logit
from .poms_model import (
    Campaign,
    CampaignStage,
    CampaignsTag,
    JobType,
    CampaignDependency,
    CampaignRecovery,
    CampaignStageSnapshot,
    Experiment,
    Experimenter,
    LoginSetup,
    RecoveryType,
    Submission,
    SubmissionHistory,
    SubmissionStatus,
)
from .utc import utc
from .SAMSpecifics import sam_specifics


class StagesPOMS:
    """
       Business logic for CampaignStage related items
    """

    # h3. __init__
    def __init__(self, ps):
        """
            initialize ourself with a reference back to the overall poms_service
        """
        self.poms_service = ps

    # h3. get_campaign_stage_id
    def get_campaign_stage_id(self, ctx, campaign_name, campaign_stage_name):
        """
            return the campaign stage id for a stage name in our experiment/campaign
        """
        stage_id = (
            ctx.db.query(CampaignStage.campaign_stage_id)
            .select_from(CampaignStage)
            .join(CampaignStage.campaign_obj)
            .filter(
                CampaignStage.name == campaign_stage_name,
                CampaignStage.campaign_obj.has(Campaign.name == campaign_name),
                CampaignStage.experiment == ctx.experiment,
            )
            .scalar()
        )
        return stage_id if stage_id else None

    # h3. get_campaign_stage_name
    def get_campaign_stage_name(self, ctx, campaign_stage_id):
        """
            return the campaign stage name for a stage id in our experiment
        """
        names = (
            ctx.db.query(Campaign.name, CampaignStage.name)
            .select_from(CampaignStage)
            .join(CampaignStage.campaign_obj)
            .filter(CampaignStage.campaign_stage_id == campaign_stage_id, CampaignStage.experiment == ctx.experiment)
            .first()
        )
        return names if names else None

    # h3. campaign_stage_edit
    def campaign_stage_edit(self, ctx, **kwargs):
        """
            callback for campaign stage edit screens to update campaign record
            takes action = 'edit'/'add'/ etc.
            sesshandle is the cherrypy.session instead of cherrypy.session.get method
        """
        c_s = None
        data = {}
        experimenter = ctx.db.query(Experimenter).filter(Experimenter.username == ctx.username).scalar()
        experiment = ctx.experiment

        message = None
        data["exp_selections"] = (
            ctx.db.query(Experiment).filter(~Experiment.experiment.in_(["root", "public"])).order_by(Experiment.experiment)
        )
        # for k,v in kwargs.items():
        #    print ' k=%s, v=%s ' %(k,v)
        action = kwargs.pop("action", None)
        # pcl_call == 1 means the method was access through the poms_client.
        pcl_call = int(kwargs.pop("pcl_call", 0))
        # email is the info we know about the ctx.username in POMS DB.
        pc_username = kwargs.pop("pc_username", None)
        campaign_id = kwargs.pop("ae_campaign_id", None)

        if action == "delete":
            name = kwargs.get("ae_stage_name", kwargs.get("name", None))
            self.poms_service.permissions.can_modify(
                ctx, "CampaignStage", name=name, campaign_id=campaign_id, experiment=experiment
            )
            if isinstance(name, str):
                name = name.strip()
            if pcl_call == 1:
                campaign_stage_id = (
                    ctx.db.query(CampaignStage)
                    .filter(
                        CampaignStage.name == name,
                        CampaignStage.experiment == experiment,
                        CampaignStage.campaign_id == campaign_id,
                    )
                    .one()
                    .campaign_stage_id
                    if name
                    else None
                )
            else:
                campaign_stage_id = kwargs.pop("campaign_stage_id")

            if campaign_stage_id:
                try:
                    (
                        ctx.db.query(CampaignDependency)
                        .filter(
                            or_(
                                CampaignDependency.needs_campaign_stage_id == campaign_stage_id,
                                CampaignDependency.provides_campaign_stage_id == campaign_stage_id,
                            )
                        )
                        .delete(synchronize_session=False)
                    )
                    (
                        ctx.db.query(CampaignStage)
                        .filter(CampaignStage.campaign_stage_id == campaign_stage_id)
                        .delete(synchronize_session=False)
                    )
                    ctx.db.commit()
                except SQLAlchemyError as e:
                    message = "The campaign stage {}, has been used and may not be deleted.".format(name)
                    logit.log(message)
                    logit.log(" ".join(e.args))
                    ctx.db.rollback()
                    raise

        elif action in ("add", "edit"):
            logit.log("campaign_stage_edit: add or edit case")
            name = kwargs.pop("ae_stage_name")
            if isinstance(name, str):
                name = name.strip()
            # active = (kwargs.pop('ae_active') in ('True', 'true', '1', 'Active', True, 1))
            split_type = kwargs.pop("ae_split_type", None)
            default_clear_cronjob = (kwargs.pop('ae_cronjob') in ('True', 'true', '1', 'Active', True, 1))
            vo_role = kwargs.pop("ae_vo_role")
            software_version = kwargs.pop("ae_software_version")
            dataset = kwargs.pop("ae_dataset")
            campaign_type = kwargs.pop("ae_campaign_type", "test")

            completion_type = kwargs.pop("ae_completion_type")
            completion_pct = kwargs.pop("ae_completion_pct")
            depends = kwargs.pop("ae_depends", "[]")

            param_overrides = kwargs.pop("ae_param_overrides", "[]")
            if param_overrides:
                param_overrides = json.loads(param_overrides)

            test_param_overrides = kwargs.pop("ae_test_param_overrides", "[]")
            if test_param_overrides:
                test_param_overrides = json.loads(test_param_overrides)

            if pcl_call == 1:
                launch_name = kwargs.pop("ae_launch_name")
                if isinstance(launch_name, str):
                    launch_name = launch_name.strip()
                campaign_definition_name = kwargs.pop("ae_campaign_definition")
                if isinstance(campaign_definition_name, str):
                    campaign_definition_name = campaign_definition_name.strip()
                # all this variables depend on the arguments passed.
                experimenter = ctx.get_experimenter()

                if experimenter:
                    experimenter_id = experimenter.experimenter_id
                else:
                    experimenter_id = 0

                # print("************* exp={}, launch_name={}, campaign_definition_name={}"
                #        .format(exp, launch_name, campaign_definition_name))
                login_setup_id = (
                    ctx.db.query(LoginSetup)
                    .filter(LoginSetup.experiment == experiment)
                    .filter(LoginSetup.name == launch_name)
                    .one()
                    .login_setup_id
                )
                job_type_id = (
                    ctx.db.query(JobType)
                    .filter(JobType.name == campaign_definition_name, JobType.experiment == experiment)
                    .one()
                    .job_type_id
                )
                if action == "edit":
                    c_s = (
                        ctx.db.query(CampaignStage)
                        .filter(
                            CampaignStage.name == name,
                            CampaignStage.experiment == experiment,
                            CampaignStage.campaign_id == campaign_id,
                        )
                        .one()
                    )
                    if c_s:
                        campaign_stage_id = c_s.campaign_stage_id
                    else:
                        campaign_stage_id = None
                else:
                    pass
            else:
                if "ae_campaign_stage_id" in kwargs:
                    campaign_stage_id = kwargs.pop("ae_campaign_stage_id")
                job_type_id = kwargs.pop("ae_campaign_definition_id")
                login_setup_id = kwargs.pop("ae_launch_id")
                experimenter_id = kwargs.pop("experimenter_id")

            if depends and depends != "[]":
                depends = json.loads(depends)
            else:
                depends = {"campaign_stages": [], "file_patterns": []}

            # backwards combatability
            if "campaigns" in depends and "campaign_stages" not in depends:
                depends["campaign_stages"] = depends["campaigns"]

            # fail if they're setting up a trivial infinite loop
            if split_type in [None, "None", "none", "Draining"] and name in [x[0] for x in depends["campaign_stages"]]:

                raise cherrypy.HTTPError(
                    404,
                    "This edit would make an infinite loop. "
                    "Go Back in your browser and set cs_split_type or remove self-dependency.",
                )

            try:
                if action == "add":
                    if not completion_pct:
                        completion_pct = 95
                    if ctx.role not in ("analysis", "production"):
                        message = "Your active role must be analysis " "or production to add a campaign."
                    else:
                        c_s = CampaignStage(
                            name=name,
                            experiment=experiment,
                            vo_role=vo_role,
                            default_clear_cronjob=default_clear_cronjob,
                            # active=active,
                            cs_split_type=split_type,
                            software_version=software_version,
                            dataset=dataset,
                            test_param_overrides=test_param_overrides,
                            param_overrides=param_overrides,
                            login_setup_id=login_setup_id,
                            job_type_id=job_type_id,
                            completion_type=completion_type,
                            completion_pct=completion_pct,
                            creator=experimenter_id,
                            created=datetime.now(utc),
                            creator_role=ctx.role,
                            campaign_stage_type=campaign_type,
                            campaign_id=campaign_id,
                        )
                        ctx.db.add(c_s)
                        ctx.db.commit()
                        campaign_stage_id = c_s.campaign_stage_id
                elif action == "edit":
                    columns = {
                        "name": name,
                        "vo_role": vo_role,
                        # "active": active,
                        "cs_split_type": split_type,
                        "default_clear_cronjob":default_clear_cronjob,
                        "software_version": software_version,
                        "dataset": dataset,
                        "param_overrides": param_overrides,
                        "test_param_overrides": test_param_overrides,
                        "job_type_id": job_type_id,
                        "login_setup_id": login_setup_id,
                        "updated": datetime.now(utc),
                        "updater": experimenter_id,
                        "completion_type": completion_type,
                        "completion_pct": completion_pct,
                        "campaign_id": campaign_id,
                    }

                    if c_s and split_type != c_s.split_type:
                        # clear last split if changing split type
                        columns["cs_last_split"] = None
                    if c_s and dataset != c_s.dataset:
                        # clear last split if changing dataset
                        columns["cs_last_split"] = None

                    ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).update(columns)
                # now redo dependencies
                (
                    ctx.db.query(CampaignDependency)
                    .filter(CampaignDependency.provides_campaign_stage_id == campaign_stage_id)
                    .delete(synchronize_session=False)
                )
                logit.log("depends for %s(%s) are: %s" % (campaign_stage_id, name, depends))
                if "campaign_stages" in depends:
                    dep_stages = (
                        ctx.db.query(CampaignStage)
                        .filter(
                            CampaignStage.name.in_(depends["campaign_stages"]),
                            CampaignStage.campaign_id == campaign_id,
                            CampaignStage.experiment == experiment,
                        )
                        .all()
                    )
                elif "campaigns" in depends:
                    # backwards compatibility
                    dep_stages = (
                        ctx.db.query(CampaignStage)
                        .filter(
                            CampaignStage.name.in_(depends["campaigns"]),
                            CampaignStage.campaign_id == campaign_id,
                            CampaignStage.experiment == experiment,
                        )
                        .all()
                    )
                else:
                    dep_stages = {}
                for (i, stage) in enumerate(dep_stages):
                    logit.log("trying to add dependency for: {}".format(stage.name))
                    dep = CampaignDependency(
                        provides_campaign_stage_id=campaign_stage_id,
                        needs_campaign_stage_id=stage.campaign_stage_id,
                        file_patterns=depends["file_patterns"][i],
                    )
                    ctx.db.add(dep)
                ctx.db.commit()
            except IntegrityError as exc:
                message = "Integrity error: " "You are most likely using a name " "which already exists in database."
                logit.log(" ".join(exc.args))
                ctx.db.rollback()
            except SQLAlchemyError as exc:
                message = "SQLAlchemyError: " "Please report this to the administrator." "Message: {}".format(" ".join(exc.args))
                logit.log(" ".join(exc.args))
                ctx.db.rollback()
            else:
                ctx.db.commit()

        # Find campaign_stages
        if experiment:  # cuz the default is find
            if kwargs.get("update_view", None) is None:
                # view flags not specified, use defaults
                data["view_active"] = "view_active"
                data["view_inactive"] = None
                data["view_mine"] = experimenter.experimenter_id
                data["view_others"] = experimenter.experimenter_id
                data["view_analysis"] = "view_analysis" if ctx.role in ("analysis", "superuser") else None
                data["view_production"] = (
                    "view_production" if ctx.role in ("production", "production-shifter", "superuser") else None
                )
            else:
                data["view_active"] = kwargs.get("view_active", None)
                data["view_inactive"] = kwargs.get("view_inactive", None)
                data["view_mine"] = kwargs.get("view_mine", None)
                data["view_others"] = kwargs.get("view_others", None)
                data["view_analysis"] = kwargs.get("view_analysis", None)
                data["view_production"] = kwargs.get("view_production", None)
            # for testing ui...
            # data['authorized'] = True
            state = kwargs.pop("state", None)
            jumpto = kwargs.get("jump_to_campaign", None)
            data["state"] = state
            data["curr_experiment"] = experiment
            data["authorized"] = []
            cquery = ctx.db.query(CampaignStage, Campaign).outerjoin(Campaign).filter(CampaignStage.experiment == experiment)
            if data["view_analysis"] and data["view_production"]:
                pass
            elif data["view_analysis"]:
                cquery = cquery.filter(CampaignStage.creator_role == "analysis")
            elif data["view_production"]:
                cquery = cquery.filter(CampaignStage.creator_role == "production")

            if data["view_mine"] and data["view_others"]:
                pass
            elif data["view_mine"]:
                cquery = cquery.filter(CampaignStage.creator == data["view_mine"])
            elif data["view_others"]:
                cquery = cquery.filter(CampaignStage.creator != data["view_others"])

            cquery = cquery.order_by(Campaign.name, CampaignStage.name)
            # this bit has to go onto cquery last
            # -- make sure if we're jumping to a given campaign id
            # that we *have* it in the list...
            if jumpto is not None:
                c2 = (
                    ctx.db.query(CampaignStage, Campaign)
                    .filter(CampaignStage.campaign_stage_id == jumpto)
                    .filter(CampaignStage.campaign_id == Campaign.campaign_id)
                )
                # we have to use union_all() and not union()to avoid
                # postgres whining about not knowing how to compare JSON
                # fields... sigh.  (It could just string compare them...)
                cquery = c2.union_all(cquery)

            data["campaign_stages"] = cquery.all()
            data["definitions"] = ctx.db.query(JobType).filter(JobType.experiment == experiment).order_by(JobType.name)
            data["templates"] = ctx.db.query(LoginSetup).filter(LoginSetup.experiment == experiment).order_by(LoginSetup.name)
            csq = data["campaign_stages"]

            for c_s in csq:
                if self.poms_service.permissions.is_superuser(ctx):
                    data["authorized"].append(True)
                elif c_s.CampaignStage.creator_role == "production" and ctx.role == "production":
                    data["authorized"].append(True)
                elif c_s.CampaignStage.creator_role == ctx.role and c_s.CampaignStage.creator == experimenter.experimenter_id:
                    data["authorized"].append(True)
                else:
                    data["authorized"].append(False)

            depends = {}
            for c in csq:
                cid = c.CampaignStage.campaign_stage_id
                sql = ctx.db.query(
                    CampaignDependency.provides_campaign_stage_id, CampaignStage.name, CampaignDependency.file_patterns
                ).filter(
                    CampaignDependency.provides_campaign_stage_id == cid,
                    CampaignStage.campaign_stage_id == CampaignDependency.needs_campaign_stage_id,
                )
                deps = {"campaign_stages": [row[1] for row in sql.all()], "file_patterns": [row[2] for row in sql.all()]}
                depends[cid] = json.dumps(deps)
            data["depends"] = depends

        data["message"] = message
        return data

    # h3. update_campaign_stage
    def update_campaign_stage(self, ctx, campaign_stage, **kwargs):
        print("****** reached update_campaign_stage")
        print(f"****** campaign_stage: '{campaign_stage}', type: {type(campaign_stage)}")
        stage = None
        if isinstance(campaign_stage, list):
            campaign_name, stage_name = campaign_stage
            stage = (
                ctx.db.query(CampaignStage)
                .filter(
                    CampaignStage.name == stage_name,
                    CampaignStage.campaign_obj.has(Campaign.name == campaign_name),
                    CampaignStage.experiment == ctx.experiment,
                )
                .scalar()
            )
        else:
            try:
                campaign_stage = int(campaign_stage)
                stage = ctx.db.query(CampaignStage).get(campaign_stage)
            except Exception:
                print("*** Oops, unrecognized arg type!")
        if not stage:
            return None

        cols = [c for c in CampaignStage.__table__.columns.keys() if "creat" not in c and "update" not in c and "_id" not in c]
        updated = False
        for name in kwargs:
            if name in cols:
                print(f"k={name!r}, v={kwargs[name]}, t={type(kwargs[name])}")  # DEBUG
                # change the attribute
                if str(CampaignStage.__table__.columns[name].type) == "JSON":
                    setattr(stage, name, ast.literal_eval(kwargs[name]))
                else:
                    setattr(stage, name, kwargs[name])
                updated = True
        if updated:
            setattr(stage, "updated", datetime.now(utc))
            setattr(stage, "updater", ctx.get_experimenter().experimenter_id)
            ctx.db.commit()  # Update DB
        return "Success"

    # h3. campaign_stage_edit_query
    def campaign_stage_edit_query(self, ctx, **kwargs):
        """
            return data for a specific stage
        """

        data = {}
        ae_launch_id = kwargs.pop("ae_launch_id", None)
        ae_campaign_definition_id = kwargs.pop("ae_campaign_definition_id", None)

        if ae_launch_id:
            template = {}
            temp = ctx.db.query(LoginSetup).filter(LoginSetup.login_setup_id == ae_launch_id).one()
            template["launch_host"] = temp.launch_host
            template["launch_account"] = temp.launch_account
            template["launch_setup"] = temp.launch_setup
            data["template"] = template

        if ae_campaign_definition_id:
            definition = {}
            cdef = ctx.db.query(JobType).filter(JobType.job_type_id == ae_campaign_definition_id).one()
            definition["input_files_per_job"] = cdef.input_files_per_job
            definition["output_files_per_job"] = cdef.output_files_per_job
            definition["launch_script"] = cdef.launch_script
            definition["definition_parameters"] = cdef.definition_parameters
            data["definition"] = definition
        return json.dumps(data)

    # h3. update_stage_param_overrides
    def update_stage_param_overrides(self, ctx, campaign_stage, param_overrides=None, test_param_overrides=None):
        """
        """
        print("****** reached update_stage_param_overrides")
        print(f"****** campaign_stage: '{campaign_stage}', type: {type(campaign_stage)}")
        print(f"****** param_overrides: '{param_overrides}'")
        stage = None
        if isinstance(campaign_stage, list):
            campaign_name, stage_name = campaign_stage
            stage = (
                ctx.db.query(CampaignStage)
                .filter(
                    CampaignStage.name == stage_name,
                    CampaignStage.campaign_obj.has(Campaign.name == campaign_name),
                    CampaignStage.experiment == ctx.experiment,
                )
                .scalar()
            )
        else:
            try:
                campaign_stage = int(campaign_stage)
                stage = ctx.db.query(CampaignStage).get(campaign_stage)
            except Exception:
                print("*** Oops, unrecognized arg type!")
        if not stage:
            return None

        if param_overrides:
            # Process param_overrides
            param_overrides = OrderedDict(ast.literal_eval(param_overrides))
            po = stage.param_overrides
            for p in po:
                print(f"------ p: {p}")
            opo = OrderedDict(po)
            for k in param_overrides:  # For all new k/v pairs
                if param_overrides[k]:
                    opo[k] = param_overrides[k]  # Update or add new value
                else:
                    opo.pop(k, None)  # Remove k/v if new v is empty
            stage.param_overrides = list(opo.items())  # Update the record
            ctx.db.commit()  # Update DB
            # return str(stage.param_overrides)
        if test_param_overrides:
            # Process test_param_overrides
            test_param_overrides = OrderedDict(ast.literal_eval(test_param_overrides))
            po = stage.test_param_overrides
            for p in po:
                print(f"------ p: {p}")
            opo = OrderedDict(po)
            for k in test_param_overrides:  # For all new k/v pairs
                if test_param_overrides[k]:
                    opo[k] = test_param_overrides[k]  # Update or add new value
                else:
                    opo.pop(k, None)  # Remove k/v if new v is empty
            stage.test_param_overrides = list(opo.items())  # Update the record
            ctx.db.commit()  # Update DB
        return str(
            (stage.param_overrides if param_overrides else None, stage.test_param_overrides if test_param_overrides else None)
        )

    # h3. show_campaign_stages
    def show_campaign_stages(self, ctx, campaign_ids=None, campaign_name=None, **kwargs):
        """
            give campaign information about campaign_stages with activity
            in the time window for a given experiment
        :rtype: object
        """
        base_link = "show_campaign_stages/%s/%s?" % (ctx.experiment, ctx.role)
        (tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays) = self.poms_service.utilsPOMS.handle_dates(
            ctx, base_link
        )
        experimenter = ctx.get_experimenter()

        csq = (
            ctx.db.query(CampaignStage)
            .options(joinedload("experiment_obj"))
            .options(joinedload("campaign_obj"))
            .options(joinedload(CampaignStage.experimenter_holder_obj))
            .options(joinedload(CampaignStage.experimenter_creator_obj))
            .options(joinedload(CampaignStage.experimenter_updater_obj))
            .order_by(CampaignStage.experiment)
        )

        if ctx.experiment:
            csq = csq.filter(CampaignStage.experiment == ctx.experiment)

        data = {}
        if kwargs.get("update_view", None) is None:
            # view flags not specified, use defaults
            data["view_active"] = "view_active"
            data["view_inactive"] = None
            data["view_mine"] = experimenter.experimenter_id
            data["view_others"] = experimenter.experimenter_id
            data["view_analysis"] = "view_analysis" if ctx.role in ("analysis", "superuser") else None
            data["view_production"] = "view_production" if ctx.role in ("production", "production-shifter", "superuser") else None
        else:
            data["view_active"] = kwargs.get("view_active", None)
            data["view_inactive"] = kwargs.get("view_inactive", None)
            data["view_mine"] = kwargs.get("view_mine", None)
            data["view_others"] = kwargs.get("view_others", None)
            data["view_analysis"] = kwargs.get("view_analysis", None)
            data["view_production"] = kwargs.get("view_production", None)

        if data["view_analysis"] and data["view_production"]:
            pass
        elif data["view_analysis"]:
            csq = csq.filter(CampaignStage.creator_role == "analysis")
        elif data["view_production"]:
            csq = csq.filter(CampaignStage.creator_role == "production")

        if data["view_mine"] and data["view_others"]:
            pass
        elif data["view_mine"]:
            csq = csq.filter(CampaignStage.creator == experimenter.experimenter_id)
        elif data["view_others"]:
            csq = csq.filter(CampaignStage.creator != experimenter.experimenter_id)

        if data["view_active"] and data["view_inactive"]:
            pass
        elif data["view_active"]:
            csq = csq.filter(CampaignStage.campaign_obj.has(Campaign.active.is_(True)))
        elif data["view_inactive"]:
            csq = csq.filter(CampaignStage.campaign_obj.has(Campaign.active.is_(False)))

        if campaign_ids:
            campaign_ids = campaign_ids.split(",")
            csq = csq.filter(CampaignStage.campaign_stage_id.in_(campaign_ids))

        if campaign_name:
            csq = csq.join(Campaign).filter(Campaign.name == campaign_name)

            # for now we comment out it. When we have a lot of data,
            # we may need to use these filters.
            # We will let the client filter it in show_campaign_stages.html
            # with tablesorter for now.
            # if holder:
            # csq = csq.filter(Campaingn.hold_experimenters_id == holder)

            # if creator_role:
            # csq = csq.filter(Campaingn.creator_role == creator_role)
        campaign_stages = csq.all()
        logit.log(logit.DEBUG, "show_campaign_stages: back from query")
        # check for authorization
        data["authorized"] = []
        for c_s in campaign_stages:
            if ctx.role != "analysis":
                data["authorized"].append(True)
            elif c_s.creator == experimenter.experimenter_id:
                data["authorized"].append(True)
            else:
                data["authorized"].append(False)
        return campaign_stages, tmin, tmax, tmins, tmaxs, tdays, nextlink, prevlink, time_range_string, data

    # h3. reset_campaign_split
    def reset_campaign_split(self, ctx, campaign_stage_id):
        """
            reset a campaign_stages cs_last_split field so the sequence
            starts over
        """
        campaign_stage_id = int(campaign_stage_id)

        c_s = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).one()
        c_s.cs_last_split = None
        ctx.db.commit()

    # h3. update_campaign_split
    def update_campaign_split(self, ctx, campaign_stage_id, campaign_stage_snapshot_id, last_split_custom_val):
        """
            reset a campaign_stages cs_last_split field so the sequence
            starts over
        """
        campaign_stage_id = int(campaign_stage_id)
        id_to_use = None
        # ask for forgiveness rather than permission for a bad value
        try:
            s = int(last_split_custom_val) + 1
            id_to_use = int(last_split_custom_val)
        except Exception:
            if campaign_stage_snapshot_id:
                id_to_use = int(campaign_stage_snapshot_id)
            else:
                logit.log(logit.ERROR, "update_campaign_split: improper custom value entered")
                raise TypeError("The value entered is not formatted properly, please enter an integer value corresponding to a campaign stage id.")

        try:
            c_s = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).one()
            if id_to_use == 0:
                c_s.cs_last_split  = None
                ctx.db.commit()
                return
        except Exception:
            logit.log(logit.ERROR, "update_campaign_split: campaign stage not found")
            raise AssertionError("Campaign stage not found")
        try:
            c_s_s = ctx.db.query(CampaignStageSnapshot).filter(CampaignStageSnapshot.campaign_stage_snapshot_id == id_to_use).one()
        except Exception:
            logit.log(logit.ERROR, "update_campaign_split: campaign stage snapshot %s not found" %id_to_use)
            raise AssertionError("Campaign stage snapshot %s not found" %id_to_use)
            
        if c_s.campaign_id != c_s_s.campaign_stage.campaign_id:
            logit.log(logit.ERROR, "update_campaign_split: requested campaign stage snapshot does not belong to this campaign")
            raise AssertionError("Requested campaign stage snapshot does not belong to this campaign")
        if c_s.cs_split_type != c_s_s.cs_split_type:
            logit.log(logit.ERROR, "update_campaign_split: requested campaign stage snapshot does not match the split type of this campaign stage")
            raise AssertionError("Requested campaign stage snapshot does not match the split type of this campaign stage")
        
        c_s.cs_last_split = id_to_use
        ctx.db.commit()

    # h3. campaign_stage_info
    def campaign_stage_info(self, ctx, campaign_stage_id):
        """
           Give information related to a campaign stage for the campaign_stage_info page
        """

        campaign_stage_id = int(campaign_stage_id)

        campaign_stage_info = (
            ctx.db.query(CampaignStage, Experimenter)
            .filter(CampaignStage.campaign_stage_id == campaign_stage_id, CampaignStage.creator == Experimenter.experimenter_id)
            .first()
        )
        # get existing campaign stage snapshots
        campaign_stage_snapshots_db = (
            ctx.db.query(CampaignStageSnapshot)
            .filter(CampaignStageSnapshot.campaign_stage_id == campaign_stage_id, CampaignStageSnapshot.cs_split_type == campaign_stage_info.CampaignStage.cs_split_type)
            .order_by(desc(CampaignStageSnapshot.campaign_stage_snapshot_id))
            .all()
        )
        campaign_stage_snapshots = []
        if campaign_stage_snapshots_db:
                for item in campaign_stage_snapshots_db:
                        if item.updated:
                            campaign_stage_snapshots.append([item.campaign_stage_snapshot_id, item.cs_split_type, (item.updated).strftime("%Y-%m-%d %H:%M:%S"), item.cs_last_split])
                        else:
                            campaign_stage_snapshots.append([item.campaign_stage_snapshot_id, item.cs_split_type, (item.created).strftime("%Y-%m-%d %H:%M:%S"), item.cs_last_split])
        # default to time window of campaign
        if ctx.tmin is None and ctx.tdays is None:
            ctx.tmin = campaign_stage_info.CampaignStage.created
            ctx.tmax = datetime.now(utc)

        tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays = self.poms_service.utilsPOMS.handle_dates(
            ctx, "campaign_stage_info/%s/%s?" % (ctx.experiment, ctx.role)
        )

        last_activity_l = (
            ctx.db.query(func.max(Submission.updated)).filter(Submission.campaign_stage_id == campaign_stage_id).first()
        )
        logit.log("got last_activity_l %s" % repr(last_activity_l))
        if last_activity_l[0] and datetime.now(utc) - last_activity_l[0] > timedelta(days=7):
            last_activity = last_activity_l[0].strftime("%Y-%m-%d %H:%M:%S")
        else:
            last_activity = ""
        logit.log("after: last_activity %s" % repr(last_activity))

        campaign_definition_info = (
            ctx.db.query(JobType, Experimenter)
            .filter(
                JobType.job_type_id == campaign_stage_info.CampaignStage.job_type_id,
                JobType.creator == Experimenter.experimenter_id,
            )
            .first()
        )
        login_setup_info = (
            ctx.db.query(LoginSetup, Experimenter)
            .filter(
                LoginSetup.login_setup_id == campaign_stage_info.CampaignStage.login_setup_id,
                LoginSetup.creator == Experimenter.experimenter_id,
            )
            .first()
        )
        campaigns = ctx.db.query(Campaign).join(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).all()

        launched_campaigns = (
            ctx.db.query(CampaignStageSnapshot).filter(CampaignStageSnapshot.campaign_stage_id == campaign_stage_id).all()
        )

        #
        # cloned from show_campaign_stages, but for a one row table..
        #
        campaign_stage = campaign_stage_info[0]
        counts = {}
        counts_keys = {}
        dirname = "{}/private/logs/poms/launches/campaign_{}".format(os.environ["HOME"], campaign_stage_id)
        launch_flist = glob.glob("{}/*".format(dirname))

        if len(launch_flist) > 500:
            launch_flist = launch_flist[:500]

        launch_flist = list(map(os.path.basename, launch_flist))

        recent_submission_list = (
            ctx.db.query(Submission.submission_id, func.max(SubmissionHistory.status_id))
            .filter(SubmissionHistory.submission_id == Submission.submission_id)
            .filter(Submission.campaign_stage_id == campaign_stage_id)
            .filter(Submission.created > datetime.now(utc) - timedelta(days=7))
            .group_by(Submission.submission_id)
            .all()
        )

        recent_submissions = {}
        for (sid, status) in recent_submission_list:
            recent_submissions[sid] = status

        # put our campaign_stage id in the link
        campaign_kibana_link_format = ctx.config_get("campaign_kibana_link_format")
        logit.log("got format {}".format(campaign_kibana_link_format))
        kibana_link = campaign_kibana_link_format.format(campaign_stage_id)

        dep_svg = self.poms_service.campaignsPOMS.campaign_deps_svg(ctx, campaign_stage_id=campaign_stage_id)

        return (
            campaign_stage_info,
            time_range_string,
            tmins,
            tmaxs,
            tdays,
            campaign_definition_info,
            login_setup_info,
            campaigns,
            launched_campaigns,
            None,
            campaign_stage,
            counts_keys,
            counts,
            launch_flist,
            kibana_link,
            dep_svg,
            last_activity,
            recent_submissions,
            campaign_stage_snapshots
        )

    # h3. campaign_stage_submissions
    def campaign_stage_submissions(self, ctx, campaign_name="", stage_name="", campaign_stage_id=None, campaign_id=None):
        """
           Show submissions from a campaign stage
        """
        data = {"tmin": ctx.tmin, "tmax": ctx.tmax, "tdays": ctx.tdays}
        if campaign_name and campaign_id in (None, "None", ""):
            campaign_id = (
                ctx.db.query(Campaign.campaign_id)
                .filter(Campaign.name == campaign_name, Campaign.experiment == ctx.experiment)
                .scalar()
            )
            if not campaign_id:
                data["submissions"] = []
                return data

        if stage_name not in (None, "None", "*", "") and campaign_stage_id in (None, "None", ""):
            campaign_stage_id = (
                ctx.db.query(CampaignStage.campaign_stage_id)
                .filter(
                    CampaignStage.name == stage_name,
                    CampaignStage.experiment == ctx.experiment,
                    CampaignStage.campaign_id == campaign_id,
                )
                .scalar()
            )
            if not campaign_stage_id:
                data["submissions"] = []
                return data

        if campaign_id in (None, "None", "") and campaign_stage_id in (None, "None", ""):
            raise AssertionError("campaign_stage_submissions needs either campaign_id or campaign_stage_id not None")

        if campaign_id not in (None, "None", ""):
            campaign_stage_rows = (
                ctx.db.query(CampaignStage.campaign_stage_id).filter(CampaignStage.campaign_id == campaign_id).all()
            )
            campaign_stage_ids = [row[0] for row in campaign_stage_rows]

        if campaign_stage_id not in (None, "None", ""):
            campaign_stage_ids = [campaign_stage_id]

        # if we're not given any time info, do from start of campaign stage
        if not (ctx.tmin or ctx.tmax or ctx.tdays):
            logit.log("=== no time info, picking...")
            crows = ctx.db.query(CampaignStage.created).filter(CampaignStage.campaign_stage_id.in_(campaign_stage_ids)).all()
            if crows:
                ctx.tmin = crows[0][0].strftime("%Y-%m-%d %H:%M:%S")
                ctx.tmax = datetime.now(utc).strftime("%Y-%m-%d %H:%M:%S")
                logit.log("picking campaign date range %s .. %s" % (ctx.tmin, ctx.tmax))

        base_link = "campaign_stage_submissions/{}/{}?campaign_name={}&stage_name={}&campaign_stage_id={}&campaign_id={}&".format(
            ctx.experiment, ctx.role, campaign_name, stage_name, campaign_stage_id, campaign_id
        )
        (tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays) = self.poms_service.utilsPOMS.handle_dates(
            ctx, base_link
        )
        data = {
            "tmin": tmin,
            "tmax": tmax,
            "nextlink": nextlink,
            "prevlink": prevlink,
            "tdays": tdays,
            "tminsec": tmin.strftime("%s"),
        }

        subhist = aliased(SubmissionHistory)
        subq = ctx.db.query(func.max(subhist.created)).filter(SubmissionHistory.submission_id == subhist.submission_id)

        config = configparser.ConfigParser()
        config.read("../webservice/poms.ini")
        launch_commands = config.get("launch_commands", "projre").split(",")
        
        try:
            subs = ctx.db.query(Submission).filter(Submission.campaign_stage_id.in_(campaign_stage_ids)).all()
            for sub in subs:
                if not sub.project:
                    logit.log("no project: %s" % sub.submission_id)
                    project = None
                    for projre in launch_commands:
                        logit.log("command executed: %s" % sub.command_executed)
                        m = re.search(projre, sub.command_executed)
                        if m:
                            project = m.group(1)
                            logit.log("project name: %s" % project)
                            break
                    if project:
                        sub.project = project
                        ctx.db.add(sub)
                        ctx.db.flush()
        except Exception as e:
            logit.log("Error fetching/saving project name: %s" % repr(e))
        tuples = (
            ctx.db.query(Submission, SubmissionHistory, SubmissionStatus)
            .join("experimenter_creator_obj")
            .filter(
                Submission.campaign_stage_id.in_(campaign_stage_ids),
                SubmissionHistory.submission_id == Submission.submission_id,
                SubmissionStatus.status_id == SubmissionHistory.status_id,
                or_(
                    and_(Submission.created > tmin, Submission.created < tmax),
                    and_(Submission.updated > tmin, Submission.updated < tmax),
                ),
            )
            .filter(SubmissionHistory.created == subq)
            .order_by(SubmissionHistory.submission_id)
        ).all()

        # figure dependency depth
        depends = {}
        depth = {}
        darrow = {}
        sids = []
        slist = []

        for tup in tuples:
            sid = tup.Submission.submission_id
            pd = tup.Submission.submission_params.get("dataset", "")
            sids.append(sid)
            slist.append(tup.Submission)
            m = re.match(r"poms_(depends|recover)_([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12})_(.*)_[0-9]", pd)
            long = True
            if not m:
                m = re.match(r"poms_(depends|recover)_(.*)_[0-9]", pd)
                long = False
            if m:
                depends[sid] = int(m.group(3)) if long else int(m.group(2))
                darrow[sid] = "&#x21b3;" if m.group(1) == "depends" else "&#x21ba;"
            else:
                depends[sid] = None
                darrow[sid] = ""
            depth[sid] = 0
        sids.sort()
        for sid in sids:
            if depends[sid] and depends[sid] in depth:
                depth[sid] = depth[depends[sid]] + 1
        data["depends"] = depends
        data["depth"] = depth
        data["darrow"] = darrow

        (
            summary_list,
            some_kids_decl_needed,
            some_kids_needed,
            base_dim_list,
            output_files,
            output_list,
            all_kids_decl_needed,
            some_kids_list,
            some_kids_decl_list,
            all_kids_decl_list,
        ) = sam_specifics(ctx).get_file_stats_for_submissions(slist, ctx.experiment, just_output=True)

        submissions = []
        i = 0
        for tup in tuples:
            jjid = tup.Submission.jobsub_job_id
            full_jjid = jjid
            if not jjid:
                jjid = "s" + str(tup.Submission.submission_id)
                full_jjid = "unknown.0@unknown.un.known"
            else:
                jjid = "s%s<br>%s" % (
                    str(tup.Submission.submission_id),
                    str(jjid).replace("fifebatch", "").replace(".fnal.gov", ""),
                )

            row = {
                "submission_id": tup.Submission.submission_id,
                "jobsub_job_id": tup.Submission.jobsub_job_id,
                "created": tup.Submission.created,
                "creator": tup.Submission.experimenter_creator_obj.username,
                "status": tup.SubmissionStatus.status,
                "jobsub_cluster": full_jjid[: full_jjid.find("@")],
                "jobsub_schedd": full_jjid[full_jjid.find("@") + 1 :],
                "campaign_stage_name": tup.Submission.campaign_stage_obj.name,
                "available_output": output_list[i],
                "output_dims": output_files[i],
            }
            submissions.append(row)
            data["submissions"] = submissions
            i = i + 1
        return data

    # h3. get_dataset_for
    def get_dataset_for(self, ctx, camp, test_launch):
        """
            use the split_type modules to get the next dataset for
            launch for a given campaign
        """
        if test_launch and camp.test_split_type:
            split_type = camp.test_split_type
        else:
            split_type = camp.cs_split_type

        if not split_type or split_type == "None" or split_type == "none":
            return camp.dataset

        # clean up split_type -- de-white-space it
        split_type = split_type.replace(" ", "")
        split_type = split_type.replace("\n", "")

        #
        # the module name is the first part of the string, i.e.
        # fred_by_whatever(xxx) -> 'fred'
        # new_localtime -> 'new'
        #
        p1 = split_type.find("(")
        p2 = split_type.find("_")
        if p1 < p2 and p1 > 0:
            pass
        elif p2 < p1 and p2 > 0:
            p1 = p2

        if p1 < 0:
            p1 = len(split_type)

        modname = split_type[0:p1]

        mod = importlib.import_module("poms.webservice.split_types." + modname)
        split_class = getattr(mod, modname)

        splitter = split_class(camp, ctx.sam, ctx.db)

        try:
            res = splitter.next()
        except StopIteration:
            if camp.default_clear_cronjob:
                self.update_launch_schedule(ctx, camp.campaign_stage_id, delete="y")
            raise AssertionError("No more splits in this campaign.")

        ctx.db.commit()
        return res

    # h3. schedule_launch
    def schedule_launch(self, ctx, campaign_stage_id):
        """
            return crontab info for cron launches for campaign
        """
        c_s = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
        my_crontab = CronTab(user=True)
        citer = my_crontab.find_comment("POMS_CAMPAIGN_ID={}".format(campaign_stage_id))
        # there should be only zero or one...
        job = None
        for job in citer:
            break

        # any launch outputs to look at?
        #
        dirname = "{}/private/logs/poms/launches/campaign_{}".format(os.environ["HOME"], campaign_stage_id)
        launch_flist = glob.glob("{}/*".format(dirname))

        if len(launch_flist) > 500:
            launch_flist = launch_flist[:500]

        launch_flist = list(map(os.path.basename, launch_flist))
        return c_s, job, launch_flist

    # h3. mark_campaign_hold
    def mark_campaign_hold(self, ctx, campaign_stage_ids, is_hold, clear_cron=True):
        session_experimenter = ctx.get_experimenter()
        for cs in ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id.in_(campaign_stage_ids)).all():
            if is_hold in ("Hold", "Queue"):
                cs.hold_experimenter_id = session_experimenter.experimenter_id
                cs.role_held_with = ctx.role
                if clear_cron:
                    for csi in campaign_stage_ids:
                        self.update_launch_schedule(ctx, csi, delete=True)
            elif is_hold == "Release":
                cs.hold_experimenter_id = None
                cs.role_held_with = None
            else:
                raise ctx.HTTPError(400, "The action is not supported. You can only Hold/Queue or Release.")
            ctx.db.add(cs)
        ctx.db.commit()

    # h3. update_launch_schedule
    def update_launch_schedule(
        self, ctx, campaign_stage_id, dowlist="", domlist="", monthly="", month="", hourlist="", submit="", minlist="", delete=""
    ):
        """
            callback for changing the launch schedule
        """

        experimenter = ctx.get_experimenter()

        # deal with single item list silliness
        if isinstance(minlist, str):
            minlist = minlist.split(",")
        if isinstance(hourlist, str):
            hourlist = hourlist.split(",")
        if isinstance(dowlist, str):
            dowlist = dowlist.split(",")
        if isinstance(domlist, str):
            domlist = domlist.split(",")

        logit.log("hourlist is {} ".format(hourlist))

        minevery = None
        if minlist and minlist[0] == "*":
            minlist = None
        elif minlist[0][:2] == "*/":
            minevery = int(minlist[0][2:])
            minlist = None
        else:
            minlist = [int(x) for x in minlist if x]

        hourevery = None
        if hourlist and hourlist[0] == "*":
            hourlist = None
        elif hourlist[0][:2] == "*/":
            hourevery = int(hourlist[0][2:])
            hourlist = None
        else:
            hourlist = [int(x) for x in hourlist if x]

        dowevery = None
        if dowlist and dowlist[0] == "*":
            dowlist = None
        elif dowlist[0][:2] == "*/":
            dowevery = int(dowlist[0][2:])
            dowlist = None
        else:
            # dowlist[0] = [int(x) for x in dowlist if x ]
            pass

        domevery = None
        if domlist and domlist[0] == "*":
            domlist = None
        elif domlist[0][:2] == "*/":
            domevery = int(domlist[0][2:])
            domlist = None
        else:
            domlist = [int(x) for x in domlist if x]

        my_crontab = CronTab(user=True)
        # clean out old
        my_crontab.remove_all(comment="POMS_CAMPAIGN_ID={}".format(campaign_stage_id))

        if not delete:

            # make job for new -- use current link for product
            pdir = os.environ.get("POMS_DIR", "/etc/poms")
            if pdir.find("/current/") <= 0:
                # try to find a current symlink path that points here
                tpdir = pdir[: pdir.rfind("poms", 0, len(pdir) - 1) + 4] + "/current"
                if os.path.exists(tpdir):
                    pdir = tpdir

            job = my_crontab.new(
                command="{}/cron/launcher --campaign_stage_id={} --launcher={}".format(
                    pdir, campaign_stage_id, experimenter.experimenter_id
                ),
                comment="POMS_CAMPAIGN_ID={}".format(campaign_stage_id),
            )

            # set timing...
            if dowevery:
                job.dow.every(dowevery)
            if dowlist:
                job.dow.on(*dowlist)

            if minevery:
                job.minute.every(minevery)
            if minlist:
                job.minute.on(*minlist)

            if hourevery:
                job.hour.every(hourevery)
            if hourlist:
                job.hour.on(*hourlist)

            if domevery:
                job.day.every(domevery)
            if domlist:
                job.day.on(*domlist)

            job.enable()

        my_crontab.write()
