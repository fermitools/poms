#!/usr/bin/env python

"""
This module contain the methods that allow to create campaign_stages, definitions and templates.
List of methods:
login_setup_edit, campaign_definition_edit, campaign_stage_edit, campaign_stage_edit_query.
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

import cherrypy
from crontab import CronTab
from sqlalchemy import and_, distinct, func, or_
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import joinedload, attributes

from . import logit, time_grid
from .poms_model import (Campaign,
                         CampaignStage,
                         JobType,
                         CampaignDependency,
                         CampaignRecovery,
                         CampaignStageSnapshot,
                         Experiment,
                         Experimenter,
                         LoginSetup,
                         RecoveryType,
                         Campaign,
                         Submission,
                         SubmissionHistory)
from .pomscache import pomscache_10
from .utc import utc


class CampaignsPOMS:
    """
       Business logic for CampaignStage related items
    """

    def __init__(self, ps):
        """
            initialize ourself with a reference back to the overall poms_service
        """
        self.poms_service = ps

    def login_setup_edit(self, dbhandle, seshandle, *args, **kwargs):
        """
            callback to actually change launch templates from edit screen
        """
        data = {}
        template = None
        message = None
        ae_launch_id = None
        data['exp_selections'] = (dbhandle.query(Experiment)
                                  .filter(~Experiment.experiment.in_(['root', 'public']))
                                  .order_by(Experiment.experiment))
        action = kwargs.pop('action', None)
        exp = seshandle('experimenter').session_experiment
        experimenter = seshandle('experimenter')
        se_role = experimenter.session_role
        pcl_call = int(kwargs.pop('pcl_call', 0))
        pc_username = kwargs.pop('pc_username', None)
        if isinstance(pc_username, str):
            pc_username = pc_username.strip()

        if action == 'delete':
            ae_launch_name = kwargs.pop('ae_launch_name')
            name = ae_launch_name
            try:
                dbhandle.query(LoginSetup).filter(LoginSetup.experiment == exp).filter(
                    LoginSetup.name == name).delete(synchronize_session=False)
                dbhandle.commit()
            except SQLAlchemyError as e:
                message = "The launch template, %s, has been used and may not be deleted." % name
                logit.log(message)
                logit.log(' '.join(e.args))
                dbhandle.rollback()

        if action == 'add' or action == 'edit':
            if pcl_call == 1:
                ae_launch_name = kwargs.pop('ae_launch_name')
                if isinstance(ae_launch_name, str):
                    ae_launch_name = ae_launch_name.strip()
                name = ae_launch_name
                experimenter = dbhandle.query(Experimenter).filter(
                    Experimenter.username == pc_username).first()
                if experimenter:
                    experimenter_id = experimenter.experimenter_id
                else:
                    experimenter_id = 0

                if action == 'edit':
                    ae_launch_id = dbhandle.query(LoginSetup).filter(LoginSetup.experiment == exp).filter(
                        LoginSetup.name == name).first().login_setup_id
                ae_launch_host = kwargs.pop('ae_launch_host', None)
                ae_launch_account = kwargs.pop('ae_launch_account', None)
                ae_launch_setup = kwargs.pop('ae_launch_setup', None)
                if ae_launch_host in [None, ""]:
                    ae_launch_host = dbhandle.query(LoginSetup).filter(LoginSetup.experiment == exp).filter(
                        LoginSetup.name == name).first().launch_host
                if ae_launch_account in [None, ""]:
                    ae_launch_account = dbhandle.query(LoginSetup).filter(LoginSetup.experiment == exp).filter(
                        LoginSetup.name == name).first().launch_account
                if ae_launch_setup in [None, ""]:
                    ae_launch_account = dbhandle.query(LoginSetup).filter(LoginSetup.experiment == exp).filter(
                        LoginSetup.name == name).first().launch_setup
            else:
                ae_launch_name = kwargs.pop('ae_launch_name')
                if isinstance(ae_launch_name, str):
                    ae_launch_name = ae_launch_name.strip()
                ae_launch_id = kwargs.pop('ae_launch_id')
                experimenter_id = kwargs.pop('experimenter_id')
                ae_launch_host = kwargs.pop('ae_launch_host')
                ae_launch_account = kwargs.pop('ae_launch_account')
                ae_launch_setup = kwargs.pop('ae_launch_setup')

            try:
                if action == 'add':
                    role = seshandle('experimenter').session_role
                    if role == 'root' or role == 'coordinator':
                        raise cherrypy.HTTPError(401, 'You are not authorized to add launch template.')
                    else:
                        template = LoginSetup(experiment=exp, name=ae_launch_name, launch_host=ae_launch_host,
                                              launch_account=ae_launch_account,
                                              launch_setup=ae_launch_setup, creator=experimenter_id,
                                              created=datetime.now(utc), creator_role=role)
                    dbhandle.add(template)
                    dbhandle.commit()
                    data['login_setup_id'] = template.login_setup_id
                else:
                    columns = {
                        "name": ae_launch_name,
                        "launch_host": ae_launch_host,
                        "launch_account": ae_launch_account,
                        "launch_setup": ae_launch_setup,
                        "updated": datetime.now(utc),
                        "updater": experimenter_id
                    }
                    template = (dbhandle.query(LoginSetup)
                                .filter(LoginSetup.login_setup_id == ae_launch_id).update(columns))
                    dbhandle.commit()
                    data['login_setup_id'] = ae_launch_id

            except IntegrityError as e:
                message = "Integrity error - you are most likely using a name which already exists in database."
                logit.log(' '.join(e.args))
                dbhandle.rollback()
            except SQLAlchemyError as e:
                message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % ' '.join(e.args)
                logit.log(' '.join(e.args))
                dbhandle.rollback()
            except:
                message = 'unexpected error ! \n' + traceback.format_exc(4)
                logit.log(' '.join(message))
                dbhandle.rollback()

        # Find templates
        if exp:  # cuz the default is find
            if kwargs.get('update_view',None) == None:
                # view flags not specified, use defaults
                data['view_active'] = 'view_active'
                data['view_inactive'] = None
                data['view_mine'] = experimenter.experimenter_id
                data['view_others'] = experimenter.experimenter_id
                data['view_analysis'] = 'view_analysis' if se_role in ('analysis','coordinator') else None
                data['view_production'] =  'view_production' if se_role in ('production','coordinator') else None
            else:
                data['view_active'] = kwargs.get('view_active', None)
                data['view_inactive'] = kwargs.get('view_inactive', None)
                data['view_mine'] = kwargs.get('view_mine', None)
                data['view_others'] = kwargs.get('view_others', None)
                data['view_analysis'] = kwargs.get('view_analysis', None)
                data['view_production'] = kwargs.get('view_production', None)
            data['curr_experiment'] = exp
            data['authorized'] = []
            se_role = seshandle('experimenter').session_role

            q = (dbhandle.query(LoginSetup, Experiment).join(Experiment)
                                 .filter(LoginSetup.experiment == exp)
                                 .order_by(LoginSetup.name))

            if data['view_analysis'] and data['view_production']:
                pass
            elif data['view_analysis']:
                q = q.filter(LoginSetup.creator_role == 'analysis')
            elif data['view_production']:
                q = q.filter(LoginSetup.creator_role == 'production')

            if data['view_mine'] and data['view_others']:
                pass
            elif data['view_mine']:
                q = q.filter(LoginSetup.creator == data['view_mine'] )
            elif data['view_others']:
                q = q.filter(LoginSetup.creator != data['view_others'] )

            # LoginSetups don't have an active field(yet?)
            if data['view_active'] and data['view_inactive']:
                pass
            elif data['view_active']:
                pass
            elif data['view_others']:
                pass

            data['templates'] = q.all()

            for lt in data['templates']:
                if se_role in ('root', 'coordinator'):
                    data['authorized'].append(True)
                elif se_role == 'production':
                    data['authorized'].append(True)
                elif se_role == 'analysis' and lt.LoginSetup.creator == seshandle('experimenter').experimenter_id:
                    data['authorized'].append(True)
                else:
                    data['authorized'].append(False)
        data['message'] = message
        return data

    def get_campaign_id(self, dbhandle, seshandle, campaign_name):
        c = dbhandle.query(Campaign).filter(Campaign.name == campaign_name, Campaign.experiment == seshandle('experimenter').session_experiment).first()
        if not c:
            c = Campaign(name=campaign_name,
                            experiment=seshandle('experimenter').session_experiment,
                            creator=seshandle('experimenter').experimenter_id,
                            creator_role=seshandle('experimenter').session_role)
            dbhandle.add(c)
            dbhandle.commit()

        return c.campaign_id
       
           

    def campaign_add_name(self, dbhandle, seshandle, *args, **kwargs):
        """
            Add a new campaign name.
        """
        data = {}
        name = kwargs.get('campaign_name')
        data['message'] = "ok"
        try:
            experiment = seshandle('experimenter').session_experiment
            camp = Campaign(name=name,
                            experiment=experiment,
                            creator=seshandle('experimenter').experimenter_id,
                            creator_role=seshandle('experimenter').session_role)
            dbhandle.add(camp)
            dbhandle.commit()
            cs = CampaignStage(name="stage0", experiment=experiment,
                               campaign_id=camp.campaign_id,
                               active=False,
                               #
                               completion_pct="95",
                               completion_type="complete",
                               cs_split_type="None",
                               dataset="from_parent",
                               job_type_id=(dbhandle.query(JobType.job_type_id)
                                            .filter(JobType.name == "generic_fife_process", JobType.experiment == experiment)
                                            .scalar()),
                               login_setup_id=(dbhandle.query(LoginSetup.login_setup_id)
                                               .filter(LoginSetup.name == "generic_fife_launch", LoginSetup.experiment == experiment)
                                               .scalar()),
                               param_overrides="[]",
                               software_version="v1_0",
                               test_param_overrides="[]",
                               vo_role="Production",
                               #
                               creator=seshandle('experimenter').experimenter_id,
                               creator_role=seshandle('experimenter').session_role,
                               created=datetime.now(utc),
                               campaign_type="regular")
            dbhandle.add(cs)
        except IntegrityError as e:
            data['message'] = "Integrity error - you are most likely using a name which already exists in database."
            logit.log(' '.join(e.args))
            dbhandle.rollback()
        except SQLAlchemyError as e:
            data['message'] = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % ' '.join(e.args)
            logit.log(' '.join(e.args))
            dbhandle.rollback()
        else:
            dbhandle.commit()
        return json.dumps(data)

    def campaign_rename_name(self, dbhandle, seshandle, *args, **kwargs):
        data = {}
        data['message'] = "ok"
        try:
            campaign_id = kwargs.get('campaign_id')
            new_name = kwargs.get('new_campaign_name')
            new_name = new_name.strip()
            if new_name == "":
                data['message'] = "Please supply a new name."
            else:
                columns = {
                    "name": new_name,
                }
                dbhandle.query(Campaign).filter(Campaign.campaign_id==campaign_id).update(columns)
                dbhandle.commit()
        except SQLAlchemyError as e:
            data['message'] = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % ' '.join(e.args)
            logit.log(' '.join(e.args))
            dbhandle.rollback()
        else:
            dbhandle.commit()
        return json.dumps(data)


    def campaign_list(self, dbhandle):
        """
            Return list of all campaign_stage_id s and names. --
            This is actually for Landscape to use.
        """
        data = dbhandle.query(CampaignStage.campaign_stage_id, CampaignStage.name, CampaignStage.experiment).all()
        return [r._asdict() for r in data]


    def campaign_definition_edit(self, dbhandle, seshandle, *args, **kwargs):
        """
            callback from edit screen/client.
        """
        data = {}
        message = None
        data['exp_selections'] = (dbhandle.query(Experiment)
                                  .filter(~Experiment.experiment.in_(["root", "public"]))
                                  .order_by(Experiment.experiment))
        action = kwargs.pop('action', None)
        experimenter  = seshandle('experimenter')
        exp = seshandle('experimenter').session_experiment
        r = seshandle('experimenter').session_role
        data['curr_experiment'] = exp
        # added for poms_client
        pcl_call = int(kwargs.pop('pcl_call', 0))  # pcl_call == 1 means the method was access through the poms_client.
        pc_username = kwargs.pop('pc_username', None)  # email is the info we know about the user in POMS DB.

        if action == 'delete':
            name = kwargs.pop('ae_definition_name')
            if isinstance(name, str):
                name = name.strip()
            if pcl_call == 1:  # Enter here if the access was from the poms_client
                cid = job_type_id = dbhandle.query(JobType).filter(
                    JobType.name == name).first().job_type_id
            else:
                cid = kwargs.pop('job_type_id')
            try:
                dbhandle.query(CampaignRecovery).filter(CampaignRecovery.job_type_id == cid).delete(synchronize_session=False)
                dbhandle.query(JobType).filter(JobType.job_type_id == cid).delete(synchronize_session=False)
                dbhandle.commit()
            except SQLAlchemyError as e:
                message = 'The campaign definition, %s, has been used and may not be deleted.' % name
                logit.log(message)
                logit.log(' '.join(e.args))
                dbhandle.rollback()

        if action == 'add' or action == 'edit':
            job_type_id = None
            definition_parameters = kwargs.pop('ae_definition_parameters')
            if definition_parameters:
                definition_parameters = json.loads(definition_parameters)
            if pcl_call == 1:  # Enter here if the access was from the poms_client
                name = kwargs.pop('ae_definition_name')
                if isinstance(name, str):
                    name = name.strip()

                experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == pc_username).first()
                if experimenter:
                    experimenter_id = experimenter.experimenter_id
                else:
                    experimenter_id = 0

                if action == 'edit':
                    job_type_id = dbhandle.query(JobType).filter(
                        JobType.name == name).first().job_type_id  # Check here!
                else:
                    pass
                input_files_per_job = kwargs.pop('ae_input_files_per_job', 0)
                output_files_per_job = kwargs.pop('ae_output_files_per_job', 0)
                output_file_patterns = kwargs.pop('ae_output_file_patterns')
                launch_script = kwargs.pop('ae_launch_script')
                recoveries = kwargs.pop('ae_definition_recovery', "[]")
                # Getting the info that was not passed by the poms_client arguments
                if input_files_per_job in (None, ""):
                    input_files_per_job = dbhandle.query(JobType).filter(
                        JobType.job_type_id == job_type_id).first().input_files_per_job
                if output_files_per_job in (None, ""):
                    output_files_per_job = dbhandle.query(JobType).filter(
                        JobType.job_type_id == job_type_id).first().output_files_per_job
                if output_file_patterns in (None, ""):
                    output_file_patterns = dbhandle.query(JobType).filter(
                        JobType.job_type_id == job_type_id).first().output_file_patterns
                if launch_script in (None, ""):
                    launch_script = dbhandle.query(JobType).filter(
                        JobType.job_type_id == job_type_id).first().launch_script
                if definition_parameters in (None, ""):
                    definition_parameters = dbhandle.query(JobType).filter(
                        JobType.job_type_id == job_type_id).first().definition_parameters
            else:
                experimenter_id = kwargs.pop('experimenter_id')
                job_type_id = kwargs.pop('ae_campaign_definition_id')
                name = kwargs.pop('ae_definition_name')
                if isinstance(name, str):
                    name = name.strip()
                input_files_per_job = kwargs.pop('ae_input_files_per_job', 0)
                output_files_per_job = kwargs.pop('ae_output_files_per_job', 0)
                output_file_patterns = kwargs.pop('ae_output_file_patterns')
                launch_script = kwargs.pop('ae_launch_script')
                recoveries = kwargs.pop('ae_definition_recovery')
            try:
                if action == 'add':
                    role = seshandle('experimenter').session_role
                    if role == 'root' or role == 'coordinator':
                        raise cherrypy.HTTPError(401, 'You are not authorized to add campaign definition.')
                    else:
                        cd = JobType(name=name, experiment=exp,
                                     input_files_per_job=input_files_per_job,
                                     output_files_per_job=output_files_per_job,
                                     output_file_patterns=output_file_patterns,
                                     launch_script=launch_script,
                                     definition_parameters=definition_parameters,
                                     creator=experimenter_id, created=datetime.now(utc), creator_role=role)

                    dbhandle.add(cd)
                    dbhandle.flush()
                    job_type_id = cd.job_type_id
                else:
                    columns = {
                        "name": name,
                        "input_files_per_job": input_files_per_job,
                        "output_files_per_job": output_files_per_job,
                        "output_file_patterns": output_file_patterns,
                        "launch_script": launch_script,
                        "definition_parameters": definition_parameters,
                        "updated": datetime.now(utc),
                        "updater": experimenter_id
                    }
                    cd = dbhandle.query(JobType).filter(
                        JobType.job_type_id == job_type_id).update(columns)

                # now fixup recoveries -- clean out existing ones, and
                # add listed ones.
                if pcl_call == 0:
                    dbhandle.query(CampaignRecovery).filter(
                        CampaignRecovery.job_type_id == job_type_id).delete(synchronize_session=False)
                    i = 0
                    for rtn in json.loads(recoveries):
                        rect = rtn[0]
                        recpar = rtn[1]
                        rt = dbhandle.query(RecoveryType).filter(RecoveryType.name == rect).first()
                        cr = CampaignRecovery(job_type_id=job_type_id, recovery_order=i,
                                              recovery_type=rt, param_overrides=recpar)
                        dbhandle.add(cr)
                    dbhandle.commit()
                else:
                    pass  # We need to define later if it is going to be possible to modify the recovery type from the client.

            except IntegrityError as e:
                message = "Integrity error - you are most likely using a name which already exists in database."
                logit.log(' '.join(e.args))
                dbhandle.rollback()
            except SQLAlchemyError as e:
                message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % ' '.join(e.args)
                logit.log(' '.join(e.args))
                dbhandle.rollback()
            else:
                dbhandle.commit()

        # Find definitions
        if exp:  # cuz the default is find

            if kwargs.get('update_view',None) == None:
                # view flags not specified, use defaults
                data['view_active'] = 'view_active'
                data['view_inactive'] = None
                data['view_mine'] = experimenter.experimenter_id
                data['view_others'] = experimenter.experimenter_id
                data['view_analysis'] = 'view_analysis' if r in ('analysis','coordinator') else None
                data['view_production'] =  'view_production' if r in ('production','coordinator') else None
            else:
                data['view_active'] = kwargs.get('view_active', None)
                data['view_inactive'] = kwargs.get('view_inactive', None)
                data['view_mine'] = kwargs.get('view_mine', None)
                data['view_others'] = kwargs.get('view_others', None)
                data['view_analysis'] = kwargs.get('view_analysis', None)
                data['view_production'] = kwargs.get('view_production', None)

            data['authorized'] = []
            # for testing ui...
            # data['authorized'] = True
            q = (dbhandle.query(JobType, Experiment)
                  .join(Experiment)
                  .filter(JobType.experiment == exp)
                  .order_by(JobType.name))

            if data['view_analysis'] and data['view_production']:
                pass
            elif data['view_analysis']:
                q = q.filter(JobType.creator_role == 'analysis')
            elif data['view_production']:
                q = q.filter(JobType.creator_role == 'production')

            if data['view_mine'] and data['view_others']:
                pass
            elif data['view_mine']:
                q = q.filter(JobType.creator == data['view_mine'] )
            elif data['view_others']:
                q = q.filter(JobType.creator != data['view_others'] )

            # JobTypes don't have an active field(yet?)
            if data['view_active'] and data['view_inactive']:
                pass
            elif data['view_active']:
                pass
            elif data['view_others']:
                pass

            data['definitions'] = q.all()
            cids = []
            for df in data['definitions']:
                cids.append(df.JobType.job_type_id)
                if r in ['root', 'coordinator']:
                    data['authorized'].append(True)
                elif df.JobType.creator_role == 'production' and r == "production":
                    data['authorized'].append(True)
                elif df.JobType.creator_role == r and df.JobType.creator == seshandle('experimenter').experimenter_id:
                    data['authorized'].append(True)
                else:
                    data['authorized'].append(False)

            # Build the recoveries for each campaign.
            cids = []
            recs_dict = {}
            for cid in cids:
                recs = (dbhandle.query(CampaignRecovery)
                        .join(JobType).options(joinedload(CampaignRecovery.recovery_type))
                        .filter(CampaignRecovery.job_type_id == cid, JobType.experiment == exp)
                        .order_by(CampaignRecovery.job_type_id, CampaignRecovery.recovery_order))
                rec_list = deque()
                for rec in recs:
                    if isinstance(rec.param_overrides, str):
                        if rec.param_overrides in ('', '{}', '[]'):
                            rec.param_overrides = "[]"
                        rec_vals = [rec.recovery_type.name, json.loads(rec.param_overrides)]
                    else:
                        rec_vals = [rec.recovery_type.name, rec.param_overrides]

                    # rec_vals=[rec.recovery_type.name,rec.param_overrides]
                    rec_list.append(rec_vals)
                recs_dict[cid] = json.dumps(list(rec_list))

            data['recoveries'] = recs_dict
            data['rtypes'] = (dbhandle.query(RecoveryType.name, RecoveryType.description).order_by(RecoveryType.name).all())

        data['message'] = message
        return data

    def make_test_campaign_for(self, dbhandle, sesshandle, campaign_def_id, campaign_def_name):
        """
            Build a test_campaign for a given campaign definition
        """
        cs = dbhandle.query(CampaignStage).filter(CampaignStage.job_type_id == campaign_def_id,
                                                  CampaignStage.name == "_test_%s" % campaign_def_name).first()
        if not cs:
            lt = dbhandle.query(LoginSetup).filter(
                LoginSetup.experiment == sesshandle.get('experimenter').session_experiment).first()
            cs = CampaignStage()
            cs.job_type_id = campaign_def_id
            cs.name = "_test_%s" % campaign_def_name
            cs.experiment = sesshandle.get('experimenter').session_experiment
            cs.creator = sesshandle.get('experimenter').experimenter_id
            cs.created = datetime.now(utc)
            cs.updated = datetime.now(utc)
            cs.vo_role = "Production"
            cs.creator_role = "production"
            cs.dataset = ""
            cs.login_setup_id = lt.login_setup_id
            cs.software_version = ""
            cs.campaign_type = 'regular'
            dbhandle.add(cs)
            dbhandle.commit()
            cs = dbhandle.query(CampaignStage).filter(CampaignStage.job_type_id == campaign_def_id,
                                                      CampaignStage.name == "_test_%s" % campaign_def_name).first()
        return cs.campaign_stage_id


    def campaign_stage_edit(self, dbhandle, sesshandle, *args, **kwargs):
        """
            callback for campaign stage edit screens to update campaign record
            takes action = 'edit'/'add'/ etc.
            sesshandle is the cherrypy.session instead of cherrypy.session.get method
        """
        data = {}
        experimenter = sesshandle.get('experimenter')
        role = sesshandle.get('experimenter').session_role or 'production'
        user_id = sesshandle.get('experimenter').experimenter_id
        message = None
        exp = sesshandle.get('experimenter').session_experiment
        data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root", "public"])).order_by(Experiment.experiment)
        # for k,v in kwargs.items():
        #    print ' k=%s, v=%s ' %(k,v)
        action = kwargs.pop('action', None)
        pcl_call = int(kwargs.pop('pcl_call', 0))  # pcl_call == 1 means the method was access through the poms_client.
        pc_username = kwargs.pop('pc_username', None)  # email is the info we know about the user in POMS DB.

        if action == 'delete':
            name = kwargs.get('ae_stage_name', kwargs.get('name', None))
            if isinstance(name, str):
                name = name.strip()
            if pcl_call == 1:
                campaign_stage_id = dbhandle.query(CampaignStage).filter(CampaignStage.name == name).first().campaign_stage_id if name else None
            else:
                campaign_stage_id = kwargs.pop('campaign_stage_id')
            try:
                unlink = kwargs.pop('unlink', None)
                print("######################### unlink: {}".format(unlink))
                if unlink:
                    unlink = json.loads(unlink)
                if campaign_stage_id:
                    (dbhandle.query(CampaignDependency)
                     .filter(or_(CampaignDependency.needs_campaign_stage_id == campaign_stage_id,
                                 CampaignDependency.provides_campaign_stage_id == campaign_stage_id))
                     .delete(synchronize_session=False))
                if unlink is None:
                    dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).delete(synchronize_session=False)
                else:
                    if isinstance(unlink, (int, str)):
                        cs = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
                        cs.campaign_id = None
                    else:
                        qq = (dbhandle.query(CampaignDependency)
                              .filter(CampaignDependency.provider.has(CampaignStage.name == unlink[0]))
                              .filter(CampaignDependency.consumer.has(CampaignStage.name == unlink[1]))
                              .delete(synchronize_session=False))
                dbhandle.commit()
            # except Exception as e:
            except SQLAlchemyError as e:
                message = "The campaign stage, {}, has been used and may not be deleted.".format(name)
                logit.log(message)
                logit.log(' '.join(e.args))
                dbhandle.rollback()
                raise

        elif action in ('add', 'edit'):
            campaign_id = kwargs.pop('ae_campaign_name')
            name = kwargs.pop('ae_stage_name')
            if isinstance(name, str):
                name = name.strip()
            active = (kwargs.pop('ae_active') in ('True', 'true', '1', 'Active', True, 1))
            split_type = kwargs.pop('ae_split_type', None)
            vo_role = kwargs.pop('ae_vo_role')
            software_version = kwargs.pop('ae_software_version')
            dataset = kwargs.pop('ae_dataset')
            campaign_type = kwargs.pop('ae_campaign_type', 'test')

            completion_type = kwargs.pop('ae_completion_type')
            completion_pct = kwargs.pop('ae_completion_pct')
            depends = kwargs.pop('ae_depends', "[]")
            param_overrides = kwargs.pop('ae_param_overrides', "[]")
            if param_overrides:
                param_overrides = json.loads(param_overrides)

            test_param_overrides = kwargs.pop('ae_test_param_overrides', "[]")
            if test_param_overrides:
                test_param_overrides = json.loads(test_param_overrides)

            if pcl_call == 1:
                launch_name = kwargs.pop('ae_launch_name')
                if isinstance(launch_name, str):
                    launch_name = launch_name.strip()
                campaign_definition_name = kwargs.pop('ae_campaign_definition')
                if isinstance(campaign_definition_name, str):
                    campaign_definition_name = campaign_definition_name.strip()
                # all this variables depend on the arguments passed.
                experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == pc_username).first()

                if experimenter:
                    experimenter_id = experimenter.experimenter_id
                else:
                    experimenter_id = 0

                # print("************* exp={}, launch_name={}, campaign_definition_name={}".format(exp, launch_name, campaign_definition_name))
                login_setup_id = (dbhandle.query(LoginSetup)
                                  .filter(LoginSetup.experiment == exp)
                                  .filter(LoginSetup.name == launch_name).first().login_setup_id)
                job_type_id = dbhandle.query(JobType).filter(
                    JobType.name == campaign_definition_name).first().job_type_id
                if action == 'edit':
                    campaign_stage_id = dbhandle.query(CampaignStage).filter(CampaignStage.name == name , CampaignStage.experiment == exp).first().campaign_stage_id
                else:
                    pass
            else:
                if 'ae_campaign_stage_id' in kwargs:
                    campaign_stage_id = kwargs.pop('ae_campaign_stage_id')
                job_type_id = kwargs.pop('ae_campaign_definition_id')
                login_setup_id = kwargs.pop('ae_launch_id')
                experimenter_id = kwargs.pop('experimenter_id')

            if depends and depends != "[]":
                depends = json.loads(depends)
            else:
                depends = {"campaign_stages": [], "file_patterns": []}
            try:
                if action == 'add':
                    if not completion_pct:
                        completion_pct = 95
                    if role not in ('analysis', 'production'):
                        message = 'Your active role must be analysis or production to add a campaign.'
                    else:
                        cs = CampaignStage(name=name, experiment=exp, vo_role=vo_role,
                                           active=active, cs_split_type=split_type,
                                           software_version=software_version, dataset=dataset,
                                           test_param_overrides=test_param_overrides,
                                           param_overrides=param_overrides, login_setup_id=login_setup_id,
                                           job_type_id=job_type_id,
                                           completion_type=completion_type, completion_pct=completion_pct,
                                           creator=experimenter_id, created=datetime.now(utc),
                                           creator_role=role, campaign_type=campaign_type, campaign_id=campaign_id)
                        dbhandle.add(cs)
                        dbhandle.commit()
                        campaign_stage_id = cs.campaign_stage_id
                elif action == 'edit':
                    columns = {
                        "name": name,
                        "vo_role": vo_role,
                        "active": active,
                        "cs_split_type": split_type,
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
                        "campaign_id": campaign_id
                    }
                    dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).update(columns)
                # now redo dependencies
                dbhandle.query(CampaignDependency).filter(CampaignDependency.provides_campaign_stage_id == campaign_stage_id).delete(synchronize_session=False)
                logit.log("depends for %s(%s) are: %s" % (campaign_stage_id, name, depends))
                if 'campaign_stages' in depends:
                    dep_stages = dbhandle.query(CampaignStage).filter(CampaignStage.name.in_(depends['campaign_stages']), CampaignStage.experiment == exp).all()
                elif 'campaigns' in depends:
                    # backwards combatability
                    dep_stages = dbhandle.query(CampaignStage).filter(CampaignStage.name.in_(depends['campaigns']), CampaignStage.experiment == exp).all()
                else:
                    dep_stages = {}
                for (i, stage) in enumerate(dep_stages):
                    logit.log("trying to add dependency for: {}".format(stage.name))
                    dep = CampaignDependency(provides_campaign_stage_id=campaign_stage_id,
                                             needs_campaign_stage_id=stage.campaign_stage_id,
                                             file_patterns=depends['file_patterns'][i])
                    dbhandle.add(dep)
                dbhandle.commit()
            except IntegrityError as e:
                message = "Integrity error - you are most likely using a name which already exists in database."
                logit.log(' '.join(e.args))
                dbhandle.rollback()
            except SQLAlchemyError as e:
                message = "SQLAlchemyError.  Please report this to the administrator.   Message: {}".format(' '.join(e.args))
                logit.log(' '.join(e.args))
                dbhandle.rollback()
            else:
                dbhandle.commit()

        # Find campaign_stages
        if exp:  # cuz the default is find
            if kwargs.get('update_view', None) is None:
                # view flags not specified, use defaults
                data['view_active'] = 'view_active'
                data['view_inactive'] = None
                data['view_mine'] = experimenter.experimenter_id
                data['view_others'] = experimenter.experimenter_id
                data['view_analysis'] = 'view_analysis' if role in ('analysis', 'coordinator') else None
                data['view_production'] = 'view_production' if role in ('production', 'coordinator') else None
            else:
                data['view_active'] = kwargs.get('view_active', None)
                data['view_inactive'] = kwargs.get('view_inactive', None)
                data['view_mine'] = kwargs.get('view_mine', None)
                data['view_others'] = kwargs.get('view_others', None)
                data['view_analysis'] = kwargs.get('view_analysis', None)
                data['view_production'] = kwargs.get('view_production', None)
            # for testing ui...
            # data['authorized'] = True
            state = kwargs.pop('state', None)
            if state is None:
                state = sesshandle.get('campaign_edit.state', 'state_active')

            jumpto = kwargs.pop('jump_to_campaign', None)

            sesshandle['campaign_edit.state'] = state
            data['state'] = state
            data['curr_experiment'] = exp
            data['authorized'] = []
            cquery = (dbhandle.query(CampaignStage, Campaign)
                      .outerjoin(Campaign)
                      .filter(CampaignStage.experiment == exp)
                     )
            if data['view_analysis'] and data['view_production']:
                pass
            elif data['view_analysis']:
                cquery = cquery.filter(CampaignStage.creator_role == 'analysis')
            elif data['view_production']:
                cquery = cquery.filter(CampaignStage.creator_role == 'production')

            if data['view_mine'] and data['view_others']:
                pass
            elif data['view_mine']:
                cquery = cquery.filter(CampaignStage.creator == data['view_mine'] )
            elif data['view_others']:
                cquery = cquery.filter(CampaignStage.creator != data['view_others'] )

            if data['view_active'] and data['view_inactive']:
                pass
            elif data['view_active']:
                cquery = cquery.filter(CampaignStage.active == True)
            elif data['view_inactive']:
                cquery = cquery.filter(CampaignStage.active == False)

            cquery = cquery.order_by(Campaign.name, CampaignStage.name)
            # this bit has to go onto cquery last
            # -- make sure if we're jumping to a given campaign id
            # that we *have* it in the list...
            if jumpto is not None:
                c2 = (dbhandle.query(CampaignStage,Campaign).filter(CampaignStage.campaign_stage_id == jumpto)
                      .filter(CampaignStage.campaign_id == Campaign.campaign_id))
                # we have to use union_all() and not union()to avoid
                # postgres whining about not knowing how to compare JSON
                # fields... sigh.  (It could just string compare them...)
                cquery = c2.union_all(cquery)

            data['campaign_stages'] = cquery.all()
            data['definitions'] = dbhandle.query(JobType).filter(JobType.experiment == exp).order_by(JobType.name)
            data['templates'] = (dbhandle.query(LoginSetup)
                                 .filter(LoginSetup.experiment == exp).order_by(LoginSetup.name))
            cq = data['campaign_stages']

            for cs in cq:
                if role in ('root', 'coordinator'):
                    data['authorized'].append(True)
                elif cs.CampaignStage.creator_role == 'production' and sesshandle.get('experimenter').session_role == 'production':
                    data['authorized'].append(True)
                elif cs.CampaignStage.creator_role == role and cs.CampaignStage.creator == sesshandle.get('experimenter').experimenter_id:
                    data['authorized'].append(True)
                else:
                    data['authorized'].append(False)

            depends = {}
            for c in cq:
                cid = c.CampaignStage.campaign_stage_id
                sql = (dbhandle.query(CampaignDependency.provides_campaign_stage_id, CampaignStage.name, CampaignDependency.file_patterns)
                       .filter(CampaignDependency.provides_campaign_stage_id == cid,
                               CampaignStage.campaign_stage_id == CampaignDependency.needs_campaign_stage_id))
                deps = {
                    "campaign_stages": [row[1] for row in sql.all()],
                    "file_patterns": [row[2] for row in sql.all()]
                }
                depends[cid] = json.dumps(deps)
            data['depends'] = depends

            #Get the campain names
            campquery = (dbhandle.query(Campaign)
                         .filter(Campaign.experiment == sesshandle.get('experimenter').session_experiment)
                         .order_by(Campaign.name)
                        )
            if sesshandle.get('experimenter').session_role != 'production':
                campquery.filter(Campaign.creator == sesshandle.get('experimenter').experimenter_id)
                campquery.filter(Campaign.creator_role == 'analysis')
            data['campaigns'] = campquery.all()
        data['message'] = message
        return data


    def campaign_stage_edit_query(self, dbhandle, *args, **kwargs):
        """
            return data for a specific stage
        """

        data = {}
        ae_launch_id = kwargs.pop('ae_launch_id', None)
        ae_campaign_definition_id = kwargs.pop('ae_campaign_definition_id', None)

        if ae_launch_id:
            template = {}
            temp = dbhandle.query(LoginSetup).filter(LoginSetup.login_setup_id == ae_launch_id).first()
            template['launch_host'] = temp.launch_host
            template['launch_account'] = temp.launch_account
            template['launch_setup'] = temp.launch_setup
            data['template'] = template

        if ae_campaign_definition_id:
            definition = {}
            cdef = dbhandle.query(JobType).filter(JobType.job_type_id == ae_campaign_definition_id).first()
            definition['input_files_per_job'] = cdef.input_files_per_job
            definition['output_files_per_job'] = cdef.output_files_per_job
            definition['launch_script'] = cdef.launch_script
            definition['definition_parameters'] = cdef.definition_parameters
            data['definition'] = definition
        return json.dumps(data)


    def new_task_for_campaign(self, dbhandle, campaign_name, command_executed, experimenter_name, dataset_name=None):
        '''
            Get a new task-id for a given campaign
        '''
        if isinstance(campaign_name, str):
            campaign_name = campaign_name.strip()
        cs = dbhandle.query(CampaignStage).filter(CampaignStage.name == campaign_name).first()
        e = dbhandle.query(Experimenter).filter(Experimenter.username == experimenter_name).first()
        s = Submission()
        s.campaign_stage_id = cs.campaign_stage_id
        s.job_type_id = cs.job_type_id
        s.status = 'started'
        s.created = datetime.now(utc)
        s.updated = datetime.now(utc)
        s.updater = e.experimenter_id
        s.creator = e.experimenter_id
        s.command_executed = command_executed
        if dataset_name:
            s.input_dataset = dataset_name
        dbhandle.add(s)
        dbhandle.commit()
        return "Submission=%d" % s.submission_id


    def campaign_deps_ini(self, dbhandle, config_get, session_experiment, name=None, stage_id=None, login_setup=None, job_type=None):
        res = []
        campaign_stages = []
        jts = set()
        lts = set()
        the_campaign = None

        if job_type is not None:
            res.append("# with job_type %s" % job_type)
            cd = dbhandle.query(JobType).filter(JobType.name == job_type, JobType.experiment == session_experiment).first()
            if cd:
                jts.add(cd)

        if login_setup is not None:
            res.append("# with login_setup: %s" % login_setup)
            lt = dbhandle.query(LoginSetup).filter(LoginSetup.name == login_setup, LoginSetup.experiment == session_experiment).first()
            if lt:
                lts.add(lt)

        if name is not None:
            the_campaign = dbhandle.query(Campaign).filter(Campaign.name == name).scalar()
            #
            campaign_stages = dbhandle.query(CampaignStage).join(Campaign).filter(
                Campaign.name == name,
                CampaignStage.campaign_id == Campaign.campaign_id).all()

        if stage_id is not None:
            cidl1 = dbhandle.query(CampaignDependency.needs_campaign_stage_id).filter(CampaignDependency.provides_campaign_stage_id == stage_id).all()
            cidl2 = dbhandle.query(CampaignDependency.provides_campaign_stage_id).filter(CampaignDependency.needs_campaign_stage_id == stage_id).all()
            s = set([stage_id])
            s.update(cidl1)
            s.update(cidl2)
            campaign_stages = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id.in_(s)).all()

        cnames = {}
        for cs in campaign_stages:
            cnames[cs.campaign_stage_id] = cs.name

        # lookup relevant dependencies
        dmap = {}
        for cid in cnames.keys():
            dmap[cid] = []


        fpmap = {}
        for cid in cnames.keys():
            cdl = (dbhandle.query(CampaignDependency)
                   .filter(CampaignDependency.provides_campaign_stage_id == cid)
                   .filter(CampaignDependency.needs_campaign_stage_id.in_(cnames.keys())).all())
            for cd in cdl:
                if cd.needs_campaign_stage_id in cnames.keys():
                    dmap[cid].append(cd.needs_campaign_stage_id)
                    fpmap[(cid, cd.needs_campaign_stage_id)] = cd.file_patterns

        #------------

        # sort by dependencies(?)
        cidl = list(cnames.keys())
        for cid in cnames.keys():
            for dcid in dmap[cid]:
                if cidl.index(dcid) < cidl.index(cid):
                    cidl[cidl.index(dcid)], cidl[cidl.index(cid)] = cidl[cidl.index(cid)], cidl[cidl.index(dcid)]

        if the_campaign:
            res.append("[campaign]")
            res.append("experiment=%s" % the_campaign.experiment)
            res.append("poms_role=%s" % the_campaign.creator_role)
            res.append("name=%s" % the_campaign.name)

            res.append("campaign_stage_list=%s" % " ".join(map(cnames.get, cidl)))
            res.append("")

        for cs in campaign_stages:
            res.append("[campaign_stage %s]" % cs.name)
            # res.append("name=%s" % cs.name)
            res.append("vo_role=%s" % cs.vo_role)
            res.append("state=%s" % ("Active" if cs.active else "Inactive"))
            res.append("software_version=%s" % cs.software_version)
            res.append("dataset=%s" % cs.dataset)
            res.append("cs_split_type=%s" % cs.cs_split_type)
            res.append("completion_type=%s" % cs.completion_type)
            res.append("completion_pct=%s" % cs.completion_pct)
            res.append("param_overrides=%s" % json.dumps(cs.param_overrides or []))
            res.append("test_param_overrides=%s" % json.dumps(cs.test_param_overrides or []))
            res.append("login_setup=%s" % cs.login_setup_obj.name)
            res.append("job_type=%s" % cs.job_type_obj.name)
            jts.add(cs.job_type_obj)
            lts.add(cs.login_setup_obj)
            res.append("")

        for lt in lts:
            res.append("[login_setup %s]" % lt.name)
            res.append("host=%s" % lt.launch_host)
            res.append("account=%s" % lt.launch_account)
            res.append("setup=%s" % lt.launch_setup)
            res.append("")

        for jt in jts:
            res.append("[job_type %s]" % jt.name)
            res.append("launch_script=%s" % jt.launch_script)
            res.append("parameters=%s" % json.dumps(jt.definition_parameters))
            res.append("output_file_patterns=%s" % jt.output_file_patterns)
            res.append("")
            # still need: recovery launches

        # still need dependencies
        deps = deque()
        for cid in dmap.keys():
            if len(dmap[cid]) == 0:
                continue
            res.append("[dependencies %s]" % cnames[cid])
            i = 0
            for dcid in dmap[cid]:
                i = i + 1
                res.append("campaign_stage_%d = %s" % (i, cnames[dcid]))
                res.append("file_pattern_%d = %s" % (i, fpmap[(cid, dcid)]))
            res.append("")

        res.append("")

        return "\n".join(res).replace("%", "%%")


    def campaign_deps_svg(self, dbhandle, config_get, campaign_name=None, campaign_stage_id=None):
        '''
            return campaign dependencies as an SVG graph
            uses "dot" to generate the drawing
        '''
        if campaign_name is not None:
            cl = dbhandle.query(CampaignStage).join(Campaign).filter(Campaign.name == campaign_name,
                                                                     CampaignStage.campaign_id == Campaign.campaign_id).all()

        if campaign_stage_id is not None:
            cidl1 = dbhandle.query(CampaignDependency.needs_campaign_stage_id).filter(
                CampaignDependency.provides_campaign_stage_id == campaign_stage_id).all()
            cidl2 = dbhandle.query(CampaignDependency.provides_campaign_stage_id).filter(
                CampaignDependency.needs_campaign_stage_id == campaign_stage_id).all()
            s = set([campaign_stage_id])
            s.update(cidl1)
            s.update(cidl2)
            cl = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id.in_(s)).all()

        c_ids = deque()
        for cs in cl:
            c_ids.append(cs.campaign_stage_id)

        logit.log(logit.INFO, "campaign_deps: c_ids=%s" % repr(c_ids))

        try:
            pdot = subprocess.Popen("tee /tmp/dotstuff | dot -Tsvg", shell=True, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE, universal_newlines=True)
            pdot.stdin.write('digraph {}Dependencies {{\n'.format(campaign_name))
            pdot.stdin.write('node [shape=box, style=rounded, color=lightgrey, fontcolor=black]\nrankdir = "LR";\n')
            baseurl = "{}/campaign_info?campaign_stage_id=".format(config_get("pomspath"))

            locatedmap = self.poms_service.taskPOMS.running_submissions(dbhandle, c_ids, status_list=["Located"])

            logit.log(logit.INFO, "campaign_deps: locatedmap=%s" % repr(locatedmap))
            for cs in cl:
                tot = dbhandle.query(func.count(Submission.submission_id)).filter(Submission.campaign_stage_id == cs.campaign_stage_id).one()[0]
                ltot = locatedmap[cs.campaign_stage_id]
                logit.log(logit.INFO, "campaign_deps: tot=%s" % repr(tot))
                pdot.stdin.write(
                    'cs{:d} [URL="{}{:d}",label="{}\\nSubmissions {:d} Located {:d}",color={}];\n'.format(cs.campaign_stage_id,
                          baseurl,
                          cs.campaign_stage_id,
                          cs.name,
                          tot,
                          ltot,
                          ("darkgreen" if ltot == tot else "black")))

            cdl = (dbhandle.query(CampaignDependency).filter(CampaignDependency.needs_campaign_stage_id.in_(c_ids)).all())

            for cd in cdl:
                if cd.needs_campaign_stage_id in c_ids and cd.provides_campaign_stage_id in c_ids:
                    pdot.stdin.write('cs{:d} -> cs{:d};\n'.format(cd.needs_campaign_stage_id, cd.provides_campaign_stage_id))

            pdot.stdin.write('}\n')
            pdot.stdin.close()
            text = pdot.stdout.read()
            pdot.wait()
        except:
            raise
            text = ""
            raise
        #return bytes(text, encoding="utf-8")
        return text

    def show_campaigns(self, dbhandle, sesshandle, experimenter, *args, **kwargs):
        action = kwargs.get('action', None)
        msg = "OK"
        se_role = experimenter.session_role
        if action == 'delete':
            campaign_id = kwargs.get('del_campaign_id')
            campaign = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
            if experimenter.is_authorized(campaign):
                subs = dbhandle.query(Submission).join(CampaignStage, Submission.campaign_stage_id == CampaignStage.campaign_stage_id).filter(CampaignStage.campaign_id == campaign_id)
                if subs.count() > 0:
                    msg = "This campaign has been submitted.  It cannot be deleted."
                else:
                    dbhandle.query(CampaignDependency).filter(
                        or_(
                            CampaignDependency.provider.has(CampaignStage.campaign_id == campaign_id),
                            CampaignDependency.consumer.has(CampaignStage.campaign_id == campaign_id))
                    ).delete(synchronize_session=False)
                    dbhandle.query(CampaignStage).filter(CampaignStage.campaign_id == campaign_id).delete(synchronize_session=False)
                    dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).delete(synchronize_session=False)
                    dbhandle.commit()
                    msg = "Campaign named %s with campaign_id %s and related CampagnStages were deleted ." % (kwargs.get('del_campaign_name'), campaign_id)
            else:
                msg = "You are not authorized to delete campaigns."

        data = {
            'authorized' : []
        }
        q = (dbhandle.query(Campaign)
             .options(joinedload(Campaign.experimenter_creator_obj))
             .filter(Campaign.experiment == experimenter.session_experiment)
             .order_by(Campaign.name))

        if kwargs.get('update_view', None) is None:
            # view flags not specified, use defaults
            data['view_active'] = 'view_active'
            data['view_inactive'] = None
            data['view_mine'] = experimenter.experimenter_id
            data['view_others'] = experimenter.experimenter_id
            data['view_analysis'] = 'view_analysis' if se_role in ('analysis', 'coordinator') else None
            data['view_production'] =  'view_production' if se_role in ('production', 'coordinator') else None
        else:
            data['view_active'] = kwargs.get('view_active', None)
            data['view_inactive'] = kwargs.get('view_inactive', None)
            data['view_mine'] = kwargs.get('view_mine', None)
            data['view_others'] = kwargs.get('view_others', None)
            data['view_analysis'] = kwargs.get('view_analysis', None)
            data['view_production'] = kwargs.get('view_production', None)

        if data['view_analysis'] and data['view_production']:
            pass
        elif data['view_analysis']:
            q = q.filter(Campaign.creator_role == 'analysis')
        elif data['view_production']:
            q = q.filter(Campaign.creator_role == 'production')

        if data['view_mine'] and data['view_others']:
            pass
        elif data['view_mine']:
            q = q.filter(Campaign.creator == data['view_mine'] )
        elif data['view_others']:
            q = q.filter(Campaign.creator != data['view_others'] )

        # Campaigns don't have an active field(yet?)
        if data['view_active'] and data['view_inactive']:
            pass
        elif data['view_active']:
            pass
        elif data['view_others']:
            pass

        tl = q.all()

        if not tl:
            return tl, "", msg, data
        role = sesshandle.get('experimenter').session_role
        for cs in tl:
            if role == 'coordinator':
                data['authorized'].append(True)
            elif cs.creator_role == 'production' and role == 'production':
                data['authorized'].append(True)
            elif cs.creator_role == role and cs.creator == sesshandle.get('experimenter').experimenter_id:
                data['authorized'].append(True)
            else:
                data['authorized'].append(False)

        last_activity_l = dbhandle.query(func.max(Submission.updated)).join(CampaignStage,Submission.campaign_stage_id == CampaignStage.campaign_stage_id).join(Campaign,CampaignStage.campaign_id == Campaign.campaign_id).filter(Campaign.experiment == experimenter.session_experiment).first()
        last_activity = ""
        if last_activity_l and last_activity_l and last_activity_l[0]:
            if datetime.now(utc) - last_activity_l[0] > timedelta(days=7):
                last_activity = last_activity_l[0].strftime("%Y-%m-%d %H:%M:%S")
        return tl, last_activity, msg, data


    def show_campaign_stages(self, dbhandle, samhandle, campaign_ids=None, tmin=None, tmax=None, tdays=7,
                             active=True, campaign_name=None, holder=None, role_held_with=None, sesshandler=None, **kwargs):
        """
            give campaign information about campaign_stages with activity in the
            time window for a given experiment
        :rtype: object
        """
        (tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'show_campaign_stages?')

        experimenter = sesshandler('experimenter')
        experiment = experimenter.session_experiment
        se_role = experimenter.session_role

        cq = (dbhandle.query(CampaignStage)
              .options(joinedload('experiment_obj'))
              .options(joinedload('campaign_obj'))
              .options(joinedload(CampaignStage.experimenter_holder_obj))
              .order_by(CampaignStage.experiment)
              .options(joinedload(CampaignStage.experimenter_creator_obj))
             )

        if experiment:
            cq = cq.filter(CampaignStage.experiment == experiment)

        data = {}
        if kwargs.get('update_view',None) == None:
            # view flags not specified, use defaults
            data['view_active'] = 'view_active'
            data['view_inactive'] = None
            data['view_mine'] = experimenter.experimenter_id
            data['view_others'] = experimenter.experimenter_id
            data['view_analysis'] = 'view_analysis' if se_role in ('analysis','coordinator') else None
            data['view_production'] =  'view_production' if se_role in ('production','coordinator') else None
        else:
            data['view_active'] = kwargs.get('view_active', None)
            data['view_inactive'] = kwargs.get('view_inactive', None)
            data['view_mine'] = kwargs.get('view_mine', None)
            data['view_others'] = kwargs.get('view_others', None)
            data['view_analysis'] = kwargs.get('view_analysis', None)
            data['view_production'] = kwargs.get('view_production', None)

        if data['view_analysis'] and data['view_production']:
            pass
        elif data['view_analysis']:
            cq = cq.filter(CampaignStage.creator_role == 'analysis')
        elif data['view_production']:
            cq = cq.filter(CampaignStage.creator_role == 'production')

        if data['view_mine'] and data['view_others']:
            pass
        elif data['view_mine']:
            cq = cq.filter(CampaignStage.creator == data['view_mine'])
        elif data['view_others']:
            cq = cq.filter(CampaignStage.creator != data['view_others'])

        if data['view_active'] and data['view_inactive']:
            pass
        elif data['view_active']:
            cq = cq.filter(CampaignStage.active == True)
        elif data['view_inactive']:
            cq = cq.filter(CampaignStage.active == False)


        if campaign_ids:
            campaign_ids = campaign_ids.split(",")
            cq = cq.filter(CampaignStage.campaign_stage_id.in_(campaign_ids))

        if campaign_name:
            cq = cq.join(Campaign).filter(Campaign.name == campaign_name)

            # for now we comment out it. When we have a lot of data, we may need to use these filters.
            # We will let the client filter it in show_campaign_stages.html with tablesorter for now.
            # if holder:
            # cq = cq.filter(Campaingn.hold_experimenters_id == holder)

            # if creator_role:
            # cq = cq.filter(Campaingn.creator_role == creator_role)
        campaign_stages = cq.all()
        logit.log(logit.DEBUG, "show_campaign_stages: back from query")
        # check for authorization
        data['authorized'] = []
        for cs in campaign_stages:
            if se_role != 'analysis':
                data['authorized'].append(True)
            elif cs.creator == sesshandler('experimenter').experimenter_id:
                data['authorized'].append(True)
            else:
                data['authorized'].append(False)
        return campaign_stages, tmin, tmax, tmins, tmaxs, tdays, nextlink, prevlink, time_range_string, data


    def reset_campaign_split(self, dbhandle, samhandle, campaign_stage_id):
        """
            reset a campaign_stages cs_last_split field so the sequence
            starts over
        """
        campaign_stage_id = int(campaign_stage_id)

        cs = (dbhandle.query(CampaignStage)
              .filter(CampaignStage.campaign_stage_id == campaign_stage_id)
              .first())
        cs.cs_last_split = None
        dbhandle.commit()

    # @pomscache.cache_on_arguments()
    def campaign_info(self, dbhandle, samhandle, err_res, config_get, campaign_stage_id, tmin=None, tmax=None, tdays=None):
        """
           Give information related to a campaign for the campaign_info page
        """

        campaign_stage_id = int(campaign_stage_id)

        campaign_info = (dbhandle.query(CampaignStage, Experimenter)
                         .filter(CampaignStage.campaign_stage_id == campaign_stage_id, CampaignStage.creator == Experimenter.experimenter_id)
                         .first())

        # default to time window of campaign
        if tmin is None and tdays is None:
            tmin = campaign_info.CampaignStage.created
            tmax = datetime.now(utc)

        tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays = self.poms_service.utilsPOMS.handle_dates(
            tmin, tmax, tdays, 'campaign_info?')

        last_activity_l = dbhandle.query(func.max(Submission.updated)).filter(Submission.campaign_stage_id == campaign_stage_id).first()
        logit.log("got last_activity_l %s" % repr(last_activity_l))
        if last_activity_l[0] and datetime.now(utc) - last_activity_l[0] > timedelta(days=7):
            last_activity = last_activity_l[0].strftime("%Y-%m-%d %H:%M:%S")
        else:
            last_activity = ""
        logit.log("after: last_activity %s" % repr(last_activity))

        campaign_definition_info = (dbhandle.query(JobType, Experimenter)
                                    .filter(JobType.job_type_id == campaign_info.CampaignStage.job_type_id,
                                            JobType.creator == Experimenter.experimenter_id)
                                    .first())
        login_setup_info = (dbhandle.query(LoginSetup, Experimenter)
                                .filter(LoginSetup.login_setup_id == campaign_info.CampaignStage.login_setup_id,
                                        LoginSetup.creator == Experimenter.experimenter_id)
                                .first())
        campaigns = dbhandle.query(Campaign).join(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).all()

        launched_campaigns = dbhandle.query(CampaignStageSnapshot).filter(CampaignStageSnapshot.campaign_stage_id == campaign_stage_id).all()

        #
        # cloned from show_campaign_stages, but for a one row table..
        #
        campaign = campaign_info[0]
        counts = {}
        counts_keys = {}
        # cil = [cs.campaign_stage_id for cs in cl]
        # dimlist, pendings = self.poms_service.filesPOMS.get_pending_for_campaigns(dbhandle, samhandle, cil, tmin, tmax)
        # effs = self.poms_service.jobsPOMS.get_efficiency(dbhandle, cil,tmin, tmax)
        # counts[campaign_stage_id]['efficiency'] = effs[0]
        # if pendings:
        #    counts[campaign_stage_id]['pending'] = pendings[0]
        # counts_keys[campaign_stage_id] = list(counts[campaign_stage_id].keys())
        #
        # any launch outputs to look at?
        #
        dirname = "{}/private/logs/poms/launches/campaign_{}".format(os.environ['HOME'], campaign_stage_id)
        launch_flist = glob.glob('{}/*'.format(dirname))
        launch_flist = list(map(os.path.basename, launch_flist))

        # put our campaign id in the link
        campaign_kibana_link_format = config_get('campaign_kibana_link_format')
        logit.log("got format {}".format(campaign_kibana_link_format))
        kibana_link = campaign_kibana_link_format.format(campaign_stage_id)

        dep_svg = self.campaign_deps_svg(dbhandle, config_get, campaign_stage_id=campaign_stage_id)
        return (campaign_info,
                time_range_string,
                tmins, tmaxs, tdays,
                campaign_definition_info, login_setup_info,
                campaigns, launched_campaigns, None,
                campaign, counts_keys, counts, launch_flist, kibana_link,
                dep_svg, last_activity
                )

    # @pomscache_10.cache_on_arguments()
    def campaign_time_bars(self, dbhandle, campaign_stage_id=None, campaign=None, tmin=None, tmax=None, tdays=1):
        """
            Give time-bars for Tasks for this campaign in a time window
            using the time_grid code
        """
        if campaign_stage_id == None:
            base_link = 'campaign_time_bars?campaign={}&'.format(campaign)
        else:
            base_link = 'campaign_time_bars?campaign_stage_id={}&'.format(campaign_stage_id)

        (
            tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays
        ) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays,base_link)
        tg = time_grid.time_grid()
        key = tg.key()

        class fakerow:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        sl = deque()

        if campaign_stage_id is not None:
            icampaign_id = int(campaign_stage_id)
            q = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == icampaign_id)
            cpl = q.all()
            name = cpl[0].name
        elif campaign is not None and campaign != "":
            q = dbhandle.query(CampaignStage).join(Campaign).filter(
                CampaignStage.campaign_id == Campaign.campaign_id,
                Campaign.name == campaign)
            cpl = q.all()
            name = campaign
        else:
            err_res = "404 Permission Denied."
            return "Neither CampaignStage nor Campaign found"

        job_counts_list = deque()
        cidl = deque()
        for cs in cpl:
            cidl.append(cs.campaign_stage_id)

        qr = dbhandle.query(SubmissionHistory).join(Submission).filter(Submission.campaign_stage_id.in_(cidl),
                                                           SubmissionHistory.submission_id == Submission.submission_id,
                                                           or_(and_(Submission.created > tmin, Submission.created < tmax),
                                                               and_(Submission.updated > tmin,
                                                                    Submission.updated < tmax))).order_by(SubmissionHistory.submission_id,
                                                                                                    SubmissionHistory.created).all()
        items = deque()
        extramap = OrderedDict()

        if cpl[0].dataset in (None, 'None','none'):
             url_template= "https://fifemon.fnal.gov/monitor/d/000000115/job-cluster-summary?var-cluster=%(jobsub_cluster)s&var-schedd=%(jobsub_schedd)s&from=%(tminsec)s000&to=now&refresh=3m&orgId=1"
        else:
             url_template= "https://fifemon.fnal.gov/monitor/d/000000188/dag-cluster-summary?var-cluster=%(jobsub_cluster)s&var-schedd=%(jobsub_schedd)s&from=%(tminsec)s000&to=now&refresh=3m&orgId=1"

        failedlaunch_url_template = self.poms_service.path + "/list_launch_file?campaign_stage_id=%(campaign_stage_id)s&fname=%(created_s)s_%(creator)s"

        for th in qr:
            jjid = th.submission_obj.jobsub_job_id
            full_jjid = jjid
            if not jjid:
                jjid = 's' + str(th.submission_id)
                full_jjid="unknown.0@unknown.un.known"
            else:
                jjid = str(jjid).replace('fifebatch', '').replace('.fnal.gov', '')

            if campaign is not None:
                jjid += "<br>%s" % th.submission_obj.campaign_stage_obj.name


            if th.status not in ("Completed", "Located", "Failed", "Removed"):
                extramap[jjid] = ('<a href="{}/kill_jobs?submission_id={:d}&act=hold"><i class="ui pause icon"></i></a>'
                                  '<a href="{}/kill_jobs?submission_id={:d}&act=release"><i class="ui play icon"></i></a>'
                                  '<a href="{}/kill_jobs?submission_id={:d}&act=kill"><i class="ui trash icon"></i></a>'
                                  ).format(self.poms_service.path, th.submission_id,
                                           self.poms_service.path, th.submission_id,
                                           self.poms_service.path, th.submission_id)
            else:
                extramap[jjid] = '&nbsp; &nbsp; &nbsp; &nbsp;'

            items.append(fakerow(submission_id=th.submission_id,
                                 created=th.created.replace(tzinfo=utc),
                                 tmin=th.submission_obj.created - timedelta(minutes=15),
                                 tmax=th.submission_obj.updated,
                                 tminsec=tmin.strftime("%s"),
                                 status=th.status,
                                 jobsub_job_id=jjid,
                                 jobsub_cluster=full_jjid[:jjid.find('.')],
                                 jobsub_schedd=full_jjid[jjid.find('@') + 1:],
                                 creator=th.submission_obj.experimenter_creator_obj.username,
                                 campaign_stage_id=th.submission_obj.campaign_stage_id,
                                 created_s=th.submission_obj.created.strftime("%Y%m%d_%H%M%S")
                                 ))
            if th.status == 'LaunchFailed':
                items[-1].url = failedlaunch_url_template % items[-1].__dict__
            else:
                items[-1].url = url_template % items[-1].__dict__

        logit.log("campaign_time_bars: items: " + repr(items))
        blob = tg.render_query_blob(tmin, tmax, items, 'jobsub_job_id',
                                    extramap=extramap)
        return "", blob, name, str(tmin)[:16], str(tmax)[:16], nextlink, prevlink, tdays, key, extramap


    def register_poms_campaign(self, dbhandle, experiment, campaign_name, version, user=None, campaign_definition=None,
                               dataset="", role="Production", cr_role="production", sesshandler=None, params=[]):
        """
            update or add a campaign by experiment and name...
        """
        changed = False
        if dataset is None:
            dataset = ''
        if user is None:
            user = 4
        else:
            u = dbhandle.query(Experimenter).filter(Experimenter.username == user).first()
            if u:
                user = u.experimenter_id

        if campaign_definition not in (None, "None"):
            cd = dbhandle.query(JobType).filter(JobType.name == campaign_definition,
                                                JobType.experiment == experiment).first()
        else:
            cd = dbhandle.query(JobType).filter(JobType.name.ilike(r"%generic%"),
                                                JobType.experiment == experiment).first()

        ld = dbhandle.query(LoginSetup).filter(LoginSetup.name.ilike(r"%generic%"),
                                               LoginSetup.experiment == experiment).first()

        logit.log("campaign_definition = {} ".format(cd))

        cs = dbhandle.query(CampaignStage).filter(CampaignStage.experiment == experiment, CampaignStage.name == campaign_name).first()
        if cs:
            changed = False
        else:
            cs = CampaignStage(experiment=experiment, name=campaign_name, creator=user, created=datetime.now(utc),
                               software_version=version, job_type_id=cd.job_type_id,
                               login_setup_id=ld.login_setup_id, vo_role=role, dataset='',
                               creator_role=cr_role, campaign_type='regular')

        if version:
            cs.software_verison = version
            changed = True

        if dataset:
            cs.dataset = dataset
            changed = True

        if user:
            cs.experimenter = user
            changed = True

        logit.log("register_campaign -- campaign is %s" % cs.__dict__)

        if changed:
            cs.updated = datetime.now(utc)
            cs.updator = user
            dbhandle.add(cs)
            dbhandle.commit()

        return cs.campaign_stage_id


    def get_dataset_for(self, dbhandle, samhandle, err_res, camp):
        '''
            use the split_type modules to get the next dataset for
            launch for a given campaign
        '''

        if not camp.cs_split_type or camp.cs_split_type == 'None' or camp.cs_split_type == 'none':
            return camp.dataset

        # clean up split_type -- de-white-space it
        camp.cs_split_type = camp.cs_split_type.replace(' ','')
        camp.cs_split_type = camp.cs_split_type.replace('\n','')

        #
        # the module name is the first part of the string, i.e.
        # fred_by_whatever(xxx) -> 'fred'
        # new_localtime -> 'new'
        #
        p1 = camp.cs_split_type.find('(')
        p2 = camp.cs_split_type.find('_')
        if p1 < p2 and p1 > 0:
            pass
        elif p2 < p1 and p2 > 0:
            p1 = p2

        if p1 < 0:
            p1 = len(camp.cs_split_type)

        modname = camp.cs_split_type[0:p1]

        mod = importlib.import_module('poms.webservice.split_types.' + modname)
        split_class = getattr(mod, modname)

        splitter = split_class(camp, samhandle, dbhandle)

        try:
            res = splitter.next()
        except StopIteration:
            raise err_res(404, 'No more splits in this campaign')

        dbhandle.commit()
        return res

    def list_launch_file(self, campaign_stage_id, fname, login_setup_id=None):
        '''
            get launch output file and return the lines as a list
        '''
        if login_setup_id:
            dirname = '{}/private/logs/poms/launches/template_tests_{}'.format(os.environ['HOME'], login_setup_id)
        else:
            dirname = '{}/private/logs/poms/launches/campaign_{}'.format(os.environ['HOME'], campaign_stage_id)
        lf = open('{}/{}'.format(dirname, fname), 'r')
        sb = os.fstat(lf.fileno())
        lines = lf.readlines()
        lf.close()
        # if file is recent set refresh to watch it
        if (time.time() - sb[8]) < 5:
            refresh = 3
        elif (time.time() - sb[8]) < 30:
            refresh = 10
        else:
            refresh = 0
        return lines, refresh

    def schedule_launch(self, dbhandle, campaign_stage_id):
        '''
            return crontab info for cron launches for campaign
        '''
        cs = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
        my_crontab = CronTab(user=True)
        citer = my_crontab.find_comment('POMS_CAMPAIGN_ID={}'.format(campaign_stage_id))
        # there should be only zero or one...
        job = None
        for job in citer:
            break

        # any launch outputs to look at?
        #
        dirname = '{}/private/logs/poms/launches/campaign_{}'.format(os.environ['HOME'], campaign_stage_id)
        launch_flist = glob.glob('{}/*'.format(dirname))
        launch_flist = list(map(os.path.basename, launch_flist))
        return cs, job, launch_flist

    def update_launch_schedule(self, campaign_stage_id, dowlist='', domlist='', monthly='', month='', hourlist='', submit='',
                               minlist='', delete='', user=''):
        '''
            callback for changing the launch schedule
        '''

        # deal with single item list silliness
        if isinstance(minlist, str):
            minlist = minlist.split(',')
        if isinstance(hourlist, str):
            hourlist = hourlist.split(',')
        if isinstance(dowlist, str):
            dowlist = dowlist.split(',')
        if isinstance(domlist, str):
            domlist = domlist.split(',')

        logit.log('hourlist is {} '.format(hourlist))

        if minlist[0] == '*':
            minlist = None
        else:
            minlist = [int(x) for x in minlist if x != '']

        if hourlist[0] == '*':
            hourlist = None
        else:
            hourlist = [int(x) for x in hourlist if x != '']

        if dowlist[0] == '*':
            dowlist = None
        else:
            # dowlist[0] = [int(x) for x in dowlist if x != '']
            pass

        if domlist[0] == '*':
            domlist = None
        else:
            domlist = [int(x) for x in domlist if x != '']

        my_crontab = CronTab(user=True)
        # clean out old
        my_crontab.remove_all(comment='POMS_CAMPAIGN_ID={}'.format(campaign_stage_id))

        if not delete:

            # make job for new -- use current link for product
            pdir = os.environ.get('POMS_DIR', '/etc/poms')
            if not pdir.find('/current/') > 0:
                # try to find a current symlink path that points here
                tpdir = pdir[:pdir.rfind('poms', 0, len(pdir) - 1) + 4] + '/current'
                if os.path.exists(tpdir):
                    pdir = tpdir

            job = my_crontab.new(command='{}/cron/launcher --campaign_stage_id={} --launcher={}'.format(pdir, campaign_stage_id, user),
                                 comment='POMS_CAMPAIGN_ID={}'.format(campaign_stage_id))

            # set timing...
            if dowlist:
                job.dow.on(*dowlist)

            if minlist:
                job.minute.on(*minlist)

            if hourlist:
                job.hour.on(*hourlist)

            if domlist:
                job.day.on(*domlist)

            job.enable()

        my_crontab.write()


    def get_recovery_list_for_campaign_def(self, dbhandle, campaign_def):
        '''
            return the recovery list for a given campaign_def
        '''
        rlist = (dbhandle.query(CampaignRecovery)
                 .options(joinedload(CampaignRecovery.recovery_type))
                 .filter(CampaignRecovery.job_type_id == campaign_def.job_type_id)
                 .order_by(CampaignRecovery.recovery_order))

        # convert to a real list...
        l = deque()
        for r in rlist:
            l.append(r)
        rlist = l

        return rlist


    def make_stale_campaigns_inactive(self, dbhandle, err_res):
        '''
            turn off active flag on campaign_stages without recent activity
        '''
        lastweek = datetime.now(utc) - timedelta(days=7)
        recent_sq = dbhandle.query(distinct(Submission.campaign_stage_id)).filter(Submission.created > lastweek)

        stale = (dbhandle.query(CampaignStage)
                 .filter(CampaignStage.created < lastweek, CampaignStage.campaign_stage_id.notin_(recent_sq), CampaignStage.active == True)
                 .update({"active": False}, synchronize_session=False))

        dbhandle.commit()

        return []


    def echo(self, dbhandle, sesshandle, *args, **kwargs):
        form = kwargs.get('form', None)
        #VP~ print("******************* Get the form: '{}'".format(form))
        everything = json.loads(form)
        stages = everything['stages']
        print("############## {}".format([s.get('id') for s in stages]))
        return stages


    def save_campaign(self, dbhandle, sesshandle, *args, **kwargs):
        """
        """
        role = sesshandle.get('experimenter').session_role or 'production'
        user_id = sesshandle.get('experimenter').experimenter_id
        exp = sesshandle.get('experimenter').session_experiment

        data = kwargs.get('form', None)
        everything = json.loads(data)

        stages = everything['stages']
        print("############## stages: {}".format([s.get('id') for s in stages]))

        campaign = [s for s in stages if s.get('id').startswith('campaign ')][0]
        c_old_name = campaign.get('id').split(' ')[1]
        c_new_name = campaign.get('label')
        clean = campaign.get('clean')
        defaults = campaign.get('form')
        position = campaign.get('position')
        print("############## i: '{}', l: '{}', c: '{}', f: '{}', p: '{}'".format(c_old_name, c_new_name, clean, defaults, position))

        the_campaign = dbhandle.query(Campaign).filter(Campaign.name == c_old_name, Campaign.experiment == exp).scalar()
        if the_campaign:
            the_campaign.defaults = defaults
            if c_new_name != c_old_name:
                the_campaign.name = c_new_name
        else:   # we do not have a campaign in the db for this experiment so create the campaign and then do the linking
            the_campaign = Campaign()
            the_campaign.name = c_new_name
            the_campaign.defaults = defaults
            the_campaign.experiment = exp
            the_campaign.creator = user_id
            the_campaign.creator_role = role
            dbhandle.add(the_campaign)
        dbhandle.commit()

        old_stages = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_obj.has(Campaign.name == c_new_name)).all()
        old_stage_names = set([s.name for s in old_stages])

        # new_stages = tuple(filter(lambda s: not s.get('id').startswith('campaign '), stages))
        new_stages = [s for s in stages if not s.get('id').startswith('campaign ')]
        new_stage_names = set([s.get('id') for s in new_stages])

        deleted_stages = old_stage_names - new_stage_names
        if deleted_stages:
            # (dbhandle.query(CampaignStage)                    # We DON'T delete the stages
            #  .filter(CampaignStage.name.in_(deleted_stages))
            #  .delete(synchronize_session=False))
            cs_list = dbhandle.query(CampaignStage).filter(CampaignStage.name.in_(deleted_stages))  # Get the stage list
            for cs in cs_list:
                cs.campaign_id = None                   # Detach the stage from campaign
            dbhandle.query(CampaignDependency).filter(
                or_(
                    CampaignDependency.provider.has(CampaignStage.name.in_(deleted_stages)),
                    CampaignDependency.consumer.has(CampaignStage.name.in_(deleted_stages)))
            ).delete(synchronize_session=False)         # Delete the stage dependencies if any
            dbhandle.commit()

        for stage in new_stages:
            old_name = stage.get('id')
            new_name = stage.get('label')
            position = stage.get('position')    # Ignore for now
            clean = stage.get('clean')
            form = stage.get('form')
            form = {k: (form[k] or defaults[k]) for k in form}   # Use the field if provided otherwise use defaults
            print("############## i: '{}', l: '{}', c: '{}', f: '{}', p: '{}'".format(old_name, new_name, clean, form, position))

            active = (form.pop('state') in ('True', 'true', '1', 'Active'))
            completion_pct = form.pop('completion_pct')
            completion_type = form.pop('completion_type')
            split_type = form.pop('split_type', None)
            dataset = form.pop('dataset')
            job_type = form.pop('job_type')
            login_setup = form.pop('login_setup')
            print("################ login_setup: '{}'".format(login_setup))
            param_overrides = form.pop('param_overrides', "[]")
            print("################ param_overrides: '{}'".format(param_overrides))
            if param_overrides:
                param_overrides = json.loads(param_overrides)
            software_version = form.pop('software_version')
            test_param_overrides = form.pop('test_param_overrides', "[]")
            if test_param_overrides:
                test_param_overrides = json.loads(test_param_overrides)
            vo_role = form.pop('vo_role')

            stage_type = form.pop('stage_type', 'test')

            login_setup_id = (dbhandle.query(LoginSetup.login_setup_id)
                              .filter(LoginSetup.experiment == exp)
                              .filter(LoginSetup.name == login_setup).scalar())
            job_type_id = (dbhandle.query(JobType.job_type_id)
                           .filter(JobType.experiment == exp)
                           .filter(JobType.name == job_type).scalar())

            if old_name in old_stage_names:
                #VP~ obj = dbhandle.query(CampaignStage).filter(CampaignStage.name == old_name).scalar()  # Get stage by the old name
                obj = (dbhandle.query(CampaignStage)
                       .filter(CampaignStage.campaign_id == the_campaign.campaign_id)
                       .filter(CampaignStage.name == old_name).scalar())  # Get stage by the old name
                obj.name = new_name     # Update the name using provided new_name
                if not clean:           # Update all fields from the form
                    obj.completion_pct = completion_pct
                    obj.completion_type = completion_type
                    obj.cs_split_type = split_type
                    obj.dataset = dataset
                    obj.job_type_id = job_type_id
                    obj.login_setup_id = login_setup_id
                    obj.param_overrides = param_overrides
                    obj.software_version = software_version
                    obj.test_param_overrides = test_param_overrides
                    obj.vo_role = vo_role
                    obj.campaign_type = stage_type
                    obj.active = active
            else:       # If this is a new stage then create and store it
                cs = CampaignStage(name=new_name, experiment=exp,
                                   campaign_id=the_campaign.campaign_id,
                                   active=active,
                                   #
                                   completion_pct=completion_pct,
                                   completion_type=completion_type,
                                   cs_split_type=split_type,
                                   dataset=dataset,
                                   job_type_id=job_type_id,
                                   login_setup_id=login_setup_id,
                                   param_overrides=param_overrides,
                                   software_version=software_version,
                                   test_param_overrides=test_param_overrides,
                                   vo_role=vo_role,
                                   #
                                   creator=user_id, created=datetime.now(utc),
                                   creator_role=role, campaign_type=stage_type)
                dbhandle.add(cs)
            dbhandle.commit()

        dependencies = everything['dependencies']
        dbhandle.query(CampaignDependency).filter(
            or_(
                CampaignDependency.provider.has(CampaignStage.campaign_id == the_campaign.campaign_id),
                CampaignDependency.consumer.has(CampaignStage.campaign_id == the_campaign.campaign_id))
        ).delete(synchronize_session=False)

        for dependency in dependencies:
            from_name = dependency['fromId']
            to_name = dependency['toId']
            form = dependency.get('form')
            from_id = (dbhandle.query(CampaignStage.campaign_stage_id)
                       #VP~ .filter(CampaignStage.campaign_id != None)
                       .filter(CampaignStage.campaign_id == the_campaign.campaign_id)
                       .filter(CampaignStage.experiment == exp)
                       .filter(CampaignStage.name == from_name).scalar())
            to_id = (dbhandle.query(CampaignStage.campaign_stage_id)
                     #VP~ .filter(CampaignStage.campaign_id != None)
                     .filter(CampaignStage.campaign_id == the_campaign.campaign_id)
                     .filter(CampaignStage.experiment == exp)
                     .filter(CampaignStage.name == to_name).scalar())

            file_pattern = list(form.values())[0]
            dep = CampaignDependency(provides_campaign_stage_id=to_id,
                                     needs_campaign_stage_id=from_id,
                                     file_patterns=file_pattern)
            dbhandle.add(dep)
        dbhandle.commit()
        return dependencies
