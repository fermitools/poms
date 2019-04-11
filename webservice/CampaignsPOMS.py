#!/usr/bin/env python

"""
This module contain the methods that allow to create campaign_stages, definitions and templates.
List of methods:
login_setup_edit, job_type_edit, campaign_stage_edit, campaign_stage_edit_query.
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
from sqlalchemy import and_, distinct, func, or_, text, Integer
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

    def login_setup_edit(self, dbhandle, username, exp, se_role, *args, **kwargs):
        """
            callback to actually change launch templates from edit screen
        """

        experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == username).first()
        data = {}
        template = None
        message = None
        ae_launch_id = None
        data['exp_selections'] = (
            dbhandle.query(Experiment).filter(~Experiment.experiment.in_(['root', 'public'])).order_by(Experiment.experiment)
        )
        action = kwargs.pop('action', None)
        logit.log("login_setup_edit: action is: %s , args %s" % (action, repr(args)))
        pcl_call = int(kwargs.pop('pcl_call', 0))
        pc_username = kwargs.pop('pc_username', None)
        if isinstance(pc_username, str):
            pc_username = pc_username.strip()

        ae_launch_name = kwargs.get('ae_launch_name', '')
        ae_launch_name = ae_launch_name.strip()
        ls = dbhandle.query(LoginSetup).filter(LoginSetup.experiment == exp).filter(LoginSetup.name == ae_launch_name).first()

        if ls and not exp == ls.experiment:
            raise PermissionError("You are not acting as the right experiment")

        if action == 'delete':
            self.poms_service.permissions.can_modify(dbhandle, user, experiment, se_role, "LoginSetup", name = name, experiment = experiment)
            name = ae_launch_name
            try:
                dbhandle.query(LoginSetup).filter(LoginSetup.experiment == exp).filter(LoginSetup.name == name).delete(
                    synchronize_session=False
                )
                dbhandle.commit()
            except SQLAlchemyError as exc:
                message = "The launch template, %s, has been used and may not be deleted." % name
                logit.log(message)
                logit.log(' '.join(exc.args))
                dbhandle.rollback()

        if action in ('add', 'edit'):
            logit.log('login_setup_edit: add,edit case')
            if pcl_call == 1:
                if isinstance(ae_launch_name, str):
                    ae_launch_name = ae_launch_name.strip()
                name = ae_launch_name
                experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == pc_username).first()
                if experimenter:
                    experimenter_id = experimenter.experimenter_id
                else:
                    experimenter_id = 0

                if action == 'edit':
                    ae_launch_id = (
                        dbhandle.query(LoginSetup).filter(LoginSetup.experiment == exp).filter(LoginSetup.name == name).first()
                    ).login_setup_id
                ae_launch_host = kwargs.pop('ae_launch_host', None)
                ae_launch_account = kwargs.pop('ae_launch_account', None)
                ae_launch_setup = kwargs.pop('ae_launch_setup', None)
            else:
                if isinstance(ae_launch_name, str):
                    ae_launch_name = ae_launch_name.strip()
                ae_launch_id = kwargs.pop('ae_launch_id')
                experimenter_id = kwargs.pop('experimenter_id')
                ae_launch_host = kwargs.pop('ae_launch_host')
                ae_launch_account = kwargs.pop('ae_launch_account')
                ae_launch_setup = kwargs.pop('ae_launch_setup')

            try:
                if action == 'add':
                    logit.log("adding new LoginSetup...")
                    role = se_role
                    if role in ('root', 'superuser'):
                        raise cherrypy.HTTPError(status=401, message='You are not authorized to add launch template.')
                    exists = (
                        dbhandle.query(LoginSetup).filter(LoginSetup.experiment == exp).filter(LoginSetup.name == ae_launch_name)
                    ).first()
                    if exists:
                        message = "A login setup named %s already exists." % ae_launch_name
                    else:
                        template = LoginSetup(
                            experiment=exp,
                            name=ae_launch_name,
                            launch_host=ae_launch_host,
                            launch_account=ae_launch_account,
                            launch_setup=ae_launch_setup,
                            creator=experimenter_id,
                            created=datetime.now(utc),
                            creator_role=role,
                        )
                        dbhandle.add(template)
                        dbhandle.commit()
                        data['login_setup_id'] = template.login_setup_id
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
                    template = dbhandle.query(LoginSetup).filter(LoginSetup.login_setup_id == ae_launch_id).update(columns)
                    dbhandle.commit()
                    data['login_setup_id'] = ae_launch_id

            except IntegrityError as exc:
                message = "Integrity error: " "you are most likely using a name which " "already exists in database."
                logit.log(' '.join(exc.args))
                dbhandle.rollback()
                raise
            except SQLAlchemyError as exc:
                message = "SQLAlchemyError: " "Please report this to the administrator. " "Message: %s" % ' '.join(exc.args)
                logit.log(' '.join(exc.args))
                dbhandle.rollback()
                raise
            except BaseException:
                message = 'unexpected error ! \n' + traceback.format_exc(4)
                logit.log(' '.join(message))
                dbhandle.rollback()
                raise

        # Find templates
        if exp:  # cuz the default is find
            if kwargs.get('update_view', None) is None:
                # view flags not specified, use defaults
                data['view_active'] = 'view_active'
                data['view_inactive'] = None
                data['view_mine'] = experimenter.experimenter_id
                data['view_others'] = experimenter.experimenter_id
                data['view_analysis'] = 'view_analysis' if se_role in ('analysis', 'superuser') else None
                data['view_production'] = 'view_production' if se_role in ('production', 'superuser') else None
            else:
                data['view_active'] = kwargs.get('view_active', None)
                data['view_inactive'] = kwargs.get('view_inactive', None)
                data['view_mine'] = kwargs.get('view_mine', None)
                data['view_others'] = kwargs.get('view_others', None)
                data['view_analysis'] = kwargs.get('view_analysis', None)
                data['view_production'] = kwargs.get('view_production', None)
            data['curr_experiment'] = exp
            data['authorized'] = []

            q = (
                dbhandle.query(LoginSetup, Experiment)
                .join(Experiment)
                .filter(LoginSetup.experiment == exp)
                .order_by(LoginSetup.name)
            )

            if data['view_analysis'] and data['view_production']:
                pass
            elif data['view_analysis']:
                q = q.filter(LoginSetup.creator_role == 'analysis')
            elif data['view_production']:
                q = q.filter(LoginSetup.creator_role == 'production')

            if data['view_mine'] and data['view_others']:
                pass
            elif data['view_mine']:
                q = q.filter(LoginSetup.creator == data['view_mine'])
            elif data['view_others']:
                q = q.filter(LoginSetup.creator != data['view_others'])

            # LoginSetups don't have an active field(yet?)
            if data['view_active'] and data['view_inactive']:
                pass
            elif data['view_active']:
                pass
            elif data['view_others']:
                pass

            data['templates'] = q.all()

            for l_t in data['templates']:
                if se_role in ('root', 'superuser'):
                    data['authorized'].append(True)
                elif se_role == 'production':
                    data['authorized'].append(True)
                elif se_role == 'analysis' and l_t.LoginSetup.creator == experimenter.experimenter_id:
                    data['authorized'].append(True)
                else:
                    data['authorized'].append(False)
        data['message'] = message
        return data

    def get_campaign_id(self, dbhandle, experiment, campaign_name):
        '''
            return the campaign id for a campaign name in our experiment
        '''
        camp = dbhandle.query(Campaign).filter(Campaign.name == campaign_name, Campaign.experiment == experiment).scalar()
        return camp.campaign_id if camp else None


    def get_campaign_name(self, dbhandle, experiment, campaign_id):
        '''
            return the campaign name for a campaign id in our experiment
        '''
        camp = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id, Campaign.experiment == experiment).scalar()
        return camp.name if camp else None


    def get_campaign_stage_name(self, dbhandle, experiment, campaign_stage_id):
        '''
            return the campaign stage name for a stage id in our experiment
        '''
        stage = dbhandle.query(CampaignStage).filter(
            CampaignStage.campaign_stage_id == campaign_stage_id,
            CampaignStage.experiment == experiment,
        ).scalar()
        return stage.name if stage else None


    def campaign_add_name(self, dbhandle, user, experiment, role, **kwargs):
        """
            Add a new campaign name.
        """
        experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == user).first()
        data = {}
        name = kwargs.get('campaign_name')
        data['message'] = "ok"
        try:
            camp = Campaign(name=name, experiment=experiment, creator=experimenter.experimenter_id, creator_role=role)
            dbhandle.add(camp)
            dbhandle.commit()
            c_s = CampaignStage(
                name="stage0",
                experiment=experiment,
                campaign_id=camp.campaign_id,
                # active=False,
                #
                completion_pct="95",
                completion_type="complete",
                cs_split_type="None",
                dataset="from_parent",
                job_type_id=(
                    dbhandle.query(JobType.job_type_id).filter(JobType.name == "generic", JobType.experiment == experiment).scalar()
                    or dbhandle.query(JobType.job_type_id)
                    .filter(JobType.name == "generic", JobType.experiment == 'samdev')
                    .scalar()
                ),
                login_setup_id=(
                    dbhandle.query(LoginSetup.login_setup_id)
                    .filter(LoginSetup.name == "generic", LoginSetup.experiment == experiment)
                    .scalar()
                    or dbhandle.query(LoginSetup.login_setup_id)
                    .filter(LoginSetup.name == "generic", LoginSetup.experiment == 'samdev')
                    .scalar()
                ),
                param_overrides=[],
                software_version="v1_0",
                test_param_overrides=[],
                vo_role="Production",
                #
                creator=experimenter.experimenter_id,
                creator_role=role,
                created=datetime.now(utc),
                campaign_stage_type="regular",
            )
            dbhandle.add(c_s)
        except IntegrityError as exc:
            data['message'] = "Integrity error: " "you are most likely using a name which " "already exists in database."
            logit.log(' '.join(exc.args))
            dbhandle.rollback()
        except SQLAlchemyError as exc:
            data['message'] = "SQLAlchemyError: " "Please report this to the administrator. " "Message: %s" % ' '.join(exc.args)
            logit.log(' '.join(exc.args))
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

    def launch_campaign(self, dbhandle, getconfig, gethead, samhandle, experiment, role, user, basedir, campaign_id, launcher,
                        dataset_override=None, parent_submission_id=None, param_overrides=None,
                        test_login_setup=None, test_launch=False, output_commands=False):

        '''
            Find the starting stage in a campaign, and launch it with
            launch_jobs(campaign_stage_id=...)
        '''

        # subquery to count dependencies
        q = (
            text(
                "select campaign_stage_id from campaign_stages "
                " where campaign_id = :campaign_id "
                "   and 0 = (select count(campaign_dep_id) "
                "  from campaign_dependencies "
                " where provides_campaign_stage_id = campaign_stage_id)"
            )
            .bindparams(campaign_id=campaign_id)
            .columns(campaign_stage_id=Integer)
        )

        stages = dbhandle.execute(q).fetchall()

        logit.log("launch_campaign: got stages %s" % repr(stages))

        if len(stages) == 1:
            return self.poms_service.taskPOMS.launch_jobs(dbhandle, getconfig, gethead, samhandle, experiment, role,
                                                          user, basedir, stages[0][0], launcher, dataset_override,
                                                          parent_submission_id, param_overrides,
                                                          test_login_setup, test_launch, output_commands)
        raise AssertionError("Cannot determine which stage in campaign to launch of %d candidates" % len(stages))

    def get_recoveries(self, dbhandle, cid):
        '''
        Build the recoveries dict for job_types cids
        '''
        recs = (
            dbhandle.query(CampaignRecovery)
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
                if rec.param_overrides in ('', '{}', '[]'):
                    rec.param_overrides = []
                rec_vals = [rec.recovery_type.name, json.loads(rec.param_overrides)]
            else:
                rec_vals = [rec.recovery_type.name, rec.param_overrides]

            rec_list.append(rec_vals)

        logit.log("get_recoveries(%d) returning %s" % (cid, repr(rec_list)))
        return rec_list

    def fixup_recoveries(self, dbhandle, job_type_id, recoveries):
        '''
         fixup_recoveries -- factored out so we can use it
            from either edit endpoint.
         Given a JSON dump of the recoveries, clean out old
         recoveriy entries, add new ones.  It probably should
         check if they're actually different before doing this..
        '''
        (dbhandle.query(CampaignRecovery).filter(CampaignRecovery.job_type_id == job_type_id).delete(synchronize_session=False))
        i = 0
        for rtn in json.loads(recoveries):
            rect = rtn[0]
            recpar = rtn[1]
            rt = dbhandle.query(RecoveryType).filter(RecoveryType.name == rect).first()
            cr = CampaignRecovery(job_type_id=job_type_id, recovery_order=i, recovery_type=rt, param_overrides=recpar)
            i = i + 1
            dbhandle.add(cr)

    def job_type_edit(self, dbhandle, username, exp, ses_role, *args, **kwargs):
        """
            callback from edit screen/client.
        """
        data = {}
        message = None
        data['exp_selections'] = (
            dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root", "public"])).order_by(Experiment.experiment)
        )
        action = kwargs.pop('action', None)
        experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == username).first()
        data['curr_experiment'] = exp
        pcl_call = int(kwargs.pop('pcl_call', 0))
        # email is the info we know about the user in POMS DB.
        pc_username = kwargs.pop('pc_username', None)

        if action == 'delete':
            self.poms_service.permissions.can_modify(dbhandle, user, experiment, se_role, "JobType", name=name, experiment=experiment)
            name = kwargs.pop('ae_definition_name')
            if isinstance(name, str):
                name = name.strip()
            if pcl_call == 1:  # Enter here if the access was from the poms_client
                cid = job_type_id = dbhandle.query(JobType).filter(JobType.name == name).first().job_type_id
            else:
                cid = kwargs.pop('job_type_id')
            try:
                (dbhandle.query(CampaignRecovery).filter(CampaignRecovery.job_type_id == cid).delete(synchronize_session=False))
                (dbhandle.query(JobType).filter(JobType.job_type_id == cid).delete(synchronize_session=False))
                dbhandle.commit()
            except SQLAlchemyError as exc:
                message = ('The campaign definition, %s, ' 'has been used and may not be deleted.') % name
                logit.log(message)
                logit.log(' '.join(exc.args))
                dbhandle.rollback()

        if action in ('add', 'edit'):
            logit.log("job_type_edit: add or exit case")
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
                    job_type_id = dbhandle.query(JobType).filter(JobType.name == name).first().job_type_id  # Check here!
                else:
                    pass
                input_files_per_job = kwargs.pop('ae_input_files_per_job', 0)
                output_files_per_job = kwargs.pop('ae_output_files_per_job', 0)
                output_file_patterns = kwargs.pop('ae_output_file_patterns')
                launch_script = kwargs.pop('ae_launch_script')
                recoveries = kwargs.pop('ae_definition_recovery', "[]")

                # Getting the info that was not passed by the poms_client
                # arguments

                if output_file_patterns in (None, ""):
                    output_file_patterns = '%'

                if launch_script in (None, ""):
                    raise AssertionError("launch_script is required")

                if definition_parameters in (None, ""):
                    definition_parameters = []
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
                    role = ses_role
                    if role in ('root', 'superuser'):
                        raise cherrypy.HTTPError(status=401, message=('You are not authorized ' 'to add campaign definition.'))
                    else:
                        j_t = JobType(
                            name=name,
                            experiment=exp,
                            input_files_per_job=input_files_per_job,
                            output_files_per_job=output_files_per_job,
                            output_file_patterns=output_file_patterns,
                            launch_script=launch_script,
                            definition_parameters=definition_parameters,
                            creator=experimenter_id,
                            created=datetime.now(utc),
                            creator_role=role,
                        )

                    dbhandle.add(j_t)
                    dbhandle.flush()
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
                    j_t = dbhandle.query(JobType).filter(JobType.job_type_id == job_type_id).update(columns)

                if recoveries:
                    self.fixup_recoveries(dbhandle, job_type_id, recoveries)
                    dbhandle.commit()

            except IntegrityError as exc:
                message = "Integrity error: " "you are most likely using a name which " "already exists in database."
                logit.log(' '.join(exc.args))
                dbhandle.rollback()
            except SQLAlchemyError as exc:
                message = "SQLAlchemyError: " "Please report this to the administrator. " "Message: %s" % ' '.join(exc.args)
                logit.log(' '.join(exc.args))
                dbhandle.rollback()
            else:
                dbhandle.commit()

        # Find definitions
        if exp:  # cuz the default is find

            if kwargs.get('update_view', None) is None:
                # view flags not specified, use defaults
                data['view_active'] = 'view_active'
                data['view_inactive'] = None
                data['view_mine'] = experimenter.experimenter_id
                data['view_others'] = experimenter.experimenter_id
                data['view_analysis'] = 'view_analysis' if ses_role in ('analysis', 'superuser') else None
                data['view_production'] = 'view_production' if ses_role in ('production', 'superuser') else None
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
            q = dbhandle.query(JobType, Experiment).join(Experiment).filter(JobType.experiment == exp).order_by(JobType.name)

            if data['view_analysis'] and data['view_production']:
                pass
            elif data['view_analysis']:
                q = q.filter(JobType.creator_role == 'analysis')
            elif data['view_production']:
                q = q.filter(JobType.creator_role == 'production')

            if data['view_mine'] and data['view_others']:
                pass
            elif data['view_mine']:
                q = q.filter(JobType.creator == data['view_mine'])
            elif data['view_others']:
                q = q.filter(JobType.creator != data['view_others'])

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
                if ses_role in ['root', 'superuser']:
                    data['authorized'].append(True)
                elif df.JobType.creator_role == 'production' and ses_role == "production":
                    data['authorized'].append(True)
                elif df.JobType.creator_role == ses_role and df.JobType.creator == experimenter.experimenter_id:
                    data['authorized'].append(True)
                else:
                    data['authorized'].append(False)

            recs_dict = {}
            for cid in cids:
                recs_dict[cid] = json.dumps(self.get_recoveries(dbhandle, cid))
            data['recoveries'] = recs_dict
            data['rtypes'] = dbhandle.query(RecoveryType.name, RecoveryType.description).order_by(RecoveryType.name).all()

        data['message'] = message
        return data

    def make_test_campaign_for(self, dbhandle, user, experiment, role, campaign_def_id, campaign_def_name):
        """
            Build a test_campaign for a given campaign definition
        """
        experimenter_id = dbhandle.query(Experimenter.experimenter_id).filter(Experimenter.username == user).first()

        c_s = (
            dbhandle.query(CampaignStage)
            .filter(CampaignStage.job_type_id == campaign_def_id, CampaignStage.name == "_test_%s" % campaign_def_name)
            .first()
        )
        if not c_s:
            l_t = dbhandle.query(LoginSetup).filter(LoginSetup.experiment == experiment).first()
            c_s = CampaignStage()
            c_s.job_type_id = campaign_def_id
            c_s.name = "_test_%s" % campaign_def_name
            c_s.experiment = experiment
            c_s.creator = experimenter_id
            c_s.created = datetime.now(utc)
            c_s.updated = datetime.now(utc)
            c_s.vo_role = "Production" if role == "production" else "Analysis"
            c_s.creator_role = role
            c_s.dataset = ""
            c_s.login_setup_id = l_t.login_setup_id
            c_s.software_version = ""
            c_s.campaign_stage_type = 'regular'
            dbhandle.add(c_s)
            dbhandle.commit()
            c_s = (
                dbhandle.query(CampaignStage)
                .filter(CampaignStage.job_type_id == campaign_def_id, CampaignStage.name == "_test_%s" % campaign_def_name)
                .first()
            )
        return c_s.campaign_stage_id

    def campaign_stage_edit(self, dbhandle, user, exp, role, *args, **kwargs):
        """
            callback for campaign stage edit screens to update campaign record
            takes action = 'edit'/'add'/ etc.
            sesshandle is the cherrypy.session instead of cherrypy.session.get method
        """
        data = {}
        experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == user).first()

        message = None
        data['exp_selections'] = (
            dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root", "public"])).order_by(Experiment.experiment)
        )
        # for k,v in kwargs.items():
        #    print ' k=%s, v=%s ' %(k,v)
        action = kwargs.pop('action', None)
        # pcl_call == 1 means the method was access through the poms_client.
        pcl_call = int(kwargs.pop('pcl_call', 0))
        # email is the info we know about the user in POMS DB.
        pc_username = kwargs.pop('pc_username', None)
        campaign_id = kwargs.pop('ae_campaign_id', None)

        if action == 'delete':
            self.poms_service.permissions.can_modify(dbhandle, user, experiment, se_role, "CampaignStage", name=name, campaign_id=campaign_id, experiment=experiment)
            name = kwargs.get('ae_stage_name', kwargs.get('name', None))
            if isinstance(name, str):
                name = name.strip()
            if pcl_call == 1:
                campaign_stage_id = (
                    dbhandle.query(CampaignStage)
                    .filter(CampaignStage.name == name, CampaignStage.experiment == exp, CampaignStage.campaign_id == campaign_id)
                    .first()
                    .campaign_stage_id
                    if name
                    else None
                )
            else:
                campaign_stage_id = kwargs.pop('campaign_stage_id')

            if campaign_stage_id:
                try:
                    (
                        dbhandle.query(CampaignDependency)
                        .filter(
                            or_(
                                CampaignDependency.needs_campaign_stage_id == campaign_stage_id,
                                CampaignDependency.provides_campaign_stage_id == campaign_stage_id,
                            )
                        )
                        .delete(synchronize_session=False)
                    )
                    (
                        dbhandle.query(CampaignStage)
                        .filter(CampaignStage.campaign_stage_id == campaign_stage_id)
                        .delete(synchronize_session=False)
                    )
                    dbhandle.commit()
                except SQLAlchemyError as e:
                    message = "The campaign stage {}, has been used and may not be deleted.".format(name)
                    logit.log(message)
                    logit.log(' '.join(e.args))
                    dbhandle.rollback()
                    raise

        elif action in ('add', 'edit'):
            logit.log("campaign_stage_edit: add or edit case")
            name = kwargs.pop('ae_stage_name')
            if isinstance(name, str):
                name = name.strip()
            # active = (kwargs.pop('ae_active') in ('True', 'true', '1', 'Active', True, 1))
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

                # print("************* exp={}, launch_name={}, campaign_definition_name={}"
                #        .format(exp, launch_name, campaign_definition_name))
                login_setup_id = (
                    dbhandle.query(LoginSetup)
                    .filter(LoginSetup.experiment == exp)
                    .filter(LoginSetup.name == launch_name)
                    .first()
                    .login_setup_id
                )
                job_type_id = (
                    dbhandle.query(JobType)
                    .filter(JobType.name == campaign_definition_name, JobType.experiment == exp)
                    .first()
                    .job_type_id
                )
                if action == 'edit':
                    c_s = (
                        dbhandle.query(CampaignStage)
                        .filter(
                            CampaignStage.name == name, CampaignStage.experiment == exp, CampaignStage.campaign_id == campaign_id
                        )
                        .first()
                    )
                    if c_s:
                        campaign_stage_id = c_s.campaign_stage_id
                    else:
                        campaign_stage_id = None
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

            # backwards combatability
            if 'campaigns' in depends and not 'campaign_stages' in depends:
                depends['campaign_stages'] = depends['campaigns']

            # fail if they're setting up a trivial infinite loop
            if split_type in [None, 'None', 'none', 'Draining'] and name in [x[0] for x in depends['campaign_stages']]:

                raise cherrypy.HTTPError(
                    404,
                    "This edit would make an infinite loop. "
                    "Go Back in your browser and set cs_split_type or remove self-dependency.",
                )

            try:
                if action == 'add':
                    if not completion_pct:
                        completion_pct = 95
                    if role not in ('analysis', 'production'):
                        message = 'Your active role must be analysis ' 'or production to add a campaign.'
                    else:
                        c_s = CampaignStage(
                            name=name,
                            experiment=exp,
                            vo_role=vo_role,
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
                            creator_role=role,
                            campaign_stage_type=campaign_type,
                            campaign_id=campaign_id,
                        )
                        dbhandle.add(c_s)
                        dbhandle.commit()
                        campaign_stage_id = c_s.campaign_stage_id
                elif action == 'edit':
                    columns = {
                        "name": name,
                        "vo_role": vo_role,
                        # "active": active,
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
                        "campaign_id": campaign_id,
                    }
                    dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).update(columns)
                # now redo dependencies
                (
                    dbhandle.query(CampaignDependency)
                    .filter(CampaignDependency.provides_campaign_stage_id == campaign_stage_id)
                    .delete(synchronize_session=False)
                )
                logit.log("depends for %s(%s) are: %s" % (campaign_stage_id, name, depends))
                if 'campaign_stages' in depends:
                    dep_stages = (
                        dbhandle.query(CampaignStage)
                        .filter(
                            CampaignStage.name.in_(depends['campaign_stages']),
                            CampaignStage.campaign_id == campaign_id,
                            CampaignStage.experiment == exp,
                        )
                        .all()
                    )
                elif 'campaigns' in depends:
                    # backwards compatibility
                    dep_stages = (
                        dbhandle.query(CampaignStage)
                        .filter(
                            CampaignStage.name.in_(depends['campaigns']),
                            CampaignStage.campaign_id == campaign_id,
                            CampaignStage.experiment == exp,
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
                        file_patterns=depends['file_patterns'][i],
                    )
                    dbhandle.add(dep)
                dbhandle.commit()
            except IntegrityError as exc:
                message = "Integrity error: " "You are most likely using a name " "which already exists in database."
                logit.log(' '.join(exc.args))
                dbhandle.rollback()
            except SQLAlchemyError as exc:
                message = "SQLAlchemyError: " "Please report this to the administrator." "Message: {}".format(' '.join(exc.args))
                logit.log(' '.join(exc.args))
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
                data['view_analysis'] = 'view_analysis' if role in ('analysis', 'superuser') else None
                data['view_production'] = 'view_production' if role in ('production', 'superuser') else None
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
            jumpto = kwargs.pop('jump_to_campaign', None)
            data['state'] = state
            data['curr_experiment'] = exp
            data['authorized'] = []
            cquery = dbhandle.query(CampaignStage, Campaign).outerjoin(Campaign).filter(CampaignStage.experiment == exp)
            if data['view_analysis'] and data['view_production']:
                pass
            elif data['view_analysis']:
                cquery = cquery.filter(CampaignStage.creator_role == 'analysis')
            elif data['view_production']:
                cquery = cquery.filter(CampaignStage.creator_role == 'production')

            if data['view_mine'] and data['view_others']:
                pass
            elif data['view_mine']:
                cquery = cquery.filter(CampaignStage.creator == data['view_mine'])
            elif data['view_others']:
                cquery = cquery.filter(CampaignStage.creator != data['view_others'])

            cquery = cquery.order_by(Campaign.name, CampaignStage.name)
            # this bit has to go onto cquery last
            # -- make sure if we're jumping to a given campaign id
            # that we *have* it in the list...
            if jumpto is not None:
                c2 = (
                    dbhandle.query(CampaignStage, Campaign)
                    .filter(CampaignStage.campaign_stage_id == jumpto)
                    .filter(CampaignStage.campaign_id == Campaign.campaign_id)
                )
                # we have to use union_all() and not union()to avoid
                # postgres whining about not knowing how to compare JSON
                # fields... sigh.  (It could just string compare them...)
                cquery = c2.union_all(cquery)

            data['campaign_stages'] = cquery.all()
            data['definitions'] = dbhandle.query(JobType).filter(JobType.experiment == exp).order_by(JobType.name)
            data['templates'] = dbhandle.query(LoginSetup).filter(LoginSetup.experiment == exp).order_by(LoginSetup.name)
            csq = data['campaign_stages']

            for c_s in csq:
                if role in ('root', 'superuser'):
                    data['authorized'].append(True)
                elif c_s.CampaignStage.creator_role == 'production' and role == 'production':
                    data['authorized'].append(True)
                elif c_s.CampaignStage.creator_role == role and c_s.CampaignStage.creator == experimenter.experimenter_id:
                    data['authorized'].append(True)
                else:
                    data['authorized'].append(False)

            depends = {}
            for c in csq:
                cid = c.CampaignStage.campaign_stage_id
                sql = dbhandle.query(
                    CampaignDependency.provides_campaign_stage_id, CampaignStage.name, CampaignDependency.file_patterns
                ).filter(
                    CampaignDependency.provides_campaign_stage_id == cid,
                    CampaignStage.campaign_stage_id == CampaignDependency.needs_campaign_stage_id,
                )
                deps = {"campaign_stages": [row[1] for row in sql.all()], "file_patterns": [row[2] for row in sql.all()]}
                depends[cid] = json.dumps(deps)
            data['depends'] = depends

        data['message'] = message
        return data

    def campaign_stage_edit_query(self, dbhandle, **kwargs):
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
        c_s = dbhandle.query(CampaignStage).filter(CampaignStage.name == campaign_name).first()
        who = dbhandle.query(Experimenter).filter(Experimenter.username == experimenter_name).first()
        s = Submission()
        s.campaign_stage_id = c_s.campaign_stage_id
        s.job_type_id = c_s.job_type_id
        s.status = 'started'
        s.created = datetime.now(utc)
        s.updated = datetime.now(utc)
        s.updater = who.experimenter_id
        s.creator = who.experimenter_id
        s.command_executed = command_executed
        if dataset_name:
            s.input_dataset = dataset_name
        dbhandle.add(s)
        dbhandle.commit()
        return "Submission=%d" % s.submission_id

    def campaign_deps_ini(self, dbhandle, experiment, name=None, stage_id=None, login_setup=None, job_type=None, full=None):
        '''
            Generate ini-format dump of campaign and dependencies
        '''
        res = []
        campaign_stages = []
        jts = set()
        lts = set()
        the_campaign = None

        if job_type is not None:
            res.append("# with job_type %s" % job_type)
            j_t = dbhandle.query(JobType).filter(JobType.name == job_type, JobType.experiment == experiment).first()
            if j_t:
                jts.add(j_t)

        if login_setup is not None:
            res.append("# with login_setup: %s" % login_setup)
            l_t = (
                dbhandle.query(LoginSetup)
                .filter(LoginSetup.name == login_setup, LoginSetup.experiment == experiment)
                .first()
            )
            if l_t:
                lts.add(l_t)

        if name is not None:
            the_campaign = (
                dbhandle.query(Campaign).filter(Campaign.name == name, Campaign.experiment == experiment).scalar()
            )

            if the_campaign is None:
                return f"Error: Campaign '{name}' was not found for '{experiment}' experiment"
            #
            # campaign_stages = dbhandle.query(CampaignStage).join(Campaign).filter(
            #     Campaign.name == name,
            #     CampaignStage.campaign_id == Campaign.campaign_id).all()
            campaign_stages = the_campaign.stages.all()

        if stage_id is not None:
            cidl1 = (
                dbhandle.query(CampaignDependency.needs_campaign_stage_id)
                .filter(CampaignDependency.provides_campaign_stage_id == stage_id)
                .all()
            )
            cidl2 = (
                dbhandle.query(CampaignDependency.provides_campaign_stage_id)
                .filter(CampaignDependency.needs_campaign_stage_id == stage_id)
                .all()
            )
            s = set([stage_id])
            s.update(cidl1)
            s.update(cidl2)
            campaign_stages = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id.in_(s)).all()
        cnames = {}
        for c_s in campaign_stages:
            cnames[c_s.campaign_stage_id] = c_s.name

        # lookup relevant dependencies
        dmap = {}
        for cid in cnames:
            dmap[cid] = []

        fpmap = {}
        for cid in cnames:
            c_dl = (
                dbhandle.query(CampaignDependency)
                .filter(CampaignDependency.provides_campaign_stage_id == cid)
                .filter(CampaignDependency.needs_campaign_stage_id.in_(cnames.keys()))
                .all()
            )
            for c_d in c_dl:
                if c_d.needs_campaign_stage_id in cnames.keys():
                    dmap[cid].append(c_d.needs_campaign_stage_id)
                    fpmap[(cid, c_d.needs_campaign_stage_id)] = c_d.file_patterns

        # sort by dependencies(?)
        cidl = list(cnames.keys())
        for cid in cidl:
            for dcid in dmap[cid]:
                if cidl.index(dcid) < cidl.index(cid):
                    cidl[cidl.index(dcid)], cidl[cidl.index(cid)] = cidl[cidl.index(cid)], cidl[cidl.index(dcid)]

        if the_campaign:
            res.append("[campaign]")
            res.append("experiment=%s" % the_campaign.experiment)
            res.append("poms_role=%s" % the_campaign.creator_role)
            res.append("name=%s" % the_campaign.name)
            res.append("state=%s" % ("Active" if the_campaign.active else "Inactive"))

            res.append("campaign_stage_list=%s" % ",".join(map(cnames.get, cidl)))
            res.append("")

            # if full in ('1', 'y', 'Y', 't', 'T'):
            if True:
                positions = None
                defaults = the_campaign.defaults or {}
                if defaults:
                    if "defaults" in defaults:
                        positions = defaults["positions"]
                        defaults = defaults["defaults"]

                    if defaults:
                        res.append("[campaign_defaults]")
                        # for (k, v) in defaults.items():
                        #     res.append("%s=%s" % (k, v))
                        res.append("vo_role=%s" % defaults.get("vo_role"))
                        # res.append("state=%s" % defaults.get("state"))
                        res.append("software_version=%s" % defaults.get("software_version"))
                        res.append("dataset_or_split_data=%s" % defaults.get("dataset"))
                        res.append("cs_split_type=%s" % defaults.get("cs_split_type"))
                        res.append("completion_type=%s" % defaults.get("completion_type"))
                        res.append("completion_pct=%s" % defaults.get("completion_pct"))
                        res.append("param_overrides=%s" % (defaults.get("param_overrides") or "[]"))
                        res.append("test_param_overrides=%s" % (defaults.get("test_param_overrides") or "[]"))
                        res.append("login_setup=%s" % (defaults.get("login_setup") or "generic"))
                        res.append("job_type=%s" % (defaults.get("job_type") or "generic"))
                        res.append("")
                    else:
                        defaults = {}

            if full in ('1', 'y', 'Y', 't', 'T'):
                if positions:
                    res.append("[node_positions]")
                    for (n, (k, v)) in enumerate(positions.items()):
                        res.append('nxy{}=["{}", {}, {}]'.format(n, k, v['x'], v['y']))
                    res.append("")

        for c_s in campaign_stages:
            res.append("[campaign_stage %s]" % c_s.name)
            # res.append("name=%s" % c_s.name)
            if c_s.vo_role != defaults.get("vo_role"):
                res.append("vo_role=%s" % c_s.vo_role)
            if c_s.software_version != defaults.get("software_version"):
                res.append("software_version=%s" % c_s.software_version)
            if c_s.dataset != defaults.get("dataset_or_split_data"):
                res.append("dataset_or_split_data=%s" % c_s.dataset)
            if c_s.cs_split_type != defaults.get("cs_split_type"):
                res.append("cs_split_type=%s" % c_s.cs_split_type)
            if c_s.completion_type != defaults.get("completion_type"):
                res.append("completion_type=%s" % c_s.completion_type)
            if str(c_s.completion_pct) != defaults.get("completion_pct"):
                res.append("completion_pct=%s" % c_s.completion_pct)
            if json.dumps(c_s.param_overrides) != defaults.get("param_overrides"):
                res.append("param_overrides=%s" % json.dumps(c_s.param_overrides or []))
            if json.dumps(c_s.test_param_overrides) != defaults.get("test_param_overrides"):
                res.append("test_param_overrides=%s" % json.dumps(c_s.test_param_overrides or []))
            if c_s.login_setup_obj.name != defaults.get("login_setup"):
                res.append("login_setup=%s" % c_s.login_setup_obj.name)
            if c_s.job_type_obj.name != defaults.get("job_type"):
                res.append("job_type=%s" % c_s.job_type_obj.name)
            jts.add(c_s.job_type_obj)
            lts.add(c_s.login_setup_obj)
            res.append("")

        for l_t in lts:
            res.append("[login_setup %s]" % l_t.name)
            res.append("host=%s" % l_t.launch_host)
            res.append("account=%s" % l_t.launch_account)
            res.append("setup=%s" % (l_t.launch_setup.replace("\r", ";").replace("\n", ";").replace(";;", ";").replace(";;", ";")))
            res.append("")

        for j_t in jts:
            res.append("[job_type %s]" % j_t.name)
            res.append("launch_script=%s" % j_t.launch_script)
            res.append("parameters=%s" % json.dumps(j_t.definition_parameters))
            res.append("output_file_patterns=%s" % j_t.output_file_patterns)
            res.append("recoveries = %s" % json.dumps(self.get_recoveries(dbhandle, j_t.job_type_id)))
            res.append("")

        # still need dependencies
        for cid in dmap:
            if not dmap[cid]:
                continue
            res.append("[dependencies %s]" % cnames[cid])
            i = 0
            for dcid in dmap[cid]:
                i = i + 1
                res.append("campaign_stage_%d = %s" % (i, cnames[dcid]))
                res.append("file_pattern_%d = %s" % (i, fpmap[(cid, dcid)]))
            res.append("")

        res.append("")

        return "\n".join(res)


    def campaign_deps_svg(self, dbhandle, config_get, campaign_name=None, campaign_stage_id=None):
        """
            return campaign dependencies as an SVG graph
            uses "dot" to generate the drawing
        """
        if campaign_name is not None:
            csl = (
                dbhandle.query(CampaignStage)
                .join(Campaign)
                .filter(Campaign.name == campaign_name, CampaignStage.campaign_id == Campaign.campaign_id)
                .all()
            )

        if campaign_stage_id is not None:
            cidl1 = (
                dbhandle.query(CampaignDependency.needs_campaign_stage_id)
                .filter(CampaignDependency.provides_campaign_stage_id == campaign_stage_id)
                .all()
            )
            cidl2 = (
                dbhandle.query(CampaignDependency.provides_campaign_stage_id)
                .filter(CampaignDependency.needs_campaign_stage_id == campaign_stage_id)
                .all()
            )
            s = set([campaign_stage_id])
            s.update(cidl1)
            s.update(cidl2)
            csl = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id.in_(s)).all()

        c_ids = deque()
        for c_s in csl:
            c_ids.append(c_s.campaign_stage_id)

        logit.log(logit.INFO, "campaign_deps: c_ids=%s" % repr(c_ids))

        try:
            pdot = subprocess.Popen(
                "tee /tmp/dotstuff | dot -Tsvg", shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines=True
            )
            pdot.stdin.write('digraph "{} Dependencies" {{\n'.format(campaign_name))
            pdot.stdin.write('node [shape=box, style=rounded, ' 'color=lightgrey, fontcolor=black]\n' 'rankdir = "LR";\n')
            baseurl = "{}/campaign_stage_info?campaign_stage_id=".format(config_get("pomspath"))

            for c_s in csl:
                tot = (
                    dbhandle.query(func.count(Submission.submission_id))
                    .filter(Submission.campaign_stage_id == c_s.campaign_stage_id)
                    .one()
                )[0]
                logit.log(logit.INFO, "campaign_deps: tot=%s" % repr(tot))
                pdot.stdin.write(
                    ('c_s{:d} [URL="{}{:d}",label="{}\\n' 'Submissions {:d}",color={}];\n').format(
                        c_s.campaign_stage_id, baseurl, c_s.campaign_stage_id, c_s.name, tot, "black"
                    )
                )

            c_dl = dbhandle.query(CampaignDependency).filter(CampaignDependency.needs_campaign_stage_id.in_(c_ids)).all()

            for c_d in c_dl:
                if c_d.needs_campaign_stage_id in c_ids and c_d.provides_campaign_stage_id in c_ids:
                    pdot.stdin.write('c_s{:d} -> c_s{:d};\n'.format(c_d.needs_campaign_stage_id, c_d.provides_campaign_stage_id))

            pdot.stdin.write('}\n')
            pdot.stdin.close()
            ptext = pdot.stdout.read()
            pdot.wait()
        except BaseException:
            ptext = ""
            raise
        # return bytes(ptext, encoding="utf-8")
        return ptext

    def show_campaigns(self, dbhandle, user, experiment, role, **kwargs):
        '''
            Return data for campaigns table for current experiment, etc.
        '''
        experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == user).first()
        action = kwargs.get('action', None)
        msg = "OK"
        se_role = experimenter.session_role
        if action == 'delete':
            campaign_id = kwargs.get('del_campaign_id')
            self.poms_service.permissions.can_modify(dbhandle, user, experiment, se_role, "Campaign", item_id=campaign_id)

            campaign = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).scalar()
            subs = (
                dbhandle.query(Submission)
                .join(CampaignStage, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
                .filter(CampaignStage.campaign_id == campaign_id)
            )
            if subs.count() > 0:
                msg = "This campaign has been submitted.  It cannot be deleted."
            else:
                # Delete all dependency records for all campaign stages
                dbhandle.query(CampaignDependency).filter(
                    or_(
                        CampaignDependency.provider.has(CampaignStage.campaign_id == campaign_id),
                        CampaignDependency.consumer.has(CampaignStage.campaign_id == campaign_id),
                    )
                ).delete(synchronize_session=False)
                # Delete all campaign stages
                # dbhandle.query(CampaignStage).filter(CampaignStage.campaign_id == campaign_id).delete(synchronize_session=False)
                campaign.stages.delete()
                # Delete all campaign tag records
                dbhandle.query(CampaignsTag).filter(CampaignsTag.campaign_id == campaign_id).delete()
                # Delete the campaign
                # dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).delete(synchronize_session=False)
                dbhandle.delete(campaign)
                dbhandle.commit()
                msg = ("Campaign named %s with campaign_id %s " "and related CampagnStages were deleted.") % (
                    kwargs.get('del_campaign_name'),
                    campaign_id,
                )

        data = {'authorized': []}
        q = (
            dbhandle.query(Campaign)
            .options(joinedload(Campaign.experimenter_creator_obj))
            .filter(Campaign.experiment == experimenter.session_experiment)
            .order_by(Campaign.name)
        )

        if kwargs.get('update_view', None) is None:
            # view flags not specified, use defaults
            data['view_active'] = 'view_active'
            data['view_inactive'] = None
            data['view_mine'] = experimenter.experimenter_id
            data['view_others'] = experimenter.experimenter_id
            data['view_analysis'] = 'view_analysis' if se_role in ('analysis', 'superuser') else None
            data['view_production'] = 'view_production' if se_role in ('production', 'superuser') else None
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
            q = q.filter(Campaign.creator == experimenter.experimenter_id)
        elif data['view_others']:
            q = q.filter(Campaign.creator != experimenter.experimenter_id)

        if data['view_active'] and data['view_inactive']:
            pass
        elif data['view_active']:
            q = q.filter(Campaign.active == True)  # Do NOT optimize the condition!
        elif data['view_inactive']:
            q = q.filter(Campaign.active != True)  # Do NOT optimize the condition!

        csl = q.all()

        if not csl:
            return csl, "", msg, data
        for c_s in csl:
            if role == 'superuser':
                data['authorized'].append(True)
            elif c_s.creator_role == 'production' and role == 'production':
                data['authorized'].append(True)
            elif c_s.creator_role == role and c_s.creator == experimenter.experimenter_id:
                data['authorized'].append(True)
            else:
                data['authorized'].append(False)

        last_activity_l = (
            dbhandle.query(func.max(Submission.updated))
            .join(CampaignStage, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
            .join(Campaign, CampaignStage.campaign_id == Campaign.campaign_id)
            .filter(Campaign.experiment == experimenter.session_experiment)
            .first()
        )
        last_activity = ""
        if last_activity_l and last_activity_l[0]:
            if datetime.now(utc) - last_activity_l[0] > timedelta(days=7):
                last_activity = last_activity_l[0].strftime("%Y-%m-%d %H:%M:%S")
        return csl, last_activity, msg, data

    def show_campaign_stages(self, dbhandle, user, experiment, se_role, campaign_ids=None, tmin=None, tmax=None, tdays=7, campaign_name=None, **kwargs):
        """
            give campaign information about campaign_stages with activity
            in the time window for a given experiment
        :rtype: object
        """
        (tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays) = self.poms_service.utilsPOMS.handle_dates(
            tmin, tmax, tdays, 'show_campaign_stages/%s/%s?' % (experiment, se_role)
        )
        experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == user).first()

        csq = (
            dbhandle.query(CampaignStage)
            .options(joinedload('experiment_obj'))
            .options(joinedload('campaign_obj'))
            .options(joinedload(CampaignStage.experimenter_holder_obj))
            .options(joinedload(CampaignStage.experimenter_creator_obj))
            .options(joinedload(CampaignStage.experimenter_updater_obj))
            .order_by(CampaignStage.experiment)
        )

        if experiment:
            csq = csq.filter(CampaignStage.experiment == experiment)

        data = {}
        if kwargs.get('update_view', None) is None:
            # view flags not specified, use defaults
            data['view_active'] = 'view_active'
            data['view_inactive'] = None
            data['view_mine'] = experimenter.experimenter_id
            data['view_others'] = experimenter.experimenter_id
            data['view_analysis'] = 'view_analysis' if se_role in ('analysis', 'superuser') else None
            data['view_production'] = 'view_production' if se_role in ('production', 'superuser') else None
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
            csq = csq.filter(CampaignStage.creator_role == 'analysis')
        elif data['view_production']:
            csq = csq.filter(CampaignStage.creator_role == 'production')

        if data['view_mine'] and data['view_others']:
            pass
        elif data['view_mine']:
            csq = csq.filter(CampaignStage.creator == experimenter.experimenter_id)
        elif data['view_others']:
            csq = csq.filter(CampaignStage.creator != experimenter.experimenter_id)

        if data['view_active'] and data['view_inactive']:
            pass
        elif data['view_active']:
            csq = csq.filter(CampaignStage.campaign_obj.has(Campaign.active == True))  # Do NOT optimize the condition!
        elif data['view_inactive']:
            csq = csq.filter(CampaignStage.campaign_obj.has(Campaign.active != True))  # Do NOT optimize the condition!

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
        data['authorized'] = []
        for c_s in campaign_stages:
            if se_role != 'analysis':
                data['authorized'].append(True)
            elif c_s.creator == experimenter.experimenter_id:
                data['authorized'].append(True)
            else:
                data['authorized'].append(False)
        return campaign_stages, tmin, tmax, tmins, tmaxs, tdays, nextlink, prevlink, time_range_string, data

    def reset_campaign_split(self, dbhandle, campaign_stage_id):
        """
            reset a campaign_stages cs_last_split field so the sequence
            starts over
        """
        campaign_stage_id = int(campaign_stage_id)

        c_s = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
        c_s.cs_last_split = None
        dbhandle.commit()

    # @pomscache.cache_on_arguments()
    def campaign_stage_info(self, dbhandle, config_get, experiment, role, campaign_stage_id, tmin=None, tmax=None, tdays=None):
        """
           Give information related to a campaign stage for the campaign_stage_info page
        """

        campaign_stage_id = int(campaign_stage_id)

        campaign_stage_info = (
            dbhandle.query(CampaignStage, Experimenter)
            .filter(CampaignStage.campaign_stage_id == campaign_stage_id, CampaignStage.creator == Experimenter.experimenter_id)
            .first()
        )

        # default to time window of campaign
        if tmin is None and tdays is None:
            tmin = campaign_stage_info.CampaignStage.created
            tmax = datetime.now(utc)

        tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays = self.poms_service.utilsPOMS.handle_dates(
            tmin, tmax, tdays, 'campaign_stage_info/%s/%s?' % (experiment, role)
        )

        last_activity_l = (
            dbhandle.query(func.max(Submission.updated)).filter(Submission.campaign_stage_id == campaign_stage_id).first()
        )
        logit.log("got last_activity_l %s" % repr(last_activity_l))
        if last_activity_l[0] and datetime.now(utc) - last_activity_l[0] > timedelta(days=7):
            last_activity = last_activity_l[0].strftime("%Y-%m-%d %H:%M:%S")
        else:
            last_activity = ""
        logit.log("after: last_activity %s" % repr(last_activity))

        campaign_definition_info = (
            dbhandle.query(JobType, Experimenter)
            .filter(
                JobType.job_type_id == campaign_stage_info.CampaignStage.job_type_id,
                JobType.creator == Experimenter.experimenter_id,
            )
            .first()
        )
        login_setup_info = (
            dbhandle.query(LoginSetup, Experimenter)
            .filter(
                LoginSetup.login_setup_id == campaign_stage_info.CampaignStage.login_setup_id,
                LoginSetup.creator == Experimenter.experimenter_id,
            )
            .first()
        )
        campaigns = dbhandle.query(Campaign).join(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).all()

        launched_campaigns = (
            dbhandle.query(CampaignStageSnapshot).filter(CampaignStageSnapshot.campaign_stage_id == campaign_stage_id).all()
        )

        #
        # cloned from show_campaign_stages, but for a one row table..
        #
        campaign_stage = campaign_stage_info[0]
        counts = {}
        counts_keys = {}
        # cil = [c_s.campaign_stage_id for c_s in csl]
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

        recent_submission_list = (
            dbhandle.query(Submission.submission_id, func.max(SubmissionHistory.status_id))
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
        campaign_kibana_link_format = config_get('campaign_kibana_link_format')
        logit.log("got format {}".format(campaign_kibana_link_format))
        kibana_link = campaign_kibana_link_format.format(campaign_stage_id)

        dep_svg = self.campaign_deps_svg(dbhandle, config_get, campaign_stage_id=campaign_stage_id)

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
        )

    def campaign_stage_submissions(self, dbhandle, experiment, role, campaign_name='', stage_name='',
                                   campaign_stage_id=None, campaign_id=None,
                                   tmin=None, tmax=None, tdays=None):
        """
           Show submissions from a campaign stage
        """
        data = {'tmin': tmin, 'tmax': tmax, 'tdays': tdays}
        if campaign_name and campaign_id in (None, 'None', ''):
            campaign_id = dbhandle.query(Campaign.campaign_id).filter(
                Campaign.name == campaign_name,
                Campaign.experiment == experiment,
                ).scalar()
            if not campaign_id:
                data['submissions'] = []
                return data

        if stage_name not in (None, 'None', '*', '') and campaign_stage_id in (None, 'None', ''):
            campaign_stage_id = dbhandle.query(CampaignStage.campaign_stage_id).filter(
                CampaignStage.name == stage_name,
                CampaignStage.experiment == experiment,
                CampaignStage.campaign_id == campaign_id,
            ).scalar()
            if not campaign_stage_id:
                data['submissions'] = []
                return data

        if campaign_id in (None, 'None', '') and campaign_stage_id in (None, 'None', ''):
            raise AssertionError("campaign_stage_submissions needs either campaign_id or campaign_stage_id not None")

        if campaign_id not in (None, 'None', ''):
            campaign_stage_rows = (
                dbhandle.query(CampaignStage.campaign_stage_id).filter(CampaignStage.campaign_id == campaign_id).all()
            )
            campaign_stage_ids = [row[0] for row in campaign_stage_rows]

        if campaign_stage_id not in (None, 'None', ''):
            campaign_stage_ids = [campaign_stage_id]

        # if we're not given any time info, do from start of campaign stage
        if not (tmin or tmax or tdays):
            logit.log("=== no time info, picking...")
            crows = dbhandle.query(CampaignStage.created).filter(CampaignStage.campaign_stage_id.in_(campaign_stage_ids)).all()
            tmin = crows[0][0].strftime('%Y-%m-%d %H:%M:%S')
            tmax = datetime.now(utc).strftime('%Y-%m-%d %H:%M:%S')
            logit.log("picking campaign date range %s .. %s" % (tmin, tmax))

        base_link = 'campaign_stage_submissions/{}/{}?campaign_name={}&stage_name={}&campaign_stage_id={}&campaign_id={}&'.format(
            experiment, role, campaign_name, stage_name, campaign_stage_id, campaign_id
        )
        (tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays) = self.poms_service.utilsPOMS.handle_dates(
            tmin, tmax, tdays, base_link
        )
        data = {
            'tmin': tmin,
            'tmax': tmax,
            'nextlink': nextlink,
            'prevlink': prevlink,
            'tdays': tdays,
            'tminsec': tmin.strftime("%s"),
        }

        subhist = aliased(SubmissionHistory)
        subq = dbhandle.query(func.max(subhist.created)).filter(SubmissionHistory.submission_id == subhist.submission_id)

        tuples = (
            dbhandle.query(Submission, SubmissionHistory, SubmissionStatus)
            .join('experimenter_creator_obj')
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
            .order_by(SubmissionHistory.submission_id.desc())
        ).all()
        submissions = []
        for tup in tuples:
            jjid = tup.Submission.jobsub_job_id
            full_jjid = jjid
            if not jjid:
                jjid = 's' + str(tup.Submission.submission_id)
                full_jjid = "unknown.0@unknown.un.known"
            else:
                jjid = 's%s<br>%s' % (
                    str(tup.Submission.submission_id),
                    str(jjid).replace('fifebatch', '').replace('.fnal.gov', ''),
                )

            row = {
                'submission_id': tup.Submission.submission_id,
                'jobsub_job_id': tup.Submission.jobsub_job_id,
                'created': tup.Submission.created,
                'creator': tup.Submission.experimenter_creator_obj.username,
                'status': tup.SubmissionStatus.status,
                'jobsub_cluster': full_jjid[: full_jjid.find('@')],
                'jobsub_schedd': full_jjid[full_jjid.find('@') + 1:],
                'campaign_stage_name': tup.Submission.campaign_stage_obj.name,
            }
            submissions.append(row)
            data['submissions'] = submissions
        return data

    def session_status_history(self, dbhandle, submission_id):
        '''
           Return history rows
        '''
        rows = []
        tuples = (
            dbhandle.query(SubmissionHistory, SubmissionStatus)
            .filter(SubmissionHistory.submission_id == submission_id)
            .filter(SubmissionStatus.status_id == SubmissionHistory.status_id)
            .order_by(SubmissionHistory.created)
        ).all()
        for row in tuples:
            submission_id = row.SubmissionHistory.submission_id
            created = row.SubmissionHistory.created.strftime("%Y-%m-%d %H:%M:%S")
            status = row.SubmissionStatus.status
            rows.append({'created': created, 'status': status})
        return rows

    def register_poms_campaign(self, dbhandle, experiment, campaign_name, version, user=None, campaign_definition=None, dataset="",
                               role="Production", cr_role="production", params=[]):
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
            c_d = dbhandle.query(JobType).filter(JobType.name == campaign_definition, JobType.experiment == experiment).first()
        else:
            c_d = dbhandle.query(JobType).filter(JobType.name.ilike(r"%generic%"), JobType.experiment == experiment).first()

        ld = dbhandle.query(LoginSetup).filter(LoginSetup.name.ilike(r"%generic%"), LoginSetup.experiment == experiment).first()

        logit.log("campaign_definition = {} ".format(c_d))

        c_s = (
            dbhandle.query(CampaignStage)
            .filter(CampaignStage.experiment == experiment, CampaignStage.name == campaign_name)
            .first()
        )
        if c_s:
            changed = False
        else:
            c_s = CampaignStage(
                experiment=experiment,
                name=campaign_name,
                creator=user,
                created=datetime.now(utc),
                software_version=version,
                job_type_id=c_d.job_type_id,
                login_setup_id=ld.login_setup_id,
                vo_role=role,
                dataset='',
                creator_role=cr_role,
                campaign_stage_type='regular',
            )

        if version:
            c_s.software_verison = version
            changed = True

        if dataset:
            c_s.dataset = dataset
            changed = True

        if user:
            c_s.experimenter = user
            changed = True

        logit.log("register_campaign -- campaign is %s" % c_s.__dict__)

        if changed:
            c_s.updated = datetime.now(utc)
            c_s.updator = user
            dbhandle.add(c_s)
            dbhandle.commit()

        return c_s.campaign_stage_id

    def get_dataset_for(self, dbhandle, samhandle, camp):
        '''
            use the split_type modules to get the next dataset for
            launch for a given campaign
        '''

        if not camp.cs_split_type or camp.cs_split_type == 'None' or camp.cs_split_type == 'none':
            return camp.dataset

        # clean up split_type -- de-white-space it
        camp.cs_split_type = camp.cs_split_type.replace(' ', '')
        camp.cs_split_type = camp.cs_split_type.replace('\n', '')

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
            raise AssertionError('No more splits in this campaign.')

        dbhandle.commit()
        return res

    def list_launch_file(self, dbhandle, campaign_stage_id, fname, login_setup_id=None):
        '''
            get launch output file and return the lines as a list
        '''
        q = (
            dbhandle.query(CampaignStage, Campaign)
            .filter(CampaignStage.campaign_stage_id == campaign_stage_id)
            .filter(CampaignStage.campaign_id == Campaign.campaign_id)
            .first()
        )
        campaign_name = q.Campaign.name
        stage_name = q.CampaignStage.name
        if login_setup_id:
            dirname = '{}/private/logs/poms/launches/template_tests_{}'.format(os.environ['HOME'], login_setup_id)
        else:
            dirname = '{}/private/logs/poms/launches/campaign_{}'.format(os.environ['HOME'], campaign_stage_id)
        lf = open('{}/{}'.format(dirname, fname), 'r', encoding='utf-8', errors='replace')
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
        return lines, refresh, campaign_name, stage_name

    def schedule_launch(self, dbhandle, campaign_stage_id):
        '''
            return crontab info for cron launches for campaign
        '''
        c_s = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
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
        return c_s, job, launch_flist

    def update_launch_schedule(self, dbhandle, campaign_stage_id, dowlist='', domlist='', monthly='', month='', hourlist='', submit='',
                               minlist='',delete='', user=''):
        '''
            callback for changing the launch schedule
        '''

        if user:
            experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == user).first()

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

        if minlist and minlist[0] == '*':
            minlist = None
        else:
            minlist = [int(x) for x in minlist if x]

        if hourlist and hourlist[0] == '*':
            hourlist = None
        else:
            hourlist = [int(x) for x in hourlist if x]

        if dowlist and dowlist[0] == '*':
            dowlist = None
        else:
            # dowlist[0] = [int(x) for x in dowlist if x ]
            pass

        if domlist and domlist[0] == '*':
            domlist = None
        else:
            domlist = [int(x) for x in domlist if x]

        my_crontab = CronTab(user=True)
        # clean out old
        my_crontab.remove_all(comment='POMS_CAMPAIGN_ID={}'.format(campaign_stage_id))

        if not delete:

            # make job for new -- use current link for product
            pdir = os.environ.get('POMS_DIR', '/etc/poms')
            if not pdir.find('/current/') > 0:
                # try to find a current symlink path that points here
                tpdir = pdir[: pdir.rfind('poms', 0, len(pdir) - 1) + 4] + '/current'
                if os.path.exists(tpdir):
                    pdir = tpdir

            job = my_crontab.new(
                command='{}/cron/launcher --campaign_stage_id={} --launcher={}'.format(
                    pdir, campaign_stage_id, experimenter.experimenter_id
                ),
                comment='POMS_CAMPAIGN_ID={}'.format(campaign_stage_id),
            )

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
        rlist = (
            dbhandle.query(CampaignRecovery)
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

    def make_stale_campaigns_inactive(self, dbhandle):
        '''
            turn off active flag on campaigns without recent activity
        '''
        # Get stages run in the last 7 days - active stage list
        lastweek = datetime.now(utc) - timedelta(days=7)
        recent_sq = dbhandle.query(distinct(Submission.campaign_stage_id)).filter(Submission.created > lastweek)
        # Get the campaign_id of the active stages - active campaign list
        active_campaigns = dbhandle.query(distinct(CampaignStage.campaign_id)).filter(
            CampaignStage.campaign_stage_id.in_(recent_sq)
        )
        # Turn off the active flag on stale campaigns.
        stale = (
            dbhandle.query(Campaign)
            .filter(Campaign.active == True, Campaign.campaign_id.notin_(active_campaigns))     # Do NOT optimize condition!
            .update({"active": False}, synchronize_session=False)
        )

        dbhandle.commit()
        return []

    def split_type_javascript(self, dbhandle, samhandle, *args, **kwargs):
        class fake_campaign_stage:
            def __init__(self, dataset="", cs_split_type=""):
                self.dataset = dataset
                self.cs_split_type = cs_split_type

        modmap = {}
        docmap = {}
        parammap = {}
        rlist = []

        modmap['None'] = None
        docmap['None'] = "No splitting is done"
        parammap['None'] = []

        # make the import set POMS_DIR..
        importlib.import_module('poms.webservice')

        gpath = "%s/webservice/split_types/*.py" % os.environ['POMS_DIR']
        rlist.append("/* checking: %s */ " % gpath)

        split_list = glob.glob(gpath)

        modnames = [os.path.basename(x).replace('.py', '') for x in split_list]

        fake_cs = fake_campaign_stage(dataset='null', cs_split_type='')

        for modname in modnames:

            if modname == '__init__':
                continue

            fake_cs.cs_split_type = "%s(2)" % modname

            mod = importlib.import_module('poms.webservice.split_types.' + modname)
            split_class = getattr(mod, modname)
            inst = split_class(fake_cs, samhandle, dbhandle)
            poptxt = inst.edit_popup()

            if poptxt != 'null':
                modmap[modname] = '%s_edit_popup' % modname
                rlist.append(poptxt)
            else:
                modmap[modname] = None

            description = split_class.__doc__
            docmap[modname] = description
            parammap[modname] = inst.params()

        rlist.append('split_type_edit_map =')
        rlist.append(
            json.dumps(modmap)
            .replace('",', '",\n')
            .replace('null,', 'null,\n')
            .replace('": "', '": ')
            .replace('",', ',')
            .replace('"}', '}')
        )
        rlist.append(';')
        rlist.append('split_type_doc_map =')
        rlist.append(json.dumps(docmap).replace('",', '",\n'))
        rlist.append(';')
        rlist.append('split_type_param_map =')
        rlist.append(json.dumps(parammap).replace(',', ',\n'))
        rlist.append(';')
        return '\n'.join(rlist)

    def save_campaign(self, dbhandle, user, *args, **kwargs):
        """
        """

        if args:
            exp = args[0]
            role = args[1] or 'production'
        else:
            exp = kwargs['experiment']
            role = kwargs['role']

        experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == user).first()
        user_id = experimenter.experimenter_id

        data = kwargs.get('form', None)
        logit.log("save_campaigns: data: %s" % data)
        everything = json.loads(data)
        message = []

        # check permissions here, because we need to parse the json
        # to tell
        stages = everything['stages']
        campaign = [s for s in stages if s.get('id').startswith('campaign ')][0]
        c_old_name = campaign.get('id').split(' ', 1)[1]

        # permissions check, deferred from top level...
        self.poms_service.permissions.can_modify(dbhandle, user, exp, role, "Campaign", name=c_old_name, experiment=exp)

        # Process job types and login setups first
        misc = everything['misc']
        now = time.strftime("%Y.%m.%d.%H.%M")
        for el in misc:
            eid = el.get('id')
            old_name = eid.split(' ', 1)[1]
            new_name = el.get('label')
            clean = el.get('clean')
            form = el.get('form')
            #
            if not clean:
                # if new_name != old_name:
                #     name = new_name
                name = new_name
                # elif user in old_name:
                #     i = old_name.index(user)
                #     name = "{}{}.{}".format(old_name[:i], user, now)
                # else:
                #     name = "{}-{}.{}".format(new_name, user, now)
                #
                if eid.startswith("job_type "):
                    definition_parameters = form.get('parameters')
                    if definition_parameters:
                        definition_parameters = json.loads(definition_parameters)

                    job_type = JobType(
                        name=name,
                        experiment=exp,
                        # input_files_per_job=input_files_per_job,
                        # output_files_per_job=output_files_per_job,
                        output_file_patterns=form.get('output_file_patterns'),
                        launch_script=form.get('launch_script'),
                        definition_parameters=definition_parameters,
                        creator=user_id,
                        created=datetime.now(utc),
                        creator_role=role,
                    )
                    dbhandle.add(job_type)
                    try:
                        print(f"*** Creating: JobType '{name}'.")
                        dbhandle.commit()
                        recoveries = form.get('recoveries')
                        if recoveries:
                            self.fixup_recoveries(dbhandle, job_type.job_type_id, recoveries)
                        dbhandle.commit()
                    except IntegrityError:
                        message.append(f"Warning: JobType '{name}' already exists and will not change.")
                        print(f"*** DB error: {message}")
                        dbhandle.rollback()

                elif eid.startswith("login_setup "):
                    login_setup = LoginSetup(
                        name=name,
                        experiment=exp,
                        launch_host=form.get('host'),
                        launch_account=form.get('account'),
                        launch_setup=form.get('setup'),
                        creator=user_id,
                        created=datetime.now(utc),
                        creator_role=role,
                    )
                    dbhandle.add(login_setup)
                    try:
                        print(f"*** Creating: LoginSetup '{name}'.")
                        dbhandle.flush()
                        dbhandle.commit()
                    except IntegrityError:
                        message.append(f"Warning: LoginSetup '{name}' already exists and will not change.")
                        print(f"*** DB error: {message}")
                        dbhandle.rollback()

        # Now process all stages
        stages = everything['stages']
        logit.log("############## stages: {}".format([s.get('id') for s in stages]))
        campaign = [s for s in stages if s.get('id').startswith('campaign ')][0]
        c_old_name = campaign.get('id').split(' ', 1)[1]
        c_new_name = campaign.get('label')
        campaign_clean = campaign.get('clean')
        defaults = campaign.get('form')
        position = campaign.get('position')
        print(
            "##############Campaign: i: '{}', l: '{}', c: '{}', f: '{}', p: '{}'".format(
                c_old_name, c_new_name, campaign_clean, defaults, position
            )
        )

        the_campaign = dbhandle.query(Campaign).filter(Campaign.name == c_old_name, Campaign.experiment == exp).scalar()
        if the_campaign:
            # the_campaign.defaults = defaults    # Store the defaults unconditionally as they may be not stored yet
            the_campaign.defaults = {
                "defaults": defaults,
                "positions": position,
            }  # Store the defaults unconditionally as they may be not be stored yet
            if c_new_name != c_old_name:
                the_campaign.name = c_new_name
        else:  # we do not have a campaign in the db for this experiment so create the campaign and then do the linking
            the_campaign = Campaign()
            the_campaign.name = c_new_name
            the_campaign.defaults = {"defaults": defaults, "positions": position}
            the_campaign.experiment = exp
            the_campaign.creator = user_id
            the_campaign.creator_role = role
            dbhandle.add(the_campaign)
        dbhandle.commit()

        old_stages = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_obj.has(Campaign.name == c_new_name)).all()
        old_stage_names = set([s.name for s in old_stages])
        logit.log("############## old_stage_names: {}".format(old_stage_names))

        # new_stages = tuple(filter(lambda s: not s.get('id').startswith('campaign '), stages))
        new_stages = [s for s in stages if not s.get('id').startswith('campaign ')]
        new_stage_names = set([s.get('id') for s in new_stages])
        logit.log("############## new_stage_names: {}".format(new_stage_names))

        deleted_stages = old_stage_names - new_stage_names
        if deleted_stages:
            # (dbhandle.query(CampaignStage)                    # We DON'T delete the stages
            #  .filter(CampaignStage.name.in_(deleted_stages))
            #  .delete(synchronize_session=False))
            cs_list = (
                dbhandle.query(CampaignStage)
                .filter(CampaignStage.name.in_(deleted_stages))
                .filter(CampaignStage.campaign_id == the_campaign.campaign_id)
            )  # Get the stage list
            for c_s in cs_list:
                c_s.campaign_id = None  # Detach the stage from campaign
            dbhandle.query(CampaignDependency).filter(
                or_(
                    CampaignDependency.provider.has(CampaignStage.name.in_(deleted_stages)),
                    CampaignDependency.consumer.has(CampaignStage.name.in_(deleted_stages)),
                ),
                or_(
                    CampaignDependency.provider.has(CampaignStage.campaign_id == the_campaign.campaign_id),
                    CampaignDependency.consumer.has(CampaignStage.campaign_id == the_campaign.campaign_id),
                ),
            ).delete(
                synchronize_session=False
            )  # Delete the stage dependencies if any
            dbhandle.commit()

        for stage in new_stages:
            old_name = stage.get('id')
            logit.log("save_campaign: for stage loop: %s" % old_name)
            new_name = stage.get('label')
            position = stage.get('position')  # Ignore for now
            clean = stage.get('clean')
            form = stage.get('form')
            # Use the field if provided otherwise use defaults
            # form = {k: (form[k] or defaults[k]) for k in form}
            keys = set(defaults.keys()) | set(form.keys())
            form = {k: (form.get(k) or defaults.get(k)) for k in keys}
            print("############## i: '{}', l: '{}', c: '{}', f: '{}', p: '{}'".format(old_name, new_name, clean, form, position))

            active = form.pop('state', None) in ('True', 'true', '1', 'Active')

            completion_pct = form.pop('completion_pct')
            completion_type = form.pop('completion_type')
            split_type = form.pop('cs_split_type', None)
            dataset = form.pop('dataset_or_split_data')
            job_type = form.pop('job_type')
            print("################ job_type: '{}'".format(job_type))
            login_setup = form.pop('login_setup')
            print("################ login_setup: '{}'".format(login_setup))
            param_overrides = form.pop('param_overrides', None) or "[]"
            print("################ param_overrides: '{}'".format(param_overrides))
            if param_overrides:
                param_overrides = json.loads(param_overrides)
            software_version = form.pop('software_version')
            test_param_overrides = form.pop('test_param_overrides', None)
            test_param_overrides = json.loads(test_param_overrides) if test_param_overrides else None
            vo_role = form.pop('vo_role')

            stage_type = form.pop('stage_type', 'test')

            login_setup_id = (
                dbhandle.query(LoginSetup.login_setup_id)
                .filter(LoginSetup.experiment == exp)
                .filter(LoginSetup.name == login_setup)
                .scalar()
                or dbhandle.query(LoginSetup.login_setup_id)
                .filter(LoginSetup.experiment == 'samdev')
                .filter(LoginSetup.name == login_setup)
                .scalar()
            )
            if not login_setup_id:
                logit.log("save_campaign: Error: bailing on not login_setup_id login_id '%s'" % login_setup)
                message.append(f"Error: LoginSetup '{login_setup}' not found! Campaign is incomplete!")
                return {'status': "400 Bad Request", 'message': message}

            job_type_id = (
                dbhandle.query(JobType.job_type_id).filter(JobType.experiment == exp).filter(JobType.name == job_type).scalar()
                or dbhandle.query(JobType.job_type_id)
                .filter(JobType.experiment == 'samdev')
                .filter(JobType.name == job_type)
                .scalar()
            )
            if not job_type_id:
                logit.log("save_campaign: Error bailing on not job_type_id: job_type: '%s'" % job_type)
                message.append(f"Error: JobType '{job_type}' not found! Campaign is incomplete!")
                return {'status': "400 Bad Request", 'message': message}

            logit.log("save_campaign: for stage loop: here1")
            # when we just cloned a stage, we think there's old one, but there
            # isn't , hence the if obj: below
            obj = None
            if old_name in old_stage_names:
                obj = (dbhandle.query(CampaignStage)
                       .filter(CampaignStage.campaign_id == the_campaign.campaign_id)
                       .filter(CampaignStage.name == old_name)
                       .scalar()
                )  # Get stage by the old name
            if obj:
                logit.log("save_campaign: for stage loop: found campaign stage obj")
                obj.name = new_name     # Update the name using provided new_name
                if not clean or not campaign_clean:           # Update all fields from the form
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
                    obj.campaign_stage_type = stage_type
                    obj.active = active
                    obj.updater = user_id
                    obj.updated = datetime.now(utc)

                    dbhandle.flush()
            else:  # If this is a new stage then create and store it
                logit.log("for_stage_loop: new campaign stage...")
                c_s = CampaignStage(
                    name=new_name,
                    experiment=exp,
                    campaign_id=the_campaign.campaign_id,
                    # active=active,
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
                    creator=user_id,
                    created=datetime.now(utc),
                    creator_role=role,
                    campaign_stage_type=stage_type,
                )
                dbhandle.add(c_s)
                dbhandle.flush()

        dbhandle.commit()

        logit.log("save_campaign: for stage loop: here2")

        # Now process all dependencies
        dependencies = everything['dependencies']
        dbhandle.query(CampaignDependency).filter(
            or_(
                CampaignDependency.provider.has(CampaignStage.campaign_id == the_campaign.campaign_id),
                CampaignDependency.consumer.has(CampaignStage.campaign_id == the_campaign.campaign_id),
            )
        ).delete(synchronize_session=False)

        for dependency in dependencies:
            from_name = dependency['fromId']
            to_name = dependency['toId']
            form = dependency.get('form')
            from_id = (
                dbhandle.query(CampaignStage.campaign_stage_id)
                .filter(CampaignStage.campaign_id == the_campaign.campaign_id)
                .filter(CampaignStage.experiment == exp)
                .filter(CampaignStage.name == from_name)
                .scalar()
            )
            to_id = (
                dbhandle.query(CampaignStage.campaign_stage_id)
                .filter(CampaignStage.campaign_id == the_campaign.campaign_id)
                .filter(CampaignStage.experiment == exp)
                .filter(CampaignStage.name == to_name)
                .scalar()
            )

            file_pattern = list(form.values())[0]
            dep = CampaignDependency(provides_campaign_stage_id=to_id, needs_campaign_stage_id=from_id, file_patterns=file_pattern)
            dbhandle.add(dep)
            dbhandle.flush()
        dbhandle.commit()
        logit.log("save_campaign: for stage loop: here3")
        print("+++++++++++++++ Campaign saved")
        return {
            'status': "201 Created",
            'message': message or "OK",
            'campaign_id': the_campaign.campaign_id,
            'campaign_stage_ids': [(x.campaign_stage_id, x.name) for x in the_campaign.stages]
        }


    def get_jobtype_id(self, dbhandle, user, exp, role, name):
        """
           lookup job type id for name in current experiment
        """
        experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == user).first()
        user_id = experimenter.experimenter_id

        return dbhandle.query(JobType.job_type_id).filter(JobType.experiment == exp).filter(JobType.name == name).scalar()

    def get_loginsetup_id(self, dbhandle, user, exp, role, name):
        """
           lookup login setup id by name for current experiment
        """
        experimenter = dbhandle.query(Experimenter).filter(Experimenter.username == user).first()
        role = role
        user_id = experimenter.experimenter_id
        exp

        return (
            dbhandle.query(LoginSetup.login_setup_id).filter(LoginSetup.experiment == exp).filter(LoginSetup.name == name).scalar()
        )

    def mark_campaign_active(self, campaign_id, is_active, camp_l, dbhandle, user):
        logit.log("camp_l={}; is_active='{}'".format(camp_l, is_active))
        auth_error = False
        campaign_ids = (campaign_id or camp_l).split(",")
        for cid in campaign_ids:
            auth = False
            campaign = dbhandle.query(Campaign).filter(Campaign.campaign_id == cid).first()
            if campaign:
                if user.is_root():
                    auth = True
                elif user.session_experiment == campaign.experiment:
                    if user.is_superuser():
                        auth = True
                    elif user.is_production() and campaign.creator_role == 'production':
                        auth = True
                    elif user.session_role == campaign.creator_role and user.experimenter_id == campaign.creator:
                        auth = True
                    else:
                        auth_error = True
                else:
                    auth_error = True
                if auth:
                    campaign.active = is_active in ('True', 'Active', 'true', '1')
                    dbhandle.add(campaign)
                    dbhandle.commit()
                else:
                    auth_error = True
        return auth_error
