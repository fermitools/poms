#!/usr/bin/env python

"""
This module contain the methods that allow to create campaigns, definitions and templates.
List of methods:
launch_template_edit, campaign_definition_edit, campaign_edit, campaign_edit_query.
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in
poms_service.py written by Marc Mengel, Michael Gueith and Stephen White.
Date: April 28th, 2017. (changes for the POMS_client)
"""

from collections import deque, OrderedDict
from datetime import datetime, tzinfo, timedelta
import time
import json
import os
import glob
import subprocess
import importlib
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import func, desc, not_, and_, or_, distinct
from sqlalchemy.orm import subqueryload, joinedload, contains_eager

from crontab import CronTab
from .poms_model import (Experiment, Experimenter, Campaign, CampaignDependency,
                         LaunchTemplate, CampaignDefinition, CampaignRecovery,
                         CampaignsTags, Tag, CampaignSnapshot, RecoveryType, TaskHistory, Task
                         )
from . import time_grid
from .utc import utc
from .pomscache import pomscache, pomscache_10
from . import logit


class CampaignsPOMS():
    '''
       Business logic for Campaign related items
    '''

    def __init__(self, ps):
        '''
            initialize ourself with a reference back to the overall poms_service
        '''
        self.poms_service = ps

    def launch_template_edit(self, dbhandle, seshandle, *args, **kwargs):
        """
            callback to actually change launch templates from edit screen
        """
        data = {}
        message = None
        ae_launch_id = None
        data['exp_selections'] = dbhandle.query(Experiment).filter(
            ~Experiment.experiment.in_(['root', 'public'])).order_by(
            Experiment.experiment)
        action = kwargs.pop('action', None)
        exp = seshandle('experimenter').session_experiment
        pcl_call = int(kwargs.pop('pcl_call', 0))
        pc_username = kwargs.pop('pc_username', None)
        if isinstance(pc_username, str):
            pc_username = pc_username.strip()

        if action == 'delete':
            ae_launch_name = kwargs.pop('ae_launch_name')
            name = ae_launch_name
            try:
                dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment == exp).filter(
                    LaunchTemplate.name == name).delete()
                dbhandle.commit()
            except Exception as e:
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
                experimenter_id = dbhandle.query(Experimenter).filter(
                    Experimenter.username == pc_username).first().experimenter_id
                if action == 'edit':
                    ae_launch_id = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment == exp).filter(
                        LaunchTemplate.name == name).first().launch_id
                ae_launch_host = kwargs.pop('ae_launch_host', None)
                ae_launch_account = kwargs.pop('ae_launch_account', None)
                ae_launch_setup = kwargs.pop('ae_launch_setup', None)
                if ae_launch_host in [None, ""]:
                    ae_launch_host = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment == exp).filter(
                        LaunchTemplate.name == name).first().launch_host
                if ae_launch_account in [None, ""]:
                    ae_launch_account = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment == exp).filter(
                        LaunchTemplate.name == name).first().launch_account
                if ae_launch_setup in [None, ""]:
                    ae_launch_account = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment == exp).filter(
                        LaunchTemplate.name == name).first().launch_setup
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
                        template = LaunchTemplate(experiment=exp, name=ae_launch_name, launch_host=ae_launch_host,
                                                  launch_account=ae_launch_account,
                                                  launch_setup=ae_launch_setup, creator=experimenter_id,
                                                  created=datetime.now(utc), creator_role=role)
                    dbhandle.add(template)
                else:
                    columns = {
                        "name": ae_launch_name,
                        "launch_host": ae_launch_host,
                        "launch_account": ae_launch_account,
                        "launch_setup": ae_launch_setup,
                        "updated": datetime.now(utc),
                        "updater": experimenter_id
                    }
                    template = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.launch_id == ae_launch_id).update(
                        columns)
                dbhandle.commit()
            except IntegrityError as e:
                message = "Integrity error - you are most likely using a name which already exists in database."
                logit.log(' '.join(e.args))
                dbhandle.rollback()
            except SQLAlchemyError as e:
                message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % ' '.join(e.args)
                logit.log(' '.join(e.args))
                dbhandle.rollback()
            except:
                message = 'unexpected error ! '
                logit.log(' '.join(message))
                dbhandle.rollback()

        # Find templates
        if exp:  # cuz the default is find
            data['curr_experiment'] = exp
            data['authorized'] = []
            se_role = seshandle('experimenter').session_role
            if se_role == 'root' or se_role == 'coordinator':
                # One has to execuate the query here instead of sending to jinja to do it,
                # because we need to know the LaunchTemplate detail to figure the authorization.
                # Otherwise, we have to figure it out in the client/html.
                data['templates'] = dbhandle.query(LaunchTemplate, Experiment).join(Experiment).filter(
                    LaunchTemplate.experiment == exp).order_by(LaunchTemplate.name).all()
            else:
                data['templates'] = dbhandle.query(LaunchTemplate, Experiment).join(Experiment).filter(
                    LaunchTemplate.experiment == exp).filter(
                    LaunchTemplate.creator_role == se_role).order_by(LaunchTemplate.name).all()
            for lt in data['templates']:
                if se_role == 'root' or se_role == 'coordinator':
                    data['authorized'].append(True)
                elif se_role == 'production':
                    data['authorized'].append(True)
                elif se_role == 'analysis' and lt.LaunchTemplate.creator == seshandle('experimenter').experimenter_id:
                    data['authorized'].append(True)
                else:
                    data['authorized'].append(False)
        data['message'] = message
        return data

    def campaign_list(self, dbhandle):
        '''
            Return list of all campaign_id s and names. --
            This is actually for Landscape to use.
        '''
        data = dbhandle.query(Campaign.campaign_id, Campaign.name).all()
        return data

    def campaign_definition_edit(self, dbhandle, seshandle, *args, **kwargs):
        '''
            callback from edit screen/client.
        '''
        data = {}
        message = None
        data['exp_selections'] = dbhandle.query(Experiment).filter(
            ~Experiment.experiment.in_(["root", "public"])).order_by(Experiment.experiment)
        action = kwargs.pop('action', None)
        exp = seshandle('experimenter').session_experiment
        r = seshandle('experimenter').session_role
        # added for poms_client
        pcl_call = int(kwargs.pop('pcl_call', 0))  # pcl_call == 1 means the method was access through the poms_client.
        pc_username = kwargs.pop('pc_username', None)  # email is the info we know about the user in POMS DB.

        if action == 'delete':
            name = kwargs.pop('ae_definition_name')
            if isinstance(name, str):
                name = name.strip()
            if pcl_call == 1:  # Enter here if the access was from the poms_client
                cid = campaign_definition_id = dbhandle.query(CampaignDefinition).filter(
                    CampaignDefinition.name == name).first().campaign_definition_id
            else:
                cid = kwargs.pop('campaign_definition_id')
            try:
                dbhandle.query(CampaignRecovery).filter(CampaignRecovery.campaign_definition_id == cid).delete()
                dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id == cid).delete()
                dbhandle.commit()
            except Exception as e:
                message = 'The campaign definition, %s, has been used and may not be deleted.' % name
                logit.log(message)
                logit.log(' '.join(e.args))
                dbhandle.rollback()

        if action == 'add' or action == 'edit':
            campaign_definition_id = None
            if pcl_call == 1:  # Enter here if the access was from the poms_client
                name = kwargs.pop('ae_definition_name')
                if isinstance(name, str):
                    name = name.strip()
                experimenter_id = dbhandle.query(Experimenter).filter(
                    Experimenter.username == pc_username).first().experimenter_id
                if action == 'edit':
                    campaign_definition_id = dbhandle.query(CampaignDefinition).filter(
                        CampaignDefinition.name == name).first().campaign_definition_id  # Check here!
                else:
                    pass
                input_files_per_job = kwargs.pop('ae_input_files_per_job', 0)
                output_files_per_job = kwargs.pop('ae_output_files_per_job', 0)
                output_file_patterns = kwargs.pop('ae_output_file_patterns')
                launch_script = kwargs.pop('ae_launch_script')
                definition_parameters = kwargs.pop('ae_definition_parameters')
                recoveries = kwargs.pop('ae_definition_recovery', "[]")
                # Getting the info that was not passed by the poms_client arguments
                if input_files_per_job in [None, ""]:
                    input_files_per_job = dbhandle.query(CampaignDefinition).filter(
                        CampaignDefinition.campaign_definition_id == campaign_definition_id).firts().input_files_per_job
                if output_files_per_job in [None, ""]:
                    output_files_per_job = dbhandle.query(CampaignDefinition).filter(
                        CampaignDefinition.campaign_definition_id == campaign_definition_id).firts().output_files_per_job
                if output_file_patterns in [None, ""]:
                    output_file_patterns = dbhandle.query(CampaignDefinition).filter(
                        CampaignDefinition.campaign_definition_id == campaign_definition_id).firts().output_file_patterns
                if launch_script in [None, ""]:
                    launch_script = dbhandle.query(CampaignDefinition).filter(
                        CampaignDefinition.campaign_definition_id == campaign_definition_id).firts().launch_script
                if definition_parameters in [None, ""]:
                    definition_parameters = dbhandle.query(CampaignDefinition).filter(
                        CampaignDefinition.campaign_definition_id == campaign_definition_id).firts().definition_parameters
            else:
                experimenter_id = kwargs.pop('experimenter_id')
                campaign_definition_id = kwargs.pop('ae_campaign_definition_id')
                name = kwargs.pop('ae_definition_name')
                if isinstance(name, str):
                    name = name.strip()
                input_files_per_job = kwargs.pop('ae_input_files_per_job', 0)
                output_files_per_job = kwargs.pop('ae_output_files_per_job', 0)
                output_file_patterns = kwargs.pop('ae_output_file_patterns')
                launch_script = kwargs.pop('ae_launch_script')
                definition_parameters = json.loads(kwargs.pop('ae_definition_parameters'))
                recoveries = kwargs.pop('ae_definition_recovery')
            try:
                if action == 'add':
                    role = seshandle('experimenter').session_role
                    if role == 'root' or role == 'coordinator':
                        raise cherrypy.HTTPError(401, 'You are not authorized to add campaign definition.')
                    else:
                        cd = CampaignDefinition(name=name, experiment=exp,
                                                input_files_per_job=input_files_per_job,
                                                output_files_per_job=output_files_per_job,
                                                output_file_patterns=output_file_patterns,
                                                launch_script=launch_script,
                                                definition_parameters=definition_parameters,
                                                creator=experimenter_id, created=datetime.now(utc), creator_role=role)

                    dbhandle.add(cd)
                    dbhandle.flush()
                    campaign_definition_id = cd.campaign_definition_id
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
                    cd = dbhandle.query(CampaignDefinition).filter(
                        CampaignDefinition.campaign_definition_id == campaign_definition_id).update(columns)

                # now fixup recoveries -- clean out existing ones, and
                # add listed ones.
                if pcl_call == 0:
                    dbhandle.query(CampaignRecovery).filter(
                        CampaignRecovery.campaign_definition_id == campaign_definition_id).delete()
                    i = 0
                    for rtn in json.loads(recoveries):
                        rect = rtn[0]
                        recpar = rtn[1]
                        rt = dbhandle.query(RecoveryType).filter(RecoveryType.name == rect).first()
                        cr = CampaignRecovery(campaign_definition_id=campaign_definition_id, recovery_order=i,
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
            data['curr_experiment'] = exp
            data['authorized'] = []
            # for testing ui...
            # data['authorized'] = True
            if r in ['root','coordinator']:
                data['definitions'] = (dbhandle.query(CampaignDefinition, Experiment)
                                       .join(Experiment)
                                       .filter(CampaignDefinition.experiment == exp)
                                       .order_by(CampaignDefinition.name)
                                       ).all()
            else:
                data['definitions'] = (dbhandle.query(CampaignDefinition, Experiment)
                                       .join(Experiment)
                                       .filter(CampaignDefinition.experiment == exp)
                                       .filter(CampaignDefinition.creator_role == r)
                                       .order_by(CampaignDefinition.name)
                                       ).all()
            cids = []
            for df in data['definitions']:
                cids.append(df.CampaignDefinition.campaign_definition_id)
                if r in ['root', 'coordinator']:
                    data['authorized'].append(True)
                elif df.CampaignDefinition.creator_role == 'production' and r == "production":
                    data['authorized'].append(True)
                elif df.CampaignDefinition.creator_role == r \
                        and df.CampaignDefinition.creator == seshandle('experimenter').experimenter_id:
                    data['authorized'].append(True)
                else:
                    data['authorized'].append(False)

            # Build the recoveries for each campaign.
            cids = [ ]
            recs_dict = {}
            for cid in cids:
                recs = (dbhandle.query(CampaignRecovery).join(CampaignDefinition).options(
                    joinedload(CampaignRecovery.recovery_type))
                        .filter(CampaignRecovery.campaign_definition_id == cid, CampaignDefinition.experiment == exp)
                        .order_by(CampaignRecovery.campaign_definition_id, CampaignRecovery.recovery_order))
                rec_list = deque()
                for rec in recs:
                    if type(rec.param_overrides) == type(""):
                        if rec.param_overrides in ('', '{}', '[]'): rec.param_overrides = "[]"
                        rec_vals = [rec.recovery_type.name, json.loads(rec.param_overrides)]
                    else:
                        rec_vals = [rec.recovery_type.name, rec.param_overrides]

                    # rec_vals=[rec.recovery_type.name,rec.param_overrides]
                    rec_list.append(rec_vals)
                recs_dict[cid] = json.dumps(list(rec_list))

            data['recoveries'] = recs_dict
            data['rtypes'] = (
                dbhandle.query(RecoveryType.name, RecoveryType.description).order_by(RecoveryType.name).all())

        data['message'] = message
        return data

    def make_test_campaign_for(self, dbhandle, sesshandle, campaign_def_id, campaign_def_name):
        '''
            Build a test_campaign for a given campaign definition
        '''
        c = dbhandle.query(Campaign).filter(Campaign.campaign_definition_id == campaign_def_id,
                                            Campaign.name == "_test_%s" % campaign_def_name).first()
        if not c:
            lt = dbhandle.query(LaunchTemplate).filter(
                LaunchTemplate.experiment == sesshandle.get('experimenter').session_experiment).first()
            c = Campaign()
            c.campaign_definition_id = campaign_def_id
            c.name = "_test_%s" % campaign_def_name
            c.experiment = sesshandle.get('experimenter').session_experiment
            c.creator = sesshandle.get('experimenter').experimenter_id
            c.created = datetime.now(utc)
            c.updated = datetime.now(utc)
            c.vo_role = "Production"
            c.creator_role = "production"
            c.dataset = ""
            c.launch_id = lt.launch_id
            c.software_version = ""
            dbhandle.add(c)
            dbhandle.commit()
            c = dbhandle.query(Campaign).filter(Campaign.campaign_definition_id == campaign_def_id,
                                                Campaign.name == "_test_%s" % campaign_def_name).first()
        return c.campaign_id

    def campaign_edit(self, dbhandle, sesshandle, *args, **kwargs):
        """
            callback for campaign edit screens to update campaign record
            takes action = 'edit'/'add'/ etc.
            sesshandle is the cherrypy.session instead of cherrypy.session.get method
        """
        data = {}
        role = sesshandle.get('experimenter').session_role
        user_id = role = sesshandle.get('experimenter').experimenter_id
        message = None
        exp = sesshandle.get('experimenter').session_experiment
        data['exp_selections'] = dbhandle.query(Experiment).filter(
            ~Experiment.experiment.in_(["root", "public"])).order_by(Experiment.experiment)
        # for k,v in kwargs.items():
        #    print ' k=%s, v=%s ' %(k,v)
        action = kwargs.pop('action', None)
        pcl_call = int(kwargs.pop('pcl_call', 0))  # pcl_call == 1 means the method was access through the poms_client.
        pc_username = kwargs.pop('pc_username', None)  # email is the info we know about the user in POMS DB.

        if action == 'delete':
            name = kwargs.get('ae_campaign_name', kwargs.get('name', None))
            if isinstance(name, str):
                name = name.strip()
            if pcl_call == 1:
                campaign_id = dbhandle.query(Campaign).filter(Campaign.name == name).first().campaign_id
            else:
                campaign_id = kwargs.pop('campaign_id')
            try:
                dbhandle.query(CampaignDependency).filter(or_(CampaignDependency.needs_camp_id == campaign_id,
                                                              CampaignDependency.uses_camp_id == campaign_id)).delete()
                dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).delete()
                dbhandle.commit()
            except Exception as e:
                message = "The campaign, {}, has been used and may not be deleted.".format(name)
                logit.log(message)
                logit.log(' '.join(e.args))
                dbhandle.rollback()

        if action == 'add' or action == 'edit':
            name = kwargs.pop('ae_campaign_name')
            if isinstance(name, str):
                name = name.strip()
            active = kwargs.pop('ae_active')
            split_type = kwargs.pop('ae_split_type', None)
            vo_role = kwargs.pop('ae_vo_role')
            software_version = kwargs.pop('ae_software_version')
            dataset = kwargs.pop('ae_dataset')
            ###Mark

            completion_type = kwargs.pop('ae_completion_type')
            completion_pct = kwargs.pop('ae_completion_pct')
            depends = kwargs.pop('ae_depends', "[]")
            param_overrides = kwargs.pop('ae_param_overrides', "[]")
            if param_overrides:
                param_overrides = json.loads(param_overrides)

            if pcl_call == 1:
                launch_name = kwargs.pop('ae_launch_name')
                if isinstance(launch_name, str):
                    launch_name = launch_name.strip()
                campaign_definition_name = kwargs.pop('ae_campaign_definition')
                if isinstance(campaign_definition_name, str):
                    campaign_definition_name = campaign_definition_name.strip()
                # all this variables depend on the arguments passed.
                experimenter_id = dbhandle.query(Experimenter).filter(
                    Experimenter.username == pc_username).first().experimenter_id
                launch_id = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment == exp).filter(
                    LaunchTemplate.name == launch_name).first().launch_id
                campaign_definition_id = dbhandle.query(CampaignDefinition).filter(
                    CampaignDefinition.name == campaign_definition_name).first().campaign_definition_id
                if action == 'edit':
                    campaign_id = dbhandle.query(Campaign).filter(Campaign.name == name).first().campaign_id
                else:
                    pass
            else:
                campaign_id = kwargs.pop('ae_campaign_id')
                campaign_definition_id = kwargs.pop('ae_campaign_definition_id')
                launch_id = kwargs.pop('ae_launch_id')
                experimenter_id = kwargs.pop('experimenter_id')

            if depends and depends != "[]":
                depends = json.loads(depends)
            else:
                depends = {"campaigns": [], "file_patterns": []}
            try:
                if action == 'add':
                    if not completion_pct:
                        completion_pct = 95
                    if role == 'root' or role == 'coordinator':
                        raise cherrypy.HTTPError(401, 'You are not authorized to add campaign '
                                                      'definition as a supper user.')
                    else:
                        c = Campaign(name=name, experiment=exp, vo_role=vo_role,
                                     active=active, cs_split_type=split_type,
                                     software_version=software_version, dataset=dataset,
                                     param_overrides=param_overrides, launch_id=launch_id,
                                     campaign_definition_id=campaign_definition_id,
                                     completion_type=completion_type, completion_pct=completion_pct,
                                     creator=experimenter_id, created=datetime.now(utc),
                                     creator_role=role)
                    dbhandle.add(c)
                    dbhandle.commit()  ##### Is this flush() necessary or better a commit ?
                    campaign_id = c.campaign_id
                else:
                    columns = {
                        "name": name,
                        "vo_role": vo_role,
                        "active": active,
                        "cs_split_type": split_type,
                        "software_version": software_version,
                        "dataset": dataset,
                        "param_overrides": param_overrides,
                        "campaign_definition_id": campaign_definition_id,
                        "launch_id": launch_id,
                        "updated": datetime.now(utc),
                        "updater": experimenter_id,
                        "completion_type": completion_type,
                        "completion_pct": completion_pct
                    }
                    cd = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).update(columns)
                    # now redo dependencies
                dbhandle.query(CampaignDependency).filter(CampaignDependency.uses_camp_id == campaign_id).delete()
                logit.log("depends for %s are: %s" % (campaign_id, depends))
                depcamps = dbhandle.query(Campaign).filter(Campaign.name.in_(depends['campaigns'])).all()
                for i in range(len(depcamps)):
                    logit.log("trying to add dependency for: {}".format(depcamps[i].name))
                    d = CampaignDependency(uses_camp_id=campaign_id, needs_camp_id=depcamps[i].campaign_id,
                                           file_patterns=depends['file_patterns'][i])
                    dbhandle.add(d)
                dbhandle.commit()
            except IntegrityError as e:
                message = "Integrity error - you are most likely using a name which already exists in database."
                logit.log(' '.join(e.args))
                dbhandle.rollback()
            except SQLAlchemyError as e:
                message = "SQLAlchemyError.  Please report this to the administrator.   Message: {}".format(
                    ' '.join(e.args))
                logit.log(' '.join(e.args))
                dbhandle.rollback()
            else:
                dbhandle.commit()

        # Find campaigns
        if exp:  # cuz the default is find
            # for testing ui...
            # data['authorized'] = True
            state = kwargs.pop('state', None)
            if state is None:
                state = sesshandle.get('campaign_edit.state', 'state_active')
            sesshandle['campaign_edit.state'] = state
            data['state'] = state
            data['curr_experiment'] = exp
            data['authorized'] = []
            cquery = dbhandle.query(Campaign).filter(Campaign.experiment == exp)
            if state == 'state_active':
                cquery = cquery.filter(Campaign.active == True)
            elif state == 'state_inactive':
                cquery = cquery.filter(Campaign.active == False)
                cquery = cquery.order_by(Campaign.name)
            if role in ('analysis', 'production'):
                cquery = cquery.filter(Campaign.creator_role == role)
            data['campaigns'] = cquery
            data['definitions'] = dbhandle.query(CampaignDefinition).filter(
                CampaignDefinition.experiment == exp).order_by(CampaignDefinition.name)
            data['templates'] = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment == exp).order_by(
                LaunchTemplate.name)
            cq = data['campaigns'].all()
            cids = []
            for c in cq:
                cids.append(c.campaign_id)
                if role in ('root', 'coordinator'):
                    data['authorized'].append(True)
                elif c.creator_role == 'production' and sesshandle.get('experimenter').session_role == 'production':
                    data['authorized'].append(True)
                elif c.creator_role == role \
                        and c.creator == sesshandle.get('experimenter').experimenter_id:
                    data['authorized'].append(True)
                else:
                    data['authorized'].append(False)
            depends = {}
            for cid in cids:
                sql = (dbhandle.query(CampaignDependency.uses_camp_id, Campaign.name, CampaignDependency.file_patterns)
                       .filter(CampaignDependency.uses_camp_id == cid,
                               Campaign.campaign_id == CampaignDependency.needs_camp_id))
                deps = {
                    "campaigns": [row[1] for row in sql.all()],
                    "file_patterns": [row[2] for row in sql.all()]
                }
                depends[cid] = json.dumps(deps)
            data['depends'] = depends
        data['message'] = message
        return data

    def campaign_edit_query(self, dbhandle, *args, **kwargs):
        """
            return info needed by campaign edit page
        """

        data = {}
        ae_launch_id = kwargs.pop('ae_launch_id', None)
        ae_campaign_definition_id = kwargs.pop('ae_campaign_definition_id', None)

        if ae_launch_id:
            template = {}
            temp = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.launch_id == ae_launch_id).first()
            template['launch_host'] = temp.launch_host
            template['launch_account'] = temp.launch_account
            template['launch_setup'] = temp.launch_setup
            data['template'] = template

        if ae_campaign_definition_id:
            definition = {}
            cdef = dbhandle.query(CampaignDefinition).filter(
                CampaignDefinition.campaign_definition_id == ae_campaign_definition_id).first()
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
        c = dbhandle.query(Campaign).filter(Campaign.name == campaign_name).first()
        e = dbhandle.query(Experimenter).filter(Experimenter.username == experimenter_name).first()
        t = Task()
        t.campaign_id = c.campaign_id
        t.campaign_definition_id = c.campaign_definition_id
        t.task_order = 0
        t.input_dataset = "-"
        t.output_dataset = "-"
        t.status = 'started'
        t.created = datetime.now(utc)
        t.updated = datetime.now(utc)
        t.updater = e.experimenter_id
        t.creator = e.experimenter_id
        t.command_executed = command_executed
        if dataset_name:
            t.input_dataset = dataset_name
        dbhandle.add(t)
        dbhandle.commit()
        return "Task=%d" % t.task_id

    def campaign_deps_svg(self, dbhandle, config_get, tag=None, camp_id=None):
        '''
            return campaign dependencies as an SVG graph
            uses "dot" to generate the drawing
        '''
        if tag is not None:
            cl = dbhandle.query(Campaign).join(CampaignsTags, Tag).filter(Tag.tag_name == tag,
                                                                          CampaignsTags.tag_id == Tag.tag_id,
                                                                          CampaignsTags.campaign_id == Campaign.campaign_id).all()
        if camp_id is not None:
            cidl1 = dbhandle.query(CampaignDependency.needs_camp_id).filter(
                CampaignDependency.uses_camp_id == camp_id).all()
            cidl2 = dbhandle.query(CampaignDependency.uses_camp_id).filter(
                CampaignDependency.needs_camp_id == camp_id).all()
            s = set([camp_id])
            s.update(cidl1)
            s.update(cidl2)
            cl = dbhandle.query(Campaign).filter(Campaign.campaign_id.in_(s)).all()

        c_ids = deque()
        try:
            pdot = subprocess.Popen("tee /tmp/dotstuff | dot -Tsvg", shell=True, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE, universal_newlines=True)
            pdot.stdin.write('digraph {}Dependencies {{\n'.format(tag))
            pdot.stdin.write('node [shape=box, style=rounded, color=lightgrey, fontcolor=black]\nrankdir = "LR";\n')
            baseurl = "{}/campaign_info?campaign_id=".format(config_get("pomspath"))

            for c in cl:
                tcl = dbhandle.query(func.count(Task.status), Task.status).group_by(Task.status).filter(
                    Task.campaign_id == c.campaign_id).all()
                tot = 0
                ltot = 0
                for (count, status) in tcl:
                    tot = tot + count
                    if status == 'Located':
                        ltot = count
                c_ids.append(c.campaign_id)
                pdot.stdin.write(
                    'c{:d} [URL="{}{:d}",label="{}\\nSubmissions {:d} Located {:d}",color={}];\n'.format(c.campaign_id,
                                                                                                         baseurl,
                                                                                                         c.campaign_id,
                                                                                                         c.name,
                                                                                                         tot,
                                                                                                         ltot,
                                                                                                         (
                                                                                                             "darkgreen" if ltot == tot else "black")))

            cdl = dbhandle.query(CampaignDependency).filter(CampaignDependency.needs_camp_id.in_(c_ids)).all()

            for cd in cdl:
                pdot.stdin.write('c{:d} -> c{:d};\n'.format(cd.needs_camp_id, cd.uses_camp_id))

            pdot.stdin.write('}\n')
            pdot.stdin.close()
            text = pdot.stdout.read()
            pdot.wait()
        except:
            text = ""
        return bytes(text, encoding="utf-8")

    def show_campaigns(self, dbhandle, samhandle, campaign_ids=None, tmin=None, tmax=None, tdays=7,
                       active=True, tag=None, holder=None, role_held_with=None, sesshandler=None):

        """
            give campaign information about campaigns with activity in the
            time window for a given experiment
        :rtype: object
        """
        (tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays) = \
            self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'show_campaigns?')

        experiment = sesshandler('experimenter').session_experiment
        se_role = sesshandler('experimenter').session_role

        cq = (dbhandle.query(Campaign)
              .outerjoin(CampaignsTags)
              .options(joinedload('campaigns_tags'))
              .options(joinedload('experiment_obj'))
              .options(joinedload(Campaign.experimenter_holder_obj))
              .order_by(Campaign.experiment)
              .options(joinedload(Campaign.experimenter_creator_obj))
              )

        if experiment:
            cq = cq.filter(Campaign.experiment == experiment)

        if se_role in ('production', 'analysis'):
            cq = cq.filter(Campaign.creator_role == se_role)

        if campaign_ids:
            campaign_ids = campaign_ids.split(",")
            cq = cq.filter(Campaign.campaign_id.in_(campaign_ids))

        if tag:
            cq = cq.join(Tag).filter(Tag.tag_name == tag)

            # for now we comment out it. When we have a lot of data, we may need to use these filters.
            # We will let the client filter it in show_campaigns.html with tablesorter for now.
            # if holder:
            # cq = cq.filter(Campaingn.hold_experimenters_id == holder)

            # if creator_role:
            # cq = cq.filter(Campaingn.creator_role == creator_role)

        campaigns = cq.all()
        logit.log(logit.DEBUG, "show_campaigns: back from query")
        # check for authorization
        data = {}
        data['authorized'] = []
        for c in campaigns:
            if se_role != 'analysis':
                data['authorized'].append(True)
            elif c.creator == sesshandler('experimenter').experimenter_id:
                data['authorized'].append(True)
            else:
                data['authorized'].append(False)

        return campaigns, tmin, tmax, tmins, tmaxs, tdays, nextlink, prevlink, time_range_string, data

    def reset_campaign_split(self, dbhandle, samhandle, campaign_id):
        """
            reset a campaigns cs_last_split field so the sequence
            starts over
        """
        campaign_id = int(campaign_id)

        c = (dbhandle.query(Campaign)
             .filter(Campaign.campaign_id == campaign_id)
             .first())
        c.cs_last_split = None
        dbhandle.commit()

    # @pomscache.cache_on_arguments()
    def campaign_info(self, dbhandle, samhandle, err_res, config_get, campaign_id, tmin=None, tmax=None, tdays=None):
        """
           Give information related to a campaign for the campaign_info page
        """

        campaign_id = int(campaign_id)

        campaign_info = (dbhandle.query(Campaign, Experimenter)
                         .filter(Campaign.campaign_id == campaign_id, Campaign.creator == Experimenter.experimenter_id)
                         .first())

        # default to time window of campaign
        if tmin is None and tdays is None:
            tmin = campaign_info.Campaign.created
            tmax = datetime.now(utc)

        tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays = self.poms_service.utilsPOMS.handle_dates(
            tmin, tmax, tdays, 'campaign_info?')

        last_activity_l = dbhandle.query(func.max(Task.updated)).filter(Task.campaign_id == campaign_id).first()
        logit.log("got last_activity_l %s" % repr(last_activity_l))
        if last_activity_l[0] and datetime.now(utc) - last_activity_l[0] > timedelta(days=7):
            last_activity = last_activity_l[0].strftime("%Y-%m-%d %H:%M:%S")
        else:
            last_activity = ""
        logit.log("after: last_activity %s" % repr(last_activity))

        campaign_definition_info = (dbhandle.query(CampaignDefinition, Experimenter)
                                    .filter(
            CampaignDefinition.campaign_definition_id == campaign_info.Campaign.campaign_definition_id,
            CampaignDefinition.creator == Experimenter.experimenter_id)
                                    .first())
        launch_template_info = (dbhandle.query(LaunchTemplate, Experimenter)
                                .filter(LaunchTemplate.launch_id == campaign_info.Campaign.launch_id,
                                        LaunchTemplate.creator == Experimenter.experimenter_id)
                                .first())
        tags = dbhandle.query(Tag).filter(CampaignsTags.campaign_id == campaign_id,
                                          CampaignsTags.tag_id == Tag.tag_id).all()

        launched_campaigns = dbhandle.query(CampaignSnapshot).filter(CampaignSnapshot.campaign_id == campaign_id).all()

        #
        # cloned from show_campaigns, but for a one row table..
        #
        campaign = campaign_info[0]
        counts = {}
        counts_keys = {}
        # cil = [c.campaign_id for c in cl]
        # dimlist, pendings = self.poms_service.filesPOMS.get_pending_for_campaigns(dbhandle, samhandle, cil, tmin, tmax)
        # effs = self.poms_service.jobsPOMS.get_efficiency(dbhandle, cil,tmin, tmax)
        # counts[campaign_id] = self.poms_service.triagePOMS.job_counts(dbhandle,tmax = tmax, tmin = tmin, tdays = tdays, campaign_id = campaign_id)
        # counts[campaign_id]['efficiency'] = effs[0]
        # if pendings:
        #    counts[campaign_id]['pending'] = pendings[0]
        # counts_keys[campaign_id] = list(counts[campaign_id].keys())
        #
        # any launch outputs to look at?
        #
        dirname = "{}/private/logs/poms/launches/campaign_{}".format(os.environ['HOME'], campaign_id)
        launch_flist = glob.glob('{}/*'.format(dirname))
        launch_flist = list(map(os.path.basename, launch_flist))

        # put our campaign id in the link
        campaign_kibana_link_format = config_get('campaign_kibana_link_format')
        logit.log("got format {}".format(campaign_kibana_link_format))
        kibana_link = campaign_kibana_link_format.format(campaign_id)

        dep_svg = self.campaign_deps_svg(dbhandle, config_get, camp_id=campaign_id)
        return (campaign_info,
                time_range_string,
                tmins, tmaxs, tdays,
                campaign_definition_info, launch_template_info,
                tags, launched_campaigns, None,
                campaign, counts_keys, counts, launch_flist, kibana_link,
                dep_svg, last_activity
                )

    @pomscache_10.cache_on_arguments()
    def campaign_time_bars(self, dbhandle, campaign_id=None, tag=None, tmin=None, tmax=None, tdays=1):
        """
            Give time-bars for Tasks for this campaign in a time window
            using the time_grid code
        """
        (
            tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays
        ) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays,
                                                     'campaign_time_bars?campaign_id={}&'.format(campaign_id))
        tg = time_grid.time_grid()
        key = tg.key()

        class fakerow:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        sl = deque()
        # sl.append(self.filesPOMS.format_self.triagePOMS.job_counts(dbhandle,))

        if campaign_id is not None:
            icampaign_id = int(campaign_id)
            q = dbhandle.query(Campaign).filter(Campaign.campaign_id == icampaign_id)
            cpl = q.all()
            name = cpl[0].name
        elif tag is not None and tag != "":
            q = dbhandle.query(Campaign).join(CampaignsTags, Tag).filter(
                Campaign.campaign_id == CampaignsTags.campaign_id, Tag.tag_id == CampaignsTags.tag_id,
                Tag.tag_name == tag)
            cpl = q.all()
            name = tag
        else:
            err_res = "404 Permission Denied."
            return "Neither Campaign nor Tag found"

        job_counts_list = deque()
        cidl = deque()
        for cp in cpl:
            job_counts_list.append(
                self.poms_service.filesPOMS.format_job_counts(dbhandle, campaign_id=cp.campaign_id, tmin=tmin,
                                                              tmax=tmax, tdays=tdays, range_string=time_range_string,
                                                              title_bits="Campaign %s" % cp.name))
            cidl.append(cp.campaign_id)

        job_counts = "<p></p>\n".join(job_counts_list)

        qr = dbhandle.query(TaskHistory).join(Task).filter(Task.campaign_id.in_(cidl),
                                                           TaskHistory.task_id == Task.task_id,
                                                           or_(and_(Task.created > tmin, Task.created < tmax),
                                                               and_(Task.updated > tmin,
                                                                    Task.updated < tmax))).order_by(TaskHistory.task_id,
                                                                                                    TaskHistory.created).all()
        items = deque()
        extramap = OrderedDict()
        for th in qr:
            jjid = self.poms_service.taskPOMS.task_min_job(dbhandle, th.task_id)
            if not jjid:
                jjid = 't' + str(th.task_id)
            else:
                jjid = jjid.replace('fifebatch', '').replace('.fnal.gov', '')

            if tag is not None:
                jjid = jjid + "<br>" + th.task_obj.campaign_obj.name

            if th.status != "Completed" and th.status != "Located":
                extramap[
                    jjid] = '<a href="{}/kill_jobs?task_id={:d}&act=hold"><i class="ui pause icon"></i></a><a href="{}/kill_jobs?task_id={:d}&act=release"><i class="ui play icon"></i></a><a href="{}/kill_jobs?task_id={:d}&act=kill"><i class="ui trash icon"></i></a>'.format(
                    self.poms_service.path, th.task_id, self.poms_service.path, th.task_id, self.poms_service.path,
                    th.task_id)
            else:
                extramap[jjid] = '&nbsp; &nbsp; &nbsp; &nbsp;'

            items.append(fakerow(task_id=th.task_id,
                                 created=th.created.replace(tzinfo=utc),
                                 tmin=th.task_obj.created - timedelta(minutes=15),
                                 tmax=th.task_obj.updated,
                                 status=th.status,
                                 jobsub_job_id=jjid))

        logit.log("campaign_time_bars: items: " + repr(items))

        blob = tg.render_query_blob(tmin, tmax, items, 'jobsub_job_id',
                                    url_template=self.poms_service.path + '/show_task_jobs?task_id=%(task_id)s&tmin=%(tmin)19.19s&tdays=1',
                                    extramap=extramap)

        return job_counts, blob, name, str(tmin)[:16], str(tmax)[:16], nextlink, prevlink, tdays, key, extramap

    def register_poms_campaign(self, dbhandle, experiment, campaign_name, version, user=None, campaign_definition=None,
                               dataset="", role="analysis", sesshandler=None, params=[]):
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

        if campaign_definition is not None and campaign_definition != "None":
            cd = dbhandle.query(CampaignDefinition).filter(Campaign.name == campaign_definition,
                                                           Campaign.experiment == experiment).first()
        else:
            cd = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.name.ilike("%generic%"),
                                                           Campaign.experiment == experiment).first()

        ld = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.name.ilike("%generic%"),
                                                   LaunchTemplate.experiment == experiment).first()

        logit.log("campaign_definition = {} ".format(cd))

        c = dbhandle.query(Campaign).filter(Campaign.experiment == experiment, Campaign.name == campaign_name).first()
        if c:
            changed = False
        else:
            c = Campaign(experiment=experiment, name=campaign_name, creator=user, created=datetime.now(utc),
                         software_version=version, campaign_definition_id=cd.campaign_definition_id,
                         launch_id=ld.launch_id, vo_role=role, dataset='',
                         creator_role=sesshandler('experimenter').session_role)

        if version:
            c.software_verison = version
            changed = True

        if dataset:
            c.dataset = dataset
            changed = True

        if user:
            c.experimenter = user
            changed = True

        logit.log("register_campaign -- campaign is %s" % c.__dict__)

        if changed:
            c.updated = datetime.now(utc)
            c.updator = user
            dbhandle.add(c)
            dbhandle.commit()

        return c.campaign_id

    def get_dataset_for(self, dbhandle, samhandle, err_res, camp):
        '''
            use the split_type modules to get the next dataset for
            launch for a given campaign
        '''

        if not camp.cs_split_type or camp.cs_split_type == 'None':
            return camp.dataset

        #
        # the module name is the first part of the string, i.e.
        # fred_by_whatever(xxx) -> 'fred'
        # new_localtime -> 'new'
        #
        p1 = camp.cs_split_type.find('_')
        if p1 < 0:
            p1 = camp.cs_split_type.find('(')
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

    def list_launch_file(self, campaign_id, fname, launch_template_id=None):
        '''
            get launch output file and return the lines as a list
        '''
        if launch_template_id:
            dirname = '{}/private/logs/poms/launches/template_tests_{}'.format(os.environ['HOME'], launch_template_id)
        else:
            dirname = '{}/private/logs/poms/launches/campaign_{}'.format(os.environ['HOME'], campaign_id)
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

    def schedule_launch(self, dbhandle, campaign_id):
        '''
            return crontab info for cron launches for campaign
        '''
        c = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        my_crontab = CronTab(user=True)
        citer = my_crontab.find_comment('POMS_CAMPAIGN_ID={}'.format(campaign_id))
        # there should be only zero or one...
        job = None
        for job in citer:
            break

        # any launch outputs to look at?
        #
        dirname = '{}/private/logs/poms/launches/campaign_{}'.format(os.environ['HOME'], campaign_id)
        launch_flist = glob.glob('{}/*'.format(dirname))
        launch_flist = list(map(os.path.basename, launch_flist))
        return c, job, launch_flist

    def update_launch_schedule(self, campaign_id, dowlist='', domlist='', monthly='', month='', hourlist='', submit='',
                               minlist='', delete=''):
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
        my_crontab.remove_all(comment='POMS_CAMPAIGN_ID={}'.format(campaign_id))

        if not delete:

            # make job for new -- use current link for product
            pdir = os.environ.get('POMS_DIR', '/etc/poms')
            if not pdir.find('/current/') > 0:
                # try to find a current symlink path that points here
                tpdir = pdir[:pdir.rfind('poms', 0, len(pdir) - 1) + 4] + '/current'
                if os.path.exists(tpdir):
                    pdir = tpdir

            job = my_crontab.new(command='{}/cron/launcher --campaign_id={}'.format(pdir, campaign_id),
                                 comment='POMS_CAMPAIGN_ID={}'.format(campaign_id))

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
                 .filter(CampaignRecovery.campaign_definition_id == campaign_def.campaign_definition_id)
                 .order_by(CampaignRecovery.recovery_order))

        # convert to a real list...
        l = deque()
        for r in rlist:
            l.append(r)
        rlist = l

        return rlist

    def make_stale_campaigns_inactive(self, dbhandle, err_res):
        '''
            turn off active flag on campaigns without recent activity
        '''
        lastweek = datetime.now(utc) - timedelta(days=7)
        recent_sq = dbhandle.query(distinct(Task.campaign_id)).filter(Task.created > lastweek)

        stale = (dbhandle.query(Campaign)
                 .filter(Campaign.created < lastweek, Campaign.campaign_id.notin_(recent_sq), Campaign.active == True)
                 .update({"active": False}, synchronize_session=False))

        dbhandle.commit()

        return []
