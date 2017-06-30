#!/usr/bin/env python

'''
This module contain the methods that allow to create campaigns, definitions and templates.
List of methods:  launch_template_edit, campaign_definition_edit, campaign_edit, campaign_edit_query.
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White.
Date: April 28th, 2017. (changes for the POMS_client)
'''

from . import logit
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from sqlalchemy import func, desc, not_, and_, or_
from .poms_model import (Experiment, Experimenter, Campaign, CampaignDependency,
    LaunchTemplate, CampaignDefinition, CampaignRecovery,
    CampaignsTags, Tag, CampaignSnapshot, RecoveryType, TaskHistory, Task
)
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from crontab import CronTab
from datetime import datetime, tzinfo,timedelta
import time
from . import time_grid
import json
from .utc import utc
import os
import glob
from .pomscache import pomscache, pomscache_10
import subprocess



class CampaignsPOMS():


    def __init__(self, ps):
        self.poms_service=ps


    def launch_template_edit(self, dbhandle, seshandle, *args, **kwargs):
        data = {}
        message = None
        data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)
        action = kwargs.pop('action',None)
        exp = kwargs.pop('experiment',None)
        exp = seshandle('experimenter').session_experiment
        pcl_call = int(kwargs.pop('pcl_call', 0))
        pc_username = kwargs.pop('pc_username',None)

        if action == 'delete':
            ae_launch_name = kwargs.pop('ae_launch_name')
            name = ae_launch_name
            try:
                dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).delete()
                dbhandle.commit()
            except Exception as e:
                message = "The launch template, %s, has been used and may not be deleted." % name
                logit.log(message)
                logit.log(' '.join(e.args))
                dbhandle.rollback()

        if action == 'add' or action == 'edit':
            if pcl_call == 1:
                ae_launch_name = kwargs.pop('ae_launch_name')
                name = ae_launch_name
                experimenter_id = dbhandle.query(Experimenter).filter(Experimenter.username == pc_username).first().experimenter_id
                if action == 'edit':
                    ae_launch_id = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).first().launch_id
                else:
                    print("I'm action =! add therefore there is no ae_launch_id save")
                ae_launch_host = kwargs.pop('ae_launch_host', None)
                ae_launch_account = kwargs.pop('ae_launch_account', None)
                ae_launch_setup = kwargs.pop('ae_launch_setup', None)
                if ae_launch_host in [None,""]:
                    ae_launch_host=dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).first().launch_host
                if ae_launch_account in [None,""]:
                    ae_launch_account=dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).first().launch_account
                if ae_launch_setup in [None,""]:
                    ae_launch_account=dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).first().launch_setup
            else:
                ae_launch_name = kwargs.pop('ae_launch_name')
                ae_launch_id = kwargs.pop('ae_launch_id')
                experimenter_id = kwargs.pop('experimenter_id')
                ae_launch_host = kwargs.pop('ae_launch_host')
                ae_launch_account = kwargs.pop('ae_launch_account')
                ae_launch_setup = kwargs.pop('ae_launch_setup')

            try:
                if action == 'add':
                    template = LaunchTemplate(experiment=exp, name=ae_launch_name, launch_host=ae_launch_host, launch_account=ae_launch_account,
                                                launch_setup=ae_launch_setup,creator = experimenter_id, created = datetime.now(utc))
                    dbhandle.add(template)
                else:
                    columns = {
                                "name":           ae_launch_name,
                                "launch_host":    ae_launch_host,
                                "launch_account": ae_launch_account,
                                "launch_setup":   ae_launch_setup,
                                "updated":        datetime.now(utc),
                                "updater":        experimenter_id
                                }
                    template = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.launch_id==ae_launch_id).update(columns)
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

        # Find templates
        if exp: # cuz the default is find
            data['curr_experiment'] = exp
            data['authorized'] = seshandle('experimenter').is_authorized(exp)
            data['templates'] = dbhandle.query(LaunchTemplate,Experiment).join(Experiment).filter(LaunchTemplate.experiment==exp).order_by(LaunchTemplate.name)
        data['message'] = message
        return data


    def campaign_definition_edit(self, dbhandle, seshandle, *args, **kwargs):
        data = {}
        message = None
        data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)
        action = kwargs.pop('action',None)
        exp = seshandle('experimenter').session_experiment
        #added for poms_client
        pcl_call = int(kwargs.pop('pcl_call', 0)) #pcl_call == 1 means the method was access through the poms_client.
        pc_username = kwargs.pop('pc_username',None) #email is the info we know about the user in POMS DB.

        if action == 'delete':
            name = kwargs.pop('ae_definition_name')
            if pcl_call == 1: #Enter here if the access was from the poms_client
                cid=campaign_definition_id=dbhandle.query(CampaignDefinition).filter(CampaignDefinition.name==name).first().campaign_definition_id
            else:
                cid = kwargs.pop('campaign_definition_id')
            try:
                dbhandle.query(CampaignRecovery).filter(CampaignRecovery.campaign_definition_id==cid).delete()
                dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==cid).delete()
                dbhandle.commit()
            except Exception as e:
                message = "The campaign definition, %s, has been used and may not be deleted." % name
                logit.log(message)
                logit.log(' '.join(e.args))
                dbhandle.rollback()

        if action == 'add' or action == 'edit':
            if pcl_call == 1: #Enter here if the access was from the poms_client
                name = kwargs.pop('ae_definition_name')
                experimenter_id = dbhandle.query(Experimenter).filter(Experimenter.username == pc_username).first().experimenter_id
                if action == 'edit':
                    campaign_definition_id=dbhandle.query(CampaignDefinition).filter(CampaignDefinition.name==name).first().campaign_definition_id #Check here!
                else:
                    pass
                input_files_per_job = kwargs.pop('ae_input_files_per_job',0)
                output_files_per_job = kwargs.pop('ae_output_files_per_job',0)
                output_file_patterns = kwargs.pop('ae_output_file_patterns')
                launch_script = kwargs.pop('ae_launch_script')
                definition_parameters = kwargs.pop('ae_definition_parameters')
                recoveries = kwargs.pop('ae_definition_recovery',"[]")
                #Guetting the info that was not passed by the poms_client arguments
                if input_files_per_job in [None,""]:
                    input_files_per_job = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==campaign_definition_id).firts().input_files_per_job
                if output_files_per_job in [None,""]:
                    output_files_per_job= dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==campaign_definition_id).firts().output_files_per_job
                if output_file_patterns in [None,""]:
                    output_file_patterns = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==campaign_definition_id).firts().output_file_patterns
                if launch_script in [None,""]:
                    launch_script = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==campaign_definition_id).firts().launch_script
                if definition_parameters in [None,""]:
                    definition_parameters = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==campaign_definition_id).firts().definition_parameters
            else:
                experimenter_id = kwargs.pop('experimenter_id')
                campaign_definition_id = kwargs.pop('ae_campaign_definition_id')
                name = kwargs.pop('ae_definition_name')
                input_files_per_job = kwargs.pop('ae_input_files_per_job',0)
                output_files_per_job = kwargs.pop('ae_output_files_per_job',0)
                output_file_patterns = kwargs.pop('ae_output_file_patterns')
                launch_script = kwargs.pop('ae_launch_script')
                definition_parameters = json.loads(kwargs.pop('ae_definition_parameters'))
                recoveries = kwargs.pop('ae_definition_recovery')
            try:
                if action == 'add':
                    cd = CampaignDefinition( name=name, experiment=exp,
                            input_files_per_job=input_files_per_job, output_files_per_job = output_files_per_job,
                            output_file_patterns = output_file_patterns,
                            launch_script=launch_script, definition_parameters=definition_parameters,
                            creator=experimenter_id, created=datetime.now(utc))

                    dbhandle.add(cd)
                    dbhandle.flush()
                    campaign_definition_id = cd.campaign_definition_id
                else:
                    columns = {
                                "name":                  name,
                                "input_files_per_job":   input_files_per_job,
                                "output_files_per_job":  output_files_per_job,
                                "output_file_patterns":  output_file_patterns,
                                "launch_script":         launch_script,
                                "definition_parameters": definition_parameters,
                                "updated":               datetime.now(utc),
                                "updater":               experimenter_id
                                }
                    cd = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==campaign_definition_id).update(columns)

                # now fixup recoveries -- clean out existing ones, and
                # add listed ones.
                if pcl_call == 0:
                    dbhandle.query(CampaignRecovery).filter(CampaignRecovery.campaign_definition_id == campaign_definition_id).delete()
                    i = 0
                    for rtn in json.loads(recoveries):
                        rect   = rtn[0]
                        recpar = rtn[1]
                        rt = dbhandle.query(RecoveryType).filter(RecoveryType.name==rect).first()
                        cr = CampaignRecovery(campaign_definition_id = campaign_definition_id, recovery_order = i, recovery_type = rt, param_overrides = recpar)
                        dbhandle.add(cr)
                    dbhandle.commit()
                else:
                    pass #We need to define later if it is going to be possible to modify the recovery type from the client.

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
        if exp: # cuz the default is find
            data['curr_experiment'] = exp
            data['authorized'] = seshandle('experimenter').is_authorized(exp)
            # for testing ui...
            #data['authorized'] = True
            data['definitions'] = (dbhandle.query(CampaignDefinition,Experiment)
                                    .join(Experiment)
                                    .filter(CampaignDefinition.experiment==exp)
                                    .order_by(CampaignDefinition.name)
                                    )

            # Build the recoveries for each campaign.
            cids = [row[0].campaign_definition_id for row in data['definitions'].all()]
            recs_dict = {}
            for cid in cids:
                recs = (dbhandle.query(CampaignRecovery).join(CampaignDefinition).options(joinedload(CampaignRecovery.recovery_type))
                    .filter(CampaignRecovery.campaign_definition_id == cid,CampaignDefinition.experiment == exp)
                    .order_by(CampaignRecovery.campaign_definition_id, CampaignRecovery.recovery_order))
                rec_list  = []
                for rec in recs:
                    if  type(rec.param_overrides) == type(""):
                        if rec.param_overrides in ('','{}','[]'): rec.param_overrides="[]"
                        rec_vals=[rec.recovery_type.name,json.loads(rec.param_overrides)]
                    else:
                        rec_vals=[rec.recovery_type.name,rec.param_overrides]

                    #rec_vals=[rec.recovery_type.name,rec.param_overrides]
                    rec_list.append(rec_vals)
                recs_dict[cid] = json.dumps(rec_list)

            data['recoveries'] = recs_dict
            data['rtypes'] = (dbhandle.query(RecoveryType.name,RecoveryType.description).order_by(RecoveryType.name).all())

        data['message'] = message
        return data


    def campaign_edit(self, dbhandle, sesshandle, *args, **kwargs):
        data = {}
        message = None
        exp = sesshandle.get('experimenter').session_experiment
        data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)
        #for k,v in kwargs.items():
        #    print ' k=%s, v=%s ' %(k,v)
        action = kwargs.pop('action',None)
        pcl_call = int(kwargs.pop('pcl_call', 0)) #pcl_call == 1 means the method was access through the poms_client.
        pc_username = kwargs.pop('pc_username',None) #email is the info we know about the user in POMS DB.

        if action == 'delete':
            name = kwargs.pop('ae_campaign_name')
            if pcl_call==1:
                campaign_id=dbhandle.query(Campaign).filter(Campaign.name==name).first().campaign_id
            else:
                campaign_id = kwargs.pop('campaign_id')
            try:
                dbhandle.query(CampaignDependency).filter(or_(CampaignDependency.needs_camp_id==campaign_id,
                                CampaignDependency.uses_camp_id==campaign_id)).delete()
                dbhandle.query(Campaign).filter(Campaign.campaign_id==campaign_id).delete()
                dbhandle.commit()
            except Exception as e:
                message = "The campaign, %s, has been used and may not be deleted." % name
                logit.log(message)
                logit.log(' '.join(e.args))
                dbhandle.rollback()

        if action == 'add' or action == 'edit':
            name = kwargs.pop('ae_campaign_name')
            active = kwargs.pop('ae_active')
            split_type = kwargs.pop('ae_split_type')
            vo_role = kwargs.pop('ae_vo_role')
            software_version = kwargs.pop('ae_software_version')
            dataset = kwargs.pop('ae_dataset')
            ###Mark

            completion_type = kwargs.pop('ae_completion_type')
            completion_pct =  kwargs.pop('ae_completion_pct')
            depends = kwargs.pop('ae_depends',"[]")
            param_overrides = kwargs.pop('ae_param_overrides',"[]")
            if param_overrides:param_overrides = json.loads(param_overrides)

            if pcl_call == 1:
                launch_name=kwargs.pop('ae_launch_name')
                campaign_definition_name=kwargs.pop('ae_campaign_definition')
                #all this variables depend on the arguments passed.
                experimenter_id = dbhandle.query(Experimenter).filter(Experimenter.username == pc_username).first().experimenter_id
                launch_id=dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==launch_name).first().launch_id
                campaign_definition_id =dbhandle.query(CampaignDefinition).filter(CampaignDefinition.name==campaign_definition_name).first().campaign_definition_id
                if action == 'edit':
                    campaign_id=dbhandle.query(Campaign).filter(Campaign.name==name).first().campaign_id
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
                    if not completion_pct:completion_pct=95
                    c = Campaign(name=name, experiment=exp,vo_role=vo_role,
                                active=active, cs_split_type = split_type,
                                software_version=software_version, dataset=dataset,
                                param_overrides=param_overrides, launch_id=launch_id,
                                campaign_definition_id=campaign_definition_id,
                                completion_type=completion_type,completion_pct=completion_pct,
                                creator=experimenter_id, created=datetime.now(utc))
                    dbhandle.add(c)
                    dbhandle.commit() ##### Is this flush() necessary or better a commit ?
                    campaign_id = c.campaign_id
                else:
                    columns = {
                                "name":                  name,
                                "vo_role":               vo_role,
                                "active":                active,
                                "cs_split_type":         split_type,
                                "software_version":      software_version,
                                "dataset" :              dataset,
                                "param_overrides":       param_overrides,
                                "campaign_definition_id":campaign_definition_id,
                                "launch_id":             launch_id,
                                "updated":               datetime.now(utc),
                                "updater":               experimenter_id,
                                "completion_type":       completion_type,
                                "completion_pct":       completion_pct
                                }
                    cd = dbhandle.query(Campaign).filter(Campaign.campaign_id==campaign_id).update(columns)
            # now redo dependencies
                dbhandle.query(CampaignDependency).filter(CampaignDependency.uses_camp_id == campaign_id).delete()
                logit.log("depends for %s are: %s" % (campaign_id, depends))
                depcamps = dbhandle.query(Campaign).filter(Campaign.name.in_(depends['campaigns'])).all()
                for i in range(len(depcamps)):
                    logit.log("trying to add dependency for: %s" % depcamps[i].name)
                    d = CampaignDependency(uses_camp_id = campaign_id, needs_camp_id = depcamps[i].campaign_id, file_patterns=depends['file_patterns'][i])
                    dbhandle.add(d)
                dbhandle.commit()
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

        # Find campaigns
        if exp: # cuz the default is find
            # for testing ui...
            #data['authorized'] = True
            state = kwargs.pop('state',None)
            if state == None:
                state = sesshandle.get('campaign_edit.state','state_active')
            sesshandle['campaign_edit.state'] = state
            data['state'] = state
            data['curr_experiment'] = exp
            data['authorized'] = sesshandle.get('experimenter').is_authorized(exp)
            cquery = dbhandle.query(Campaign).filter(Campaign.experiment==exp)
            if state == 'state_active':
                cquery = cquery.filter(Campaign.active==True)
            elif state == 'state_inactive':
                cquery = cquery.filter(Campaign.active==False)
            cquery = cquery.order_by(Campaign.name)
            data['campaigns'] = cquery
            data['definitions'] = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.experiment==exp).order_by(CampaignDefinition.name)
            data['templates'] = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).order_by(LaunchTemplate.name)
            cids = [c.campaign_id for c in data['campaigns'].all()]
            depends = {}
            for cid in cids:
                sql = (dbhandle.query(CampaignDependency.uses_camp_id, Campaign.name, CampaignDependency.file_patterns )
                        .filter(CampaignDependency.uses_camp_id == cid,
                        Campaign.campaign_id == CampaignDependency.needs_camp_id))
                deps = {
                        "campaigns"     : [row[1] for row  in sql.all()],
                        "file_patterns" : [row[2] for row  in sql.all()]
                        }
                depends[cid] = json.dumps(deps)
            data['depends'] = depends
        data['message'] = message
        return data

    def campaign_edit_query(self, dbhandle, *args, **kwargs):
        data = {}
        ae_launch_id = kwargs.pop('ae_launch_id',None)
        ae_campaign_definition_id = kwargs.pop('ae_campaign_definition_id',None)

        if ae_launch_id:
            template = {}
            temp = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.launch_id==ae_launch_id).first()
            template['launch_host'] = temp.launch_host
            template['launch_account'] = temp.launch_account
            template['launch_setup'] = temp.launch_setup
            data['template'] = template

        if ae_campaign_definition_id:
            definition = {}
            cdef = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==ae_campaign_definition_id).first()
            definition['input_files_per_job'] = cdef.input_files_per_job
            definition['output_files_per_job'] = cdef.output_files_per_job
            definition['launch_script'] = cdef.launch_script
            definition['definition_parameters'] = cdef.definition_parameters
            data['definition'] = definition
        return json.dumps(data)


    def new_task_for_campaign(dbhandle , campaign_name, command_executed, experimenter_name, dataset_name = None):
        c = dbhandle.query(Campaign).filter(Campaign.name == campaign_name).first()
        e = dbhandle.query(Experimenter).filter(Experimenter.username==experimenter_name).first()
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

    def campaign_deps_svg(self, dbhandle, config, tag):
        cl = dbhandle.query(Campaign).join(CampaignsTags,Tag).filter(Tag.tag_name == tag, CampaignsTags.tag_id == Tag.tag_id, CampaignsTags.campaign_id == Campaign.campaign_id).all()
        c_ids = []
        pdot = subprocess.Popen("tee /tmp/dotstuff | dot -Tsvg", shell=True, stdin = subprocess.PIPE, stdout=subprocess.PIPE, universal_newlines = True)
        pdot.stdin.write('digraph %sDependencies {\n' % tag)
        pdot.stdin.write('node [shape=box, style=rounded, color=lightgrey, fontcolor=black]\nrankdir = "LR";\n')
        baseurl="%s/campaign_info?campaign_id=" % config.get("pomspath")

        for c in cl:
            tcl = dbhandle.query(func.count(Task.status), Task.status).group_by(Task.status).filter(Task.campaign_id == c.campaign_id).all()
            tot = 0
            ltot = 0
            for (count, status) in tcl:
                 tot = tot + count
                 if status == 'Located':
                     ltot = count
            c_ids.append(c.campaign_id)
            pdot.stdin.write('c%d [URL="%s%d",label="%s\\nSubmissions %d Located %d",color=%s];\n' % (
                c.campaign_id,
                baseurl,
                c.campaign_id,
                c.name,
                tot,
                ltot,
                ("darkgreen" if ltot == tot else "black")
                ))

        cdl = dbhandle.query(CampaignDependency).filter(CampaignDependency.needs_camp_id.in_(c_ids)).all()

        for cd in cdl:
             pdot.stdin.write('c%d -> c%d;\n' % ( cd.needs_camp_id, cd.uses_camp_id ))

        pdot.stdin.write('}\n')
        pdot.stdin.close()
        text = pdot.stdout.read()
        pdot.wait()
        return bytes(text,encoding="utf-8")

    @pomscache.cache_on_arguments()
    def show_campaigns(self, dbhandle, samhandle,  campaign_id=None, experiment=None, tmin=None, tmax=None, tdays=7, active=True, tag = None):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string, tdays = self.poms_service.utilsPOMS.handle_dates(tmin,tmax,tdays,'show_campaigns?')

        #cq = dbhandle.query(Campaign).filter(Campaign.active==active).order_by(Campaign.experiment)

        logit.log(logit.DEBUG, "show_campaigns: querying active")
        cq = dbhandle.query(Campaign).options(joinedload('experiment_obj')).filter(Campaign.active==active).order_by(Campaign.experiment)

        if experiment:
            cq = cq.filter(Campaign.experiment==experiment)

        if tag:
            cq = cq.join(CampaignsTags).join(Tag).filter(Tag.tag_name == tag)

        cl = cq.all()
        logit.log(logit.DEBUG,"show_campaigns: back from query")

        counts = {}
        counts_keys = {}

        return cl, tmin, tmax, tmins, tmaxs, tdays, nextlink, prevlink, time_range_string


    # @pomscache.cache_on_arguments()
    def campaign_info(self, dbhandle, samhandle, err_res, config_get, campaign_id,  tmin = None, tmax = None, tdays = None):
        campaign_id = int(campaign_id)

        Campaign_info = dbhandle.query(Campaign, Experimenter).filter(Campaign.campaign_id == campaign_id, Campaign.creator == Experimenter.experimenter_id).first()

        # default to time window of campaign
        if tmin == None and tdays == None and tdays == None:
            tmin = Campaign_info.Campaign.created
            tmax = datetime.now(utc)

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string, tdays = self.poms_service.utilsPOMS.handle_dates(tmin,tmax,tdays,'campaign_info?')

        Campaign_definition_info =  dbhandle.query(CampaignDefinition, Experimenter).filter(CampaignDefinition.campaign_definition_id == Campaign_info.Campaign.campaign_definition_id, CampaignDefinition.creator == Experimenter.experimenter_id ).first()
        Launch_template_info = dbhandle.query(LaunchTemplate, Experimenter).filter(LaunchTemplate.launch_id == Campaign_info.Campaign.launch_id, LaunchTemplate.creator == Experimenter.experimenter_id).first()
        tags = dbhandle.query(Tag).filter(CampaignsTags.campaign_id==campaign_id, CampaignsTags.tag_id==Tag.tag_id).all()

        launched_campaigns = dbhandle.query(CampaignSnapshot).filter(CampaignSnapshot.campaign_id == campaign_id).all()

        #
        # cloned from show_campaigns, but for a one row table..
        #
        cl = [Campaign_info[0]]
        counts = {}
        counts_keys = {}
        cil = [c.campaign_id for c in cl]
        dimlist, pendings = self.poms_service.filesPOMS.get_pending_for_campaigns(dbhandle, samhandle, cil, tmin, tmax)
        effs = self.poms_service.jobsPOMS.get_efficiency(dbhandle, cil,tmin, tmax)
        counts[campaign_id] = self.poms_service.triagePOMS.job_counts(dbhandle,tmax = tmax, tmin = tmin, tdays = tdays, campaign_id = campaign_id)
        counts[campaign_id]['efficiency'] = effs[0]
        if pendings:
            counts[campaign_id]['pending'] = pendings[0]
        counts_keys[campaign_id] = list(counts[campaign_id].keys())
        #
        # any launch outputs to look at?
        #
        dirname="%s/private/logs/poms/launches/campaign_%s" % (
           os.environ['HOME'],campaign_id)
        launch_flist = glob.glob('%s/*' % dirname)
        launch_flist = list(map(os.path.basename, launch_flist))

        # put our campaign id in the link
        campaign_kibana_link_format = config_get('campaign_kibana_link_format')
        logit.log("got format %s" %  campaign_kibana_link_format)
        kibana_link = campaign_kibana_link_format % campaign_id

        return Campaign_info, time_range_string, tmins, tmaxs, tdays, Campaign_definition_info, Launch_template_info, tags, launched_campaigns, dimlist, cl, counts_keys, counts, launch_flist, kibana_link


    @pomscache_10.cache_on_arguments()
    def campaign_time_bars(self, dbhandle, campaign_id = None, tag = None, tmin = None, tmax = None, tdays = 1):
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string,tdays = self.poms_service.utilsPOMS.handle_dates(tmin, tmax,tdays,'campaign_time_bars?campaign_id=%s&'% campaign_id)
        tg = time_grid.time_grid()
        key = tg.key()

        class fakerow:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        sl = []
        # sl.append(self.filesPOMS.format_self.triagePOMS.job_counts(dbhandle,))

        if campaign_id != None:
            icampaign_id = int(campaign_id)
            q = dbhandle.query(Campaign).filter(Campaign.campaign_id == icampaign_id)
            cpl = q.all()
            name = cpl[0].name
        elif tag != None and tag != "":
            q = dbhandle.query(Campaign).join(CampaignsTags,Tag).filter(Campaign.campaign_id == CampaignsTags.campaign_id, Tag.tag_id == CampaignsTags.tag_id, Tag.tag_name == tag)
            cpl = q.all()
            name = tag
        else:
            err_res="404 Permission Denied."
            return "Neither Campaign nor Tag found"

        job_counts_list = []
        cidl = []
        for cp in cpl:
             job_counts_list.append(cp.name)
             job_counts_list.append( self.poms_service.filesPOMS.format_job_counts(dbhandle, campaign_id = cp.campaign_id, tmin = tmin, tmax = tmax, tdays = tdays, range_string = time_range_string))
             cidl.append(cp.campaign_id)

        job_counts = "\n".join(job_counts_list)

        qr = dbhandle.query(TaskHistory).join(Task).filter(Task.campaign_id.in_(cidl), TaskHistory.task_id == Task.task_id , or_(and_(Task.created > tmin, Task.created < tmax),and_(Task.updated > tmin, Task.updated < tmax)) ).order_by(TaskHistory.task_id,TaskHistory.created).all()
        items = []
        extramap = {}
        for th in qr:
            jjid = self.poms_service.taskPOMS.task_min_job(dbhandle, th.task_id)
            if not jjid:
                jjid= 't' + str(th.task_id)
            else:
                jjid = jjid.replace('fifebatch','').replace('.fnal.gov','')
            if th.status != "Completed" and th.status != "Located":
                extramap[jjid] = '<a href="%s/kill_jobs?task_id=%d"><i class="ui trash icon"></i></a>' % (self.poms_service.path, th.task_id)
            else:
                extramap[jjid] = '&nbsp; &nbsp; &nbsp; &nbsp;'

            items.append(fakerow(task_id = th.task_id,
                                  created = th.created.replace(tzinfo = utc),
                                  tmin = th.task_obj.created - timedelta(minutes=15),
                                  tmax = th.task_obj.updated,
                                  status = th.status,
                                  jobsub_job_id = jjid))

        blob = tg.render_query_blob(tmin, tmax, items, 'jobsub_job_id', url_template = self.poms_service.path + '/show_task_jobs?task_id=%(task_id)s&tmin=%(tmin)19.19s&tdays=1',extramap = extramap )

        return job_counts, blob, name, str(tmin)[:16], str(tmax)[:16], nextlink, prevlink, tdays,key, extramap


    def register_poms_campaign(self, dbhandle, experiment,  campaign_name, version, user = None, campaign_definition = None, dataset = "", role = "Analysis", params = []):
         if dataset == None:
              dataset = ''
         if user == None:
              user = 4
         else:
              u = dbhandle.query(Experimenter).filter(Experimenter.username==user).first()
              if u:
                   user = u.experimenter_id


         if campaign_definition != None and campaign_definition != "None":
              cd = dbhandle.query(CampaignDefinition).filter(Campaign.name == campaign_definition, Campaign.experiment == experiment).first()
         else:
              cd = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.name.ilike("%generic%"), Campaign.experiment == experiment).first()

         ld = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.name.ilike("%generic%"), LaunchTemplate.experiment == experiment).first()

         logit.log("campaign_definition = %s " % cd)

         c = dbhandle.query(Campaign).filter( Campaign.experiment == experiment, Campaign.name == campaign_name).first()
         if c:
             changed = False
         else:
             c = Campaign(experiment = experiment, name = campaign_name, creator = user, created = datetime.now(utc), software_version = version, campaign_definition_id=cd.campaign_definition_id, launch_id = ld.launch_id, vo_role = role, dataset = '')

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
        res = None

        if camp.cs_split_type == None or camp.cs_split_type in [ '', 'draining','None' ]:
            # no split to do, it is a draining dataset, etc.
            res =  camp.dataset

        elif camp.cs_split_type == 'list':
            # we were given a list of datasets..
            l = camp.dataset.split(',')
            if camp.cs_last_split == '' or camp.cs_last_split == None:
                camp.cs_last_split = -1
            camp.cs_last_split += 1

            if camp.cs_last_split >= len(l):
                raise err_res(404, 'No more splits in this campaign')

            res = l[camp.cs_last_split]

            dbhandle.add(camp)
            dbhandle.commit()

        elif camp.cs_split_type.startswith('mod_') or camp.cs_split_type.startswith('mod('):
            m = int(camp.cs_split_type[4:].strip(')'))
            if camp.cs_last_split == '' or camp.cs_last_split == None:
                camp.cs_last_split = -1
            camp.cs_last_split += 1

            if camp.cs_last_split >= m:
                raise err_res(404, 'No more splits in this campaign')
            new = camp.dataset + "_slice%d" % camp.cs_last_split
            samhandle.create_definition(camp.campaign_definition_obj.experiment, new,  "defname: %s with stride %d offset %d" % (camp.dataset, m, camp.cs_last_split))

            res = new

            dbhandle.add(camp)
            dbhandle.commit()

        elif ( camp.cs_split_type.startswith('new(') or
             camp.cs_split_type == 'new' or
             camp.cs_split_type == 'new_local' ):

            # default parameters
            tfts = 1800.0 # half an hour
            twindow = 604800.0     # one week
            tround = 1             # one second
            tlocaltime = 0         # assume GMT
            tfirsttime = None          # override start time

            if camp.cs_split_type[3:] == '_local':
                tlocaltime = 1

            # if they specified any, grab them ...
            if camp.cs_split_type[3] == '(':
                parms = camp.cs_split_type[4:].split(',')
                for p in parms:
                    pmult = 1
                    if p.endswith(')'): p=p[:-1]
                    if p.endswith('w'): pmult = 604800; p=p[:-1]
                    if p.endswith('d'): pmult = 86400; p=p[:-1]
                    if p.endswith('h'): pmult = 3600; p=p[:-1]
                    if p.endswith('m'): pmult = 60; p=p[:-1]
                    if p.endswith('s'): pmult = 1; p=p[:-1]
                    if p.startswith('window='): twindow = float(p[7:]) * pmult
                    if p.startswith('round='): tround = float(p[6:]) * pmult
                    if p.startswith('fts='): tfts = float(p[4:]) * pmult
                    if p.startswith('localtime='): tlocaltime = float(p[10:]) * pmult
                    if p.startswith('firsttime='): tfirsttime = float(p[10:]) * pmult

            # make sure time-window is a multiple of rounding factor
            twindow = int(twindow) - (int(twindow) % int(tround))

            # pick a boundary time, which will be our default start time
            # if we've not been run before, and also as a boundary to
            # say we have nothing to do yet if our last run isnt that far
            # back...
            #   go back one time window (plus fts delay) and then
            # round down to nearest tround to get a start time...
            # then later ones should come out even.

            bound_time = time.time() - tfts - twindow
            bound_time = int(bound_time) - (int(bound_time) % int(tround))

            if camp.cs_last_split == '' or camp.cs_last_split == None:
                if tfirsttime:
                    stime = tfirsttime
                else:
                    stime = bound_time
                etime = stime + twindow
            else:
                if camp.cs_last_split >= bound_time:
                     raise err_res(404, 'Not enough new data yet')

                stime = camp.cs_last_split
                etime = stime + twindow

            if tlocaltime:
                sstime = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(stime))
                setime = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(etime))
            else:
                sstime = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(stime))
                setime = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(etime))

            new = camp.dataset + "_since_%s" % int(stime)

            samhandle.create_definition(
                camp.campaign_definition_obj.experiment,
                new,
                "defname: %s and end_time > '%s' and end_time <= '%s'" % (
                      camp.dataset, sstime, setime
                )
            )

            # mark end time for start of next run
            camp.cs_last_split = etime
            res = new

            dbhandle.add(camp)
            dbhandle.commit()

        return res


    def list_launch_file(self, campaign_id, fname ):
        dirname="%s/private/logs/poms/launches/campaign_%s" % (
           os.environ['HOME'],campaign_id)
        lf = open("%s/%s" % (dirname, fname), "r")
        sb = os.fstat(lf.fileno())
        lines = lf.readlines()
        lf.close()
        # if file is recent set refresh to watch it
        if (time.time() - sb[8]) < 5 :
            refresh = 3
        elif (time.time() - sb[8]) < 30 :
            refresh = 10
        else:
            refresh = 0
        return lines, refresh


    def schedule_launch(self, dbhandle, campaign_id ):
        c = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        my_crontab = CronTab(user=True)
        citer = my_crontab.find_comment("POMS_CAMPAIGN_ID=%s" % campaign_id)
        # there should be only zero or one...
        job = None
        for job in citer:
            break

        # any launch outputs to look at?
        #
        dirname="%s/private/logs/poms/launches/campaign_%s" % (
           os.environ['HOME'],campaign_id)
        launch_flist = glob.glob('%s/*' % dirname)
        launch_flist = list(map(os.path.basename, launch_flist))
        return c, job, launch_flist


    def update_launch_schedule(self, campaign_id, dowlist = '',  domlist = '', monthly = '', month = '', hourlist = '', submit = '' , minlist = '', delete = ''):

        # deal with single item list silliness
        if isinstance(minlist, str):
           minlist = minlist.split(",")
        if isinstance(hourlist, str):
           hourlist = hourlist.split(",")
        if isinstance(dowlist, str):
           dowlist = dowlist.split(",")
        if isinstance(domlist, str):
           domlist = domlist.split(",")

        logit.log("hourlist is %s " % hourlist)

        if minlist[0] == "*":
            minlist = None
        else:
            minlist = [int(x) for x in minlist if x != '']

        if hourlist[0] == "*":
            hourlist = None
        else:
            hourlist = [int(x) for x in hourlist if x != '']

        if dowlist[0] == "*":
            dowlist = None
        else:
            # dowlist[0] = [int(x) for x in dowlist if x != '']
            pass

        if domlist[0] == "*":
            domlist = None
        else:
            domlist = [int(x) for x in domlist if x != '']

        my_crontab = CronTab(user=True)
        # clean out old
        my_crontab.remove_all(comment="POMS_CAMPAIGN_ID=%s" % campaign_id)

        if not delete:

            # make job for new -- use current link for product
            pdir=os.environ.get("POMS_DIR","/etc/poms")
            pdir=pdir[:pdir.rfind("poms",0,len(pdir)-1)+4] + "/current"
            job = my_crontab.new(command="%s/cron/launcher --campaign_id=%s" % (
                              pdir, campaign_id),
                              comment="POMS_CAMPAIGN_ID=%s" % campaign_id)

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
        rlist = dbhandle.query(CampaignRecovery).options(joinedload(CampaignRecovery.recovery_type)).filter(CampaignRecovery.campaign_definition_id == campaign_def.campaign_definition_id).order_by(CampaignRecovery.recovery_order)

        # convert to a real list...
        l = []
        for r in rlist:
            l.append(r)
        rlist = l

        return rlist


    def make_stale_campaigns_inactive(self, dbhandle, err_res):
        lastweek = datetime.now(utc) - timedelta(days=7)
        cp = dbhandle.query(Task.campaign_id).filter(Task.created > lastweek).group_by(Task.campaign_id).all()
        sc = []
        for cid in cp:
            sc.append(cid)

        stale =  dbhandle.query(Campaign).filter(Campaign.created > lastweek, Campaign.campaign_id.notin_(sc), Campaign.active == True).all()
        res=[]
        for c in stale:
            res.append(c.name)
            c.active=False
            dbhandle.add(c)


        dbhandle.commit()

        return res
