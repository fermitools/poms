#!/usr/bin/env python

'''
This module contain the methods that allow to create campaigns, definitions and templates.
List of methods:  launch_template_edit, campaign_definition_edit, campaign_edit, campaign_edit_query.
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White.
Date: September 30, 2016.
'''

from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from model.poms_model import (Experiment, Experimenter, Campaign, CampaignDependency,
    LaunchTemplate, CampaignDefinition, CampaignRecovery,
    CampaignsTags, Tag, CampaignSnapshot, RecoveryType, TaskHistory, Task
)
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from sqlalchemy import or_, and_ , not_
from crontab import CronTab
from datetime import datetime, tzinfo,timedelta
import time
import time_grid
import json
from utc import utc
import os
import glob



class CampaignsPOMS():


    def __init__(self, ps):
        self.poms_service=ps


    def launch_template_edit(self, dbhandle, loghandle, seshandle, pcl_call = 0,*args, **kwargs):
        data = {}
        message = None
        data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)
        action = kwargs.pop('action',None)
        exp = kwargs.pop('experiment',None)
        pcl_call = kwarg.pop('pcl_call')
        pc_email = kwarg.pop('pc_email',None)
        if action == 'delete':
            name = kwargs.pop('name')
            try:
                dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).delete()
                dbhandle.commit()
            except Exception, e:
                message = "The launch template, %s, has been used and may not be deleted." % name
                loghandle(message)
                loghandle(e.message)
                dbhandle.rollback()

        if action == 'add' or action == 'edit':
            if pcl_call == 1:
                experimenter_id = dbhandle.query(Experimenter).filter(Experimenter.email == pc_email).first().experimenter_id
                ae_launch_id = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).fist().launch_id
                ae_launch_name = kwargs.pop('ae_launch_name')
                ae_launch_host = kwargs.pop('ae_launch_host')
                ae_launch_account = kwargs.pop('ae_launch_account')
                ae_launch_setup = kwargs.pop('ae_launch_setup')
                if ae_launch_host in [None,""]:
                    ae_launch_host=dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).fist().launch_host
                if ae_launch_account in [None,""]:
                    ae_launch_account=dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).fist().launch_account
                if ae_launch_setup in [None,""]:
                    ae_launch_account=dbhandle.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).fist().launch_setup
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
            except IntegrityError, e:
                message = "Integrity error - you are most likely using a name which already exists in database."
                loghandle(e.message)
                dbhandle.rollback()
            except SQLAlchemyError, e:
                message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % e.message
                loghandle(e.message)
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


    def campaign_definition_edit(self, dbhandle, loghandle, seshandle, *args, **kwargs):
        data = {}
        message = None
        data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)
        action = kwargs.pop('action',None)
        exp = kwargs.pop('experiment',None)
        if action == 'delete':
            name = kwargs.pop('name')
            cid = kwargs.pop('campaign_definition_id')
            try:
                dbhandle.query(CampaignRecovery).filter(CampaignRecovery.campaign_definition_id==cid).delete()
                dbhandle.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==cid).delete()
                dbhandle.commit()
            except Exception, e:
                message = "The campaign definition, %s, has been used and may not be deleted." % name
                loghandle(message)
                loghandle(e.message)
                dbhandle.rollback()

        if action == 'add' or action == 'edit':
            campaign_definition_id = kwargs.pop('ae_campaign_definition_id')
            name = kwargs.pop('ae_definition_name')
            input_files_per_job = kwargs.pop('ae_input_files_per_job')
            output_files_per_job = kwargs.pop('ae_output_files_per_job')
            output_file_patterns = kwargs.pop('ae_output_file_patterns')
            launch_script = kwargs.pop('ae_launch_script')
            definition_parameters = kwargs.pop('ae_definition_parameters')
            recoveries = kwargs.pop('ae_definition_recovery')
            experimenter_id = kwargs.pop('experimenter_id')
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
                dbhandle.query(CampaignRecovery).filter(CampaignRecovery.campaign_definition_id == campaign_definition_id).delete()
                i = 0
                for rtn in json.loads(recoveries):
                    rt = dbhandle.query(RecoveryType).filter(RecoveryType.name==rtn).first()
                    cr = CampaignRecovery(campaign_definition_id = campaign_definition_id, recovery_order = i, recovery_type = rt)
                    dbhandle.add(cr)
                dbhandle.commit()
            except IntegrityError, e:
                message = "Integrity error - you are most likely using a name which already exists in database."
                loghandle(e.message)
                dbhandle.rollback()
            except SQLAlchemyError, e:
                message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % e.message
                loghandle(e.message)
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
                    print ' cid= %s rec name= %s rec order =%s rec parms= %s' %(cid,rec.recovery_type.name,rec.recovery_order,rec.param_overrides)
                    #rec_list.append(rec.recovery_type.name )
                    #new="""
                    co_vals= '%s' %rec.param_overrides
                    rec_vals=[rec.recovery_type.name,co_vals]
                    rec_list.append(rec_vals)
                    #"""
                recs_dict[cid] = json.dumps(rec_list)
                print ' cid= %s, json dump= %s' %(cid,recs_dict[cid])
            data['recoveries'] = recs_dict
            data['rtypes'] = (dbhandle.query(RecoveryType.name,RecoveryType.description).order_by(RecoveryType.name).all())

        data['message'] = message
        return data


    def campaign_edit(self, dbhandle, loghandle, sesshandle, *args, **kwargs):
        data = {}
        message = None
        data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)
        #for k,v in kwargs.items():
        #    print ' k=%s, v=%s ' %(k,v)
        action = kwargs.pop('action',None)
        exp = kwargs.pop('experiment',None)
        if action == 'delete':
            campaign_id = kwargs.pop('campaign_id')
            name = kwargs.pop('name')
            try:
                dbhandle.query(CampaignDependency).filter(or_(CampaignDependency.needs_camp_id==campaign_id,
                                CampaignDependency.uses_camp_id==campaign_id)).delete()
                dbhandle.query(Campaign).filter(Campaign.campaign_id==campaign_id).delete()
                dbhandle.commit()
            except Exception, e:
                message = "The campaign, %s, has been used and may not be deleted." % name
                loghandle(message)
                loghandle(e.message)
                dbhandle.rollback()

        if action == 'add' or action == 'edit':
            campaign_id = kwargs.pop('ae_campaign_id')
            name = kwargs.pop('ae_campaign_name')
            active = kwargs.pop('ae_active')
            split_type = kwargs.pop('ae_split_type')
            vo_role = kwargs.pop('ae_vo_role')
            software_version = kwargs.pop('ae_software_version')
            dataset = kwargs.pop('ae_dataset')
            param_overrides = kwargs.pop('ae_param_overrides')
            campaign_definition_id = kwargs.pop('ae_campaign_definition_id')
            launch_id = kwargs.pop('ae_launch_id')
            experimenter_id = kwargs.pop('experimenter_id')
            completion_type = kwargs.pop('ae_completion_type')
            completion_pct =  kwargs.pop('ae_completion_pct')
            depends = kwargs.pop('ae_depends')
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
                    dbhandle.flush() ##### Is this flush() necessary or better a commit ?
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
                loghandle("depends for %s are: %s" % (campaign_id, depends))
                depcamps = dbhandle.query(Campaign).filter(Campaign.name.in_(depends['campaigns'])).all()
                for i in range(len(depcamps)):
                    loghandle("trying to add dependency for: %s" % depcamps[i].name)
                    d = CampaignDependency(uses_camp_id = campaign_id, needs_camp_id = depcamps[i].campaign_id, file_patterns=depends['file_patterns'][i])
                    dbhandle.add(d)
                dbhandle.commit()
            except IntegrityError, e:
                message = "Integrity error - you are most likely using a name which already exists in database."
                loghandle(e.message)
                dbhandle.rollback()
            except SQLAlchemyError, e:
                message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % e.message
                loghandle(e.message)
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
        e = dbhandle.query(Experimenter).filter(like_)(Experimenter.email,"%s@%%" % experimenter_name ).first()
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


    def show_campaigns(self, dbhandle, loghandle, samhandle, campaign_id=None, experiment=None, tmin=None, tmax=None, tdays=1, active=True):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.poms_service.utilsPOMS.handle_dates(tmin,tmax,tdays,'show_campaigns?')

        #cq = dbhandle.query(Campaign).filter(Campaign.active==active).order_by(Campaign.experiment)
        cq = dbhandle.query(Campaign).options(joinedload('experiment_obj')).filter(Campaign.active==active).order_by(Campaign.experiment)

        if experiment:
            cq = cq.filter(Campaign.experiment==experiment)

        cl = cq.all()

        counts = {}
        counts_keys = {}

        dimlist, pendings = self.poms_service.filesPOMS.get_pending_for_campaigns(dbhandle, loghandle, samhandle, cl, tmin, tmax)
        effs = self.poms_service.jobsPOMS.get_efficiency(dbhandle, loghandle, cl, tmin, tmax)

        i = 0
        for c in cl:
            counts[c.campaign_id] = self.poms_service.triagePOMS.job_counts(dbhandle, tmax=tmax, tmin=tmin, tdays=tdays, campaign_id=c.campaign_id)
            counts[c.campaign_id]['efficiency'] = effs[i]
            if len(pendings) > i:
                counts[c.campaign_id]['pending'] = pendings[i]
            counts_keys[c.campaign_id] = counts[c.campaign_id].keys()
            i = i + 1
        return counts, counts_keys, cl, dimlist, tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string


    def campaign_info(self, dbhandle, loghandle, samhandle, err_res, campaign_id,  tmin = None, tmax = None, tdays = None):
        campaign_id = int(campaign_id)

        Campaign_info = dbhandle.query(Campaign, Experimenter).filter(Campaign.campaign_id == campaign_id, Campaign.creator == Experimenter.experimenter_id).first()

        # default to time window of campaign
        if tmin == None and tdays == None and tdays == None:
            tmin = Campaign_info.Campaign.created
            tmax = datetime.now(utc)

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.poms_service.utilsPOMS.handle_dates(tmin,tmax,tdays,'campaign_info?')

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
        dimlist, pendings = self.poms_service.filesPOMS.get_pending_for_campaigns(dbhandle, loghandle, samhandle, cl, tmin, tmax)
        effs = self.poms_service.jobsPOMS.get_efficiency(dbhandle, loghandle, cl,tmin, tmax)
        counts[campaign_id] = self.poms_service.triagePOMS.job_counts(dbhandle,tmax = tmax, tmin = tmin, tdays = tdays, campaign_id = campaign_id)
        counts[campaign_id]['efficiency'] = effs[0]
        if pendings:
            counts[campaign_id]['pending'] = pendings[0]
        counts_keys[campaign_id] = counts[campaign_id].keys()
        #
        # any launch outputs to look at?
        #
        dirname="%s/private/logs/poms/launches/campaign_%s" % (
           os.environ['HOME'],campaign_id)
        launch_flist = glob.glob('%s/*' % dirname)
        launch_flist = map(os.path.basename, launch_flist)
        return Campaign_info, time_range_string, tmins, tmaxs, Campaign_definition_info, Launch_template_info, tags, launched_campaigns, dimlist, cl, counts_keys, counts, launch_flist


    def campaign_time_bars(self, dbhandle, campaign_id = None, tag = None, tmin = None, tmax = None, tdays = 1):
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.poms_service.utilsPOMS.handle_dates(tmin, tmax,tdays,'campaign_time_bars?campaign_id=%s&'% campaign_id)
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


    def register_poms_campaign(self, dbhandle, loghandle, experiment,  campaign_name, version, user = None, campaign_definition = None, dataset = "", role = "Analysis", params = []):
         if user == None:
              user = 4
         else:
              u = dbhandle.query(Experimenter).filter(Experimenter.email.like("%s@%%" % user)).first()
              if u:
                   user = u.experimenter_id


         if campaign_definition != None and campaign_definition != "None":
              cd = dbhandle.query(CampaignDefinition).filter(Campaign.name == campaign_definition, Campaign.experiment == experiment).first()
         else:
              cd = dbhandle.query(CampaignDefinition).filter(CampaignDefinition.name.like("%generic%"), Campaign.experiment == experiment).first()

         ld = dbhandle.query(LaunchTemplate).filter(LaunchTemplate.name.like("%generic%"), LaunchTemplate.experiment == experiment).first()

         loghandle("campaign_definition = %s " % cd)

         c = dbhandle.query(Campaign).filter( Campaign.experiment == experiment, Campaign.name == campaign_name).first()
         if c:
             changed = False
         else:
             c = Campaign(experiment = experiment, name = campaign_name, creator = user, created = datetime.now(utc), software_version = version, campaign_definition_id=cd.campaign_definition_id, launch_id = ld.launch_id, vo_role = role)

         if version:
               c.software_verison = version
               changed = True

         if dataset:
               c.dataset = dataset
               changed = True

         if user:
               c.experimenter = user
               changed = True

         loghandle("register_campaign -- campaign is %s" % c.__dict__)

         if changed:
                c.updated = datetime.now(utc)
                c.updator = user
                dbhandle.add(c)
                dbhandle.commit()

         return c.campaign_id


    def get_dataset_for(self, dbhandle, samhandle, err_res, camp):
        res = None

        if camp.cs_split_type == None or camp.cs_split_type in [ '', 'draining','None' ]:
            # no split to do, it is a draining datset, etc.
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
        lines = lf.readlines()
        lf.close()
        return lines


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
        launch_flist = map(os.path.basename, launch_flist)
        return c, job, launch_flist


    def update_launch_schedule(self, loghandle, campaign_id, dowlist = None,  domlist = None, monthly = None, month = None, hourlist = None, submit = None , minlist = None, delete = None):

        # deal with single item list silliness
        if isinstance(minlist, basestring):
           minlist = minlist.split(",")
        if isinstance(hourlist, basestring):
           hourlist = hourlist.split(",")
        if isinstance(dowlist, basestring):
           dowlist = dowlist.split(",")
        if isinstance(domlist, basestring):
           domlist = domlist.split(",")

        loghandle("hourlist is %s " % hourlist)

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
            pdir=pdir[:pdir.rfind("poms")+4] + "/current"
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
        if not self.poms_service.accessPOMS.can_report_data(cherrypy.request.headers.get, cherrypy.log, cherrypy.session.get)():
             raise err_res(401, 'You are not authorized to access this resource')
        lastweek = datetime.now(utc) - timedelta(days=7)
        cp = dbhandle.query(Task.campaign_id).filter(Task.created > lastweek).group_by(Task.campaign_id).all()
        sc = []
        for cid in cp:
            sc.append(cid)

        stale =  dbhandle.query(Campaign).filter(Campaign.campaign_id.notin_(sc), Campaign.active == True).all()
        res=[]
        for c in stale:
            res.append(c.name)
            c.active=False
            dbhandle.add(c)


        dbhandle.commit()

        return res
