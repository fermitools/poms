#!/usr/bin/env python

'''
This module contain the methods that allow to create campaigns, definitions and templates.
List of methods:  launch_template_edit, campaign_definition_edit, campaign_edit, campaign_edit_query.
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White.
Date: September 30, 2016.
'''

from model.poms_model import Experiment, Campaign, LaunchTemplate, CampaignDefinition, CampaignRecovery
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from utc import utc


class CampaignsPOMS():

    def launch_template_edit(self, dbhandle, loghandle, seshandle, *args, **kwargs):
        data = {}
        message = None
        data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)
        action = kwargs.pop('action',None)
        exp = kwargs.pop('experiment',None)
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
            ae_launch_id = kwargs.pop('ae_launch_id')
            ae_launch_name = kwargs.pop('ae_launch_name')
            ae_launch_host = kwargs.pop('ae_launch_host')
            ae_launch_account = kwargs.pop('ae_launch_account')
            ae_launch_setup = kwargs.pop('ae_launch_setup')
            experimenter_id = kwargs.pop('experimenter_id')
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
            data['templates'] = dbhandle(LaunchTemplate,Experiment).join(Experiment).filter(LaunchTemplate.experiment==exp).order_by(LaunchTemplate.name)
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
                loghandle.rollback()

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
                rec_list.append(rec.recovery_type.name )
            recs_dict[cid] = json.dumps(rec_list)
            data['recoveries'] = recs_dict
            data['rtypes'] = (dbhandle.query(RecoveryType.name,RecoveryType.description).order_by(RecoveryType.name).all())

        data['message'] = message
        return data


    def campaign_edit(self, dbhandle, loghandle, sesshandle, *args, **kwargs):
        data = {}
        message = None
        data['exp_selections'] = dbhandle.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)
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
            depends = kwargs.pop('ae_depends')
            if depends and depends != "[]":
                depends = json.loads(depends)
            else:
                depends = {"campaigns": [], "file_patterns": []}
            try:
                if action == 'add':
                    c = Campaign(name=name, experiment=exp,vo_role=vo_role,
                                active=active, cs_split_type = split_type,
                                software_version=software_version, dataset=dataset,
                                param_overrides=param_overrides, launch_id=launch_id,
                                campaign_definition_id=campaign_definition_id,
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
                                "updater":               experimenter_id
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
                sql = (db.query(CampaignDependency.uses_camp_id, Campaign.name, CampaignDependency.file_patterns )
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


    def show_campaigns(self, dbhandle, experiment = None, tmin = None, tmax = None, tdays = 1, active = True):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.poms_service.utilsPOMS.handle_dates(tmin,tmax,tdays,'show_campaigns?')

        cq = dbhandle.query(Campaign).filter(Campaign.active == active ).order_by(Campaign.experiment)

        if experiment:
            cq = cq.filter(Campaign.experiment == experiment)

        cl = cq.all()

        counts = {}
        counts_keys = {}

        dimlist, pendings = self.poms_service.get_pending_for_campaigns(cl, tmin, tmax)
        effs = self.poms_service.get_efficiency(cl, tmin, tmax)

        i = 0
        for c in cl:
            counts[c.campaign_id] = self.poms_service.triagePOMS.job_counts(dbhandle, tmax = tmax, tmin = tmin, tdays = tdays, campaign_id = c.campaign_id)
            counts[c.campaign_id]['efficiency'] = effs[i]
            counts[c.campaign_id]['pending'] = pendings[i]
            counts_keys[c.campaign_id] = counts[c.campaign_id].keys()
            i = i + 1
        return counts, counts_keys, cl, dimlist, tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string


    def campaign_info(self, dbhandle, err_res, campaign_id, tmin = None, tmax = None, tdays = None):
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
        dimlist, pendings = self.poms_service.get_pending_for_campaigns(cl, tmin, tmax)
        effs = self.poms_service.get_efficiency(cl, tmin, tmax)
        counts[campaign_id] = self.poms_service.triagePOMS.job_counts(dbhandle,tmax = tmax, tmin = tmin, tdays = tdays, campaign_id = campaign_id)
        counts[campaign_id]['efficiency'] = effs[0]
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
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.utilsPOMS.handle_dates(tmin, tmax,tdays,'campaign_time_bars?campaign_id=%s&'% campaign_id)
        tg = time_grid.time_grid()
        key = tg.key()

        class fakerow:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        sl = []
        # sl.append(self.filesPOMS.format_self.triagePOMS.job_counts(dbhandle,))

        q = dbhandle.query(Campaign)
        if campaign_id != None:
            q = q.filter(Campaign.campaign_id == campaign_id)
            cpl = q.all()
            name = cpl[0].name
        elif tag != None and tag != "":
            q = q.join(CampaignsTags,Tag).filter(Campaign.campaign_id == CampaignsTags.campaign_id,
                        Tag.tag_id == CampaignsTags.tag_id, Tag.tag_name == tag)
            cpl = q.all()
            name = tag
        else:
            err_res="404 Permission Denied."
            return "Neither Campaign nor Tag found"

        job_counts_list = []
        cidl = []
        for cp in cpl:
             job_counts_list.append(cp.name)
             job_counts_list.append( self.poms_service.filesPOMS.format_job_counts(campaign_id = cp.campaign_id, tmin = tmin, tmax = tmax, tdays = tdays, range_string = time_range_string))
             cidl.append(cp.campaign_id)

        job_counts = "\n".join(job_counts_list)

        qr = dbhandle.query(TaskHistory).join(Task).filter(Task.campaign_id.in_(cidl), TaskHistory.task_id == Task.task_id , or_(and_(Task.created > tmin, Task.created < tmax),and_(Task.updated > tmin, Task.updated < tmax)) ).order_by(TaskHistory.task_id,TaskHistory.created).all()
        items = []
        extramap = {}
        for th in qr:
            jjid = self.poms_service.task_min_job(th.task_id)
            if not jjid:
                jjid= 't' + str(th.task_id)
            else:
                jjid = jjid.replace('fifebatch','').replace('.fnal.gov','')
            if th.status != "Completed" and th.status != "Located":
                extramap[jjid] = '<a href="%s/kill_jobs?task_id=%d"><i class="ui trash icon"></i></a>' % (self.path, th.task_id)
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
