#!/usr/bin/env python
'''
This module contain the methods that handle the Task.
List of methods: wrapup_tasks,
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py
written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
'''

#from model.poms_model import Experiment, Job, Task, Campaign, Tag, JobFile
from datetime import datetime
#from LaunchPOMS import launch_recovery_if_needed
#from poms_service import poms_service

import time_grid
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from sqlalchemy import func
from utc import utc
from datetime import datetime, timedelta
import condor_log_parser
# our own logging handle, goes to cherrypy

import logging
logger = logging.getLogger('cherrypy.error')

from model.poms_model import Service, ServiceDowntime, Experimenter, Experiment, ExperimentsExperimenters, Job, JobHistory, Task, CampaignDefinition, TaskHistory, Campaign, LaunchTemplate, Tag, CampaignsTags, JobFile, CampaignSnapshot, CampaignDefinitionSnapshot,LaunchTemplateSnapshot,CampaignRecovery,RecoveryType, CampaignDependency


class TaskPOMS:

    def __init__(self, ps):
        self.poms_service=ps
        self.task_min_job_cache = {}

    def create_task(self, dbhandle, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor = None ):
        first,last,email = creator.split(' ')
        creator = self.poms_service.get_or_add_experimenter(first, last, email)
        exp = self.poms_service.get_or_add_experiment(experiment)
        td = self.poms_service.get_or_add_taskdef(taskdef, creator, exp)
        camp = self.poms_service.get_or_add_campaign(exp,td,creator)
        t = Task()
        t.campaign_id = camp.campaign_id
        #t.campaign_definition_id = td.campaign_definition_id
        t.task_order = 0
        t.input_dataset = input_dataset
        t.output_dataset = output_dataset
        t.waitingfor = waitingfor
        t.order = 0
        t.creator = creator.experimenter_id
        t.created = datetime.now(utc)
        t.status = "created"
        t.task_parameters = params
        t.waiting_threshold = 5
        t.updater = creator.experimenter_id
        t.updated = datetime.now(utc)

        dbhandle.add(t)
        dbhandle.commit()
        return str(t.task_id)


    def wrapup_tasks(self, dbhandle, loghandle, samhandle, getconfig): # this function call another function that is not in this module, it use a poms_service object passed as an argument at the init.
        now =  datetime.now(utc)
        res = ["wrapping up:"]

        #
        # make jobs which completed with no output files located.
        subq = dbhandle.query(func.count(JobFile.file_name)).filter(JobFile.job_id == Job.job_id, JobFile.file_type == 'output')
        dbhandle.query(Job).filter(subq == 0).update({'status':'Located'})
        #
        # check active tasks to see if they're completed/located
        for task in dbhandle.query(Task).options(subqueryload(Task.jobs)).filter(Task.status != "Completed", Task.status != "Located").all():
            total = 0
            running = 0
            for j in task.jobs:
                total = total + 1
                if j.status != "Completed" and j.status != "Located":
                    running = running + 1
            res.append("Task %d total %d running %d " % (task.task_id, total, running))
            if (total > 0 and running == 0) or (total == 0 and  now - task.created > timedelta(days= 2)):
                task.status = "Completed"
                task.updated = datetime.now(utc)
                dbhandle.add(task)
                # and check job logs for final runtime, cpu-time etc.
                condor_log_parser.get_joblogs(dbhandle, 
                   self.task_min_job(dbhandle, task.task_id),
                   task.campaign_snap_obj.experiment, 
                   task.campaign_snap_obj.vo_role)

        # mark them all completed, so we can look them over..
        dbhandle.commit()
        lookup_task_list = []
        lookup_dims_list = []
        lookup_exp_list = []
        #
        # move launch stuff etc, to one place, so we can keep the table rows 
        # so we need a list...
        #
        finish_up_tasks = {}
        n_completed = 0
        n_stale = 0
        n_project = 0
        n_located = 0
        # try with joinedload()...
        for task in dbhandle.query(Task).with_for_update(of=Task).options(joinedload(Task.jobs)).options(joinedload(Task.campaign_snap_obj)).options(joinedload(Task.campaign_definition_snap_obj)).filter(Task.status == "Completed").all():
            n_completed = n_completed + 1
            # if it's been 2 days, just declare it located; its as
            # located as its going to get...
            if (now - task.updated > timedelta(days=2)):
                n_located = n_located + 1
                n_stale = n_stale + 1
                task.status = "Located"
                for j in task.jobs:
                    j.status = "Located"
                    j.output_files_declared = True
                task.updated = datetime.now(utc)
                dbhandle.add(task)

            elif task.project:
                # task had a sam project, add to the list to look
                # up in sam
                n_project = n_project + 1
                basedims = "snapshot_for_project_name %s " % task.project
                allkiddims = basedims
                for pat in str(task.campaign_definition_snap_obj.output_file_patterns).split(','):
                    if pat == 'None':
                        pat = '%'
                    allkiddims = "%s and isparentof: ( file_name '%s' and version '%s' with availability physical ) " % (allkiddims, pat, task.campaign_snap_obj.software_version)

                lookup_exp_list.append(task.campaign_snap_obj.experiment)
                lookup_task_list.append(task)
                lookup_dims_list.append(allkiddims)
            else:
                # we don't have a project, guess off of located jobs
                locflag = True
                for j in task.jobs:
                    if j.status != "Located":
                        locflag = False

                if locflag:
                    n_located = n_located + 1
                    task.status = "Located"
                    for j in task.jobs:
                        j.status = "Located"
                        j.output_files_declared = True
                    task.updated = datetime.now(utc)
                    dbhandle.add(task)

            if task.status == "Located":
                finish_up_tasks[task.task_id] = task
                dbhandle.add(task)

        summary_list = samhandle.fetch_info_list(lookup_task_list)
        count_list = samhandle.count_files_list(lookup_exp_list,lookup_dims_list)
        thresholds = []
        loghandle("wrapup_tasks: summary_list: %s" % repr(summary_list)) ### Check if that is working

        for i in range(len(summary_list)):
            # XXX
            # this is using a 90% threshold, this ought to be
            # a tunable in the campaign_definition.  Basically we consider it
            # located if 90% of the files it consumed have suitable kids...
            # cfrac = lookup_task_list[i].campaign_definition_snap_obj.cfrac
            cfrac = 0.9
            threshold = (summary_list[i].get('tot_consumed',0) * cfrac)
            thresholds.append(threshold)
            if float(count_list[i]) >= threshold and threshold > 0:
                n_located = n_located + 1
                task = lookup_task_list[i]
                if task.status == "Completed":
                    task.status = "Located"
                    finish_up_tasks[task.task_id] = task
                for j in task.jobs:
                    j.status = "Located"
                    j.output_files_declared = True
                task.updated = datetime.now(utc)
                dbhandle.add(task)


        res.append("Counts: completed: %d stale: %d project %d: located %d" %
                    (n_completed, n_stale , n_project, n_located))

        res.append("count_list: %s" % count_list)
        res.append("thresholds: %s" % thresholds)
        res.append("lookup_dims_list: %s" % lookup_dims_list)

        dbhandle.commit()

        #
        # now, after committing to clear locks, we run through the
        # job logs for the tasks and update process stats, and 
        # launch any recovery jobs or jobs depending on us.
        # this way we don't keep the rows locked all day
        #
        for task_id, task in finish_up_tasks.items():
            # get logs for job for final cpu values, etc.
            logger.info("Starting finish_up_tasks items for task %s" % task_id)
            condor_log_parser.get_joblogs(dbhandle, 
                   self.task_min_job(dbhandle, task_id),
                   task.campaign_snap_obj.experiment, 
                   task.campaign_snap_obj.vo_role)

	    if not self.launch_recovery_if_needed(dbhandle, loghandle, samhandle, getconfig, task):
	       self.launch_dependents_if_needed(dbhandle, loghandle, samhandle, getconfig, task)

        return res


    def show_task_jobs(self, dbhandle, task_id, tmax = None, tmin = None, tdays = 1 ):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.poms_service.utilsPOMS.handle_dates(tmin, tmax,tdays,'show_task_jobs?task_id=%s' % task_id)

        jl = dbhandle.query(JobHistory,Job).filter(Job.job_id == JobHistory.job_id, Job.task_id==task_id ).order_by(JobHistory.job_id,JobHistory.created).all()
        tg = time_grid.time_grid()

        class fakerow:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
        items = []
        extramap = {}
        laststatus = None
        lastjjid = None
        for jh, j in jl:
            if j.jobsub_job_id:
                jjid= j.jobsub_job_id.replace('fifebatch','').replace('.fnal.gov','')
            else:
                jjid= 'j' + str(jh.job_id)

            if j.status != "Completed" and j.status != "Located":
                extramap[jjid] = '<a href="%s/kill_jobs?job_id=%d"><i class="ui trash icon"></i></a>' % (self.poms_service.path, jh.job_id)
            else:
                extramap[jjid] = '&nbsp; &nbsp; &nbsp; &nbsp;'
            if jh.status != laststatus or jjid != lastjjid:
                items.append(fakerow(job_id = jh.job_id,
                                    created = jh.created.replace(tzinfo=utc),
                                    status = jh.status,
                                    jobsub_job_id = jjid))
            laststatus = jh.status
            lastjjid = jjid

        job_counts = self.poms_service.filesPOMS.format_job_counts(dbhandle, task_id = task_id,tmin=tmins,tmax=tmaxs,tdays=tdays, range_string = time_range_string )
        key = tg.key(fancy=1)
        blob = tg.render_query_blob(tmin, tmax, items, 'jobsub_job_id', url_template=self.poms_service.path + '/triage_job?job_id=%(job_id)s&tmin='+tmins, extramap = extramap)
        #screendata = screendata +  tg.render_query(tmin, tmax, items, 'jobsub_job_id', url_template=self.path + '/triage_job?job_id=%(job_id)s&tmin='+tmins, extramap = extramap)

        if len(jl) > 0:
            campaign_id = jl[0][1].task_obj.campaign_id
            cname = jl[0][1].task_obj.campaign_snap_obj.name
        else:
            campaign_id = 'unknown'
            cname = 'unknown'

        task_jobsub_id = self.task_min_job(dbhandle, task_id)
        return_tuple=(blob, job_counts,task_id, str(tmin)[:16], str(tmax)[:16], extramap, key, task_jobsub_id, campaign_id, cname)
        return return_tuple

###
#No expose methods.
    def compute_status(self, dbhandle, task):
        st = self.poms_service.triagePOMS.job_counts(dbhandle, task_id = task.task_id)
        if task.status == "Located":
            return task.status
        res = "Idle"
        if (st['Held'] > 0):
            res = "Held"
        if (st['Running'] > 0):
            res = "Running"
        if (st['Completed'] > 0 and st['Idle'] == 0 and st['Held'] == 0):
            res = "Completed"
        return res


    def task_min_job(self, dbhandle, task_id): #This method deleted from the main script.
        # find the job with the logs -- minimum jobsub_job_id for this task
        # also will be nickname for the task...
        if ( self.task_min_job_cache.has_key(task_id) ):
           return self.task_min_job_cache.get(task_id)
        j = dbhandle.query(Job).filter( Job.task_id == task_id ).order_by(Job.jobsub_job_id).first()
        if j:
            self.task_min_job_cache[task_id] = j.jobsub_job_id
            return j.jobsub_job_id
        else:
            return None


    def get_task_id_for(self, dbhandle, campaign, user = None, experiment = None, command_executed = "", input_dataset = "", parent_task_id=None):
        if user == None:
             user = 4
        else:
             u = dbhandle.query(Experimenter).filter(Experimenter.email.like("%s@%%" % user)).first()
             if u:
                  user = u.experimenter_id
        q = dbhandle.query(Campaign)
        if campaign[0] in "0123456789":
            q = q.filter(Campaign.campaign_id == int(campaign))
        else:
            q = q.filter(Campaign.name.like("%%%s%%" % campaign))

        if experiment:
            q = q.filter(Campaign.experiment == experiment)

        c = q.first()
        tim = datetime.now(utc)
        t = Task(campaign_id = c.campaign_id,
                 task_order = 0,
                 input_dataset = input_dataset,
                 output_dataset = "",
                 status = "New",
                 task_parameters = "{}",
                 updater = 4,
                 creator = 4,
                 created = tim,
                 updated = tim,
                 command_executed = command_executed)

        if parent_task_id != None and parent_task_id != "None":
            t.recovery_tasks_parent = int(parent_task_id)

        self.snapshot_parts(dbhandle, t, t.campaign_id)

        dbhandle.add(t)
        dbhandle.commit()
        return t.task_id


    def snapshot_parts(self, dbhandle, t, campaign_id): ###This function was removed from the main script
         c = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
         for table, snaptable, field, sfield, tid , tfield in [
                [Campaign,CampaignSnapshot,Campaign.campaign_id,CampaignSnapshot.campaign_id,c.campaign_id, 'campaign_snap_obj' ],
                [CampaignDefinition, CampaignDefinitionSnapshot,CampaignDefinition.campaign_definition_id, CampaignDefinitionSnapshot.campaign_definition_id, c.campaign_definition_id, 'campaign_definition_snap_obj'],
                [LaunchTemplate ,LaunchTemplateSnapshot,LaunchTemplate.launch_id,LaunchTemplateSnapshot.launch_id,  c.launch_id, 'launch_template_snap_obj']]:

             i = dbhandle.query(func.max(snaptable.updated)).filter(sfield == tid).first()
             j = dbhandle.query(table).filter(field == tid).first()
             if (i[0] == None or j == None or j.updated == None or  i[0] < j.updated):
                newsnap = snaptable()
                columns = j._sa_instance_state.class_.__table__.columns
                for fieldname in columns.keys():
                     setattr(newsnap, fieldname, getattr(j,fieldname))
                dbhandle.add(newsnap)
             else:
                newsnap = dbhandle.query(snaptable).filter(snaptable.updated == i[0]).first()
             setattr(t, tfield, newsnap)
         dbhandle.add(t) #Felipe change HERE one tap + space to spaces indentation
         dbhandle.commit()


    def launch_dependents_if_needed(self, dbhandle, loghandle, samhandle, getconfig, t):
        loghandle("Entering launch_dependents_if_needed(%s)" % t.task_id)
        if not cherrypy.config.get("poms.launch_recovery_jobs",False):
            # XXX should queue for later?!?
            return 1
        cdlist = dbhandle.query(CampaignDependency).filter(CampaignDependency.needs_camp_id == t.campaign_snap_obj.campaign_id).all()

        i = 0
        for cd in cdlist:
           if cd.uses_camp_id == t.campaign_snap_obj.campaign_id:
              # self-reference, just do a normal launch
              self.poms_service.launch_jobs(cd.uses_camp_id)
           else:
              i = i + 1
              dims = "ischildof: (snapshot_for_project %s) and version %s and file_name like '%s' " % (t.project, t.campaign_snap_obj.software_version, cd.file_patterns)
              dname = "poms_depends_%d_%d" % (t.task_id,i)

              samhandle.create_definition(t.campaign_snap_obj.experiment, dname, dims)
              self.poms_service.launch_jobs(cd.uses_camp_id, dataset_override = dname)
        return 1


    def launch_recovery_if_needed(self, dbhandle, loghandle, samhandle, getconfig, t):
        loghandle("Entering launch_recovery_if_needed(%s)" % t.task_id)
        if not getconfig("poms.launch_recovery_jobs",False):
            # XXX should queue for later?!?
            return 1

        # if this is itself a recovery job, we go back to our parent
        # to do all the work, because it has the counters, etc.
        if t.parent_obj:
           t = t.parent_obj

        rlist = self.campaignsPOMS.get_recovery_list_for_campaign_def(dbhandle,t.campaign_definition_snap_obj)

        if t.recovery_position == None:
           t.recovery_position = 0

        while t.recovery_position != None and t.recovery_position < len(rlist):
            rtype = rlist[t.recovery_position].recovery_type
            t.recovery_position = t.recovery_position + 1
            if rtype.name == 'consumed_status':
                 recovery_dims = "snapshot_for_project_name %s and consumed_status != 'consumed'" % t.project
            elif rtype.name == 'proj_status':
                 recovery_dims = "snapshot_for_project_name %s and process_status != 'ok'" % t.project
            elif rtype.name == 'pending_files':
                 recovery_dims = "snapshot_for_project_name %s " % t.project
                 if t.campaign_definition_snap_obj.output_file_types:
                     oftypelist = campaign_definition_snap_obj.output_file_types.split(",")
                 else:
                     oftypelist = ["%"]

                 for oft in oftypelist:
                     recovery_dims = recovery_dims + "minus isparent: ( version %s and file_name like %s) " % (t.campaign_snap_obj.software_version, oft)
            else:
                 # default to consumed status(?)
                 recovery_dims = "snapshot_for_project_name %s and consumed_status != 'consumed'" % t.project

            nfiles = samhandle.count_files(t.campaign_snap_obj.experiment,recovery_dims)

            t.recovery_position = t.recovery_position + 1
            dbhandle.add(t)
            dbhandle.commit()

            if nfiles > 0:
                rname = "poms_recover_%d_%d" % (t.task_id,t.recovery_position)

                loghandle("launch_recovery_if_needed: creating dataset for exp=%s name=%s dims=%s" % (t.campaign_snap_obj.experiment, rname, recovery_dims))

                samhandle.create_definition(t.campaign_snap_obj.experiment, rname, recovery_dims)


                self.poms_service.launch_jobs(t.campaign_snap_obj.campaign_id, dataset_override=rname, parent_task_id = t.task_id)
                return 1

        return 0
