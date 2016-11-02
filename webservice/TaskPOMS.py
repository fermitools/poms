#!/usr/bin/env python
'''
This module contain the methods that handle the Task.
List of methods: wrapup_tasks,
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py
written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
'''

#from model.poms_model import Experiment, Job, Task, Campaign, Tag, JobFile
#from datetime import datetime
#from LaunchPOMS import launch_recovery_if_needed
#from poms_service import poms_service

import time_grid
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from utc import utc

from model.poms_model import Service, ServiceDowntime, Experimenter, Experiment, ExperimentsExperimenters, Job, JobHistory, Task, CampaignDefinition, TaskHistory, Campaign, LaunchTemplate, Tag, CampaignsTags, JobFile, CampaignSnapshot, CampaignDefinitionSnapshot,LaunchTemplateSnapshot,CampaignRecovery,RecoveryType, CampaignDependency


class TaskPOMS:

    def __init__(self, ps):
        self.poms_service=ps

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


    def wrapup_tasks(self, dbhandle, samhandle): # this function call another function that is not in this module, it use a poms_service object passed as an argument at the init.
        now =  datetime.now(utc)
        res = ["wrapping up:"]
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

        # mark them all completed, so we can look them over..
        dbhandle.commit()
        lookup_task_list = []
        lookup_dims_list = []
                lookup_exp_list = []
        n_completed = 0
        n_stale = 0
        n_project = 0
        n_located = 0
        for task in dbhandle.query(Task).with_for_update(of=Task).options(subqueryload(Task.jobs)).options(subqueryload(Task.campaign_obj,Campaign.campaign_definition_obj)).filter(Task.status == "Completed").all():
            n_completed = n_completed + 1
            # if it's been 2 days, just declare it located; its as
            # located as its going to get...
            if (now - task.updated > timedelta(days=2)):
                n_located = n_located + 1
                n_stale = n_stale + 1
                task.status = "Located"
                task.updated = datetime.now(utc)
                dbhandle.add(task)
                if not self.poms_service.launch_recovery_if_needed(task.task_id):
                    self.poms_service.launch_dependents_if_needed(task.task_id)
            elif task.project:
                # task had a sam project, add to the list to look
                # up in sam
                n_project = n_project + 1
                basedims = "snapshot_for_project_name %s " % task.project
                allkiddims = basedims
                for pat in str(task.campaign_obj.campaign_definition_obj.output_file_patterns).split(','):
                    if pat == 'None':
                        pat = '%'
                    allkiddims = "%s and isparentof: ( file_name '%s' and version '%s' with availability physical ) " % (allkiddims, pat, task.campaign_obj.software_version)
                                lookup_exp_list.append(task.campaign_obj.experiment)
                lookup_task_list.append(task)
                lookup_dims_list.append(allkiddims)
            else:
                # we don't have a project, guess off of located jobs
                 locflag = True
                for j in task.jobs:
                    if j.status != "Located":
                        locaflag = False
                if locflag:
                    n_located = n_located + 1
                    task.status = "Located"
                    task.updated = datetime.now(utc)
                    cherrypy.request.db.add(task)
                    if not self.poms_service.launch_recovery_if_needed(task.task_id):
                        self.poms_services.launch_dependents_if_needed(task.task_id)

        dbhandle.commit()
        summary_list = samhandle.fetch_info_list(lookup_task_list)
        count_list = samhandle.samweb_lite.count_files_list(lookup_exp_list,lookup_dims_list)
        thresholds = []
                cherrypy.log("wrapup_tasks: summary_list: %s" % repr(summary_list))

        for i in range(len(summary_list)):
            # XXX
            # this is using a 90% threshold, this ought to be
            # a tunable in the campaign_definition.  Basically we consider it
            # located if 90% of the files it consumed have suitable kids...
            # cfrac = lookup_task_list[i].campaign_obj.campaign_definition_obj.cfrac
            cfrac = 0.9
            threshold = (summary_list[i].get('tot_consumed',0) * cfrac)
            thresholds.append(threshold)
            if float(count_list[i]) >= threshold and threshold > 0:
                n_located = n_located + 1
                task = lookup_task_list[i]
                task.status = "Located"
                task.updated = datetime.now(utc)
                dbhandle.add(task)
                if not self.poms_service.launch_recovery_if_needed(task.task_id):
                    self.poms_servicelaunch_dependents_if_needed(task.task_id)

        res.append("Counts: completed: %d stale: %d project %d: located %d" %
                    (n_completed, n_stale , n_project, n_located))

        res.append("count_list: %s" % count_list)
        res.append("thresholds: %s" % thresholds)
        res.append("lookup_dims_list: %s" % lookup_dims_list)

        dbhandle.commit()

        return "\n".join(res)



    def show_task_jobs(self, task_id, tmax = None, tmin = None, tdays = 1 ):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.poms_service.handle_dates(tmin, tmax,tdays,'show_task_jobs?task_id=%s' % task_id)

        jl = dbhandle.query(JobHistory,Job).filter(Job.job_id == JobHistory.job_id, Job.task_id==task_id ).order_by(JobHistory.job_id,JobHistory.created).all()
        tg = self.poms_service.time_grid.time_grid()

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
                extramap[jjid] = '<a href="%s/kill_jobs?job_id=%d"><i class="ui trash icon"></i></a>' % (self.path, jh.job_id)
            else:
                extramap[jjid] = '&nbsp; &nbsp; &nbsp; &nbsp;'
            if jh.status != laststatus or jjid != lastjjid:
                items.append(fakerow(job_id = jh.job_id,
                                    created = jh.created.replace(tzinfo=utc),
                                    status = jh.status,
                                    jobsub_job_id = jjid))
            laststatus = jh.status
            lastjjid = jjid

        job_counts = self.poms_service.format_job_counts(task_id = task_id,tmin=tmins,tmax=tmaxs,tdays=tdays, range_string = time_range_string )
        key = tg.key(fancy=1)
        blob = tg.render_query_blob(tmin, tmax, items, 'jobsub_job_id', url_template=self.path + '/triage_job?job_id=%(job_id)s&tmin='+tmins, extramap = extramap)
        #screendata = screendata +  tg.render_query(tmin, tmax, items, 'jobsub_job_id', url_template=self.path + '/triage_job?job_id=%(job_id)s&tmin='+tmins, extramap = extramap)

        if len(jl) > 0:
            campaign_id = jl[0][1].task_obj.campaign_id
            cname = jl[0][1].task_obj.campaign_obj.name
        else:
            campaign_id = 'unknown'
            cname = 'unknown'

        task_jobsub_id = self.poms_service.task_min_job(task_id)
        return_tuple=(blob, job_counts,task_id, str(tmin)[:16], str(tmax)[:16], extramap, key, task_jobsub_id, campaign_id, cname)
        return return_tuple

###
#No expose methods.
    def compute_status(self, task):
        st = self.poms_service.job_counts(task_id = task.task_id)
        if task.status == "Located":
            return task.status
        res = "Idle"
        if (st['Held'] > 0):
            res = "Held"
        if (st['Running'] > 0):
            res = "Running"
        if (st['Completed'] > 0 and st['Idle'] == 0 and st['Held'] == 0):
            res = "Completed"
            # no, not here we wait for "Located" status..
            #if task.status != "Completed":
            #    if not self.launch_recovery_if_needed(task.task_id):
            #        self.launch_dependents_if_needed(task.task_id)
        return res
