#!/usr/bin/env python
'''
This module contain the methods that handle the Task.
List of methods: wrapup_tasks,
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py
written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
'''

from datetime import datetime

import time_grid
from sqlalchemy.orm import subqueryload, joinedload, contains_eager
from sqlalchemy import func, text
from utc import utc
from datetime import timedelta
import condor_log_parser
import json
from collections import OrderedDict
import subprocess
import time
import select
import os
import logit
from exceptions import KeyError

from poms.model.poms_model import (Service,
                                   # ServiceDowntime,
                                   Experimenter,
                                   # Experiment,
                                   # ExperimentsExperimenters,
                                   Job,
                                   JobHistory,
                                   Task,
                                   CampaignDefinition,
                                   # TaskHistory,
                                   Campaign,
                                   LaunchTemplate,
                                   # Tag,
                                   # CampaignsTags,
                                   # JobFile,
                                   CampaignSnapshot,
                                   CampaignDefinitionSnapshot,
                                   LaunchTemplateSnapshot,
                                   # CampaignRecovery,
                                   # RecoveryType,
                                   CampaignDependency,
                                   HeldLaunch)


#
# utility function for running commands that don't run forever...
#
def popen_read_with_timeout(cmd, totaltime=30):

    origtime = totaltime
    # start up keeping subprocess handle and pipe
    pp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    f = pp.stdout

    outlist = []
    block = " "

    # read the file, with select timeout of total time remaining
    while totaltime > 0 and len(block) > 0:
        t1 = time.time()
        r, w, e = select.select([f], [], [], totaltime)
        if f not in r:
            outlist.append("\n[...timed out after %d seconds]\n" % origtime)
            # timed out!
            pp.kill()
            break
        block = os.read(f.fileno(), 512)
        t2 = time.time()
        totaltime = totaltime - (t2 - t1)
        outlist.append(block)

    pp.wait()
    output = ''.join(outlist)
    return output


class TaskPOMS:

    def __init__(self, ps):
        self.poms_service = ps
        self.task_min_job_cache = {}

    def create_task(self, dbhandle, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor=None):
        first, last, username = creator.split(' ')
        creator = self.poms_service.get_or_add_experimenter(first, last, username)
        exp = self.poms_service.get_or_add_experiment(experiment)
        td = self.poms_service.get_or_add_taskdef(taskdef, creator, exp)
        camp = self.poms_service.get_or_add_campaign(exp, td, creator)
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


    def wrapup_tasks(self, dbhandle, samhandle, getconfig, gethead, seshandle, err_res):
        # this function call another function that is not in this module, it use a poms_service object passed as an argument at the init.
        now = datetime.now(utc)
        res = ["wrapping up:"]

        #
        # make jobs which completed with no output files have status "Located".
        t = text("update jobs set status = 'Located' where status = 'Completed' and (select count(file_name) from job_files where job_files.job_id = jobs.job_id and job_files.file_type = 'output') = 0")
        dbhandle.execute(t)

        #
        # tried to do as below, but no such luck
        # subq = dbhandle.query(func.count(JobFile.file_name)).filter(JobFile.job_id == Job.job_id, JobFile.file_type == 'output')
        # dbhandle.query(Job).filter(Job.status == "Completed", subq == 0).update({'status':'Located'}, synchronize_session='fetch')
        #
        dbhandle.commit()

        need_joblogs = []
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
            if (total > 0 and running == 0) or (total == 0 and now - task.created > timedelta(days=2)):
                task.status = "Completed"
                task.updated = datetime.now(utc)
                dbhandle.add(task)
                # and check job logs for final runtime, cpu-time etc.
                need_joblogs.append(task)

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
        for task in (dbhandle.query(Task).with_for_update(of=Task).join(CampaignSnapshot)
                     .options(joinedload(Task.jobs))
                     .options(joinedload(Task.campaign_snap_obj))
                     .options(joinedload(Task.campaign_definition_snap_obj))
                     .filter(Task.status.in_(["Completed","Running"]),
                             Task.campaign_snapshot_id == CampaignSnapshot.campaign_snapshot_id,
                             CampaignSnapshot.completion_type == "complete").all()):

            compcount = 0
            totcount = 0.1  # avoid divsion by zeo sidewas
            for j in task.jobs:
                totcount += 1
                if j.status == "Completed" or j.status == "Located":
                    compcount += 1

            cfrac = task.campaign_snap_obj.completion_pct

            res.append("completion_type: complete Task %d cfrac %d pct %f " % (task.task_id, cfrac,(compcount * 100)/totcount))

            if (compcount * 100.0) / totcount > cfrac:
                n_located = n_located + 1
                task.status = "Located"
                finish_up_tasks[task.task_id] = task
                for j in task.jobs:
                    j.status = "Located"
                    j.output_files_declared = True
                task.updated = datetime.now(utc)
                dbhandle.add(task)

        for task in (dbhandle.query(Task).with_for_update(of=Task).join(CampaignSnapshot)
                     .options(joinedload(Task.jobs))
                     .options(contains_eager(Task.campaign_snap_obj))
                     .options(joinedload(Task.campaign_definition_snap_obj))
                     .filter(Task.status == "Completed",
                             Task.campaign_snapshot_id == CampaignSnapshot.campaign_snapshot_id,
                             CampaignSnapshot.completion_type == "located").all()):
            n_completed = n_completed + 1
            # if it's been 2 days, just declare it located; its as
            # located as its going to get...


            if (now - task.updated > timedelta(days=2)):
                n_located = n_located + 1
                n_stale = n_stale + 1
                task.status = "Located"
                finish_up_tasks[task.task_id] = task
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
                loccount = 0
                totcount = 0
                for j in task.jobs:
                    totcount += 1
                    if j.status == "Located":
                        loccount += 1

                cfrac = task.campaign_snap_obj.completion_pct
                if not cfrac:
                    cfrac = 95.0

                logit.log("non-project task: %s tot %d loc %d" % (task.task_id, totcount, loccount))
                if totcount == 0 or loccount / totcount * 100 > cfrac:
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

        summary_list = samhandle.fetch_info_list(lookup_task_list, dbhandle=dbhandle)
        count_list = samhandle.count_files_list(lookup_exp_list, lookup_dims_list)
        thresholds = []
        logit.log("wrapup_tasks: summary_list: %s" % repr(summary_list))    # Check if that is working

        for i in range(len(summary_list)):
            task = lookup_task_list[i]
            cfrac = task.campaign_snap_obj.completion_pct / 100.0
            threshold = (summary_list[i].get('tot_consumed', 0) * cfrac)
            thresholds.append(threshold)
            val = float(count_list[i])
            if val >= threshold and threshold > 0:
                n_located = n_located + 1
                if task.status == "Completed":
                    task.status = "Located"
                    finish_up_tasks[task.task_id] = task
                for j in task.jobs:
                    j.status = "Located"
                    j.output_files_declared = True
                task.updated = datetime.now(utc)
                dbhandle.add(task)

        res.append("Counts: completed: %d stale: %d project %d: located %d" %
                   (n_completed, n_stale, n_project, n_located))

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
        logit.log("Starting need_joblogs loops, len %d" % len(finish_up_tasks))
        for task in need_joblogs:
                condor_log_parser.get_joblogs(dbhandle,
                                              self.task_min_job(dbhandle, task.task_id),
                                              task.campaign_snap_obj.experiment,
                                              task.campaign_snap_obj.vo_role)
        logit.log("Starting finish_up_tasks loops, len %d" % len(finish_up_tasks))

        for task_id, task in finish_up_tasks.items():
            # get logs for job for final cpu values, etc.
            logit.log("Starting finish_up_tasks items for task %s" % task_id)

            if not self.launch_recovery_if_needed(dbhandle, samhandle, getconfig, gethead, seshandle, err_res, task):
                self.launch_dependents_if_needed(dbhandle, samhandle, getconfig, gethead, seshandle, err_res, task)

        return res


    def show_task_jobs(self, dbhandle, task_id, tmax=None, tmin=None, tdays=1):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string,tdays = self.poms_service.utilsPOMS.handle_dates(tmin, tmax,tdays,'show_task_jobs?task_id=%s' % task_id)

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
                jjid = j.jobsub_job_id.replace('fifebatch', '').replace('.fnal.gov', '')
            else:
                jjid = 'j' + str(jh.job_id)

            if j.status != "Completed" and j.status != "Located":
                extramap[jjid] = '<a href="%s/kill_jobs?job_id=%d"><i class="ui trash icon"></i></a>' % (self.poms_service.path, jh.job_id)
            else:
                extramap[jjid] = '&nbsp; &nbsp; &nbsp; &nbsp;'
            if jh.status != laststatus or jjid != lastjjid:
                items.append(fakerow(job_id=jh.job_id,
                                     created=jh.created.replace(tzinfo=utc),
                                     status=jh.status,
                                     jobsub_job_id=jjid))
            laststatus = jh.status
            lastjjid = jjid

        job_counts = self.poms_service.filesPOMS.format_job_counts(dbhandle, task_id=task_id, tmin=tmins, tmax=tmaxs, tdays=tdays, range_string=time_range_string)
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

        logit.log("in compute_status, counts are %s" % repr(st))

        if task.status == "Located":
            return task.status
        res = "New"
        if (st['Idle'] > 0):
            res = "Idle"
        if (st['Held'] > 0):
            res = "Held"
        if (st['Running'] > 0):
            res = "Running"
        if (st['Completed'] > 0 and  res == "New"):
            res = "Completed"
        if (st['Located'] > 0 and  res == "New"):
            res = "Located"
        return res


    def task_min_job(self, dbhandle, task_id):  # This method deleted from the main script.
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
             u = dbhandle.query(Experimenter).filter(Experimenter.username==user).first()
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


    def launch_dependents_if_needed(self, dbhandle, samhandle, getconfig, gethead, seshandle, err_res,  t):
        logit.log("Entering launch_dependents_if_needed(%s)" % t.task_id)
        if not getconfig("poms.launch_recovery_jobs",False):
            # XXX should queue for later?!?
            logit.log("recovery launches disabled")
            return 1
        cdlist = dbhandle.query(CampaignDependency).filter(CampaignDependency.needs_camp_id == t.campaign_snap_obj.campaign_id).all()

        i = 0
        for cd in cdlist:
           if cd.uses_camp_id == t.campaign_snap_obj.campaign_id:
              # self-reference, just do a normal launch
              self.launch_jobs(dbhandle, getconfig, gethead, seshandle, samhandle, err_res, cd.uses_camp_id)
           else:
              i = i + 1
              dims = "ischildof: (snapshot_for_project_name %s) and version %s and file_name like '%s' " % (t.project, t.campaign_snap_obj.software_version, cd.file_patterns)
              dname = "poms_depends_%d_%d" % (t.task_id,i)

              samhandle.create_definition(t.campaign_snap_obj.experiment, dname, dims)
              self.launch_jobs(dbhandle, getconfig, gethead, seshandle, samhandle, err_res, cd.uses_camp_id, dataset_override = dname)
        return 1


    def launch_recovery_if_needed(self, dbhandle, samhandle, getconfig, gethead, seshandle, err_res,  t):
        logit.log("Entering launch_recovery_if_needed(%s)" % t.task_id)
        if not getconfig("poms.launch_recovery_jobs", False):
            logit.log("recovery launches disabled")
            # XXX should queue for later?!?
            return 1

        # if this is itself a recovery job, we go back to our parent
        # to do all the work, because it has the counters, etc.
        if t.parent_obj:
            t = t.parent_obj

        rlist = self.poms_service.campaignsPOMS.get_recovery_list_for_campaign_def(dbhandle, t.campaign_definition_snap_obj)

        logit.log("recovery list %s" % rlist)
        if t.recovery_position is None:
            t.recovery_position = 0

        while t.recovery_position is not None and t.recovery_position < len(rlist):
            logit.log("recovery position %d" % t.recovery_position)

            rtype = rlist[t.recovery_position].recovery_type
            # uncomment when we get db fields:
            param_overrides = rlist[t.recovery_position].param_overrides
            if rtype.name == 'consumed_status':
                recovery_dims = "for_project_name %s and consumed_status != 'consumed'" % t.project
            elif rtype.name == 'proj_status':
                recovery_dims = "project_name %s and process_status != 'ok'" % t.project
            elif rtype.name == 'pending_files':
                recovery_dims = "snapshot_for_project_name %s " % t.project
                if t.campaign_definition_snap_obj.output_file_patterns:
                    oftypelist = t.campaign_definition_snap_obj.output_file_patterns.split(",")
                else:
                    oftypelist = ["%"]

                for oft in oftypelist:
                    recovery_dims += "minus isparent: ( version %s and file_name like %s) " % (t.campaign_snap_obj.software_version, oft)
            else:
                # default to consumed status(?)
                recovery_dims = "project_name %s and consumed_status != 'consumed'" % t.project

            try:
                logit.log("counting files dims %s" % recovery_dims)
                nfiles = samhandle.count_files(t.campaign_snap_obj.experiment, recovery_dims, dbhandle=dbhandle)
            except:
                # if we can't count it, just assume there may be a few for now...
                nfiles = 1

            t.recovery_position = t.recovery_position + 1
            dbhandle.add(t)
            dbhandle.commit()

            logit.log("recovery files count %d" % nfiles)
            if nfiles > 0:
                rname = "poms_recover_%d_%d" % (t.task_id, t. recovery_position)

                logit.log("launch_recovery_if_needed: creating dataset for exp=%s name=%s dims=%s" % (t.campaign_snap_obj.experiment, rname, recovery_dims))

                samhandle.create_definition(t.campaign_snap_obj.experiment, rname, recovery_dims)


                self.launch_jobs(dbhandle, getconfig, gethead, seshandle, samhandle, err_res, t.campaign_snap_obj.campaign_id, dataset_override=rname, parent_task_id = t.task_id, param_overrides = param_overrides)
                return 1

        return 0

    def set_job_launches(self, dbhandle, hold):
        if hold not in ["hold", "allowed"]:
            return

        s = dbhandle.query(Service).with_for_update().filter(Service.name == "job_launches").first()
        s.status = hold
        dbhandle.commit()

    def get_job_launches(self, dbhandle):
        s = dbhandle.query(Service).filter(Service.name == "job_launches").first()
        return s.status

    def launch_queued_job(self, dbhandle, samhandle, getconfig, gethead, seshandle, err_res):
        if self.get_job_launches(dbhandle) == "hold":
            return "Held."

        hl = dbhandle.query(HeldLaunch).with_for_update().order_by(HeldLaunch.created).first();
        if hl:
            dbhandle.delete(hl)
            dbhandle.commit()
            self.launch_jobs(dbhandle,
                             getconfig, gethead,
                             seshandle, samhandle,
                             err_res, hl.campaign_id,
                             dataset_override=hl.dataset,
                             parent_task_id=hl.parent_task_id,
                             param_overrides=hl.param_overrides)
            return "Launched."
        else:
            return "None."

    def launch_jobs(self, dbhandle, getconfig, gethead, seshandle, samhandle,
                    err_res, campaign_id, dataset_override=None, parent_task_id=None, param_overrides=None):

        logit.log("Entering launch_jobs(%s, %s, %s)" % (campaign_id, dataset_override, parent_task_id))

        ds = time.strftime("%Y%m%d_%H%M%S")
        outdir = "%s/private/logs/poms/launches/campaign_%s" % (os.environ["HOME"], campaign_id)
        outfile = "%s/%s" % (outdir, ds)
        logit.log("trying to record launch in %s" % outfile)

        c = (dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id)
             .options(joinedload(Campaign.launch_template_obj), joinedload(Campaign.campaign_definition_obj)).first())

        if not c:
            err_res = 404
            raise KeyError

        cd = c.campaign_definition_obj
        lt = c.launch_template_obj

        if self.get_job_launches(dbhandle) == "hold":
            output = "Job launches currently held.... queuing this request"
            hl = HeldLaunch()
            hl.campaign_id = campaign_id
            hl.created = datetime.now(utc)
            hl.dataset = dataset_override
            hl.parent_task_id = parent_task_id
            hl.param_overrides = param_overrides
            dbhandle.add(hl)
            dbhandle.commit()
            lcmd = ""

            return lcmd, output, c, campaign_id, outdir, outfile

        e = seshandle('experimenter')
        xff = gethead('X-Forwarded-For', None)
        ra = gethead('Remote-Addr', None)
        if not e.is_authorized(c.experiment) and not (ra == '127.0.0.1' and xff == None):
            logit.log("launch_jobs -- experimenter not authorized")
            err_res = "404 Permission Denied."
            output = "Not Authorized: e: %s xff %s ra %s" % (e, xff, ra)
            return lcmd, output, c, campaign_id, outdir, outfile

        experimenter_login = e.username
        lt.launch_account = lt.launch_account % {"experimenter": experimenter_login}

        if dataset_override:
            dataset = dataset_override
        else:
            dataset = self.poms_service.campaignsPOMS.get_dataset_for(dbhandle, samhandle, err_res, c)

        group = c.experiment
        if group == 'samdev':
            group = 'fermilab'

        cmdl = [
            "exec 2>&1",
            "set -x",
            "export KRB5CCNAME=/tmp/krb5cc_poms_submit_%s" % group,
            "export POMS_PARENT_TASK_ID=%s" % (parent_task_id if parent_task_id else ""),
            "kinit -kt $HOME/private/keytabs/poms.keytab poms/cd/%s@FNAL.GOV || true" % self.poms_service.hostname,
            "ssh -tx %s@%s <<'EOF'" % (lt.launch_account, lt.launch_host),
            lt.launch_setup % {
                "dataset": dataset,
                "version": c.software_version,
                "group": group,
                "experimenter": experimenter_login,
            },
            "setup poms_jobsub_wrapper v0_4 -z /grid/fermiapp/products/common/db",
            "export POMS_PARENT_TASK_ID=%s" % (parent_task_id if parent_task_id else ""),
            "export POMS_TEST=%s" % ("" if "poms" in self.poms_service.hostname else "1"),
            "export POMS_CAMPAIGN_ID=%s" % c.campaign_id,
            "export POMS_TASK_DEFINITION_ID=%s" % c.campaign_definition_id,
            "export JOBSUB_GROUP=%s" % group,
        ]
        if cd.definition_parameters:
            if isinstance(cd.definition_parameters, basestring):
                params = OrderedDict(json.loads(cd.definition_parameters))
            else:
                params = OrderedDict(cd.definition_parameters)
        else:
            params = OrderedDict([])

        if c.param_overrides is not None and c.param_overrides != "":
            if isinstance(c.param_overrides, basestring):
                params.update(json.loads(c.param_overrides))
            else:
                params.update(c.param_overrides)

        if param_overrides is not None and param_overrides != "":
            if isinstance(param_overrides, basestring):
                params.update(json.loads(param_overrides))
            else:
                params.update(param_overrides)

        lcmd = cd.launch_script + " " + ' '.join((x[0] + x[1]) for x in params.items())
        lcmd = lcmd % {
            "dataset": dataset,
            "version": c.software_version,
            "group": group,
            "experimenter": experimenter_login,
        }
        cmdl.append(lcmd)
        cmdl.append('exit')
        cmdl.append('EOF')
        cmd = '\n'.join(cmdl)

        cmd = cmd.replace('\r', '')

        # make sure launch doesn't take more that half an hour...
        output = popen_read_with_timeout(cmd, 1800)     ### Question???

        if not os.path.isdir(outdir):
            os.makedirs(outdir)
        lf = open(outfile, "w")
        lf.write(output)
        lf.close()

        # always record launch...
        return lcmd, output, c, campaign_id, outdir, outfile
