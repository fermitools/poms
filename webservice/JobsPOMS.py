#!/usr/bin/env python
'''
This module contain the methods that handle the Calendar.
List of methods: active_jobs, output_pending_jobs, update_jobs
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify
version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
'''

import re
from poms.model.poms_model import Job, Task, Campaign, CampaignDefinitionSnapshot, CampaignSnapshot, JobFile, JobHistory
from datetime import datetime
from sqlalchemy.orm import joinedload
from sqlalchemy import func, not_, and_
from utc import utc
import json
import os

import logit


class JobsPOMS(object):

    def __init__(self, poms_service):
        self.poms_service = poms_service

###########
###JOBS
    def active_jobs(self, dbhandle):
        res = []
        for job in dbhandle.query(Job).filter(Job.status != "Completed", Job.status != "Located", Job.status != "Removed").execution_options(stream_results=True).all():
            if job.jobsub_job_id == "unknown":
                continue
            res.append(job.jobsub_job_id)
        logit.log("active_jobs: returning %s" % res)
        #gc.collect(2)
        return res


    def output_pending_jobs(self, dbhandle):
        res = {}
        preve = None
        prevj = None
        # it would be really cool if we could push the pattern match all the
        # way down into the query:
        #  JobFile.file_name like CampaignDefinitionSnapshot.output_file_patterns
        # but with a comma separated list of them, I don't think it works
        # directly -- we would have to convert comma to pipe...
        # for now, I'm just going to make it a regexp and filter them here.
        for e, jobsub_job_id, fname, fpattern in (dbhandle.query(CampaignSnapshot.experiment,
                                                                 Job.jobsub_job_id,
                                                                 JobFile.file_name,
                                                                 CampaignDefinitionSnapshot.output_file_patterns)
                                                  .join(Task).join(CampaignDefinitionSnapshot)
                                                  .filter(Task.campaign_definition_snap_id == CampaignDefinitionSnapshot.campaign_definition_snap_id,
                                                          Task.campaign_snapshot_id == CampaignSnapshot.campaign_snapshot_id,
                                                          Job.jobsub_job_id != "unknown",
                                                          Job.task_id == Task.task_id,
                                                          Job.job_id == JobFile.job_id,
                                                          Job.status == "Completed",
                                                          JobFile.declared == None, JobFile.file_type == 'output')
                                                  .order_by(CampaignSnapshot.experiment, Job.jobsub_job_id).all()):
            # convert fpattern "%.root,%.dat" to regexp ".*\.root|.*\.dat"
            if fpattern is None:
                fpattern = '%'
            fpattern = fpattern.replace('.', '\\.')
            fpattern = fpattern.replace('%', '.*')
            fpattern = fpattern.replace(',', '|')
            if not re.match(fpattern, fname):
                continue
            if preve != e:
                preve = e
                res[e] = {}
            if prevj != jobsub_job_id:
                prevj = jobsub_job_id
                res[e][jobsub_job_id] = []
            res[e][jobsub_job_id].append(fname)
        return res

    def update_SAM_project(self, samhandle, j, projname):
        tid = j.task_obj.task_id
        exp = j.task_obj.campaign_snap_obj.experiment
        cid = j.task_obj.campaign_snap_obj.campaign_id
        samhandle.update_project_description(exp, projname, "POMS Campaign %s Task %s" % (cid, tid))
        pass


    def bulk_update_job(self, dbhandle, rpstatus, samhandle, json_data='{}'):
        logit.log("Entering bulk_update_job(%s)" % json_data)
        ldata = json.loads(json_data)

        # make one merged entry per job_id
        data = {}
        for d in ldata:
            data[d['jobsub_job_id']] = {}

        for d in ldata:
            data[d['jobsub_job_id']].update(d)

        # figure out what tasks are involved
        foundtasks = {}
        for jid, d in data.items():
            if d['task_id']:
                foundtasks[int(d['task_id'])] = 1

        logit.log("found task ids for %s" % ",".join(map(str, foundtasks.keys())))

        # lookup what job-id's we already have database entries for
        jobs = dbhandle.query(Job).with_for_update().filter(Job.jobsub_job_id.in_(data.keys())).execution_options(stream_results=True).all()

        # make a list of jobs we can update
        jlist = []
        foundjobs = {}
        for j in jobs:
            foundjobs[j.jobsub_job_id] = j
            jlist.append(j)

        # get the tasks we have that are mentioned
        if len(foundtasks) > 0:
            tasks = dbhandle.query(Task).filter(Task.task_id.in_(foundtasks.keys())).all()
        else:
            tasks = []

        fulltasks = {}
        for t in tasks:
            fulltasks[t.task_id] = t

        logit.log("found full tasks for %s" % ",".join(map(str, fulltasks.keys())))

        # now look for jobs for which  we don't have Job ORM entries, but
        # whose Tasks we do have entries for, and make new Job entries for
        # them.
        for jid in data.keys():
            if not foundjobs.get(jid, None) and data[jid].has_key('task_id') and fulltasks.get(int(data[jid]['task_id']), None):
                logit.log("need new Job for %s" % jid)
                j = Job(jobsub_job_id=jid,
                        task_obj=fulltasks[int(data[jid]['task_id'])],
                        output_files_declared=False,
                        node_name='unknown', cpu_type='unknown', host_site='unknown', status='Idle')
                j.created = datetime.now(utc)
                j.updated = datetime.now(utc)
                jlist.append(j)
                dbhandle.add(j)
            elif not foundjobs.get(jid, 0):
                logit.log("need new Job for %s, but no task %s" % (jid, data[jid]['task_id']))
            else:
                pass


        # now actually update each such job, 'cause we should now have a
        # ORM mapped Job object for each one.
        for j in jlist:
            self.update_job_common(dbhandle, rpstatus, samhandle, j, data[j.jobsub_job_id])

        # update any related tasks status if changed
        for t in fulltasks.values():
            newstatus = self.poms_service.taskPOMS.compute_status(dbhandle, t)
            if newstatus != t.status:
                logit.log("update_job: task %d status now %s" % (t.task_id, newstatus))
                t.status = newstatus
                t.updated = datetime.now(utc)
                # jobs make inactive campaigns active again...
                if t.campaign_obj.active is not True:
                    t.campaign_obj.active = True

        dbhandle.commit()
        logit.log("Exiting bulk_update_job()")
        return "Ok."

    def update_job(self, dbhandle, rpstatus, samhandle, task_id=None, jobsub_job_id='unknown', **kwargs):

        # flag to remember to do a SAM update after we commit
        do_SAM_project = False

        if task_id == "None":
            task_id = None

        if task_id:
            task_id = int(task_id)

        # host_site = "%s_on_%s" % (jobsub_job_id, kwargs.get('slot','unknown'))

        jl = (dbhandle.query(Job).with_for_update(of=Job)
              .options(joinedload(Job.task_obj)).filter(Job.jobsub_job_id == jobsub_job_id).order_by(Job.job_id).execution_options(stream_results=True).all())
        first = True
        j = None
        for ji in jl:
            if first:
                j = ji
                first = False
            else:
            # we somehow got multiple jobs with the sam jobsub_job_id
            # mark the others as dups
                ji.jobsub_job_id = "dup_" + ji.jobsub_job_id
                dbhandle.add(ji)
                # steal any job_files
                files = [x.file_name for x in j.job_files]
                for jf in ji.job_files:
                    if jf.file_name not in files:
                        njf = JobFile(file_name=jf.file_name, file_type=jf.file_type, created=jf.created, job_obj=j)
                        dbhandle.add(njf)

                dbhandle.delete(ji)
                dbhandle.flush()

        if not j and task_id:
            t = dbhandle.query(Task).filter(Task.task_id == task_id).first()
            if t is None:
                logit.log("update_job -- no such task yet")
                rpstatus = "404 Task Not Found"
                return "No such task"
            logit.log("update_job: creating new job")
            j = Job()
            j.jobsub_job_id = jobsub_job_id.rstrip("\n")
            j.created = datetime.now(utc)
            j.updated = datetime.now(utc)
            j.task_id = task_id
            j.task_obj = t
            j.output_files_declared = False
            j.cpu_type = ''
            j.node_name = ''
            j.host_site = ''
            j.status = 'Idle'

        if j:
            oldstatus = j.status

            self.update_job_common(dbhandle, rpstatus, samhandle, j, kwargs)
            if oldstatus != j.status and j.task_obj:
                newstatus = self.poms_service.taskPOMS.compute_status(dbhandle, j.task_obj)
                if newstatus != j.task_obj.status:
                    logit.log("update_job: task %d status now %s" % (j.task_obj.task_id, newstatus))
                    j.task_obj.status = newstatus
                    j.task_obj.updated = datetime.now(utc)
                    # jobs make inactive campaigns active again...
                    if j.task_obj.campaign_obj.active is not True:
                        j.task_obj.campaign_obj.active = True

            dbhandle.add(j)
            dbhandle.commit()


            # now that we committed, do a SAM project desc. upate if needed
            if do_SAM_project:
                self.update_SAM_project(samhandle, j, kwargs.get("task_project"))
            logit.log("update_job: done job_id %d" % (j.job_id if j.job_id else -1))

        return "Ok."

    def update_job_common(self, dbhandle, rpstatus, samhandle, j, kwargs):

            oldstatus = j.status
            logit.log("update_job: updating job %d" % (j.job_id if j.job_id else -1))

            if kwargs.get('status', None) and oldstatus != kwargs.get('status') and oldstatus == 'Completed' and kwargs.get('status') != 'Located':
                # we went from Completed back to some Running/Idle state...
                # so clean out any old (wrong) Completed statuses from
                # the JobHistory... (Bug #15322)
                dbhandle.query(JobHistory).filter(JobHistory.job_id == j.job_id, JobHistory.status == 'Completed').delete()

            # first, Job string fields the db requres be not null:
            for field in ['cpu_type', 'node_name', 'host_site', 'status', 'user_exe_exit_code']:
                if field == 'status' and j.status == "Located":
                    # stick at Located, don't roll back to Completed,etc.
                    continue

                if kwargs.get(field, None):
                    setattr(j, field, kwargs[field].rstrip("\n"))
                if not getattr(j, field, None):
                    if field != 'user_exe_exit_code':
                        setattr(j, field, 'unknown')

            # first, next, output_files_declared, which also changes status
            if kwargs.get('output_files_declared', None) == "True":
                if j.status == "Completed":
                    j.output_files_declared = True
                    j.status = "Located"

            # next fields we set in our Task
            for field in ['project', 'recovery_tasks_parent']:

                if field == 'project' and j.task_obj.project is None:
                    # make a note to update project description after commit
                    do_SAM_project = True

                if kwargs.get("task_%s" % field, None) and kwargs.get("task_%s" % field) != "None" and j.task_obj:
                    setattr(j.task_obj, field, kwargs["task_%s" % field].rstrip("\n"))
                    logit.log("setting task %d %s to %s" % (j.task_obj.task_id, field, getattr(j.task_obj, field, kwargs["task_%s" % field])))

            # floating point fields need conversion
            for field in ['cpu_time', 'wall_time']:
                if kwargs.get(field, None) and kwargs[field] != "None":
                    if (isinstance(kwargs[field], basestring)):
                        setattr(j, field, float(kwargs[field].rstrip("\n")))
                    if (isinstance(kwargs[field], float)):
                        setattr(j, field, kwargs[field])

            # filenames need dumping in JobFiles table and attaching
            if kwargs.get('output_file_names', None):
                logit.log("saw output_file_names: %s" % kwargs['output_file_names'])
                if j.job_files:
                    files = [x.file_name for x in j.job_files]
                else:
                    files = []

                newfiles = kwargs['output_file_names'].split(' ')
                # don't include metadata files

                output_match_re = j.task_obj.campaign_definition_snap_obj.output_file_patterns.replace(',','|').replace('.','\\.').replace('%','.*')

                newfiles = [f for f in newfiles if f.find('.json') == -1 and f.find('.metadata') == -1]
                 
                for f in newfiles:
                    if f not in files:
                        if len(f) < 2 or f[0] == '-':  # ignore '0','-D' etc...
                            continue
                        if f.find("log") >= 0 or not re.match(output_match_re, f): 
                            ftype = "log"
                        else:
                            ftype = "output"

                        jf = JobFile(file_name=f, file_type=ftype, created=datetime.now(utc), job_obj=j)
                        j.job_files.append(jf)
                        dbhandle.add(jf)

            if kwargs.get('input_file_names', None):
                logit.log("saw input_file_names: %s" % kwargs['input_file_names'])
                if j.job_files:
                    files = [x.file_name for x in j.job_files if x.file_type == 'input']
                else:
                    files = []
                newfiles = kwargs['input_file_names'].split(' ')
                for f in newfiles:
                    if len(f) < 2 or f[0] == '-':  # ignore '0', '-D', etc...
                        continue
                    if f not in files:
                        jf = JobFile(file_name=f, file_type="input", created=datetime.now(utc), job_obj=j)
                        dbhandle.add(jf)


            # should have been handled with 'unknown' bit above, but we
            # must have put it here for a reason...
            if j.cpu_type is None:
                j.cpu_type = ''
            logit.log("update_job: db add/commit job status %s " % j.status)
            j.updated = datetime.now(utc)

    def test_job_counts(self, task_id=None, campaign_id=None):
        res = self.poms_service.job_counts(task_id, campaign_id)
        return repr(res) + self.poms_service.format_job_counts(task_id, campaign_id)

    def kill_jobs(self, dbhandle, campaign_id=None, task_id=None, job_id=None, confirm=None):
        jjil = []
        jql = None
        t = None
        if campaign_id is not None or task_id is not None:
            if campaign_id is not None:
                tl = dbhandle.query(Task).filter(Task.campaign_id == campaign_id, Task.status != 'Completed', Task.status != 'Located').all()
            else:
                tl = dbhandle.query(Task).filter(Task.task_id == task_id).all()
            if len(tl):
                c = tl[0].campaign_snap_obj
            else:
                c = None
            for t in tl:
                tjid = self.poms_service.taskPOMS.task_min_job(dbhandle, t.task_id)
                logit.log("kill_jobs: task_id %s -> tjid %s" % (t.task_id, tjid))
                # for tasks/campaigns, kill the whole group of jobs
                # by getting the leader's jobsub_job_id and taking off
                # the '.0'.
                if tjid:
                    jjil.append(tjid.replace('.0', ''))
        else:
            jql = dbhandle.query(Job).filter(Job.job_id == job_id, Job.status != 'Completed', Job.status != 'Located').execution_options(stream_results=True).all()
            c = jql[0].task_obj.campaign_snap_obj
            for j in jql:
                jjil.append(j.jobsub_job_id)

        if confirm is None:
            jijatem = 'kill_jobs_confirm.html'

            return jjil, t, campaign_id, task_id, job_id
        else:
            group = c.experiment
            if group == 'samdev':
                group = 'fermilab'
            '''
            if test == true:
                os.open("echo jobsub_rm -G %s --role %s --jobid %s 2>&1" % (group, c.vo_role, ','.join(jjil)), "r")
            '''
            f = os.popen("jobsub_rm -G %s --role %s --jobid %s 2>&1" % (group, c.vo_role, ','.join(jjil)), "r")
            output = f.read()
            f.close()

            return output, c, campaign_id, task_id, job_id

    def jobs_eff_histo(self, dbhandle, campaign_id, tmax=None, tmin=None, tdays=1):
        """  use
                  select count(job_id), floor(cpu_time * 10 / wall_time) as de
                     from jobs, tasks
                     where
                        jobs.task_id = tasks.task_id and
                        tasks.campaign_id=17 and
                        wall_time > 0 and
                        wall_time > cpu_time and
                        jobs.updated > '2016-03-10 00:00:00'
                        group by floor(cpu_time * 10 / wall_time)
                       order by de;
             to get height bars for a histogram, clicks through to
             jobs with a given efficiency...
             Need to add efficiency  (cpu_time/wall_time) as a param to
             jobs_table...

         """
        (tmin, tmax, tmins, tmaxs, nextlink, prevlink,
         time_range_string,tdays) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'jobs_eff_histo?campaign_id=%s&' % campaign_id)

        q = dbhandle.query(func.count(Job.job_id), func.floor(Job.cpu_time * 10 / Job.wall_time))
        q = q.join(Job.task_obj)
        q = q.filter(Job.task_id == Task.task_id, Task.campaign_id == campaign_id)
        q = q.filter(Job.cpu_time > 0, Job.wall_time >= Job.cpu_time)
        q = q.filter(Task.created < tmax, Task.created >= tmin)
        q = q.group_by(func.floor(Job.cpu_time * 10 / Job.wall_time))
        q = q.order_by((func.floor(Job.cpu_time * 10 / Job.wall_time)))

        qz = dbhandle.query(func.count(Job.job_id))
        qz = qz.join(Job.task_obj)
        qz = qz.filter(Job.task_id == Task.task_id, Task.campaign_id == campaign_id)
        qz = qz.filter(not_(and_(Job.cpu_time > 0, Job.wall_time >= Job.cpu_time)))
        nodata = qz.first()

        total = 0
        vals = {-1: nodata[0]}
        maxv = 0.01
        if nodata[0] > maxv:
            maxv = nodata[0]
        for row in q.all():
            vals[row[1]] = row[0]
            if row[0] > maxv:
                maxv = row[0]
            total += row[0]

        c = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        # return "total %d ; vals %s" % (total, vals)
        # return "Not yet implemented"
        return c, maxv, total, vals, tmaxs, campaign_id, tdays, str(tmin)[:16], str(tmax)[:16], nextlink, prevlink, tdays


    def get_efficiency(self, dbhandle, campaign_list, tmin, tmax):  #This method was deleted from the main script
        id_list = []
        for c in campaign_list:
            id_list.append(c.campaign_id)

        rows = (dbhandle.query(func.sum(Job.cpu_time), func.sum(Job.wall_time), Task.campaign_id).
                filter(Job.task_id == Task.task_id,
                       Task.campaign_id.in_(id_list),
                       Job.cpu_time > 0,
                       Job.wall_time > 0,
                       Task.created >= tmin, Task.created < tmax).
                group_by(Task.campaign_id).all())

        logit.log("got rows:")
        for r in rows:
            logit.log("%s" % repr(r))

        mapem = {}
        for totcpu, totwall, campaign_id in rows:
            if totcpu is not None and totwall is not None:
                mapem[campaign_id] = int(totcpu * 100.0 / totwall)
            else:
                mapem[campaign_id] = -1

        logit.log("got map: %s" % repr(mapem))

        efflist = []
        for c in campaign_list:
            efflist.append(mapem.get(c.campaign_id, -2))

        logit.log("got list: %s" % repr(efflist))
        return efflist
