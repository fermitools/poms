#!/usr/bin/env python
'''
This module contain the methods that handle the Calendar.
List of methods: active_jobs, output_pending_jobs, update_jobs
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
'''

from model.poms_model import Experiment, Job, Task, Campaign, Tag, JobFile
from datetime import datetime, timedelta
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from utc import utc
import gc
import json
#from LaunchPOMS import launch_recovery_if_needed
#from poms_service import poms_service


class JobsPOMS():

    def __init__(self, ps):
        self.poms_service=ps

###########
###JOBS
    def active_jobs(self, dbhandle):
        res = [ "[" ]
        sep=""
        for job in dbhandle.query(Job).filter(Job.status != "Completed", Job.status != "Located", Job.status != "Removed").all():
            if job.jobsub_job_id == "unknown":
                continue
            res.append( '%s "%s"' % (sep, job.jobsub_job_id))
            sep = ","
        res.append( "]" )
        res = "".join(res)
        gc.collect(2)
        return res


    def output_pending_jobs(self,dbhandle):
        res = {}
        sep=""
        preve = None
        prevj = None
        for e, jobsub_job_id, fname  in dbhandle.query(Campaign.experiment,Job.jobsub_job_id,JobFile.file_name).join(Task).filter(Task.campaign_id == Campaign.campaign_id, Job.jobsub_job_id != "unknown", Job.task_id == Task.task_id, Job.job_id == JobFile.job_id, Job.status == "Completed", JobFile.declared == None, JobFile.file_type == 'output').order_by(Campaign.experiment,Job.jobsub_job_id).all():
            if preve != e:
                preve = e
                res[e] = {}
            if prevj != jobsub_job_id:
                prevj = jobsub_job_id
                res[e][jobsub_job_id] = []
            res[e][jobsub_job_id].append(fname)
        sres =  json.dumps(res)
        res = None   #Why do you need this?
        return sres


    def update_job(self, dbhandle, loghandle, rpstatus, task_id = None, jobsub_job_id = 'unknown',  **kwargs):
        if task_id:
            task_id = int(task_id)

        host_site = "%s_on_%s" % (jobsub_job_id, kwargs.get('slot','unknown'))

        jl = dbhandle.query(Job).options(subqueryload(Job.task_obj)).filter(Job.jobsub_job_id==jobsub_job_id).order_by(Job.job_id).all()
        first = True
        j = None
        for ji in jl:
            if first:
                j = ji
                first = False
            else:
            # we somehow got multiple jobs with the sam jobsub_job_id
            # mark the others as dups
                ji.jobsub_job_id="dup_"+ji.jobsub_job_id
                dbhandle.add(ji)
                # steal any job_files
                files =  [x.file_name for x in j.job_files ]
                for jf in ji.job_files:
                    if jf.file_name not in files:
                        njf = JobFile(file_name = jf.file_name, file_type = jf.file_type, created =  jf.created, job_obj = j)
                        dbhandle.add(njf)

                dbhandle.delete(ji)
                dbhandle.flush()      #######################should we change this for dbhandle.commit()


        if not j and task_id:
            t = dbhandle.query(Task).filter(Task.task_id==task_id).first()
            if t == None:
                loghandle("update_job -- no such task yet")
                rpstatus="404 Task Not Found"
                return "No such task"
            loghandle("update_job: creating new job")
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
            loghandle("update_job: updating job %d" % (j.job_id if j.job_id else -1))

            for field in ['cpu_type', 'node_name', 'host_site', 'status', 'user_exe_exit_code']:    ######?????????????? does those fields come in **kwargs or are juste grep from the database direclty
                if field == 'status' and j.status == "Located":
                    # stick at Located, don't roll back to Completed,etc.
                    continue

                if kwargs.get(field, None):
                    setattr(j,field,kwargs[field].rstrip("\n"))
                if not getattr(j,field, None):
                    if field != 'user_exe_exit_code':
                        setattr(j,field,'unknown')

            if kwargs.get('output_files_declared', None) == "True":
                if j.status == "Completed" :
                    j.output_files_declared = True
                    j.status = "Located"

            for field in ['project','recovery_tasks_parent' ]:
                if kwargs.get("task_%s" % field, None) and kwargs.get("task_%s" % field) != "None" and j.task_obj:
                    setattr(j.task_obj,field,kwargs["task_%s"%field].rstrip("\n"))
                    loghandle("setting task %d %s to %s" % (j.task_obj.task_id, field, getattr(j.task_obj, field, kwargs["task_%s"%field])))

            for field in [ 'cpu_time', 'wall_time']:
                if kwargs.get(field, None) and kwargs[field] != "None":
                    setattr(j,field,float(kwargs[field].rstrip("\n")))

            if kwargs.get('output_file_names', None):
                loghandle("saw output_file_names: %s" % kwargs['output_file_names'])
                if j.job_files:
                    files =  [x.file_name for x in j.job_files ]
                else:
                    files = []

                newfiles = kwargs['output_file_names'].split(' ')
                # don't include metadata files
                newfiles =  [ f for f in newfiles if f.find('.json') == -1 and f.find('.metadata') == -1]
                ###Included in the merge
                for f in newfiles:
                    if not f in files:
                        if len(f) < 2 or f[0] == '-':  # ignore '0', '-D', etc...
                            continue
                        if f.find("log") >= 0:
                            ftype = "log"
                        else:
                            ftype = "output"
                        jf = JobFile(file_name = f, file_type = ftype, created =  datetime.now(utc), job_obj = j)
                        j.job_files.append(jf)
                        dbhandle.add(jf)

            if kwargs.get('input_file_names', None):
                loghandle("saw input_file_names: %s" % kwargs['input_file_names'])
                if j.job_files:
                    files =  [x.file_name for x in j.job_files if x.file_type == 'input']
                else:
                    files = []
                newfiles = kwargs['input_file_names'].split(' ')
                for f in newfiles:
                    if len(f) < 2 or f[0] == '-':  # ignore '0', '-D', etc...
                        continue
                    if not f in files:
                        jf = JobFile(file_name = f, file_type = "input", created =  datetime.now(utc), job_obj = j)
                        dbhandle.add(jf)


            if j.cpu_type == None:
                j.cpu_type = ''
            loghandle("update_job: db add/commit job status %s " %  j.status)
            j.updated =  datetime.now(utc)
            if j.task_obj:
                newstatus = self.poms_service.compute_status(j.task_obj)
                if newstatus != j.task_obj.status:
                    j.task_obj.status = newstatus
                    j.task_obj.updated = datetime.now(utc)
                    j.task_obj.campaign_snap_obj.active = True
            dbhandle.add(j)
            dbhandle.commit()
            loghandle("update_job: done job_id %d" %  (j.job_id if j.job_id else -1))

        return "Ok."


    def test_job_counts(self, task_id = None, campaign_id = None):
        res = self.poms_service.job_counts(task_id, campaign_id)
        return repr(res) + self.poms_service.format_job_counts(task_id, campaign_id)
