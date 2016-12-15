#!/usr/bin/env python
'''
This module contain the methods that handle the Calendar.
List of methods: active_jobs, output_pending_jobs, update_jobs
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
'''

from model.poms_model import Experiment, Job, Task, Campaign, Tag, JobFile
from datetime import datetime, timedelta
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from sqlalchemy import func, not_, and_
from utc import utc
import gc
import json
#from poms_service import popen_read_with_timeout
#from LaunchPOMS import launch_recovery_if_needed
#from poms_service import poms_service
from collections import OrderedDict
import subprocess
import time
import select
import os
import sys

import logging
# our own logging handle, goes to cherrypy
logger = logging.getLogger('cherrypy_error')

#
# utility function for running commands that don't run forever...
#
def popen_read_with_timeout(cmd, totaltime = 30):

    origtime = totaltime
    # start up keeping subprocess handle and pipe
    pp = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE)
    f = pp.stdout

    outlist = []
    block=" "

    # read the file, with select timeout of total time remaining
    while totaltime > 0 and len(block) > 0:
        t1 = time.time()
        r, w, e = select.select( [f],[],[], totaltime)
        if not f in r:
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
        logger.info("active_jobs: returning %s" % res)
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

        if task_id == "None":
            task_id = None

        if task_id:
            task_id = int(task_id)

        host_site = "%s_on_%s" % (jobsub_job_id, kwargs.get('slot','unknown'))

        jl = dbhandle.query(Job).with_for_update(of=Job).options(joinedload(Job.task_obj)).filter(Job.jobsub_job_id==jobsub_job_id).order_by(Job.job_id).all()
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
                newstatus = self.poms_service.taskPOMS.compute_status(dbhandle, j.task_obj)
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


    def kill_jobs(self, dbhandle, loghandle, campaign_id=None, task_id=None, job_id=None, confirm=None):
        jjil = []
        jql = None
        t = None
        if campaign_id != None or task_id != None:
            if campaign_id != None:
                tl = dbhandle.query(Task).filter(Task.campaign_id == campaign_id, Task.status != 'Completed', Task.status != 'Located').all()
            else:
                tl = dbhandle.query(Task).filter(Task.task_id == task_id).all()
            c = tl[0].campaign_snap_obj
            for t in tl:
                tjid = self.poms_service.taskPOMS.task_min_job(dbhandle, t.task_id)
                loghandle("kill_jobs: task_id %s -> tjid %s" % (t.task_id, tjid))
                # for tasks/campaigns, kill the whole group of jobs
                # by getting the leader's jobsub_job_id and taking off
                # the '.0'.
                if tjid:
                    jjil.append(tjid.replace('.0',''))
        else:
            jql = dbhandle.query(Job).filter(Job.job_id == job_id, Job.status != 'Completed', Job.status != 'Located').all()
            c = jql[0].task_obj.campaign_snap_obj
            for j in jql:
                jjil.append(j.jobsub_job_id)

        if confirm == None:
            jijatem = 'kill_jobs_confirm.html'

            template = self.jinja_env.get_template('kill_jobs_confirm.html')

            return  jjil, t, campaign_id, task_id, job_id
        else:
            group = c.experiment
            if group == 'samdev': group = 'fermilab'

            f = os.popen("jobsub_rm -G %s --role %s --jobid %s 2>&1" % (group, c.vo_role, ','.join(jjil)), "r")
            output = f.read()
            f.close()

            template = self.jinja_env.get_template('kill_jobs.html')
            return output, c, campaign_id, task_id, job_id

    def jobs_eff_histo(self, dbhandle, campaign_id, tmax = None, tmin = None, tdays = 1 ):
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
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.poms_service.utilsPOMS.handle_dates(tmin, tmax,tdays,'jobs_eff_histo?campaign_id=%s&' % campaign_id)

        q = dbhandle.query(func.count(Job.job_id), func.floor(Job.cpu_time *10/Job.wall_time))
        q = q.join(Job.task_obj)
        q = q.filter(Job.task_id == Task.task_id, Task.campaign_id == campaign_id)
        q = q.filter(Job.cpu_time > 0, Job.wall_time >= Job.cpu_time)
        q = q.filter(Task.created < tmax, Task.created >= tmin)
        q = q.group_by(func.floor(Job.cpu_time*10/Job.wall_time))
        q = q.order_by((func.floor(Job.cpu_time*10/Job.wall_time)))

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


    def get_efficiency(self, dbhandle, loghandle, campaign_list, tmin, tmax): #This method was deleted from the main script
        id_list = []
        for c in campaign_list:
            id_list.append(c.campaign_id)

        rows = (dbhandle.query( func.sum(Job.cpu_time), func.sum(Job.wall_time),Task.campaign_id).
                filter(Job.task_id == Task.task_id,
                       Task.campaign_id.in_(id_list),
                       Job.cpu_time > 0,
                       Job.wall_time > 0,
                       Task.created >= tmin, Task.created < tmax ).
                group_by(Task.campaign_id).all())

        loghandle("got rows:")
        for r in rows:
            loghandle("%s" % repr(r))

        mapem={}
        for totcpu, totwall, campaign_id in rows:
            if totcpu != None and totwall != None:
                mapem[campaign_id] = int(totcpu * 100.0 / totwall)
            else:
                mapem[campaign_id] = -1

        loghandle("got map: %s" % repr(mapem))

        efflist = []
        for c in campaign_list:
            efflist.append(mapem.get(c.campaign_id, -2))

        loghandle("got list: %s" % repr(efflist))
        return efflist


    def launch_jobs(self, dbhandle,loghandle, getconfig, gethead, seshandle, err_res, campaign_id, dataset_override = None, parent_task_id = None):

        loghandle("Entering launch_jobs(%s, %s, %s)" % (campaign_id, dataset_override, parent_task_id))
        if getconfig("poms.launches","allowed") == "hold":
            return "Job launches currently held."

        c = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).options(joinedload(Campaign.launch_template_obj),joinedload(Campaign.campaign_definition_obj)).first()
        cd = c.campaign_definition_obj
        lt = c.launch_template_obj

        e = seshandle('experimenter')
        xff = gethead('X-Forwarded-For', None)
        ra =  gethead('Remote-Addr', None)
        if not e.is_authorized(c.experiment) and not ( ra == '127.0.0.1' and xff == None):
             loghandle("launch_jobs -- experimenter not authorized")
             err_res="404 Permission Denied."
             return "Not Authorized: e: %s xff %s ra %s" % (e, xff, ra)
        experimenter_login = e.email[:e.email.find('@')]
        lt.launch_account = lt.launch_account % {
              "experimenter": experimenter_login,
        }

        if dataset_override:
            dataset = dataset_override
        else:
            dataset = self.poms_service.campaignsPOMS.get_dataset_for(dbhandle, err_res, c)

        group = c.experiment
        if group == 'samdev': group = 'fermilab'

        cmdl =  [
            "exec 2>&1",
            "export KRB5CCNAME=/tmp/krb5cc_poms_submit_%s" % group,
            "export POMS_PARENT_TASK_ID=%s" % (parent_task_id if parent_task_id else ""),
            "kinit -kt $HOME/private/keytabs/poms.keytab poms/cd/%s@FNAL.GOV || true" % self.poms_service.hostname,
            "ssh -tx %s@%s <<'EOF'" % (lt.launch_account, lt.launch_host),
            lt.launch_setup % {
              "dataset":dataset,
              "version":c.software_version,
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
           params = OrderedDict(json.loads(cd.definition_parameters))
        else:
           params = OrderedDict([])

        if c.param_overrides != None and c.param_overrides != "":
            params.update(json.loads(c.param_overrides))

        lcmd = cd.launch_script + " " + ' '.join((x[0]+x[1]) for x in params.items())
        lcmd = lcmd % {
              "dataset":dataset,
              "version":c.software_version,
              "group": group,
              "experimenter": experimenter_login,
        }
        cmdl.append(lcmd)
        cmdl.append('exit')
        cmdl.append('EOF')
        cmd = '\n'.join(cmdl)

        cmd = cmd.replace('\r','')

        # make sure launch doesn't take more that half an hour...
        output = popen_read_with_timeout(cmd, 1800) ### Question???

        # always record launch...
        ds = time.strftime("%Y%m%d_%H%M%S")
        outdir = "%s/private/logs/poms/launches/campaign_%s" % (os.environ["HOME"],campaign_id)
        outfile = "%s/%s" % (outdir, ds)
        loghandle("trying to record launch in %s" % outfile)
        return lcmd, output, c, campaign_id, outdir, outfile
