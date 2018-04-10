#!/usr/bin/env python
'''
This module contain the methods that handle the Calendar.
List of methods: active_jobs, output_pending_jobs, update_jobs
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify
version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
'''

from collections import deque
import re
from .poms_model import Job, Task, Campaign, CampaignDefinitionSnapshot, CampaignSnapshot, JobFile, JobHistory
from datetime import datetime
from sqlalchemy.orm import joinedload
from sqlalchemy.exc import OperationalError
from sqlalchemy import func, not_, and_, or_, desc
from .utc import utc
import json
import os

from . import logit
from .pomscache import pomscache, pomscache_10


class JobsPOMS(object):

    pending_files_offset = 0

    def __init__(self, poms_service):
        self.poms_service = poms_service
        self.junkre = re.compile('.*fcl|log.*|.*\.log$|ana_hist\.root$|.*\.sh$|.*\.tar$|.*\.json$|[-_0-9]*$')

    def active_jobs(self, dbhandle):
        res = deque()
        for jobsub_job_id, task_id in dbhandle.query(Job.jobsub_job_id, Job.task_id).filter(Job.status != "Completed", Job.status != "Located", Job.status != "Removed", Job.status != "Failed").execution_options(stream_results=True).all():
            if jobsub_job_id == "unknown":
                continue
            res.append((jobsub_job_id, task_id))
        logit.log("active_jobs: returning %s" % res)
        return res


    def output_pending_jobs(self, dbhandle):
        res = {}
        windowsize = 1000
        count = 0
        preve = None
        prevj = None
        # it would be really cool if we could push the pattern match all the
        # way down into the query:
        #  JobFile.file_name like CampaignDefinitionSnapshot.output_file_patterns
        # but with a comma separated list of them, I don't think it works
        # directly -- we would have to convert comma to pipe...
        # for now, I'm just going to make it a regexp and filter them here.
        for e, jobsub_job_id, fname in (dbhandle.query(
                                 Campaign.experiment,
                                 Job.jobsub_job_id,
                                 JobFile.file_name)
                  .join(Task)
                  .filter(
                          Task.status == "Completed",
                          Task.campaign_id == Campaign.campaign_id,
                          Job.task_id == Task.task_id,
                          Job.job_id == JobFile.job_id,
                          JobFile.file_type == 'output',
                          JobFile.declared == None, 
                          Job.status == "Completed",
                        )
                  .order_by(Campaign.experiment, Job.jobsub_job_id)
                  .offset(JobsPOMS.pending_files_offset)
                  .limit(windowsize)
                  .all()):

            if preve != e:
                preve = e
                res[e] = {}
            if prevj != jobsub_job_id:
                prevj = jobsub_job_id
                res[e][jobsub_job_id] = []
            if not self.junkre.match(fname):
                logit.log("adding %s to exp %s jjid %s" % (fname, e, jobsub_job_id))
                res[e][jobsub_job_id].append(fname)
            count = count + 1

        if count != 0:
            JobsPOMS.pending_files_offset = JobsPOMS.pending_files_offset  + windowsize
        else:
            JobsPOMS.pending_files_offset = 0

        logit.log("pending files offset now: %d" % JobsPOMS.pending_files_offset)

        return res

    def update_SAM_project(self, samhandle, j, projname):
        logit.log("Entering update_SAM_project(%s)" % projname)
        tid = j.task_obj.task_id
        exp = j.task_obj.campaign_snap_obj.experiment
        cid = j.task_obj.campaign_snap_obj.campaign_id
        samhandle.update_project_description(exp, projname, "POMS Campaign %s Task %s" % (cid, tid))
        pass


    def bulk_update_job(self, dbhandle, rpstatus, samhandle, json_data='{}'):
        logit.log("Entering bulk_update_job(%s)" % json_data)
        ldata = json.loads(json_data)
        del json_data

        #
        # build maps[field][value] = [list-of-ids] for tasks, jobs
        # from the data passed in 
        #
        task_updates = {}
        job_updates = {}
        new_files = deque()

        # check for task_ids we have present in the database versus ones
        # wanted by data.
        
        tids_wanted = set()
        tids_present = set()
        for r in ldata:   # make field level dictionaries
            for field, value in r.items():
                if field == 'task_id' and value:
                   tids_wanted.add(int(value))

        # build upt tids_present in loop below while getting regexes to
        # match output files, etc.
        # - tids_present.update([x[0] for x in dbhandle.query(Task.task_id).filter(Task.task_id.in_(tids_wanted))])

        #
        # using ORM, get affected tasks and campaign definition snap objs. 
        # Build up:
        #   * set of task_id's we have in database
        #   * output file regexes for each task
        #
        if len(tids_wanted) ==  0:
            tpl = []
        else:
            tq = ( dbhandle.query(Task.task_id,CampaignDefinitionSnapshot.output_file_patterns)
                .filter(Task.campaign_definition_snap_id == CampaignDefinitionSnapshot.campaign_definition_snap_id)
                .filter(Task.task_id.in_(tids_wanted)))

            tpl = tq.all()
        

        of_res = {}
        for tid, ofp in tpl:
            tids_present.add(tid)
            if not ofp:
               ofp = '%'
            of_res[tid] = ofp.replace(',','|').replace('.','\\.').replace('%','.*')

        jjid2tid = {}
        logit.log("bulk_update_job == tids_present =%s" % repr(tids_present))

        tasks_job_completed = set()

        for r in ldata:   # make field level dictionaries
            if r['task_id'] and not (int(r['task_id']) in tids_present):
                continue
            for field, value in r.items():
                if value == '' or value == None or value == 'None':
                    pass
                elif field == 'task_id':
                    jjid2tid[r['jobsub_job_id']] = value
                elif field in ("input_file_names","output_file_names"):
                    pass
                if field.startswith("task_"):
                    task_updates[field[5:]] = {}
                else:
                    job_updates[field] = {}
            if 'status' in r and 'task_id' in r and r['status'] in ('Completed','Removed'):
               tasks_job_completed.add(r['task_id'])               

        job_file_jobs = set()

        newfiles = set()
        fnames = set()
        # regexp to filter out things we copy out that are not output files..
        logit.log(" bulk_update_job: ldata1")
        for r in ldata: # make lists for [field][value] pairs
            if r['task_id'] and not (int(r['task_id']) in tids_present):
                continue
            for field, value in r.items():
                if value == '' or value == None or value == 'None':
                    pass
                elif field == 'task_id':
                    pass
                elif field in ("input_file_names","output_file_names"):
                    ftype = field.replace("_file_names","")
                    for v in value.split(' '):
                        if len(v) < 2 or v[0] == '-':
                           continue
                        if ftype == 'output' and self.junkre.match(v):
                            thisftype = 'log'
                        else:
                            thisftype = ftype
                        newfiles.add(( r['jobsub_job_id'], thisftype, v))
                        fnames.add(v)
                    job_file_jobs.add(r['jobsub_job_id'])
                elif field.startswith("task_"):
                    task_updates[field[5:]][value] = set()
                else:
                    job_updates[field][value] = deque()

        logit.log(" bulk_update_job: ldata2")

        for r in ldata: # put jobids in lists
            if r['task_id'] and not (int(r['task_id']) in tids_present):
                continue
            for field, value in r.items():
                if value == '' or value == None or value == 'None':
                    pass
                elif field == 'task_id':
                    pass
                elif field in ("input_file_names","output_file_names"):
                    pass
                elif field.startswith("task_"):
                    task_updates[field[5:]][value].add(jjid2tid[r['jobsub_job_id']])
                else:
                    job_updates[field][value].append(r['jobsub_job_id'])
 
        #
        # done with regrouping the json data, drop it.
        #
        del ldata
 
        logit.log(" bulk_update_job: ldata3")
        logit.log(" bulk_update_job: job_updates %s" % repr(job_updates))
        logit.log(" bulk_update_job: task_updates %s" % repr(task_updates))
         
        #
        # figure out what jobs we need to add/update
        #
        update_jobsub_job_ids = set()
        task_jobsub_job_ids = set()
        have_jobids = set()  
        task_jobsub_job_ids.update(jjid2tid.keys())
        update_jobsub_job_ids.update(job_updates.get('jobsub_job_id',{}).keys())

        if 0 == len(update_jobsub_job_ids) and 0 == len(task_updates) and 0 == len(newfiles):
            logit.log(" bulk_update_job: no actionable items, returning")
            return

        # we get passed some things we dont update, jobsub_job_id
        # 'cause we use that to look it up, 
        # filter out ones we don't have...
        job_fields = set([x for x in dir(Job) if x[0] != '_'])
        job_fields = job_fields - set(('metadata','jobsub_job_id'))

        kl = [k for k in job_updates.keys()]

        for cleanup in kl:
            if cleanup not in job_fields:
                del job_updates[cleanup]

        task_fields = set([x for x in dir(Task) if x[0] != '_'])

        kl = [k for k in task_updates.keys()]
        for cleanup in kl:
            if cleanup not in task_fields:
                del task_updates[cleanup]
        
        # now figure out what jobs we have already, and what ones we need
        # to insert...
        # lock the tasks the jobs are associated with briefly 
        # so the answer is correct. 


        if len(tids_wanted) == 0:
            tl2 = []
        else:
            tl2 = ( dbhandle.query(Task)
                .filter(Task.task_id.in_(tids_wanted))
                .with_for_update(of=Task)
                .order_by(Task.task_id)
                .all())
 
        if len(update_jobsub_job_ids) > 0:
            have_jobids.update( [x[0] for x in
                dbhandle.query(Job.jobsub_job_id)
                    .filter(Job.jobsub_job_id.in_(update_jobsub_job_ids))
                    .with_for_update(of=Job)
                    .order_by(Job.jobsub_job_id)
                    .all()])
        

        add_jobsub_job_ids = task_jobsub_job_ids - have_jobids

        logit.log(" bulk_update_job: ldata4")
        # now insert initial rows
       
        dbhandle.bulk_insert_mappings(Job, [
              dict( jobsub_job_id = jobsub_job_id,
                    task_id = jjid2tid[jobsub_job_id],
                    node_name = 'unknown',
                    cpu_type = 'unknown',
                    host_site = 'unknown',
                    updated = datetime.now(utc),
                    created = datetime.now(utc),
                    status = 'Idle',
                    output_files_declared = False
               )
               for jobsub_job_id in add_jobsub_job_ids if jjid2tid.get(jobsub_job_id,None)]
           )
        

        logit.log(" bulk_update_job: ldata5")

        # now update fields            
        
        for field in job_updates.keys():
            for value in job_updates[field].keys():
                if not value: # don't clear things cause we didn't get data
                   continue
                if len(job_updates[field][value]) > 0:
                    (dbhandle.query(Job)
                       .filter(Job.jobsub_job_id.in_(job_updates[field][value]))
                       .update( {field: value}, synchronize_session = False ))
        
        task_ids = set()
        task_ids.update([int(x) for x in jjid2tid.values()])

        #
        # make a list of tasks which don't have projects set yet
        # to update after we do the batch below
        #
        if len(task_ids) == 0:
            fix_task_ids = []
        else:
            fix_task_ids = (dbhandle.query(Task.task_id)
                .filter(Task.task_id.in_(task_ids))
                .filter(Task.project == None)
                .all())

        logit.log(" bulk_update_job: ldata6")

        for field in task_updates.keys():
            for value in task_updates[field].keys():
                if not value: # don't clear things cause we didn't get data
                   continue
                if len(task_updates[field][value]) > 0:
                    (dbhandle.query(Task)
                       .filter(Task.task_id.in_(task_updates[field][value]))
                       .update( { field: value } , synchronize_session = False ))
        
        #
        # now for job files, we need the job_ids for the jobsub_job_ids
        #
        logit.log(" bulk_update_job: ldata7")

        jidmap = dict( dbhandle.query(Job.jobsub_job_id, Job.job_id).filter(Job.jobsub_job_id.in_(job_file_jobs)))
        jidmap_r = dict([ (v,k) for k, v in jidmap.items()])

        # check for files already present...
        # build a query that will find a superset of the 
        # items we want, if they were there already --i.e.
        # they have one of the file names and one of the jobids
        # use it to build a python set of tuples

        fl = (dbhandle.query(JobFile.job_id, JobFile.file_type, JobFile.file_name)
                  .filter(JobFile.file_name.in_(fnames), 
                          JobFile.job_id.in_(jidmap.values())
                      )
                  .all())
        #
        fset = set([(jidmap_r[r[0]],r[1],r[2]) for r in fl])

        logit.log("existing set: %s" % repr(fset))

        newfiles = newfiles - fset

        logit.log("newfiles now: %s" % repr(newfiles))

        if len(newfiles) > 0:
            dbhandle.bulk_insert_mappings(JobFile, [
               dict( job_id = jidmap[r[0]],
                    file_type = r[1],
                    file_name = r[2],
                    created = datetime.now(utc))
               for r in newfiles ]
             )
     
        logit.log(" bulk_update_job: ldata8")
        #
        # update any related tasks status if changed
        #
        for t in tl2:
            newstatus = self.poms_service.taskPOMS.compute_status(dbhandle, t)
            if newstatus != t.status:
                logit.log("update_job: task %d status now %s" % (t.task_id, newstatus))
                t.status = newstatus
                t.updated = datetime.now(utc)
                # jobs make inactive campaigns active again...
                if t.campaign_obj.active is not True:
                    t.campaign_obj.active = True
        dbhandle.commit()

        #
        # refetch to update task projects
        # try to update projects when we first see them
        #  (for folks who start projects before launch)
        # and when we see a job completed
        #  (for folks who start projects in a DAG)
        #
        need_updates = set(fix_task_ids)
        need_updates = need_updates.union(tasks_job_completed)
        if len(need_updates) == 0:
            tl =[]
        else:
            tq = ( dbhandle.query(Task)
                .filter(Task.task_id.in_(need_updates))
                .options(joinedload(Task.campaign_definition_snap_obj)) )
            tl = tq.all()

        for t in tl:
            if t.project:
                tid = t.task_id
                exp = t.campaign_definition_snap_obj.experiment
                cid = t.campaign_id
                logit.log("Trying to update project description %d" % tid)
                samhandle.update_project_description(exp, t.project, "POMS Campaign %s Task %s" % (cid, tid))

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

        jl = (dbhandle.query(Job).with_for_update(of=Job, read=True)
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

    def failed_job(self, j, dbhandle):
        '''
           compute final state: Failed/Located
           see the wiki [[Success]] page...
        '''
        min_successful_cpu = 7
        ofcount = dbhandle.query(func.count(JobFile.file_name)).filter(JobFile.job_id == j.job_id, JobFile.file_type == 'output').first()
        ifcount = dbhandle.query(func.count(JobFile.file_name)).filter(JobFile.job_id == j.job_id, JobFile.file_type == 'input').first()
        score = 0
        if j.task_obj.project:
            if ifcount[0] != None and  ifcount[0] > 0:
                # SAM file processing case
                score = 0
                if j.cpu_time > min_successful_cpu:
                   score = score + 1
                if ofcount[0] > 1.0:
                   score = score + 1
                if j.user_exe_exit_code == 0:
                   score = score + 1
            else:
                # SAM file out of files case
                # note the cpu test is backwards in this case...
                # it shouldn't take long to figure out we have no work.
                if j.cpu_time == None or j.cpu_time < min_successful_cpu:
                   score = score + 1
                if ofcount[0] == 0:
                   score = score + 1
                if j.user_exe_exit_code == 0:
                   score = score + 1
        else:
            if ifcount[0] != None and ifcount[0] > 0:
                # non-SAM file processing case
                if j.cpu_time != None and j.cpu_time > min_successful_cpu:
                   score = score + 1
                if ofcount[0] != None and ofcount[0] > 1.0:
                   score = score + 1
                if j.user_exe_exit_code == 0:
                   score = score + 1
            else:
                # non-SAM  mc/gen case
                if j.cpu_time != None and j.cpu_time > min_successful_cpu:
                   score = score + 1
                if ofcount[0] != None  and ofcount[0] > 1.0:
                   score = score + 1
                if j.user_exe_exit_code == 0:
                   score = score + 1
        return score < 2

    def update_job_common(self, dbhandle, rpstatus, samhandle, j, kwargs):

            oldstatus = j.status
            logit.log("update_job: updating job %d" % (j.job_id if j.job_id else -1))

            if kwargs.get('status', None) and oldstatus != kwargs.get('status') and oldstatus in ('Completed','Removed','Failed') and kwargs.get('status') != 'Located':
                # we went from Completed or Removed back to some Running/Idle state...
                # so clean out any old (wrong) Completed statuses from
                # the JobHistory... (Bug #15322)
                dbhandle.query(JobHistory).filter(JobHistory.job_id == j.job_id, JobHistory.status.in_(['Completed','Removed','Failed'])).delete(synchronize_session=False)

            if kwargs.get('status', None) == 'Completed':
                    if self.failed_job(j, dbhandle):
                        kwargs['status'] = "Failed"

            # first, Job string fields the db requres be not null:
            for field in ['cpu_type', 'node_name', 'host_site', 'status', 'user_exe_exit_code', 'reason_held']:
                if field == 'status' and j.status == "Located":
                    # stick at Located, don't roll back to Completed,etc.
                    continue

                if kwargs.get(field, None):
                    setattr(j, field, str(kwargs[field]).rstrip("\n"))

                if not getattr(j, field, None):
                    if not field in ['user_exe_exit_code','reason_held']:
                        setattr(j, field, 'unknown')

            # first, next, output_files_declared, which also changes status
            if kwargs.get('output_files_declared', None) == "True":
                if j.status == "Completed":
                    j.output_files_declared = True
                    if self.failed_job(j, dbhandle):
                        j.status = "Failed"
                    else:
                        j.status = "Located"

            # next fields we set in our Task
            for field in ['project', 'recovery_tasks_parent']:

                if kwargs.get("task_%s" % field, None) and kwargs.get("task_%s" % field) != "None" and j.task_obj:
                    setattr(j.task_obj, field, str(kwargs["task_%s" % field]).rstrip("\n"))
                    logit.log("setting task %d %s to %s" % (j.task_obj.task_id, field, getattr(j.task_obj, field, kwargs["task_%s" % field])))

            # floating point fields need conversion
            for field in ['cpu_time', 'wall_time']:
                if kwargs.get(field, None) and kwargs[field] != "None":
                    if (isinstance(kwargs[field], str)):
                        setattr(j, field, float(str(kwargs[field]).rstrip("\n")))
                    if (isinstance(kwargs[field], float)):
                        setattr(j, field, kwargs[field])

            # filenames need dumping in JobFile table and attaching
            if kwargs.get('output_file_names', None):
                logit.log("saw output_file_names: %s" % kwargs['output_file_names'])
                if j.job_files:
                    files = [x.file_name for x in j.job_files]
                else:
                    files = deque()

                newfiles = kwargs['output_file_names'].split(' ')

                # don't include metadata files

                if j.task_obj.campaign_definition_snap_obj.output_file_patterns:
                   ofp = j.task_obj.campaign_definition_snap_obj.output_file_patterns
                else:
                   ofp = '%'

                output_match_re = ofp.replace(',','|').replace('.','\\.').replace('%','.*')

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
                    files = deque()
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

    def kill_jobs(self, dbhandle, campaign_id=None, task_id=None, job_id=None, confirm=None , act = 'kill'):
        jjil = deque()
        jql = None
        t = None
        if campaign_id is not None or task_id is not None:
            if campaign_id is not None:
                tl = dbhandle.query(Task).filter(Task.campaign_id == campaign_id, Task.status != 'Completed', Task.status != 'Located', Task.status != 'Failed').all()
            else:
                tl = dbhandle.query(Task).filter(Task.task_id == task_id).all()
            if len(tl):
                c = tl[0].campaign_snap_obj
                lts = tl[0].launch_template_snap_obj
                st = tl[0]
            else:
                c = None
                lts = None
               

            for t in tl:
                tjid = self.poms_service.taskPOMS.task_min_job(dbhandle, t.task_id)
                logit.log("kill_jobs: task_id %s -> tjid %s" % (t.task_id, tjid))
                # for tasks/campaigns, kill the whole group of jobs
                # by getting the leader's jobsub_job_id and taking off
                # the '.0'.
                if tjid:
                    jjil.append(tjid.replace('.0', ''))
        else:
            jql = dbhandle.query(Job).filter(Job.job_id == job_id, Job.status != 'Completed', Job.status != 'Removed', Job.status != 'Located', Job.status != 'Failed').execution_options(stream_results=True).all()

            if len(jql) == 0:
                jjil = ["(None Found)"]
            else:  
                st = jql[0].task_obj
                c = st.campaign_snap_obj
                for j in jql:
                    jjil.append(j.jobsub_job_id)
                lts = st.launch_template_snap_obj

        if confirm is None:
            jijatem = 'kill_jobs_confirm.html'

            return jjil, st, campaign_id, task_id, job_id
        elif c:
            group = c.experiment
            if group == 'samdev':
                group = 'fermilab'

            subcmd = 'q'
            if act == 'kill':
                subcmd = 'rm'
            elif act in ('hold','release'):
                subcmd = act
            else:
                raise SyntaxError("called with unknown action %s" % act)

            '''
            if test == true:
                os.open("echo jobsub_%s -G %s --role %s --jobid %s 2>&1" % (subcmd, group, c.vo_role, ','.join(jjil)), "r")
            '''

            # expand launch setup %{whatever}s tags...

            launch_setup = lts.launch_setup % {
                "dataset": c.dataset,
                "version": c.software_version,
                "group": group,
                "experimenter":  st.experimenter_creator_obj.name
                }

            cmd = """
                exec 2>&1
                export KRB5CCNAME=/tmp/krb5cc_poms_submit_%s
                kinit -kt $HOME/private/keytabs/poms.keytab poms/cd/%s@FNAL.GOV || true
                ssh %s@%s '%s; set -x; jobsub_%s -G %s --role %s --jobid %s'
            """ % (
                group,
                self.poms_service.hostname,
                lts.launch_account, 
                lts.launch_host, 
                launch_setup, 
                subcmd,
                group, 
                c.vo_role, 
                ','.join(jjil)
            )
            
            f = os.popen(cmd, "r")
            output = f.read()
            f.close()

            return output, c, campaign_id, task_id, job_id
        else:
            return "Nothing to %s!" % act,  None, 0, 0, 0 

    def jobs_time_histo(self, dbhandle, campaign_id, timetype, binsize = None, tmax=None, tmin=None, tdays=1, submit=None):
        """  histogram based on cpu_time/wall_time/aggregate copy times
         """
        (tmin, tmax, tmins, tmaxs, nextlink, prevlink,
         time_range_string,tdays) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'jobs_time_histo?timetype=%s&campaign_id=%s&' % (timetype, campaign_id))

        c = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
       
        #
        # use max of wall clock time to pick bin size..
        # also, find min jobid while we're there to see if we
        # can use it to speed up queries on job_histories(?)
        #
        res = (dbhandle.query(func.max(Job.wall_time),func.min(Job.job_id))
                 .join(Task, Job.task_id == Task.task_id)
                 .filter(Task.campaign_id == campaign_id)
                 .filter(Task.created <= tmax, Task.created >= tmin)
                 .first())
        logit.log("max wall time %s, min job_id %s" % (res[0],res[1]))
        maxwall = res[0]
        minjobid = res[1]  

        if maxwall == None:
            return c, 0.01, 0, 0, {'unk.': 0}, 0, tmaxs, campaign_id, tdays, str(tmin)[:16], str(tmax)[:16], nextlink, prevlink, tdays

        if timetype == "wall_time" or timetype == "cpu_time":
           if timetype == "wall_time":
               fname = Job.wall_time 
           else:
               fname = Job.cpu_time
 
           if binsize == None:
               binsize = maxwall/10

           binsize = float(binsize)

           qf = func.floor(fname/binsize)

           q = (dbhandle.query(func.count(Job.job_id),qf )
                .join(Task, Job.task_id == Task.task_id)
                .filter(Job.job_id >= minjobid)   # see if this speeds up
                .filter(Task.campaign_id == campaign_id)
                .filter(Task.created <= tmax, Task.created >= tmin)
                .group_by(qf)
                .order_by(qf)
               )
           qz = (dbhandle.query(func.count(Job.job_id))
                .join(Task, Job.task_id == Task.task_id)
                .filter(Job.job_id >= minjobid)   # see if this speeds up
                .filter(Task.campaign_id == campaign_id)
                .filter(Task.created <= tmax, Task.created >= tmin)
                .filter(fname == None)
               )
                   
        elif timetype == "copy_in_time" or timetype == "copy_out_time":
           if timetype == "copy_in_time":
               copy_start_status = 'running: copying files in'
           else:
               copy_start_status = 'running: copying files out'

           if binsize == None:
               binsize = maxwall / 200
               if binsize > 900:
                  binsize = 900
           binsize = float(binsize)

           sq1 = (dbhandle.query(JobHistory.job_id.label('job_id'), 
                                 JobHistory.created.label('start_t'), 
                                 JobHistory.status.label('status'), 
                                 func.max(JobHistory.created).over(partition_by = JobHistory.job_id, 
                                                                   order_by=desc(JobHistory.created),
                                                                   rows=(-1,0)
                                                                   ).label('end_t'))
                        .join(Job)
                        .join(Task)
                        .filter(JobHistory.status.in_([copy_start_status,'running','Running']))
                        .filter(JobHistory.job_id == Job.job_id)
                        .filter(JobHistory.job_id >= minjobid)   # see if this speeds up
                        .filter(Job.task_id == Task.task_id)
                        .filter(Task.campaign_id == campaign_id)
                        .filter(Task.created <= tmax, Task.created >= tmin)
                   ).subquery()
           sq2 = (dbhandle.query(sq1.c.job_id.label('job_id'), 
                                 func.sum(sq1.c.end_t - sq1.c.start_t).label('copy_time'))
                     .filter(sq1.c.status == copy_start_status)
                     .group_by(sq1.c.job_id)
                   ).subquery()
           qf = func.floor( func.extract('epoch',sq2.c.copy_time)/ binsize)
           q = (dbhandle.query(func.count(sq2.c.job_id), qf)
                .group_by(qf)
                .order_by(qf)
                ) 
           # subquery -- count of copy start entries in JobHistory
           # for this Job.job_id
           qz = (dbhandle.query(func.count(Job.job_id))
                        .join(Task, Job.task_id == Task.task_id)
                        .filter(Task.campaign_id == campaign_id)
                        .filter(Task.created <= tmax, Task.created >= tmin)
                        .filter(0 == (dbhandle.query(func.count(JobHistory.created))
                                      .filter(JobHistory.job_id == Job.job_id)
                                      .filter(JobHistory.status == copy_start_status)
                                    ).as_scalar()
                 )
                 )
        else:
            raise KeyError("invalid timetype value, should be copy_in_time, copy_out_time, wall_time, cpu_time")

        nodata = qz.first()
        total = 0
        vals = {-1: nodata[0]}
        maxv = 0.01
        maxbucket = 0.01
        if nodata[0] > maxv:
            maxv = nodata[0]

        # raise our timeout for this one...
        dbhandle.execute("SET SESSION statement_timeout = '600s';")
        for row in q.all():
            vals[row[1]] = row[0]
            if row[0] != None and row[0] > maxv:
                maxv = row[0]
            if row[1] != None and row[1] > maxbucket:
                maxbucket = row[1]
            total += row[0]


        # return "total %d ; vals %s" % (total, vals)
        # return "Not yet implemented"
        return c, maxv, maxbucket+1, total, vals, binsize, tmaxs, campaign_id, tdays, str(tmin)[:16], str(tmax)[:16], nextlink, prevlink, tdays

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
        q = q.filter(Job.cpu_time > 0,  Job.wall_time > 0, Job.cpu_time < Job.wall_time * 10)
        q = q.filter(Task.created < tmax, Task.created >= tmin)
        q = q.group_by(func.floor(Job.cpu_time * 10 / Job.wall_time))
        q = q.order_by((func.floor(Job.cpu_time * 10 / Job.wall_time)))

        qz = dbhandle.query(func.count(Job.job_id))
        qz = qz.join(Task,Job.task_id == Task.task_id) 
        qz = qz.filter(Task.campaign_id == campaign_id)
        qz = qz.filter(Task.created < tmax, Task.created >= tmin)
        qz = qz.filter(or_(not_(and_(Job.cpu_time > 0, Job.wall_time > 0, Job.cpu_time < Job.wall_time * 10)),Job.cpu_time == None,Job.wall_time==None))
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


    @pomscache.cache_on_arguments()
    def get_efficiency_map(self, dbhandle, id_list, tmin, tmax):  #This method was deleted from the main script

        if isinstance( id_list, str):
            id_list = [cid for cid in id_list.split(',') if cid]

        rows = (dbhandle.query(func.sum(Job.cpu_time), func.sum(Job.wall_time), Task.campaign_id).
                filter(Job.task_id == Task.task_id,
                       Task.campaign_id.in_(id_list),
                       Job.cpu_time > 0,
                       Job.wall_time > 0,
                       Job.cpu_time < Job.wall_time * 10,
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
        return mapem

    def get_efficiency(self, dbhandle, id_list, tmin, tmax):  #This method was deleted from the main script

        if isinstance( id_list, str):
            id_list = [int(cid) for cid in id_list.split(',') if cid]

        mapem = self.get_efficiency_map(dbhandle, id_list, tmin, tmax)
        efflist = deque()
        for cid in id_list:
            efflist.append(mapem.get(cid, -2))

        logit.log("got list: %s" % repr(efflist))
        return efflist
