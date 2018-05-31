#!/usr/bin/env python
'''
This module contain the methods that handle the Calendar.
List of methods: active_jobs, output_pending_jobs, update_jobs
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify
version of functions in poms_service.py written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
'''

from collections import deque
import re
from .poms_model import Job, Submission, CampaignStage, JobTypeSnapshot, JobFile, JobHistory
from datetime import datetime
from sqlalchemy.orm import joinedload
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
        for jobsub_job_id, submission_id in (dbhandle.query(Job.jobsub_job_id, Job.submission_id)
                                       .filter(Job.status != "Completed", Job.status != "Located", Job.status != "Removed", Job.status != "Failed")
                                       .execution_options(stream_results=True).all()):
            if jobsub_job_id == "unknown":
                continue
            res.append((jobsub_job_id, submission_id))
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
        #  JobFile.file_name like JobTypeSnapshot.output_file_patterns
        # but with a comma separated list of them, I don's think it works
        # directly -- we would have to convert comma to pipe...
        # for now, I'm just going to make it a regexp and filter them here.
        for e, jobsub_job_id, fname in (dbhandle.query(
                                 CampaignStage.experiment,
                                 Job.jobsub_job_id,
                                 JobFile.file_name)
                  .join(Submission)
                  .filter(
                          Submission.status == "Completed",
                          Submission.campaign_stage_id == CampaignStage.campaign_stage_id,
                          Job.submission_id == Submission.submission_id,
                          Job.job_id == JobFile.job_id,
                          JobFile.file_type == 'output',
                          JobFile.declared == None,
                          Job.status == "Completed",
                        )
                  .order_by(CampaignStage.experiment, Job.jobsub_job_id)
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
        sid = j.submission_obj.submission_id
        exp = j.submission_obj.campaign_stage_snapshot_obj.experiment
        cid = j.submission_obj.campaign_stage_snapshot_obj.campaign_stage_id
        samhandle.update_project_description(exp, projname, "POMS CampaignStage %s Submission %s" % (cid, sid))
        pass


    def bulk_update_job(self, dbhandle, rpstatus, samhandle, json_data='{}'):
        logit.log("Entering bulk_update_job(%s)" % json_data)
        ldata = json.loads(json_data)
        del json_data

        #
        # build maps[field][value] = [list-of-ids] for submissions, jobs
        # from the data passed in
        #
        task_updates = {}
        job_updates = {}
        new_files = deque()

        # check for submission_ids we have present in the database versus ones
        # wanted by data.

        tids_wanted = set()
        tids_present = set()
        for r in ldata:   # make field level dictionaries
            for field, value in r.items():
                if field == 'submission_id' and value:
                   tids_wanted.add(int(value))

        # build upt tids_present in loop below while getting regexes to
        # match output files, etc.
        # - tids_present.update([x[0] for x in dbhandle.query(Submission.submission_id).filter(Submission.submission_id.in_(tids_wanted))])

        #
        # using ORM, get affected submissions and campaign definition snap objs.
        # Build up:
        #   * set of submission_id's we have in database
        #   * output file regexes for each task
        #
        if len(tids_wanted) == 0:
            tpl = []
        else:
            tq = (dbhandle.query(Submission.submission_id, JobTypeSnapshot.output_file_patterns)
                  .filter(Submission.job_type_snapshot_id == JobTypeSnapshot.job_type_snapshot_id)
                  .filter(Submission.submission_id.in_(tids_wanted)))

            tpl = tq.all()

        of_res = {}
        for sid, ofp in tpl:
            tids_present.add(sid)
            if not ofp:
               ofp = '%'
            of_res[sid] = ofp.replace(',', '|').replace('.', '\\.').replace('%', '.*')

        jjid2tid = {}
        logit.log("bulk_update_job == tids_present =%s" % repr(tids_present))

        tasks_job_completed = set()

        for r in ldata:   # make field level dictionaries
            if r['submission_id'] and not (int(r['submission_id']) in tids_present):
                continue
            for field, value in r.items():
                if value in (None, 'None', ''):
                    pass
                elif field == 'submission_id':
                    jjid2tid[r['jobsub_job_id']] = value
                elif field in ("input_file_names", "output_file_names"):
                    pass
                if field.startswith("task_"):
                    task_updates[field[5:]] = {}
                else:
                    job_updates[field] = {}
            if 'status' in r and 'submission_id' in r and r['status'] in ('Completed', 'Removed'):
                tasks_job_completed.add(r['submission_id'])

        job_file_jobs = set()

        newfiles = set()
        fnames = set()
        # regexp to filter out things we copy out that are not output files..
        logit.log(" bulk_update_job: ldata1")
        for r in ldata: # make lists for [field][value] pairs
            if r['submission_id'] and not (int(r['submission_id']) in tids_present):
                continue
            for field, value in r.items():
                if value in (None, 'None', ''):
                    pass
                elif field == 'submission_id':
                    pass
                elif field in ("input_file_names", "output_file_names"):
                    ftype = field.replace("_file_names", "")
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
            if r['submission_id'] and not (int(r['submission_id']) in tids_present):
                continue
            for field, value in r.items():
                if value in (None, 'None', ''):
                    pass
                elif field == 'submission_id':
                    pass
                elif field in ("input_file_names", "output_file_names"):
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
        update_jobsub_job_ids.update(job_updates.get('jobsub_job_id', {}).keys())

        if 0 == len(update_jobsub_job_ids) and 0 == len(task_updates) and 0 == len(newfiles):
            logit.log(" bulk_update_job: no actionable items, returning")
            return

        # we get passed some things we dont update, jobsub_job_id
        # 'cause we use that to look it up,
        # filter out ones we don's have...
        job_fields = set([x for x in dir(Job) if x[0] != '_'])
        job_fields -= {'metadata', 'jobsub_job_id'}

        kl = [k for k in job_updates.keys()]

        for cleanup in kl:
            if cleanup not in job_fields:
                del job_updates[cleanup]

        task_fields = set([x for x in dir(Submission) if x[0] != '_'])

        kl = [k for k in task_updates.keys()]
        for cleanup in kl:
            if cleanup not in task_fields:
                del task_updates[cleanup]

        # now figure out what jobs we have already, and what ones we need
        # to insert...
        # lock the submissions the jobs are associated with briefly
        # so the answer is correct.

        if len(tids_wanted) == 0:
            tl2 = []
        else:
            tl2 = (dbhandle.query(Submission)
                   .filter(Submission.submission_id.in_(tids_wanted))
                   .with_for_update(of=Submission, read=True)
                   .order_by(Submission.submission_id)
                   .all())

        if len(update_jobsub_job_ids) > 0:
            have_jobids.update([x[0] for x in
                                dbhandle.query(Job.jobsub_job_id)
                               .filter(Job.jobsub_job_id.in_(update_jobsub_job_ids))
                               .with_for_update(of=Job, read=True)
                               .order_by(Job.jobsub_job_id)
                               .all()])

        add_jobsub_job_ids = task_jobsub_job_ids - have_jobids

        logit.log(" bulk_update_job: ldata4")
        # now insert initial rows

        dbhandle.bulk_insert_mappings(Job, [
            dict(jobsub_job_id=jobsub_job_id,
                 submission_id=jjid2tid[jobsub_job_id],
                 node_name='unknown',
                 cpu_type='unknown',
                 host_site='unknown',
                 updated=datetime.now(utc),
                 created=datetime.now(utc),
                 status='Idle',
                 output_files_declared=False
                 )
            for jobsub_job_id in add_jobsub_job_ids if jjid2tid.get(jobsub_job_id, None)]
                                      )
        logit.log(" bulk_update_job: ldata5")

        # commit and re-lock submissions to reduce how long we hold locks...
        dbhandle.commit()

        # now update fields

        for field in job_updates.keys():
            if len(tids_wanted) == 0:
                tl2 = []
            else:
                tl2 = (dbhandle.query(Submission)
                       .filter(Submission.submission_id.in_(tids_wanted))
                       .with_for_update(of=Submission, read=True)
                       .order_by(Submission.submission_id)
                       .all())

            for value in job_updates[field].keys():
                if not value: # don's clear things cause we didn's get data
                    continue
                if len(job_updates[field][value]) > 0:
                    (dbhandle.query(Job)
                     .filter(Job.jobsub_job_id.in_(job_updates[field][value]))
                     .update({field: value}, synchronize_session=False))

            dbhandle.commit()


        submission_ids = set()
        submission_ids.update([int(x) for x in jjid2tid.values()])

        #
        # make a list of submissions which don's have projects set yet
        # to update after we do the batch below
        #
        if len(submission_ids) == 0:
            fix_submission_ids = []
        else:
            fix_submission_ids = (dbhandle.query(Submission.submission_id)
                            .filter(Submission.submission_id.in_(submission_ids))
                            .filter(Submission.project == None)
                            .all())

        logit.log(" bulk_update_job: ldata6")

        # lock again -- long lock hold split
        if len(tids_wanted) == 0:
            tl2 = []
        else:
            tl2 = (dbhandle.query(Submission)
                   .filter(Submission.submission_id.in_(tids_wanted))
                   .with_for_update(of=Submission, read=True)
                   .order_by(Submission.submission_id)
                   .all())

        for field in task_updates.keys():
            for value in task_updates[field].keys():
                if not value:   # don's clear things cause we didn's get data
                    continue
                if len(task_updates[field][value]) > 0:
                    (dbhandle.query(Submission)
                     .filter(Submission.submission_id.in_(task_updates[field][value]))
                     .update({field: value}, synchronize_session=False))

        #
        # now for job files, we need the job_ids for the jobsub_job_ids
        #
        logit.log(" bulk_update_job: ldata7")

        jidmap = dict(dbhandle.query(Job.jobsub_job_id, Job.job_id).filter(Job.jobsub_job_id.in_(job_file_jobs)))
        jidmap_r = dict([(v, k) for k, v in jidmap.items()])

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
        fset = set([(jidmap_r[r[0]], r[1], r[2]) for r in fl])

        logit.log("existing set: %s" % repr(fset))

        newfiles = newfiles - fset

        logit.log("newfiles now: %s" % repr(newfiles))

        if len(newfiles) > 0:
            dbhandle.bulk_insert_mappings(JobFile,
                                          [dict(job_id=jidmap[r[0]],
                                                file_type=r[1],
                                                file_name=r[2],
                                                created=datetime.now(utc))
                                           for r in newfiles]
                                          )

        logit.log(" bulk_update_job: ldata8")
        #
        # update any related submissions status if changed
        #
        for s in tl2:
            newstatus = self.poms_service.taskPOMS.compute_status(dbhandle, s)
            if newstatus != s.status:
                logit.log("update_job: task %d status now %s" % (s.submission_id, newstatus))
                s.status = newstatus
                s.updated = datetime.now(utc)
                # jobs make inactive campaign_stages active again...
                if s.campaign_stage_obj.active is not True:
                    s.campaign_stage_obj.active = True

        dbhandle.commit()

        #
        # refetch to update task projects
        # try to update projects when we first see them
        #  (for folks who start projects before launch)
        # and when we see a job completed
        #  (for folks who start projects in a DAG)
        #
        need_updates = set(fix_submission_ids)
        need_updates = need_updates.union(tasks_job_completed)
        if len(need_updates) == 0:
            tl = []
        else:
            tq = (dbhandle.query(Submission)
                  .filter(Submission.submission_id.in_(need_updates))
                  .options(joinedload(Submission.job_type_snapshot_obj)))
            tl = tq.all()

        for s in tl:
            if s.project:
                sid = s.submission_id
                exp = s.job_type_snapshot_obj.experiment
                cid = s.campaign_stage_id
                logit.log("Trying to update project description %d" % sid)
                samhandle.update_project_description(exp, s.project, "POMS CampaignStage %s Submission %s" % (cid, sid))

        logit.log("Exiting bulk_update_job()")

        return "Ok."


    def update_job(self, dbhandle, rpstatus, samhandle, submission_id=None, jobsub_job_id='unknown', **kwargs):

        # flag to remember to do a SAM update after we commit
        do_SAM_project = False

        if submission_id == "None":
            submission_id = None

        if submission_id:
            submission_id = int(submission_id)

        # host_site = "%s_on_%s" % (jobsub_job_id, kwargs.get('slot','unknown'))

        jl = (dbhandle.query(Job).with_for_update(of=Job, read=True)
              .options(joinedload(Job.submission_obj)).filter(Job.jobsub_job_id == jobsub_job_id)
              .order_by(Job.job_id).execution_options(stream_results=True).all())
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

        if not j and submission_id:
            s = dbhandle.query(Submission).filter(Submission.submission_id == submission_id).first()
            if s is None:
                logit.log("update_job -- no such task yet")
                rpstatus = "404 Submission Not Found"
                return "No such task"
            logit.log("update_job: creating new job")
            j = Job()
            j.jobsub_job_id = jobsub_job_id.rstrip("\n")
            j.created = datetime.now(utc)
            j.updated = datetime.now(utc)
            j.submission_id = submission_id
            j.submission_obj = s
            j.output_files_declared = False
            j.cpu_type = ''
            j.node_name = ''
            j.host_site = ''
            j.status = 'Idle'

        if j:
            oldstatus = j.status

            self.update_job_common(dbhandle, rpstatus, samhandle, j, kwargs)
            if oldstatus != j.status and j.submission_obj:
                newstatus = self.poms_service.taskPOMS.compute_status(dbhandle, j.submission_obj)
                if newstatus != j.submission_obj.status:
                    logit.log("update_job: task %d status now %s" % (j.submission_obj.submission_id, newstatus))
                    j.submission_obj.status = newstatus
                    j.submission_obj.updated = datetime.now(utc)
                    # jobs make inactive campaign_stages active again...
                    if j.submission_obj.campaign_stage_obj.active is not True:
                        j.submission_obj.campaign_stage_obj.active = True

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
        if j.submission_obj.project:
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
                # it shouldn's take long to figure out we have no work.
                if j.cpu_time is None or j.cpu_time < min_successful_cpu:
                    score = score + 1
                if ofcount[0] == 0:
                    score = score + 1
                if j.user_exe_exit_code == 0:
                    score = score + 1
        else:
            if ifcount[0] is not None and ifcount[0] > 0:
                # non-SAM file processing case
                if j.cpu_time is not None and j.cpu_time > min_successful_cpu:
                    score = score + 1
                if ofcount[0] is not None and ofcount[0] > 1.0:
                    score = score + 1
                if j.user_exe_exit_code == 0:
                    score = score + 1
            else:
                # non-SAM  mc/gen case
                if j.cpu_time is not None and j.cpu_time > min_successful_cpu:
                    score = score + 1
                if ofcount[0] is not None and ofcount[0] > 1.0:
                    score = score + 1
                if j.user_exe_exit_code == 0:
                    score = score + 1
        return score < 2


    def update_job_common(self, dbhandle, rpstatus, samhandle, j, kwargs):

        oldstatus = j.status
        logit.log("update_job: updating job %d" % (j.job_id if j.job_id else -1))

        if (kwargs.get('status', None) and
                oldstatus != kwargs.get('status') and
                oldstatus in ('Completed', 'Removed', 'Failed') and
                kwargs.get('status') != 'Located'):
            # we went from Completed or Removed back to some Running/Idle state...
            # so clean out any old (wrong) Completed statuses from
            # the JobHistory... (Bug #15322)
            dbhandle.query(JobHistory).filter(JobHistory.job_id == j.job_id,
                                              JobHistory.status.in_(['Completed', 'Removed', 'Failed'])).delete(synchronize_session=False)

        if kwargs.get('status', None) == 'Completed':
            if self.failed_job(j, dbhandle):
                kwargs['status'] = "Failed"

        # first, Job string fields the db requres be not null:
        for field in ['cpu_type', 'node_name', 'host_site', 'status', 'user_exe_exit_code', 'reason_held']:
            if field == 'status' and j.status == "Located":
                # stick at Located, don's roll back to Completed,etc.
                continue

            if kwargs.get(field, None):
                setattr(j, field, str(kwargs[field]).rstrip("\n"))

            if not getattr(j, field, None):
                if not field in ['user_exe_exit_code', 'reason_held']:
                    setattr(j, field, 'unknown')

        # first, next, output_files_declared, which also changes status
        if kwargs.get('output_files_declared', None) == "True":
            if j.status == "Completed":
                j.output_files_declared = True
                if self.failed_job(j, dbhandle):
                    j.status = "Failed"
                else:
                    j.status = "Located"

        # next fields we set in our Submission
        for field in ['project', 'recovery_tasks_parent']:

            if kwargs.get("task_%s" % field, None) and kwargs.get("task_%s" % field) != "None" and j.submission_obj:
                setattr(j.submission_obj, field, str(kwargs["task_%s" % field]).rstrip("\n"))
                logit.log("setting task %d %s to %s" % (j.submission_obj.submission_id, field, getattr(j.submission_obj, field, kwargs["task_%s" % field])))

        # floating point fields need conversion
        for field in ['cpu_time', 'wall_time']:
            if kwargs.get(field, None) and kwargs[field] != "None":
                if isinstance(kwargs[field], str):
                    setattr(j, field, float(str(kwargs[field]).rstrip("\n")))
                if isinstance(kwargs[field], float):
                    setattr(j, field, kwargs[field])

        # filenames need dumping in JobFile table and attaching
        if kwargs.get('output_file_names', None):
            logit.log("saw output_file_names: %s" % kwargs['output_file_names'])
            if j.job_files:
                files = [x.file_name for x in j.job_files]
            else:
                files = deque()

            newfiles = kwargs['output_file_names'].split(' ')

            # don's include metadata files

            if j.submission_obj.job_type_snapshot_obj.output_file_patterns:
                ofp = j.submission_obj.job_type_snapshot_obj.output_file_patterns
            else:
                ofp = '%'

            output_match_re = ofp.replace(',', '|').replace('.', '\\.').replace('%', '.*')

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


    def test_job_counts(self, submission_id=None, campaign_stage_id=None):
        res = self.poms_service.job_counts(submission_id, campaign_stage_id)
        return repr(res) + self.poms_service.format_job_counts(submission_id, campaign_stage_id)


    def kill_jobs(self, dbhandle, campaign_stage_id=None, submission_id=None, job_id=None, confirm=None, act='kill'):
        jjil = deque()
        jql = None
        s = None
        if campaign_stage_id is not None or submission_id is not None:
            if campaign_stage_id is not None:
                tl = dbhandle.query(Submission).filter(Submission.campaign_stage_id == campaign_stage_id,
                                                 Submission.status != 'Completed', Submission.status != 'Located', Submission.status != 'Failed').all()
            else:
                tl = dbhandle.query(Submission).filter(Submission.submission_id == submission_id).all()
            if len(tl):
                cs = tl[0].campaign_stage_snapshot_obj
                lts = tl[0].launch_template_snap_obj
                st = tl[0]
            else:
                cs = None
                lts = None


            for s in tl:
                tjid = self.poms_service.taskPOMS.task_min_job(dbhandle, s.submission_id)
                logit.log("kill_jobs: submission_id %s -> tjid %s" % (s.submission_id, tjid))
                # for submissions/campaign_stages, kill the whole group of jobs
                # by getting the leader's jobsub_job_id and taking off
                # the '.0'.
                if tjid:
                    jjil.append(tjid.replace('.0', ''))
        else:
            jql = dbhandle.query(Job).filter(Job.job_id == job_id,
                                             Job.status != 'Completed', Job.status != 'Removed',
                                             Job.status != 'Located', Job.status != 'Failed').execution_options(stream_results=True).all()

            if len(jql) == 0:
                jjil = ["(None Found)"]
            else:
                st = jql[0].submission_obj
                cs = st.campaign_stage_snapshot_obj
                for j in jql:
                    jjil.append(j.jobsub_job_id)
                lts = st.launch_template_snap_obj

        if confirm is None:
            jijatem = 'kill_jobs_confirm.html'

            return jjil, st, campaign_stage_id, submission_id, job_id
        elif cs:
            group = cs.experiment
            if group == 'samdev':
                group = 'fermilab'

            subcmd = 'q'
            if act == 'kill':
                subcmd = 'rm'
            elif act in ('hold', 'release'):
                subcmd = act
            else:
                raise SyntaxError("called with unknown action %s" % act)

            '''
            if test == true:
                os.open("echo jobsub_%s -G %s --role %s --jobid %s 2>&1" % (subcmd, group, cs.vo_role, ','.join(jjil)), "r")
            '''

            # expand launch setup %{whatever}s campaigns...

            launch_setup = lts.launch_setup % {
                "dataset": cs.dataset,
                "version": cs.software_version,
                "group": group,
                "experimenter":  st.experimenter_creator_obj.username
                }

            launch_setup = launch_setup.replace("\n",";")

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
                cs.vo_role,
                ','.join(jjil)
            )

            f = os.popen(cmd, "r")
            output = f.read()
            f.close()

            return output, cs, campaign_stage_id, submission_id, job_id
        else:
            return "Nothing to %s!" % act, None, 0, 0, 0


    def jobs_time_histo(self, dbhandle, campaign_stage_id, timetype, binsize=None, tmax=None, tmin=None, tdays=1, submit=None):
        """  histogram based on cpu_time/wall_time/aggregate copy times
         """
        (tmin, tmax, tmins, tmaxs, nextlink, prevlink,
         time_range_string,tdays) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'jobs_time_histo?timetype=%s&campaign_stage_id=%s&' % (timetype, campaign_stage_id))

        cs = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()

        #
        # use max of wall clock time to pick bin size..
        # also, find min jobid while we're there to see if we
        # can use it to speed up queries on job_histories(?)
        #
        res = (dbhandle.query(func.max(Job.wall_time), func.min(Job.job_id))
               .join(Submission, Job.submission_id == Submission.submission_id)
               .filter(Submission.campaign_stage_id == campaign_stage_id)
               .filter(Submission.created <= tmax, Submission.created >= tmin)
               .first())
        logit.log("max wall time %s, min job_id %s" % (res[0],res[1]))
        maxwall = res[0]
        minjobid = res[1]

        if maxwall is None:
            return cs, 0.01, 0, 0, {'unk.': 0}, 0, tmaxs, campaign_stage_id, tdays, str(tmin)[:16], str(tmax)[:16], nextlink, prevlink, tdays

        if timetype == "wall_time" or timetype == "cpu_time":
            if timetype == "wall_time":
                fname = Job.wall_time
            else:
                fname = Job.cpu_time

            if binsize == None:
                binsize = maxwall / 10

            binsize = float(binsize)

            qf = func.floor(fname / binsize)

            q = (dbhandle.query(func.count(Job.job_id), qf)
                 .join(Submission, Job.submission_id == Submission.submission_id)
                 .filter(Job.job_id >= minjobid)  # see if this speeds up
                 .filter(Submission.campaign_stage_id == campaign_stage_id)
                 .filter(Submission.created <= tmax, Submission.created >= tmin)
                 .group_by(qf)
                 .order_by(qf)
                 )
            qz = (dbhandle.query(func.count(Job.job_id))
                  .join(Submission, Job.submission_id == Submission.submission_id)
                  .filter(Job.job_id >= minjobid)  # see if this speeds up
                  .filter(Submission.campaign_stage_id == campaign_stage_id)
                  .filter(Submission.created <= tmax, Submission.created >= tmin)
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
                                  func.max(JobHistory.created).over(partition_by=JobHistory.job_id,
                                                                    order_by=desc(JobHistory.created),
                                                                    rows=(-1, 0)
                                                                    ).label('end_t'))
                   .join(Job)
                   .join(Submission)
                   .filter(JobHistory.status.in_([copy_start_status, 'running', 'Running']))
                   .filter(JobHistory.job_id == Job.job_id)
                   .filter(JobHistory.job_id >= minjobid)  # see if this speeds up
                   .filter(Job.submission_id == Submission.submission_id)
                   .filter(Submission.campaign_stage_id == campaign_stage_id)
                   .filter(Submission.created <= tmax, Submission.created >= tmin)
                   ).subquery()
            sq2 = (dbhandle.query(sq1.cs.job_id.label('job_id'),
                                  func.sum(sq1.cs.end_t - sq1.cs.start_t).label('copy_time'))
                   .filter(sq1.cs.status == copy_start_status)
                   .group_by(sq1.cs.job_id)
                   ).subquery()
            qf = func.floor(func.extract('epoch', sq2.cs.copy_time) / binsize)
            q = (dbhandle.query(func.count(sq2.cs.job_id), qf)
                 .group_by(qf)
                 .order_by(qf)
                 )
            # subquery -- count of copy start entries in JobHistory
            # for this Job.job_id
            qz = (dbhandle.query(func.count(Job.job_id))
                  .join(Submission, Job.submission_id == Submission.submission_id)
                  .filter(Submission.campaign_stage_id == campaign_stage_id)
                  .filter(Submission.created <= tmax, Submission.created >= tmin)
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
        return cs, maxv, maxbucket+1, total, vals, binsize, tmaxs, campaign_stage_id, tdays, str(tmin)[:16], str(tmax)[:16], nextlink, prevlink, tdays


    def jobs_eff_histo(self, dbhandle, campaign_stage_id, tmax=None, tmin=None, tdays=1):
        """  use
                  select count(job_id), floor(cpu_time * 10 / wall_time) as de
                     from jobs, submissions
                     where
                        jobs.submission_id = submissions.submission_id and
                        submissions.campaign_stage_id=17 and
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
         time_range_string,tdays) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'jobs_eff_histo?campaign_stage_id=%s&' % campaign_stage_id)

        q = dbhandle.query(func.count(Job.job_id), func.floor(Job.cpu_time * 10 / Job.wall_time))
        q = q.join(Job.submission_obj)
        q = q.filter(Job.submission_id == Submission.submission_id, Submission.campaign_stage_id == campaign_stage_id)
        q = q.filter(Job.cpu_time > 0,  Job.wall_time > 0, Job.cpu_time < Job.wall_time * 10)
        q = q.filter(Submission.created < tmax, Submission.created >= tmin)
        q = q.group_by(func.floor(Job.cpu_time * 10 / Job.wall_time))
        q = q.order_by((func.floor(Job.cpu_time * 10 / Job.wall_time)))

        qz = dbhandle.query(func.count(Job.job_id))
        qz = qz.join(Submission,Job.submission_id == Submission.submission_id)
        qz = qz.filter(Submission.campaign_stage_id == campaign_stage_id)
        qz = qz.filter(Submission.created < tmax, Submission.created >= tmin)
        qz = qz.filter(or_(not_(and_(Job.cpu_time > 0, Job.wall_time > 0, Job.cpu_time < Job.wall_time * 10)), Job.cpu_time == None, Job.wall_time == None))
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

        cs = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
        # return "total %d ; vals %s" % (total, vals)
        # return "Not yet implemented"
        return cs, maxv, total, vals, tmaxs, campaign_stage_id, tdays, str(tmin)[:16], str(tmax)[:16], nextlink, prevlink, tdays


    @pomscache.cache_on_arguments()
    def get_efficiency_map(self, dbhandle, id_list, tmin, tmax):  #This method was deleted from the main script

        if isinstance( id_list, str):
            id_list = [cid for cid in id_list.split(',') if cid]

        rows = (dbhandle.query(func.sum(Job.cpu_time), func.sum(Job.wall_time), Submission.campaign_stage_id).
                filter(Job.submission_id == Submission.submission_id,
                       Submission.campaign_stage_id.in_(id_list),
                       Job.cpu_time > 0,
                       Job.wall_time > 0,
                       Job.cpu_time < Job.wall_time * 10,
                       Submission.created >= tmin, Submission.created < tmax).
                group_by(Submission.campaign_stage_id).all())

        logit.log("got rows:")
        for r in rows:
            logit.log("%s" % repr(r))

        mapem = {}
        for totcpu, totwall, campaign_stage_id in rows:
            if totcpu is not None and totwall is not None:
                mapem[campaign_stage_id] = int(totcpu * 100.0 / totwall)
            else:
                mapem[campaign_stage_id] = -1
        logit.log("got map: %s" % repr(mapem))
        return mapem


    def get_efficiency(self, dbhandle, id_list, tmin, tmax):  #This method was deleted from the main script

        if isinstance(id_list, str):
            id_list = [int(cid) for cid in id_list.split(',') if cid]

        mapem = self.get_efficiency_map(dbhandle, id_list, tmin, tmax)
        efflist = deque()
        for cid in id_list:
            efflist.append(mapem.get(cid, -2))

        logit.log("got list: %s" % repr(efflist))
        return efflist
