#!/usr/bin/env python
"""
This module contain the methods that handle the Submission.
List of methods: wrapup_tasks,
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py
written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
"""

import json
import os
import select
import subprocess
import time
import re
from collections import OrderedDict, deque
from datetime import datetime, timedelta

from psycopg2.extensions import QueryCanceledError
from sqlalchemy import case, func, text
from sqlalchemy.orm import joinedload

from . import condor_log_parser, logit, time_grid
from .poms_model import (CampaignStage,
                         JobType,
                         JobTypeSnapshot,
                         CampaignDependency,
                         CampaignStageSnapshot,
                         Campaign,
                         Experimenter,
                         HeldLaunch,
                         LoginSetup,
                         LoginSetupSnapshot,
                         Submission,
                         SubmissionHistory)
from .utc import utc


# from exceptions import KeyError


#
# utility function for running commands that don's run forever...
#
def popen_read_with_timeout(cmd, totaltime=30):

    origtime = totaltime
    # start up keeping subprocess handle and pipe
    pp = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    f = pp.stdout

    outlist = deque()
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


    def wrapup_tasks(self, dbhandle, samhandle, getconfig, gethead, seshandle, err_res):
        # this function call another function that is not in this module, it use a poms_service object passed as an argument at the init.
        now = datetime.now(utc)
        res = ["wrapping up:"]

        lookup_submission_list = deque()
        lookup_dims_list = deque()
        lookup_exp_list = deque()
        finish_up_submissions = deque()
        mark_located = deque()
        #
        # move launch stuff etc, to one place, so we can keep the table rows
        sq = (dbhandle.query(SubmissionHistory.submission_id, func.max(SubmissionHistory.created).label('latest')).filter(SubmissionHistory.created > datetime.now(utc) - timedelta(days=4)).group_by(SubmissionHistory.submission_id).subquery())
        completed_sids = (dbhandle.query(SubmissionHistory.submission_id).join(sq,SubmissionHistory.submission_id == sq.c.submission_id).filter(SubmissionHistory.status == 'Completed', SubmissionHistory.created == sq.c.latest).all())

        res.append("Completed submissions_ids: %s" % repr(list(completed_sids)))

        for s in (dbhandle
                      .query(Submission)
                      .join(CampaignStageSnapshot,
                            Submission.campaign_stage_snapshot_id == 
                              CampaignStageSnapshot.campaign_stage_snapshot_id)
                      .filter(Submission.submission_id.in_(completed_sids), 
                              CampaignStageSnapshot.completion_type == 
                                'complete')
                      .all()):
            res.append("completion type completed: %s" % s.submission_id)
            finish_up_submissions.append(s.submission_id)

        n_project = 0
        for s in (dbhandle
                      .query(Submission)
                      .join(CampaignStageSnapshot,Submission.campaign_stage_snapshot_id == 
                              CampaignStageSnapshot.campaign_stage_snapshot_id)
                      .filter(Submission.submission_id.in_(completed_sids), 
                              CampaignStageSnapshot.completion_type == 'located')
                      .all()):

            res.append("completion type located: %s" % s.submission_id)
            # after two days, call it on time...
            if now - s.updated > timedelta(days=2):
                finish_up_submissions.append(s.submission_id)

            elif s.project:
                # submission had a sam project, add to the list to look
                # up in sam
                n_project = n_project + 1
                basedims = "snapshot_for_project_name %s " % s.project
                allkiddims = basedims
                plist = []

                # try to get the file pattern list, either from the
                # dependencies that lead to this campaign_stage,
                # or from the job type

                for dcd in dbhandle.query(CampaignDependency).filter(CampaignDependency.needs_campaign_stage_id == s.campaign_stage_snapshot_obj.campaign_id).all():
                    if dcd.file_pattern:
                        plist.append(dcd.file_pattern)
                    else:
                        plist.append("%")

                if len(plist) == 0:
                    plist = str(s.job_type_snapshot_obj.output_file_patterns).split(',')

                logit.log("got file pattern list: %s" % repr(plist))

                for pat in plist:
                    if pat == 'None':
                        pat = '%'

                    if pat.find(' ') > 0:
                        allkiddims = "%s and isparentof: ( %s and version '%s' with availability physical ) " % (allkiddims, pat, s.campaign_stage_snapshot_obj.software_version)
                    else:
                        allkiddims = "%s and isparentof: ( file_name '%s' and version '%s' with availability physical ) " % (allkiddims, pat, s.campaign_stage_snapshot_obj.software_version)

                lookup_exp_list.append(s.campaign_stage_snapshot_obj.experiment)
                lookup_submission_list.append(s)
                lookup_dims_list.append(allkiddims)
            else:
                # it's located but there's no project, so they are 
                # defining the poms_depends_%(submission_id)s_1 dataset..
                allkiddims = "defname:poms_depends_%s_1" % s.submission_id
                lookup_exp_list.append(s.campaign_stage_snapshot_obj.experiment)
                lookup_submission_list.append(s)
                lookup_dims_list.append(allkiddims)

        dbhandle.commit()

        summary_list = samhandle.fetch_info_list(lookup_submission_list, dbhandle=dbhandle)
        count_list = samhandle.count_files_list(lookup_exp_list, lookup_dims_list)
        thresholds = deque()
        logit.log("wrapup_tasks: summary_list: %s" % repr(summary_list))    # Check if that is working
        res.append("wrapup_tasks: summary_list: %s" % repr(summary_list))

        res.append("count_list: %s" % count_list)
        res.append("thresholds: %s" % thresholds)
        res.append("lookup_dims_list: %s" % lookup_dims_list)

        for i in range(len(summary_list)):
            submission = lookup_submission_list[i]
            cfrac = submission.campaign_stage_snapshot_obj.completion_pct / 100.0
            if submission.project:
                threshold = (summary_list[i].get('tot_consumed', 0) * cfrac)
            else:
                # no project, so guess based on number of jobs in submit command?
                p1 = submission.command_executed.find('-N')
                p2 = submission.command_executed.find(' ', p1+3)
                try:
                    threshold = int(submission.command_executed[p1+3:p2])
                except:
                    threshold = 0

            thresholds.append(threshold)
            val = float(count_list[i])
            res.append("submission %s val %f threshold %f "%(submission, val, threshold))
            if val >= threshold and threshold > 0:
                res.append("adding submission %s "%submission)
                finish_up_submissions.append(submission.submission_id)

        for s in finish_up_submissions:
            res.append("marking submission %s located "%s)
            self.update_submission_status(dbhandle,s,"Located")

        dbhandle.commit()

        #
        # now, after committing to clear locks, we run through the
        # job logs for the submissions and update process stats, and
        # launch any recovery jobs or jobs depending on us.
        # this way we don's keep the rows locked all day
        #
        #logit.log("Starting need_joblogs loops, len %d" % len(finish_up_submissions))
        #if len(need_joblogs) == 0:
        #    njtl = []
        #else:
        #    njtl = dbhandle.query(Submission).filter(Submission.submission_id.in_(need_joblogs)).all()

        res.append("finish_up_submissions: %s "% repr(finish_up_submissions))

        if len(finish_up_submissions) == 0:
            futl = []
        else:
            futl = dbhandle.query(Submission).filter(Submission.submission_id.in_(finish_up_submissions)).all()

        res.append(" got list... ")

        for submission in futl:
            # get logs for job for final cpu values, etc.
            logit.log("Starting finish_up_submissions items for submission %s" % submission.submission_id)
            res.append("Starting finish_up_submissions items for submission %s" % submission.submission_id)

            if not self.launch_recovery_if_needed(dbhandle, samhandle, getconfig, gethead, seshandle, err_res, submission):
                self.launch_dependents_if_needed(dbhandle, samhandle, getconfig, gethead, seshandle, err_res, submission)

        return res

###

    def get_task_id_for(self, dbhandle, campaign, user=None, experiment=None, command_executed="", input_dataset="", parent_submission_id=None, submission_id = None):
        logit.log("get_task_id_for(user='%s',experiment='%s',command_executed='%s',input_dataset='%s',parent_submission_id='%s',submission_id='%s'" % (
             user, experiment, command_executed, input_dataset, parent_submission_id, submission_id
            ))
  
        #
        # try to extract the project name from the launch command...
        #
        project = None
        for projre in ('-e SAM_PROJECT=([^ ]*)','-e SAM_PROJECT_NAME=([^ ]*)','--sam_project ([^ ]*)') :
            m = re.search(projre, command_executed)
            if m:
                project = m.group(1)
                break

        if user is None:
            user = 4
        else:
            u = dbhandle.query(Experimenter).filter(Experimenter.username == user).first()
            if u:
                user = u.experimenter_id

        q = dbhandle.query(CampaignStage)
        if str(campaign)[0] in "0123456789":
            q = q.filter(CampaignStage.campaign_stage_id == int(campaign))
        else:
            q = q.filter(CampaignStage.name.like("%%%s%%" % campaign))

        if experiment:
            q = q.filter(CampaignStage.experiment == experiment)

        cs = q.first()
        tim = datetime.now(utc)
        if submission_id:
            s = dbhandle.query(Submission).filter(Submission.submission_id == submission_id).one()
            s.command_executed = command_executed
            s.updated = tim
        else:
            s = Submission(campaign_stage_id=cs.campaign_stage_id,
                 submission_params={},
                 project = project,
                 updater=4,
                 creator=4,
                 created=tim,
                 updated=tim,
                 command_executed=command_executed)


        if parent_submission_id is not None and parent_submission_id != "None":
            s.recovery_tasks_parent = int(parent_submission_id)


        self.snapshot_parts(dbhandle, s, s.campaign_stage_id)

        dbhandle.add(s)
        dbhandle.flush()

        if not submission_id:
            sh = SubmissionHistory(submission_id = s.submission_id, status = "New", created=tim);
            dbhandle.add(sh)
        logit.log("get_task_id_for: returning %s" % s.submission_id)
        dbhandle.commit()
        return s.submission_id

    def update_submission_status(self, dbhandle, submission_id, status):

        # get our latest history...
        sq = (dbhandle.query(
                func.max(SubmissionHistory.created).label('latest')
              ).filter(SubmissionHistory.submission_id == submission_id)
               .subquery())

        lasthist = (dbhandle.query(SubmissionHistory)
               .filter(SubmissionHistory.created == sq.c.latest)
               .first())

        # don't roll back Located
        if lasthist and lasthist.status == "Located":
            return

        # don't roll back Completed
        if lasthist and lasthist.status == "Completed" and status != "Located":
            return

        # don't put in duplicates
        if lasthist and  lasthist.status == status:
            return

        sh = SubmissionHistory()
        sh.submission_id = submission_id
        sh.status = status
        sh.created = datetime.now(utc)
        dbhandle.add(sh)

    def mark_failed_submissions(self, dbhandle):
        '''
            find all the recent submissions that are still "New" but more
            than two hours old, and mark them "LaunchFailed"
        '''
        now = datetime.now(utc)
        sq = (dbhandle.query(
                SubmissionHistory.submission_id, 
                func.max(SubmissionHistory.created).label('latest')
              ).filter(SubmissionHistory.created > datetime.now(utc) - timedelta(days=7))
               .group_by(SubmissionHistory.submission_id).subquery())

        failed_sids = (dbhandle.query(SubmissionHistory.submission_id)
                        .join(sq,SubmissionHistory.submission_id == sq.c.submission_id)
                        .filter(SubmissionHistory.status == 'New',
                                SubmissionHistory.created == sq.c.latest,
                                SubmissionHistory.created < 
                                    (now - timedelta(hours=2)))
                        .all())

        res = []
        for submission_id in failed_sids:
            res.append("updating %s" % submission_id)
            self.update_submission_status(dbhandle, submission_id,status = 'LaunchFailed')
        dbhandle.commit()
        return "\n".join(res)
        
    def running_submissions(self, dbhandle, campaign_id_list, status_list=['New','Idle','Running']):

        cl = campaign_id_list

        logit.log("INFO", "running_submissions(%s)" % repr(cl))
        sq = (dbhandle.query(
                SubmissionHistory.submission_id, 
                func.max(SubmissionHistory.created).label('latest')
              ).filter(SubmissionHistory.created > datetime.now(utc) - timedelta(days=4))
               .group_by(SubmissionHistory.submission_id).subquery())

        running_sids = (dbhandle.query(SubmissionHistory.submission_id)
                        .join(sq,SubmissionHistory.submission_id == sq.c.submission_id)
                        .filter(SubmissionHistory.status.in_(status_list),
                                SubmissionHistory.created == sq.c.latest)
                        .all())

        ccl = (dbhandle.query(CampaignStage.campaign_id, func.count(Submission.submission_id))
            .join(Submission, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
            .filter(CampaignStage.campaign_id.in_(cl), Submission.submission_id.in_(running_sids)).group_by(CampaignStage.campaign_id).all())

        # the query never returns a 0 count, so initialize result with
        # a zero count for everyone, then update with the nonzero counts
        # from the query
        res = {}
        for c in cl:
            res[c] = 0

        for row in ccl:
            res[row[0]] = row[1]           

        return res

    def force_locate_submission(self, dbhandle, submission_id):
        # this doesn't actually mark it located, rather it bumps
        # the timestamp backwards so it will look timed out...

        s = dbhandle.query(Submission).filter(Submission.submission_id == submission_id).first()
        s.updated = s.updated - timedelta(days=2)
        dbhandle.add(s)
        dbhandle.commit()
        return "Ok."
       
        
    def update_submission(self, dbhandle, submission_id, jobsub_job_id, pct_complete = None, status = None, project = None):
        s = dbhandle.query(Submission).filter(Submission.submission_id == submission_id).first()
        if not s:
            return "Unknown."

        if jobsub_job_id and s.jobsub_job_id != jobsub_job_id:
            s.jobsub_job_id = jobsub_job_id
            dbhandle.add(s)

        if project and s.project != project:
            s.project = project
            dbhandle.add(s)

        # amend status for completion percent
        if status == 'Running' and pct_complete and float(pct_complete) >= s.campaign_stage_snapshot_obj.completion_pct and s.campaign_stage_snapshot_obj.completion_type == 'complete':
            status = 'Completed'

        if status != None:
            self.update_submission_status(dbhandle, submission_id,status = status)

        dbhandle.commit()
        return "Ok." 

    def snapshot_parts(self, dbhandle, s, campaign_stage_id): 

        cs = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
        for table, snaptable, field, sfield, sid, tfield in [
                [CampaignStage, CampaignStageSnapshot,
                 CampaignStage.campaign_stage_id, CampaignStageSnapshot.campaign_stage_id,
                 cs.campaign_stage_id, 'campaign_stage_snapshot_obj'
                ],
                [JobType, JobTypeSnapshot,
                 JobType.job_type_id,
                 JobTypeSnapshot.job_type_id,
                 cs.job_type_id, 'job_type_snapshot_obj'
                ],
                [LoginSetup, LoginSetupSnapshot,
                 LoginSetup.login_setup_id,
                 LoginSetupSnapshot.login_setup_id,
                 cs.login_setup_id, 'login_setup_snap_obj'
                ]]:

            i = dbhandle.query(func.max(snaptable.updated)).filter(sfield == sid).first()
            j = dbhandle.query(table).filter(field == sid).first()
            if i[0] is None or j is None or j.updated is None or i[0] < j.updated:
                newsnap = snaptable()
                columns = j._sa_instance_state.class_.__table__.columns
                for fieldname in list(columns.keys()):
                    setattr(newsnap, fieldname, getattr(j, fieldname))
                dbhandle.add(newsnap)
            else:
                newsnap = dbhandle.query(snaptable).filter(snaptable.updated == i[0]).first()
            setattr(s, tfield, newsnap)
        dbhandle.add(s) 
        dbhandle.commit()


    def launch_dependents_if_needed(self, dbhandle, samhandle, getconfig, gethead, seshandle, err_res,  s):
        logit.log("Entering launch_dependents_if_needed(%s)" % s.submission_id)

        # if this is a recovery job, we go back to our parent
        # to do all the work
        if s.parent_obj:
            s = s.parent_obj

        if not getconfig("poms.launch_recovery_jobs", False):
            # XXX should queue for later?!?
            logit.log("recovery launches disabled")
            return 1
        cdlist = dbhandle.query(CampaignDependency).filter(CampaignDependency.needs_campaign_stage_id == s.campaign_stage_snapshot_obj.campaign_stage_id).order_by(CampaignDependency.provides_campaign_stage_id).all()

        launch_user = dbhandle.query(Experimenter).filter(Experimenter.experimenter_id == s.creator).first()
        i = 0
        for cd in cdlist:
            if cd.provides_campaign_stage_id == s.campaign_stage_snapshot_obj.campaign_stage_id:
                # self-reference, just do a normal launch
                self.launch_jobs(dbhandle, getconfig, gethead, seshandle.get, samhandle, err_res, cd.provides_campaign_stage_id, launch_user.username, test_launch = s.submission_params.get('test',False))
            else:
                i = i + 1
                if cd.file_patterns.find(' ') > 0:
                    # it is a dimension fragment, not just a file pattern
                    dim_bits = cd.file_patterns
                else:
                    dim_bits = "file_name like '%s'" % cd.file_patterns
                dims = "ischildof: (snapshot_for_project_name %s) and version %s and %s " % (s.project, s.campaign_stage_snapshot_obj.software_version, dim_bits)

                dname = "poms_depends_%d_%d" % (s.submission_id, i)

                samhandle.create_definition(s.campaign_stage_snapshot_obj.experiment, dname, dims)
                if s.submission_params and s.submission_params.get('test',False):
                    test_launch = s.submission_params.get('test',False)
                else:
                    test_launch = False

                logit.log("About to launch jobs, test_launch = %s" % test_launch)

                self.launch_jobs(dbhandle, getconfig, gethead, seshandle.get, samhandle, err_res, cd.provides_campaign_stage_id, s.creator, dataset_override = dname, test_launch = test_launch )
        return 1

    def launch_recovery_if_needed(self, dbhandle, samhandle, getconfig, gethead, seshandle, err_res,  s):
        logit.log("Entering launch_recovery_if_needed(%s)" % s.submission_id)
        if not getconfig("poms.launch_recovery_jobs", False):
            logit.log("recovery launches disabled")
            # XXX should queue for later?!?
            return 1

        # if this is itself a recovery job, we go back to our parent
        # to do all the work, because it has the counters, etc.
        if s.parent_obj:
            s = s.parent_obj

        rlist = self.poms_service.campaignsPOMS.get_recovery_list_for_campaign_def(dbhandle, s.job_type_snapshot_obj)

        logit.log("recovery list %s" % rlist)
        if s.recovery_position is None:
            s.recovery_position = 0

        while s.recovery_position is not None and s.recovery_position < len(rlist):
            logit.log("recovery position %d" % s.recovery_position)

            rtype = rlist[s.recovery_position].recovery_type
            # uncomment when we get db fields:
            param_overrides = rlist[s.recovery_position].param_overrides
            if rtype.name == 'consumed_status':
                recovery_dims = samhandle.recovery_dimensions(s.job_type_snapshot_obj.experiment, s.project, useprocess=0, dbhandle = dbhandle)
            elif rtype.name == 'proj_status':
                recovery_dims = samhandle.recovery_dimensions(s.job_type_snapshot_obj.experiment, s.project, useprocess=1, dbhandle = dbhandle)
            elif rtype.name == 'pending_files':
                recovery_dims = "snapshot_for_project_name %s minus ( " % s.project
                if s.job_type_snapshot_obj.output_file_patterns:
                    oftypelist = s.job_type_snapshot_obj.output_file_patterns.split(",")
                else:
                    oftypelist = ["%"]

                sep = ''
                for oft in oftypelist:
                    if oft.find(' ') > 0:
                        # it is a dimension not a file_name pattern
                        dim_bits = oft
                    else:
                        dim_bits = "file_name like %s" % oft
                    recovery_dims += "%s isparentof: ( version %s and %s) " % (sep,s.campaign_stage_snapshot_obj.software_version, dim_bits)
                    sep = 'and'
                snapshot_dims += ')'
            else:
                # default to consumed status(?)
                recovery_dims = "project_name %s and consumed_status != 'consumed'" % s.project

            try:
                logit.log("counting files dims %s" % recovery_dims)
                nfiles = samhandle.count_files(s.campaign_stage_snapshot_obj.experiment, recovery_dims, dbhandle=dbhandle)
            except:
                # if we can's count it, just assume there may be a few for now...
                nfiles = 1

            s.recovery_position = s.recovery_position + 1
            dbhandle.add(s)
            dbhandle.commit()

            logit.log("recovery files count %d" % nfiles)
            if nfiles > 0:
                rname = "poms_recover_%d_%d" % (s.submission_id, s. recovery_position)

                logit.log("launch_recovery_if_needed: creating dataset for exp=%s name=%s dims=%s" % (s.campaign_stage_snapshot_obj.experiment, rname, recovery_dims))

                samhandle.create_definition(s.campaign_stage_snapshot_obj.experiment, rname, recovery_dims)


                launch_user = dbhandle.query(Experimenter).filter(Experimenter.experimenter_id == s.creator).first()

                self.launch_jobs(dbhandle, getconfig, gethead, seshandle.get, samhandle,
                                 err_res, s.campaign_stage_snapshot_obj.campaign_stage_id, launch_user.username,  dataset_override=rname,
                                 parent_submission_id=s.submission_id, param_overrides=param_overrides, test_launch = s.submission_params.get('test_launch',False))
                return 1

        return 0

    def set_job_launches(self, dbhandle, hold):
        if hold not in ["hold", "allowed"]:
            return
        #XXX where do we keep held jobs now?
        return


    def get_job_launches(self, dbhandle):
        #XXX where do we keep held jobs now?
        return "allowed"

    def launch_queued_job(self, dbhandle, samhandle, getconfig, gethead, seshandle_get, err_res):
        if self.get_job_launches(dbhandle) == "hold":
            return "Held."

        hl = dbhandle.query(HeldLaunch).with_for_update(read=True).order_by(HeldLaunch.created).first()
        launch_user = dbhandle.query(Experimenter).filter(Experimenter.experimenter_id == hl.launcher).first()
        if hl:
            dbhandle.delete(hl)
            dbhandle.commit()
            self.launch_jobs(dbhandle,
                             getconfig, gethead,
                             seshandle_get, samhandle,
                             err_res, hl.campaign_stage_id,
                             launch_user.username,
                             dataset_override=hl.dataset,
                             parent_submission_id=hl.parent_submission_id,
                             param_overrides=hl.param_overrides)
            return "Launched."
        else:
            return "None."

    def launch_jobs(self, dbhandle, getconfig, gethead, seshandle_get, samhandle,
                    err_res, campaign_stage_id, launcher, dataset_override=None, parent_submission_id=None,
                    param_overrides=None, test_login_setup=None, experiment=None, test_launch = False):

        logit.log("Entering launch_jobs(%s, %s, %s)" % (campaign_stage_id, dataset_override, parent_submission_id))

        ds = time.strftime("%Y%m%d_%H%M%S")
        e = seshandle_get('experimenter')
        launcher_experimenter = dbhandle.query(Experimenter).filter(Experimenter.experimenter_id == launcher).first()

        if test_login_setup:
            lt = dbhandle.query(LoginSetup).filter(LoginSetup.login_setup_id == test_login_setup).first()
            dataset_override = "fake_test_dataset"
            cdid = "-"
            cid = "-"
            cname = "-"
            sid = "-"
            cs = None
            c_param_overrides = []
            vers = 'v0_0'
            dataset = "-"
            definition_parameters = []
            exp = e.session_experiment
            launch_script = """echo "Environment"; printenv; echo "jobsub is`which jobsub`;  echo "login_setup successful!"""
            outdir = "%s/private/logs/poms/launches/template_tests_%d" % (os.environ["HOME"], int(test_login_setup))
            outfile = "%s/%s_%s" % (outdir, ds, launcher_experimenter.username)
            logit.log("trying to record launch in %s" % outfile)
        else:
            outdir = "%s/private/logs/poms/launches/campaign_%s" % (os.environ["HOME"], campaign_stage_id)
            outfile = "%s/%s_%s" % (outdir, ds, launcher_experimenter.username)
            logit.log("trying to record launch in %s" % outfile)

            if str(campaign_stage_id)[0] in "0123456789":
                cq = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id)
            else:
                cq = dbhandle.query(CampaignStage).filter(CampaignStage.name == campaign_stage_id, CampaignStage.experiment == experiment)

            cs = cq.options(joinedload(CampaignStage.login_setup_obj), joinedload(CampaignStage.job_type_obj)).first()

            if not cs:
                raise err_res("CampaignStage id %s not found" % campaign_stage_id, status=404)

            cd = cs.job_type_obj
            lt = cs.login_setup_obj

            if self.get_job_launches(dbhandle) == "hold" or cs.hold_experimenter_id:
                # fix me!!
                output = "Job launches currently held.... queuing this request"
                logit.log("launch_jobs -- holding launch")
                hl = HeldLaunch()
                hl.campaign_stage_id = campaign_stage_id
                hl.created = datetime.now(utc)
                hl.dataset = dataset_override
                hl.parent_submission_id = parent_submission_id
                hl.param_overrides = param_overrides
                hl.launcher = launcher
                dbhandle.add(hl)
                dbhandle.commit()
                lcmd = ""

                return lcmd, cs, campaign_stage_id, outdir, outfile


            # allocate task to set ownership
            sid = self.get_task_id_for(dbhandle, campaign_stage_id, user=launcher_experimenter.username, experiment=experiment, parent_submission_id=parent_submission_id)

            if test_launch:
                dbhandle.query(Submission).filter(Submission.submission_id == sid).update({Submission.submission_params: {'test':1}});
                dbhandle.commit()

            xff = gethead('X-Forwarded-For', None)
            ra = gethead('Remote-Addr', None)
            exp = cs.experiment
            vers = cs.software_version
            launch_script = cd.launch_script
            cid = cs.campaign_stage_id

            # isssue #20990
            if cs.name == cs.campaign_obj.name:
                cname = cs.name
            elif cs.name[:len(cs.campaign_obj.name)] == cs.campaign_obj.name:
                cname = "%s::%s" % (cs.campaign_obj.name , cs.name[len(cs.campaign_obj_name):])
            else:
                cname = "%s::%s" % (cs.campaign_obj.name, cs.name)

            cdid = cs.job_type_id
            definition_parameters = cd.definition_parameters

            c_param_overrides = cs.param_overrides

            # if it is a test launch, add in the test param overrides
            # and flag the task as a test (secretly relies on poms_client
            # v3_0_0)

        if not e and not (ra == '127.0.0.1' and xff is None):
            logit.log("launch_jobs -- experimenter not authorized")
            raise err_res("Permission denied.", status=404)

        if not lt.launch_host.find(exp) >= 0 and exp != 'samdev':
            logit.log("launch_jobs -- {} is not a {} experiment node ".format(lt.launch_host, exp))
            output = "Not Authorized: {} is not a {} experiment node".format(lt.launch_host, exp)
            raise err_res(output, status=404)

        experimenter_login = e.username

        if dataset_override:
            dataset = dataset_override
        else:
            dataset = self.poms_service.campaignsPOMS.get_dataset_for(dbhandle, samhandle, err_res, cs)

        group = exp
        if group == 'samdev':
            group = 'fermilab'

        if "poms" in self.poms_service.hostname:
            poms_test=""
        elif "fermicloudmwm" in  self.poms_service.hostname:
            poms_test="int"
        else:
            poms_test="1"

        cmdl = [
            "exec 2>&1",
            "set -x",
            "export KRB5CCNAME=/tmp/krb5cc_poms_submit_%s" % group,
            "kinit -kt $HOME/private/keytabs/poms.keytab `klist -kt $HOME/private/keytabs/poms.keytab | tail -1 | sed -e 's/.* //'`|| true",
            ("ssh -tx %s@%s <<'EOF' &" % (lt.launch_account,lt.launch_host))%{
                "dataset": dataset,
                "experiment": exp,
                "version": vers,
                "group": group,
                "experimenter": experimenter_login,
            },
            #
            # This bit is a little tricky.  We want to do as little of our
            # own setup as possible after the users' launch_setup text,
            # so we don's undo stuff they may put on the front of the path
            # etc -- *except* that we need jobsub_wrapper setup.
            # so we
            # 1. do our setup except jobsub_wrapper
            # 2. do their setup stuff from the launch template
            # 3. setup *just* poms_jobsub_wrapper, so it gets on the
            #    front of the path and can intercept calls to "jobsub_submit"
            #
            "source /grid/fermiapp/products/common/etc/setups",
            "setup poms_jobsub_wrapper -g poms31 -z /grid/fermiapp/products/common/db",
            lt.launch_setup % {
                "dataset": dataset,
                "experiment": exp,
                "version": vers,
                "group": group,
                "experimenter": experimenter_login,
            },
            "UPS_OVERRIDE="" setup -j poms_jobsub_wrapper -g poms31 -z /grid/fermiapp/products/common/db, -j poms_client -g poms31 -z /grid/fermiapp/products/common/db",
            "ups active",
            "export POMS_CAMPAIGN_ID=%s" % cid,
            "export POMS_CAMPAIGN_NAME='%s'" % cname,
            "export POMS_PARENT_TASK_ID=%s" % (parent_submission_id if parent_submission_id else ""),
            "export POMS_TASK_ID=%s" % sid,
            "export POMS_LAUNCHER=%s" % launcher_experimenter.username,
            "export POMS_TEST=%s" % poms_test,
            "export POMS_TASK_DEFINITION_ID=%s" % cdid,
            "export JOBSUB_GROUP=%s" % group,
        ]
        if definition_parameters:
            if isinstance(definition_parameters, str):
                params = OrderedDict(json.loads(definition_parameters))
            else:
                params = OrderedDict(definition_parameters)
        else:
            params = OrderedDict([])

        if c_param_overrides is not None and c_param_overrides != "":
            if isinstance(c_param_overrides, str):
                params.update(json.loads(c_param_overrides))
            else:
                params.update(c_param_overrides)

        if test_launch and cs.test_param_overrides is not None:
           params.update(cs.test_param_overrides);

        if param_overrides is not None and param_overrides != "":
            if isinstance(param_overrides, str):
                params.update(json.loads(param_overrides))
            else:
                params.update(param_overrides)

        lcmd = launch_script + " " + ' '.join((x[0] + x[1]) for x in list(params.items()))
        lcmd = lcmd % {
            "dataset": dataset,
            "version": vers,
            "group": group,
            "experimenter": experimenter_login,
            "experiment": exp,
        }
        cmdl.append(lcmd)
        cmdl.append('exit')
        cmdl.append('EOF')
        cmd = '\n'.join(cmdl)

        cmd = cmd.replace('\r', '')

        if not os.path.isdir(outdir):
            os.makedirs(outdir)
        dn = open("/dev/null", "r")
        lf = open(outfile, "w")
        logit.log("actually starting launch ssh")
        pp = subprocess.Popen(cmd, shell=True, stdin=dn, stdout=lf, stderr=lf, close_fds=True)
        lf.close()
        dn.close()
        pp.wait()


        return lcmd, cs, campaign_stage_id, outdir, outfile
