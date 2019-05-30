#!/usr/bin/env python
"""
This module contain the methods that handle the Submission.
List of methods: wrapup_tasks,
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py
written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
"""

import glob
import json
import os
import re
import select
import subprocess
import time
from collections import OrderedDict, deque
from datetime import datetime, timedelta

from sqlalchemy import func, and_
from sqlalchemy.orm import joinedload
from .mail import Mail

from . import logit
from .poms_model import (
    CampaignStage,
    JobType,
    JobTypeSnapshot,
    CampaignDependency,
    CampaignRecovery,
    CampaignStageSnapshot,
    Experimenter,
    HeldLaunch,
    LoginSetup,
    LoginSetupSnapshot,
    RecoveryType,
    Submission,
    SubmissionHistory,
    SubmissionStatus,
)
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
    while totaltime > 0 and block:
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
    output = "".join(outlist)
    return output


class TaskPOMS:
    def __init__(self, ps):
        self.poms_service = ps
        # keep some status vales around so we don't have to look them up...
        self.init_status_done = False
        self.status_Located = None
        self.status_Completed = None
        self.status_New = None

    def init_statuses(self, ctx):
        if self.init_status_done:
            return
        self.status_Located = ctx.db.query(SubmissionStatus.status_id).filter(SubmissionStatus.status == "Located").first()[0]
        self.status_Completed = ctx.db.query(SubmissionStatus.status_id).filter(SubmissionStatus.status == "Completed").first()[0]
        self.status_Removed = ctx.db.query(SubmissionStatus.status_id).filter(SubmissionStatus.status == "Removed").first()[0]
        self.status_New = ctx.db.query(SubmissionStatus.status_id).filter(SubmissionStatus.status == "New").first()[0]
        self.init_status_done = True

    def campaign_stage_datasets(self, ctx):
        self.init_statuses(ctx)
        running = (
            ctx.db.query(SubmissionHistory.submission_id, func.max(SubmissionHistory.status_id))
            .filter(SubmissionHistory.created > datetime.now(utc) - timedelta(days=4))
            .group_by(SubmissionHistory.submission_id)
            .having(func.max(SubmissionHistory.status_id) < self.status_Completed)
            .all()
        )

        running_submission_ids = [x[0] for x in running]

        plist = (
            ctx.db.query(Submission.project, Submission.campaign_stage_id, CampaignStage.experiment)
            .filter(CampaignStage.campaign_stage_id == Submission.campaign_stage_id)
            .filter(Submission.submission_id.in_(running_submission_ids))
            .order_by(Submission.campaign_stage_id)
            .all()
        )

        old_cs_id = None
        this_plist = []
        res = {}
        for project, cs_id, exp in plist:
            if cs_id != old_cs_id:
                if this_plist:
                    res[old_cs_id] = [exp, this_plist]
                    # make one for old_cs_id which is this_plist
                    this_plist = []
            if project:
                this_plist.append(project)
            old_cs_id = cs_id

        if this_plist:
            res[old_cs_id] = [exp, this_plist]

        return res

    def wrapup_tasks(self, ctx):
        # this function call another function that is not in this module, it
        # use a poms_service object passed as an argument at the init.

        self.init_statuses(ctx)
        now = datetime.now(utc)
        res = ["wrapping up:"]

        lookup_submission_list = deque()
        lookup_dims_list = deque()
        lookup_exp_list = deque()
        finish_up_submissions = deque()
        mark_located = deque()
        #
        # move launch stuff etc, to one place, so we can keep the table rows

        cpairs = (
            ctx.db.query(SubmissionHistory.submission_id, func.max(SubmissionHistory.status_id).label("maxstat"))
            .filter(SubmissionHistory.created > datetime.now(utc) - timedelta(days=4))
            .group_by(SubmissionHistory.submission_id)
            .having(func.max(SubmissionHistory.status_id) == self.status_Completed)
            .all()
        )

        completed_sids = [x[0] for x in cpairs]

        logit.log("wrapup_tasks: completed sids 1: %s" % repr(completed_sids))

        # lock them all before updating...
        ll = ctx.db.query(Submission).filter(Submission.submission_id.in_(completed_sids)).with_for_update(read=True).all()
        # now find the ones that are still completed now that we have the lock
        cpairs = (
            ctx.db.query(SubmissionHistory.submission_id, func.max(SubmissionHistory.status_id).label("maxstat"))
            .filter(SubmissionHistory.submission_id.in_(completed_sids))
            .filter(SubmissionHistory.created > datetime.now(utc) - timedelta(days=4))
            .group_by(SubmissionHistory.submission_id)
            .having(func.max(SubmissionHistory.status_id) == self.status_Completed)
            .all()
        )

        completed_sids = [x[0] for x in cpairs]
        logit.log("wrapup_tasks: completed sids 2: %s" % repr(completed_sids))

        res.append("Completed submissions_ids: %s" % repr(list(completed_sids)))

        for s in (
            ctx.db.query(Submission)
            .join(
                CampaignStageSnapshot, Submission.campaign_stage_snapshot_id == CampaignStageSnapshot.campaign_stage_snapshot_id
            )
            .filter(Submission.submission_id.in_(completed_sids), CampaignStageSnapshot.completion_type == "complete")
            .all()
        ):
            res.append("completion type completed: %s" % s.submission_id)
            finish_up_submissions.append(s.submission_id)

        n_project = 0
        for s in (
            ctx.db.query(Submission)
            .join(
                CampaignStageSnapshot, Submission.campaign_stage_snapshot_id == CampaignStageSnapshot.campaign_stage_snapshot_id
            )
            .filter(Submission.submission_id.in_(completed_sids), CampaignStageSnapshot.completion_type == "located")
            .all()
        ):

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

                for dcd in (
                    ctx.db.query(CampaignDependency)
                    .filter(CampaignDependency.needs_campaign_stage_id == s.campaign_stage_snapshot_obj.campaign_stage_id)
                    .all()
                ):
                    if dcd.file_patterns:
                        plist.extend(dcd.file_patterns.split(","))
                    else:
                        plist.append("%")

                if not plist:
                    plist = str(s.job_type_snapshot_obj.output_file_patterns).split(",")

                logit.log("got file pattern list: %s" % repr(plist))

                for pat in plist:
                    if pat == "None":
                        pat = "%"

                    if pat.find(" ") > 0:
                        allkiddims = (
                            "%s and isparentof: ( %s and version '%s' and create_date > '%s'  with availability physical ) "
                            % (
                                allkiddims,
                                pat,
                                s.campaign_stage_snapshot_obj.software_version,
                                s.created.strftime("%Y-%m-%dT%H:%M:%S%z"),
                            )
                        )
                    else:
                        allkiddims = (
                            "%s and isparentof: ( file_name '%s' and version '%s' and create_date > '%s' with availability physical ) "
                            % (
                                allkiddims,
                                pat,
                                s.campaign_stage_snapshot_obj.software_version,
                                s.created.strftime("%Y-%m-%dT%H:%M:%S%z"),
                            )
                        )

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

        ctx.db.commit()

        summary_list = ctx.sam.fetch_info_list(lookup_submission_list, dbhandle=ctx.db)
        count_list = ctx.sam.count_files_list(lookup_exp_list, lookup_dims_list)
        thresholds = deque()
        logit.log("wrapup_tasks: summary_list: %s" % repr(summary_list))  # Check if that is working
        res.append("wrapup_tasks: summary_list: %s" % repr(summary_list))

        res.append("count_list: %s" % count_list)
        res.append("thresholds: %s" % thresholds)
        res.append("lookup_dims_list: %s" % lookup_dims_list)

        for i in range(len(summary_list)):
            submission = lookup_submission_list[i]
            cfrac = submission.campaign_stage_snapshot_obj.completion_pct / 100.0
            if submission.project:
                threshold = summary_list[i].get("tot_consumed", 0) * cfrac
            else:
                # no project, so guess based on number of jobs in submit
                # command?
                p1 = submission.command_executed.find("-N")
                p2 = submission.command_executed.find(" ", p1 + 3)
                try:
                    threshold = int(submission.command_executed[p1 + 3 : p2]) * cfrac
                except BaseException:
                    threshold = 0

            thresholds.append(threshold)
            val = float(count_list[i])
            res.append("submission %s val %f threshold %f " % (submission, val, threshold))
            if val >= threshold and threshold > 0:
                res.append("adding submission %s " % submission)
                finish_up_submissions.append(submission.submission_id)

        for s in finish_up_submissions:
            res.append("marking submission %s located " % s)
            self.update_submission_status(ctx, s, "Located")

        ctx.db.commit()

        #
        # now, after committing to clear locks, we run through the
        # job logs for the submissions and update process stats, and
        # launch any recovery jobs or jobs depending on us.
        # this way we don's keep the rows locked all day
        #
        # logit.log("Starting need_joblogs loops, len %d" % len(finish_up_submissions))
        # if len(need_joblogs) == 0:
        #    njtl = []
        # else:
        #    njtl = ctx.db.query(Submission).filter(Submission.submission_id.in_(need_joblogs)).all()

        res.append("finish_up_submissions: %s " % repr(finish_up_submissions))

        if len(finish_up_submissions) == 0:
            futl = []
        else:
            futl = ctx.db.query(Submission).filter(Submission.submission_id.in_(finish_up_submissions)).all()

        res.append(" got list... ")

        for submission in futl:
            # get logs for job for final cpu values, etc.
            logit.log("Starting finish_up_submissions items for submission %s" % submission.submission_id)
            res.append("Starting finish_up_submissions items for submission %s" % submission.submission_id)

            # override session experimenter to be the owner of
            # the current task in the role of the submission
            # so launch actions get done as them.

            if not self.launch_recovery_if_needed(ctx, submission, None):
                self.launch_dependents_if_needed(ctx, submission)

        return res

    ###

    def get_task_id_for(
        self,
        ctx,
        campaign,
        command_executed="",
        input_dataset="",
        parent_submission_id=None,
        submission_id=None,
        launch_time=None,
    ):
        logit.log(
            "get_task_id_for(ctx.username='%s',experiment='%s',command_executed='%s',input_dataset='%s',parent_submission_id='%s',submission_id='%s'"
            % (ctx.username, ctx.experiment, command_executed, input_dataset, parent_submission_id, submission_id)
        )

        #
        # try to extract the project name from the launch command...
        #
        project = None
        for projre in (
            "-e SAM_PROJECT=([^ ]*)",
            "-e SAM_PROJECT_NAME=([^ ]*)",
            "--sam_project ([^ ]*)",
            "--project_name=([^ ]*)",
        ):
            m = re.search(projre, command_executed)
            if m:
                project = m.group(1)
                break

        q = ctx.db.query(CampaignStage)
        if str(campaign)[0] in "0123456789":
            q = q.filter(CampaignStage.campaign_stage_id == int(campaign))
        else:
            q = q.filter(CampaignStage.name.like("%%%s%%" % campaign))

        if ctx.experiment:
            q = q.filter(CampaignStage.experiment == ctx.experiment)

        cs = q.first()
        if launch_time:
            tim = launch_time
        else:
            tim = datetime.now(utc)

        if submission_id:

            s = ctx.db.query(Submission).filter(Submission.submission_id == submission_id).one()
            s.command_executed = command_executed
            if ctx.username != "poms":
                s.creator = ctx.get_experimenter().experimenter_id

            s.updated = tim
        else:
            s = Submission(
                campaign_stage_id=cs.campaign_stage_id,
                submission_params={},
                project=project,
                updater=ctx.get_experimenter().experimenter_id,
                creator=ctx.get_experimenter().experimenter_id,
                created=tim,
                updated=tim,
                command_executed=command_executed,
            )

        if parent_submission_id is not None and parent_submission_id != "None":
            s.recovery_tasks_parent = int(parent_submission_id)

        self.snapshot_parts(ctx, s, s.campaign_stage_id)

        ctx.db.add(s)
        ctx.db.flush()

        self.init_statuses(ctx)
        if not submission_id:
            sh = SubmissionHistory(submission_id=s.submission_id, status_id=self.status_New, created=tim)
            ctx.db.add(sh)
        logit.log("get_task_id_for: returning %s" % s.submission_id)
        ctx.db.commit()
        return s.submission_id

    def update_submission_status(self, ctx, submission_id, status, when=None):
        self.init_statuses(ctx)

        if when == None:
            when = datetime.now(utc)

        # always lock the submission first to prevent races

        s = ctx.db.query(Submission).filter(Submission.submission_id == submission_id).with_for_update(read=True).first()

        status_id = (ctx.db.query(SubmissionStatus.status_id).filter(SubmissionStatus.status == status).first())[0]

        if not status_id:
            # not a known status, just bail
            return

        # get our latest history...
        sq = (
            ctx.db.query(func.max(SubmissionHistory.created).label("latest"))
            .filter(SubmissionHistory.submission_id == submission_id)
            .subquery()
        )

        lasthist = (
            ctx.db.query(SubmissionHistory)
            .filter(SubmissionHistory.created == sq.c.latest)
            .filter(SubmissionHistory.submission_id == submission_id)
            .first()
        )

        logit.log(
            "update_submission_status: submission_id: %s  newstatus %s  lasthist: status %s created %s "
            % (submission_id, status_id, lasthist.status_id, lasthist.created)
        )

        # don't roll back Located or Removed (final states)
        # note that we *don't* have LaunchFailed here, as we *could*
        # have a launch that took a Really Long Time, and we might have
        # falsely concluded that the launch failed...
        if lasthist and lasthist.status_id in (self.status_Located, self.status_Removed):
            return

        # don't roll back Completed
        if lasthist and lasthist.status_id == self.status_Completed and status_id <= self.status_Completed:
            return

        # don't put in duplicates
        if lasthist and lasthist.status_id == status_id:
            return

        sh = SubmissionHistory()
        sh.submission_id = submission_id
        sh.status_id = status_id
        sh.created = when
        ctx.db.add(sh)

        if status_id >= self.status_Completed:
            s.updated = sh.created
            ctx.db.add(s)

    def mark_failed_submissions(self, ctx):
        """
            find all the recent submissions that are still "New" but more
            than two hours old, and mark them "LaunchFailed"
        """
        self.init_statuses(ctx)
        now = datetime.now(utc)

        newtups = (
            ctx.db.query(
                SubmissionHistory.submission_id,
                func.max(SubmissionHistory.status_id).label("maxstat"),
                func.min(SubmissionHistory.created).label("firsttime"),
            )
            .filter(SubmissionHistory.created > datetime.now(utc) - timedelta(days=7))
            .group_by(SubmissionHistory.submission_id)
            .having(
                and_(
                    func.max(SubmissionHistory.status_id) == self.status_New,
                    func.min(SubmissionHistory.created) < datetime.now(utc) - timedelta(hours=4),
                )
            )
            .all()
        )

        failed_sids = [x[0] for x in newtups]

        res = []
        for submission in ctx.db.query(Submission).filter(Submission.submission_id.in_(failed_sids)).all():
            # sometimes jobs complete quickly, and we do not see them go by..
            # so if we got a jobsub_job_id, check for a job log to find
            # out what happened
            if submission.jobsub_job_id:
                job_data = get_joblogs(ctx.db, jobsub_data[0], cert, key, experiment, role)
                if job_data:
                    res.append("found job log for %s!" % submission_id)
                    if job_data.get("idle", None) and job_data["idle"].get(jobsub_job_id, None):
                        self.update_submission_status(
                            ctx.db, submission_id, status="Idle", when=job_data["idle"][submission.jobsub_job_id]
                        )
                        res.append("submission %s Idle at" % (submission_id, job_data["idle"][submission.jobsub_job_id]))

                    if job_data.get("running", None) and job_data["running"].get(submission.jobsub_job_id, None):
                        self.update_submission_status(
                            ctx.db, submission_id, status="Running", when=job_data["running"][submission.jobsub_job_id]
                        )
                        res.append("submission %s Running at" % (submission_id, job_data["idle"][submission.jobsub_job_id]))

                    if len(job_data["completed"]) == len(job_data["idle"]):
                        self.update_submission_status(ctx.db, submission_id, status="Completed")
                        res.append("submission %s Completed")

                    continue
            res.append("failing launch for %s" % submission_id)
            self.update_submission_status(ctx.db, submission_id, status="LaunchFailed")
        ctx.db.commit()
        return "\n".join(res)

    def submission_details(self, ctx, submission_id):
        submission = (
            ctx.db.query(Submission)
            .options(joinedload(Submission.campaign_stage_snapshot_obj))
            .options(joinedload(Submission.login_setup_snapshot_obj))
            .options(joinedload(Submission.job_type_snapshot_obj))
            .filter(Submission.submission_id == submission_id)
            .first()
        )
        history = (
            ctx.db.query(SubmissionHistory)
            .filter(SubmissionHistory.submission_id == submission_id)
            .order_by(SubmissionHistory.created)
            .all()
        )

        rtypes = ctx.db.query(RecoveryType).all()
        sstatuses = ctx.db.query(SubmissionStatus).all()
        rmap = {}
        smap = {}
        for rt in rtypes:
            rmap[rt.name] = (rt.recovery_type_id, rt.description)
        for sst in sstatuses:
            smap[sst.status_id] = sst.status
        #
        # newer submissions should have dataset recorded in submission_params.
        # but for older ones, we can often look it up...
        #
        if submission and submission.submission_params and submission.submission_params.get("dataset"):
            dataset = submission.submission_params.get("dataset")
        elif submission and submission.command_executed.find("--dataset") > 0:
            pos = submission.command_executed.find("--dataset")
            dataset = submission.command_executed[pos + 10 :]
            pos = dataset.find(" ")
            dataset = dataset[:pos]
        elif submission and submission.project:
            details = ctx.sam.fetch_info(submission.campaign_stage_snapshot_obj.experiment, submission.project, ctx.db)
            logit.log("got details = %s" % repr(details))
            dataset = details.get("dataset_def_name", None)
        else:
            dataset = None

        ds = (submission.created.astimezone(utc)).strftime("%Y%m%d_%H%M%S")
        ds2 = (submission.created - timedelta(seconds=0.5)).strftime("%Y%m%d_%H%M%S")
        dirname = "{}/private/logs/poms/launches/campaign_{}".format(os.environ["HOME"], submission.campaign_stage_id)

        pattern = "{}/{}*".format(dirname, ds[:-2])
        flist = glob.glob(pattern)
        pattern2 = "{}/{}*".format(dirname, ds2[:-2])
        flist.extend(glob.glob(pattern2))

        logit.log("datestamps: '%s' '%s'" % (ds, ds2))
        logit.log("found list of submission files:(%s -> %s)" % (pattern, repr(flist)))

        submission_log_format = 0
        if "{}/{}_{}_{}".format(dirname, ds, submission.experimenter_creator_obj.username, submission.submission_id) in flist:
            submission_log_format = 3
        if "{}/{}_{}_{}".format(dirname, ds2, submission.experimenter_creator_obj.username, submission.submission_id) in flist:
            ds = ds2
            submission_log_format = 3
        elif "{}/{}_{}".format(dirname, ds, submission.experimenter_creator_obj.username) in flist:
            submission_log_format = 2
        elif "{}/{}_{}".format(dirname, ds2, submission.experimenter_creator_obj.username) in flist:
            ds = ds2
            submission_log_format = 2
        elif "{}/{}".format(dirname, ds) in flist:
            submission_log_format = 1
        elif "{}/{}".format(dirname, ds2) in flist:
            ds = ds2
            submission_log_format = 1

        return submission, history, dataset, rmap, smap, ds, submission_log_format

    def running_submissions(self, ctx, campaign_id_list, status_list=["New", "Idle", "Running"]):

        cl = campaign_id_list

        logit.log("INFO", "running_submissions(%s)" % repr(cl))
        sq = (
            ctx.db.query(SubmissionHistory.submission_id, func.max(SubmissionHistory.created).label("latest"))
            .filter(SubmissionHistory.created > datetime.now(utc) - timedelta(days=4))
            .group_by(SubmissionHistory.submission_id)
            .subquery()
        )

        running_sids = (
            ctx.db.query(SubmissionHistory.submission_id)
            .join(SubmissionStatus, SubmissionStatus.status_id == SubmissionHistory.status_id)
            .join(sq, SubmissionHistory.submission_id == sq.c.submission_id)
            .filter(SubmissionStatus.status.in_(status_list), SubmissionHistory.created == sq.c.latest)
            .all()
        )

        ccl = (
            ctx.db.query(CampaignStage.campaign_id, func.count(Submission.submission_id))
            .join(Submission, Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
            .filter(CampaignStage.campaign_id.in_(cl), Submission.submission_id.in_(running_sids))
            .group_by(CampaignStage.campaign_id)
            .all()
        )

        # the query never returns a 0 count, so initialize result with
        # a zero count for everyone, then update with the nonzero counts
        # from the query
        res = {}
        for c in cl:
            res[c] = 0

        for row in ccl:
            res[row[0]] = row[1]

        return res

    def force_locate_submission(self, ctx, submission_id):
        # this doesn't actually mark it located, rather it bumps
        # the timestamp backwards so it will look timed out...

        e = ctx.db.query(Experimenter).filter(Experimenter.username == ctx.username).first()
        s = ctx.db.query(Submission).filter(Submission.submission_id == submission_id).first()
        cs = s.campaign_stage_obj
        s.updated = s.updated - timedelta(days=2)
        ctx.db.add(s)
        ctx.db.commit()
        return "Ok."

    def update_submission(self, ctx, submission_id, jobsub_job_id, pct_complete=None, status=None, project=None):

        s = ctx.db.query(Submission).filter(Submission.submission_id == submission_id).with_for_update(read=True).first()
        if not s:
            return "Unknown."

        if jobsub_job_id and s.jobsub_job_id != jobsub_job_id:
            s.jobsub_job_id = jobsub_job_id
            ctx.db.add(s)

        if project and s.project != project:
            s.project = project
            ctx.db.add(s)

        # amend status for completion percent
        if (
            status == "Running"
            and pct_complete
            and float(pct_complete) >= s.campaign_stage_snapshot_obj.completion_pct
            and s.campaign_stage_snapshot_obj.completion_type == "complete"
        ):
            status = "Completed"

        if status is not None:
            self.update_submission_status(ctx, submission_id, status=status)

        ctx.db.commit()
        return "Ok."

    def snapshot_parts(self, ctx, s, campaign_stage_id):

        cs = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
        for table, snaptable, field, sfield, sid, tfield in [
            [
                CampaignStage,
                CampaignStageSnapshot,
                CampaignStage.campaign_stage_id,
                CampaignStageSnapshot.campaign_stage_id,
                cs.campaign_stage_id,
                "campaign_stage_snapshot_obj",
            ],
            [JobType, JobTypeSnapshot, JobType.job_type_id, JobTypeSnapshot.job_type_id, cs.job_type_id, "job_type_snapshot_obj"],
            [
                LoginSetup,
                LoginSetupSnapshot,
                LoginSetup.login_setup_id,
                LoginSetupSnapshot.login_setup_id,
                cs.login_setup_id,
                "login_setup_snapshot_obj",
            ],
        ]:

            i = ctx.db.query(func.max(snaptable.updated)).filter(sfield == sid).first()
            j = ctx.db.query(table).filter(field == sid).first()
            if i[0] is None or j is None or j.updated is None or i[0] < j.updated:
                newsnap = snaptable()
                columns = j._sa_instance_state.class_.__table__.columns
                for fieldname in list(columns.keys()):
                    setattr(newsnap, fieldname, getattr(j, fieldname))
                ctx.db.add(newsnap)
            else:
                newsnap = ctx.db.query(snaptable).filter(snaptable.updated == i[0]).first()
            setattr(s, tfield, newsnap)
        ctx.db.add(s)
        ctx.db.commit()

    def launch_dependents_if_needed(self, ctx, s):
        logit.log("Entering launch_dependents_if_needed(%s)" % s.submission_id)

        # if this is itself a recovery job, we go back to our parent
        # because dependants should use the parent, not the recovery job
        if s.parent_obj:
            s = s.parent_obj

        if not ctx.config_get("poms.launch_recovery_jobs", False):
            # XXX should queue for later?!?
            logit.log("recovery launches disabled")
            return 1
        cdlist = (
            ctx.db.query(CampaignDependency)
            .filter(CampaignDependency.needs_campaign_stage_id == s.campaign_stage_snapshot_obj.campaign_stage_id)
            .order_by(CampaignDependency.provides_campaign_stage_id)
            .all()
        )

        launch_user = ctx.db.query(Experimenter).filter(Experimenter.experimenter_id == s.creator).first()
        ctx.username = launch_user.username
        ctx.role = s.campaign_stage_obj.creator_role
        ctx.experiment = s.campaign_stage_obj.experiment

        i = 0
        for cd in cdlist:
            if cd.provides_campaign_stage_id == s.campaign_stage_snapshot_obj.campaign_stage_id:
                # self-reference, just do a normal launch
                # be the role the job we're launching based from was...
                self.launch_jobs(
                    ctx,
                    cd.provides_campaign_stage_id,
                    launch_ctx.username.experimenter_id,
                    test_launch=s.submission_params.get("test", False),
                )
            else:
                i = i + 1
                if cd.file_patterns.find(" ") > 0:
                    # it is a dimension fragment, not just a file pattern
                    dim_bits = cd.file_patterns
                else:
                    dim_bits = "file_name like '%s'" % cd.file_patterns
                cdate = s.created.strftime("%Y-%m-%dT%H:%M:%S%z")
                dims = "ischildof: (snapshot_for_project_name %s) and version %s and create_date > '%s' and %s" % (
                    s.project,
                    s.campaign_stage_snapshot_obj.software_version,
                    cdate,
                    dim_bits,
                )

                dname = "poms_depends_%d_%d" % (s.submission_id, i)

                ctx.sam.create_definition(s.campaign_stage_snapshot_obj.experiment, dname, dims)
                if s.submission_params and s.submission_params.get("test", False):
                    test_launch = s.submission_params.get("test", False)
                else:
                    test_launch = False

                logit.log("About to launch jobs, test_launch = %s" % test_launch)

                self.launch_jobs(ctx, cd.provides_campaign_stage_id, s.creator, dataset_override=dname, test_launch=test_launch)
        return 1

    def launch_recovery_if_needed(self, ctx, s, recovery_type_override=None):
        logit.log("Entering launch_recovery_if_needed(%s)" % s.submission_id)
        if not ctx.config_get("poms.launch_recovery_jobs", False):
            logit.log("recovery launches disabled")
            # XXX should queue for later?!?
            return 1

        # if this is itself a recovery job, we go back to our parent
        # to do all the work, because it has the counters, etc.
        if s.parent_obj:
            s = s.parent_obj

        if recovery_type_override is not None:
            s.recovery_position = 0
            rt = ctx.db.query(RecoveryType).filter(RecoveryType.recovery_type_id == int(recovery_type_override)).all()
            # need to make a temporary CampaignRecovery
            rlist = [
                CampaignRecovery(
                    job_type_id=s.campaign_stage_obj.job_type_id,
                    recovery_order=0,
                    param_overrides=[],
                    recovery_type_id=rt[0].recovery_type_id,
                    recovery_type=rt[0],
                )
            ]
        else:
            rlist = self.poms_service.campaignsPOMS.get_recovery_list_for_campaign_def(ctx, s.job_type_snapshot_obj)

        logit.log("recovery list %s" % rlist)
        if s.recovery_position is None:
            s.recovery_position = 0

        while s.recovery_position is not None and s.recovery_position < len(rlist):
            logit.log("recovery position %d" % s.recovery_position)

            rtype = rlist[s.recovery_position].recovery_type

            # uncomment when we get db fields:
            param_overrides = rlist[s.recovery_position].param_overrides
            if rtype.name == "consumed_status":
                # not using ctx.sam.recovery_dimensions here becuase it
                # doesn't do what I want on ended incomplete projects, etc.
                # so not a good choice for our default option.
                recovery_dims = "project_name %s minus (project_name %s and consumed_status consumed)" % (s.project, s.project)
            elif rtype.name == "proj_status":
                recovery_dims = ctx.sam.recovery_dimensions(
                    s.job_type_snapshot_obj.experiment, s.project, useprocess=1, dbhandle=ctx.db
                )
            elif rtype.name == "pending_files":
                recovery_dims = "snapshot_for_project_name %s minus ( " % s.project
                if s.job_type_snapshot_obj.output_file_patterns:
                    oftypelist = s.job_type_snapshot_obj.output_file_patterns.split(",")
                else:
                    oftypelist = ["%"]

                sep = ""
                cdate = s.created.strftime("%Y-%m-%dT%H:%M:%S%z")
                for oft in oftypelist:
                    if oft.find(" ") > 0:
                        # it is a dimension not a file_name pattern
                        dim_bits = oft
                    else:
                        dim_bits = "file_name like %s" % oft

                    recovery_dims += "%s isparentof: ( version %s and %s and create_date > '%s' ) " % (
                        sep,
                        s.campaign_stage_snapshot_obj.software_version,
                        dim_bits,
                        cdate,
                    )
                    sep = "and"
                recovery_dims += ")"
            else:
                # default to consumed status(?)
                recovery_dims = "project_name %s minus (project_name %s and consumed_status consumed)" % (s.project, s.project)

            try:
                logit.log("counting files dims %s" % recovery_dims)
                nfiles = ctx.sam.count_files(s.campaign_stage_snapshot_obj.experiment, recovery_dims, dbhandle=ctx.db)
            except BaseException:
                # if we can's count it, just assume there may be a few for
                # now...
                nfiles = 1

            s.recovery_position = s.recovery_position + 1
            ctx.db.add(s)
            ctx.db.commit()

            logit.log("recovery files count %d" % nfiles)
            if nfiles > 0:
                rname = "poms_recover_%d_%d" % (s.submission_id, s.recovery_position)

                logit.log(
                    "launch_recovery_if_needed: creating dataset for exp=%s name=%s dims=%s"
                    % (s.campaign_stage_snapshot_obj.experiment, rname, recovery_dims)
                )

                ctx.sam.create_definition(s.campaign_stage_snapshot_obj.experiment, rname, recovery_dims)

                launch_user = ctx.db.query(Experimenter).filter(Experimenter.experimenter_id == s.creator).first()
                ctx.username = launch_user.username
                ctx.role = s.campaign_stage_obj.creator_role
                ctx.experiment = s.campaign_stage_obj.experiment

                # XXX launch_ctx.username.username -- should be id...
                self.launch_jobs(
                    ctx,
                    s.campaign_stage_snapshot_obj.campaign_stage_id,
                    launch_user.exerimenter_id,
                    dataset_override=rname,
                    parent_submission_id=s.submission_id,
                    param_overrides=param_overrides,
                    test_launch=s.submission_params.get("test", False),
                )
                return 1

        return 0

    def set_job_launches(self, ctx, hold):

        experimenter = ctx.get_experimenter()
        if hold not in ["hold", "allowed"]:
            return
        # keep held launches in campaign stage w/ campaign_stage_id == 0
        c = ctx.db.query(CampaignStage).with_for_update().filter(CampaignStage.campaign_stage_id == 0).first()
        if hold == "hold":
            c.hold_experimenter_id = experimenter.experimenter_id
            c.role_held_wtih = role
        else:
            c.hold_experimenter_id = None
            c.role_held_wtih = None
        ctx.db.add(c)
        ctx.db.commit()
        return

    def get_job_launches(self, ctx):
        # keep held launches in campaign stage w/ campaign_stage_id == 0
        c = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == 0).first()
        if not c:
            c = CampaignStage(
                campaign_stage_id=0,
                name="DummyLaunchStatusHOlder",
                experiment="samdev",
                completion_pct="95",
                completion_type="complete",
                cs_split_type="None",
                dataset="from_parent",
                job_type_id=(ctx.db.query(JobType.job_type_id).limit(1).scalar()),
                login_setup_id=(ctx.db.query(LoginSetup.login_setup_id).limit(1).scalar()),
                param_overrides=[],
                software_version="v1_0",
                test_param_overrides=[],
                vo_role="Production",
                creator=4,
                creator_role="production",
                created=datetime.now(utc),
                campaign_stage_type="regular",
            )
            ctx.db.add(c)
            ctx.db.commit()

        return "hold" if c.hold_experimenter_id else "allowed"

    def launch_queued_job(self, ctx):
        if self.get_job_launches(ctx) == "hold":
            return "Held."

        hl = ctx.db.query(HeldLaunch).with_for_update(read=True).order_by(HeldLaunch.created).first()
        if hl:
            launcher = hl.launcher
            campaign_stage_id = hl.campaign_stage_id
            dataset = hl.dataset
            parent_submission_id = hl.parent_submission_id
            param_overrides = hl.param_overrides
            launch_user = ctx.db.query(Experimenter).filter(Experimenter.experimenter_id == hl.launcher).first()
            if not launch_user:
                logit.log("bogus experimenter_id : %s aborting held launch" % hl.launcher)

                ctx.db.delete(hl)
                ctx.db.commit()
                return "Fail: invalid queued ctx.username"
            ctx.db.delete(hl)
            ctx.db.commit()
            cs = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()

            # override session experimenter to be the owner of
            # the current task in the role of the submission
            # so launch actions get done as them.

            ctx.experiment = cs.experiment
            ctx.role = cs.creator_role
            ctx.username = launch_user.username

            self.launch_jobs(
                ctx,
                launcher,
                campaign_stage_id,
                launch_user.experimenter_id,
                dataset_override=dataset,
                parent_submission_id=parent_submission_id,
                param_overrides=param_overrides,
            )
            return "Launched."
        else:
            return "None."

    def has_valid_proxy(self, proxyfile):
        logit.log("Checking proxy: %s" % proxyfile)
        res = os.system("voms-proxy-info -exists -valid 0:10 -file %s" % proxyfile)
        logit.log("system(voms-proxy-info... returns %d" % res)
        return os.WIFEXITED(res) and os.WEXITSTATUS(res) == 0

    def launch_jobs(
        self,
        ctx,
        campaign_stage_id,
        launcher = None,
        dataset_override=None,
        parent_submission_id=None,
        param_overrides=None,
        test_login_setup=None,
        test_launch=False,
        output_commands=False,
    ):

        logit.log("Entering launch_jobs(%s, %s, %s)" % (campaign_stage_id, dataset_override, parent_submission_id))

        if launcher == None:
            launcher = ctx.username

        launch_time = datetime.now(utc)
        ds = launch_time.strftime("%Y%m%d_%H%M%S")
        e = ctx.db.query(Experimenter).filter(Experimenter.username == ctx.username).first()
        role = ctx.role

        # at the moment we're inconsistent about whether we pass
        # launcher as ausername or experimenter_id or if its a string
        # of the integer or  an integer... sigh
        if str(launcher)[0] in ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"):
            launcher_experimenter = ctx.db.query(Experimenter).filter(Experimenter.experimenter_id == int(launcher)).first()
        else:
            launcher_experimenter = ctx.db.query(Experimenter).filter(Experimenter.username == launcher).first()

        if test_login_setup:
            lt = ctx.db.query(LoginSetup).filter(LoginSetup.login_setup_id == test_login_setup).first()
            dataset_override = "fake_test_dataset"
            cdid = "-"
            csid = "-"
            ccname = "-"
            cname = "-"
            csname = "-"
            sid = "-"
            cs = None
            c_param_overrides = []
            vers = "v0_0"
            dataset = "-"
            definition_parameters = []
            exp = experiment
            launch_script = """echo "Environment"; printenv; echo "jobsub is`which jobsub`;  echo "login_setup successful!"""
            outdir = "%s/private/logs/poms/launches/template_tests_%d" % (os.environ["HOME"], int(test_login_setup))
            outfile = "%s/%s_%s" % (outdir, ds, launcher_experimenterusername)
            logit.log("trying to record launch in %s" % outfile)
        else:
            outdir = "%s/private/logs/poms/launches/campaign_%s" % (os.environ["HOME"], campaign_stage_id)
            outfile = "%s/%s_%s" % (outdir, ds, launcher_experimenter.username)
            logit.log("trying to record launch in %s" % outfile)

            if str(campaign_stage_id)[0] in "0123456789":
                cq = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id)
            else:
                cq = ctx.db.query(CampaignStage).filter(
                    CampaignStage.name == campaign_stage_id, CampaignStage.experiment == experiment
                )

            cs = cq.options(
                joinedload(CampaignStage.campaign_obj),
                joinedload(CampaignStage.login_setup_obj),
                joinedload(CampaignStage.job_type_obj),
            ).first()

            ctx.role = cs.creator_role

            if not cs:
                raise KeyError("CampaignStage id %s not found" % campaign_stage_id)

            cd = cs.job_type_obj
            lt = cs.login_setup_obj

            # if we're launching jobs, the campaign must now be active
            if not cs.campaign_obj.active:
                cs.campaign_obj.active = True
                ctx.db.add(cs.campaign_obj)

            xff = ctx.headers_get("X-Forwarded-For", None)
            ra = ctx.headers_get("Remote-Addr", None)
            exp = cs.experiment
            vers = cs.software_version
            launch_script = cd.launch_script
            csid = cs.campaign_stage_id
            cid = cs.campaign_id

            # test_launch_flag is the one we send to Landscape via the
            # condor classad  -- either this is a particular test launch
            # or the whole campaign is marked as a test
            test_launch_flag = test_launch
            if cs.campaign_obj.campaign_type == "test":
                test_launch_flag = True

            # isssue #20990
            csname = cs.name
            cstype = cs.campaign_stage_type
            cname = cs.campaign_obj.name
            if cs.name == cs.campaign_obj.name:
                ccname = cs.name
            elif cs.name[: len(cs.campaign_obj.name)] == cs.campaign_obj.name:
                ccname = "%s__%s" % (cs.campaign_obj.name, cs.name[len(cs.campaign_obj.name) :])
            else:
                ccname = "%s__%s" % (cs.campaign_obj.name, cs.name)

            # now quoting in poms_jobsub_wrapper, but blanks still make
            # it sad so replace them
            cname = cname.replace(" ", "_")
            csname = csname.replace(" ", "_")
            ccname = ccname.replace(" ", "_")

            cdid = cs.job_type_id
            definition_parameters = cd.definition_parameters

            c_param_overrides = cs.param_overrides

            # if it is a test launch, add in the test param overrides
            # and flag the task as a test (secretly relies on poms_client
            # v3_0_0)

        if not e and not (ra == "127.0.0.1" and xff is None):
            logit.log("launch_jobs -- experimenter not authorized")
            raise PermissionError("non experimenter launch not on localhost")

        if ctx.role == "production" and not lt.launch_host.find(exp) >= 0 and exp != "samdev":
            logit.log("launch_jobs -- {} is not a {} experiment node ".format(lt.launch_host, exp))
            output = "Not Authorized: {} is not a {} experiment node".format(lt.launch_host, exp)
            raise AssertionError(output)

        if ctx.role == "analysis" and not (
            lt.launch_host in ("pomsgpvm01.fnal.gov", "fermicloud045.fnal.gov", "poms-int.fnal.gov", "pomsint.fnal.gov")
        ):
            output = "Not Authorized: {} is not a analysis launch node for exp {}".format(lt.launch_host, exp)
            raise AssertionError(output)

        experimenter_login = ctx.username

        if ctx.role == "analysis":
            sandbox = self.poms_service.filesPOMS.get_launch_sandbox(ctx)
            proxyfile = "%s/x509up_voms_%s_Analysis_%s" % (sandbox, exp, experimenter_login)
        else:
            sandbox = "$HOME"
            proxyfile = "/opt/%spro/%spro.Production.proxy" % (exp, exp)

        allheld = self.get_job_launches(ctx) == "hold"
        csheld = bool(cs.hold_experimenter_id)
        proxyheld = ctx.role == "analysis" and not self.has_valid_proxy(proxyfile)
        if allheld or csheld or proxyheld:

            if allheld:
                output = "Job launches currently held.... queuing this request"
            if csheld:
                output = "Campaign stage %s launches currently held.... queuing this request" % cs.name
            if proxyheld:
                output = "Proxy: %s not valid .... queuing this request" % os.path.basename(proxyfile)
                m = Mail()
                m.send(
                    "POMS: Queued job launch for %s:%s " % (cs.campaign_obj.name, cs.name),
                    "Due to an invalid proxy, we had to queue a job launch\n"
                    "Please upload a new proxy, and release queued jobs for this campaign",
                    "%s@fnal.gov" % cs.experimenter_creator_obj.username,
                )
                cs.hold_experimenter_id = cs.creator
                cs.role_held_with = ctx.role

            logit.log("launch_jobs -- holding launch")
            hl = HeldLaunch()
            hl.campaign_stage_id = campaign_stage_id
            hl.created = datetime.now(utc)
            hl.dataset = dataset_override
            hl.parent_submission_id = parent_submission_id
            hl.param_overrides = param_overrides
            hl.launcher = launcher_experimenter.experimenter_id
            ctx.db.add(hl)
            ctx.db.commit()
            lcmd = ""

            return lcmd, cs, campaign_stage_id, outdir, os.path.basename(output)

        if dataset_override:
            dataset = dataset_override
        else:
            dataset = self.poms_service.campaignsPOMS.get_dataset_for(ctx, cs)

        group = exp
        if group == "samdev":
            group = "fermilab"

        if "poms" in self.poms_service.hostname:
            poms_test = ""
        elif "fermicloudmwm" in self.poms_service.hostname:
            poms_test = "int"
        else:
            poms_test = "1"

        # allocate task to set ownership
        sid = self.get_task_id_for(ctx, campaign_stage_id, parent_submission_id=parent_submission_id, launch_time=launch_time)

        if sid:
            outfile = "%s_%d" % (outfile, sid)

        #
        # keep some bookkeeping flags
        #
        pdict = {}
        if dataset and dataset != "None":
            pdict["dataset"] = dataset
        if test_launch:
            pdict["test"] = 1

        if test_launch or dataset_override:
            ctx.db.query(Submission).filter(Submission.submission_id == sid).update({Submission.submission_params: pdict})
            ctx.db.commit()

        cmdl = [
            "exec 2>&1",
            "set -x",
            "export KRB5CCNAME=/tmp/krb5cc_poms_submit_%s" % group,
            "kinit -kt $HOME/private/keytabs/poms.keytab `klist -kt $HOME/private/keytabs/poms.keytab | tail -1 | sed -e 's/.* //'`|| true",
            ("ssh -tx %s@%s '" % (lt.launch_account, lt.launch_host))
            % {
                "dataset": dataset,
                "parameter": dataset,
                "experiment": exp,
                "version": vers,
                "group": group,
                "experimenter": experimenter_login,
            },
            "export UPLOADS='%s'" % sandbox,
            #
            # This bit is a little tricky.  We want to do as little of our
            # own setup as possible after the ctx.usernames' launch_setup text,
            # so we don's undo stuff they may put on the front of the path
            # etc -- *except* that we need jobsub_wrapper setup.
            # so we
            # 1. do our setup except jobsub_wrapper
            # 2. do their setup stuff from the launch template
            # 3. setup *just* poms_jobsub_wrapper, so it gets on the
            #    front of the path and can intercept calls to "jobsub_submit"
            #
            "export X509_USER_PROXY=%s" % proxyfile,
            # proxy file has to belong to us, apparently, so...
            "cp $X509_USER_PROXY /tmp/proxy$$ && export X509_USER_PROXY=/tmp/proxy$$  && chmod 0400 $X509_USER_PROXY && ls -l $X509_USER_PROXY"
            if ctx.role == "analysis"
            else ":",
            "source /grid/fermiapp/products/common/etc/setups",
            "setup poms_jobsub_wrapper -g poms41 -z /grid/fermiapp/products/common/db",
            (
                lt.launch_setup
                % {
                    "dataset": dataset,
                    "parameter": dataset,
                    "experiment": exp,
                    "version": vers,
                    "group": group,
                    "experimenter": experimenter_login,
                }
            ).replace("'", """'"'"'"""),
            'UPS_OVERRIDE="" setup -j poms_jobsub_wrapper -g poms41 -z /grid/fermiapp/products/common/db, -j poms_client -g poms31 -z /grid/fermiapp/products/common/db',
            "ups active",
            # POMS4 'properly named' items for poms_jobsub_wrapper
            "export POMS4_CAMPAIGN_STAGE_ID=%s" % csid,
            'export POMS4_CAMPAIGN_STAGE_NAME="%s"' % csname,
            "export POMS4_CAMPAIGN_STAGE_TYPE=%s" % cstype,
            "export POMS4_CAMPAIGN_ID=%s" % cid,
            'export POMS4_CAMPAIGN_NAME="%s"' % cname,
            "export POMS4_SUBMISSION_ID=%s" % sid,
            "export POMS4_CAMPAIGN_ID=%s" % cid,
            "export POMS4_TEST_LAUNCH=%s" % test_launch_flag,
            "export POMS_CAMPAIGN_ID=%s" % csid,
            'export POMS_CAMPAIGN_NAME="%s"' % ccname,
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
            try:
                params.update(cs.test_param_overrides)
            except:
                pass

        if param_overrides is not None and param_overrides != "":
            if isinstance(param_overrides, str):
                params.update(json.loads(param_overrides))
            else:
                params.update(param_overrides)

        lcmd = launch_script + " " + " ".join((x[0] + x[1]) for x in list(params.items()))
        lcmd = lcmd % {
            "dataset": dataset,
            "parameter": dataset,
            "version": vers,
            "group": group,
            "experimenter": experimenter_login,
            "experiment": exp,
        }
        lcmd = lcmd.replace("'", """'"'"'""")
        if output_commands:
            cmdl.append('echo "\n=========\nrun the following to launch the job:\n%s"' % lcmd)
            cmdl.append("/bin/bash -i")
        else:
            cmdl.append(lcmd)
        cmdl.append("exit")
        cmdl.append("' &")
        cmd = "\n".join(cmdl)

        cmd = cmd.replace("\r", "")

        if output_commands:
            cmd = cmd[cmd.find("ssh -tx") :]
            cmd = cmd[:-2]
            return cmd

        if not os.path.isdir(outdir):
            os.makedirs(outdir)
        dn = open("/dev/null", "r")
        lf = open(outfile, "w")
        logit.log("actually starting launch ssh")
        pp = subprocess.Popen(cmd, shell=True, stdin=dn, stdout=lf, stderr=lf, close_fds=True)
        lf.close()
        dn.close()
        pp.wait()

        return lcmd, cs, campaign_stage_id, outdir, os.path.basename(outfile)
