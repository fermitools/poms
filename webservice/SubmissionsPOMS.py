#!/usr/bin/env python
"""
This module contain the methods that handle the Submission.
List of methods: wrapup_tasks,
Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py
written by Marc Mengel, Michael Gueith and Stephen White. September, 2016.
"""
import configparser
import glob
import json
import os
import re
import select
import subprocess
import time
import uuid
import cherrypy

from collections import OrderedDict, deque
from datetime import datetime, timedelta

from sqlalchemy import and_, distinct, func, or_, text, Integer
from sqlalchemy.orm import joinedload
from sqlalchemy.orm.attributes import flag_modified

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
from .SAMSpecifics import sam_project_checker, sam_specifics
from .condor_log_parser import get_joblogs

# from exceptions import KeyError


#
# utility function for running commands that don's run forever...
#
# h3. popen_read_with_timeout
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


class SubmissionsPOMS:
    # h3. __init__
    def __init__(self, ps):
        self.poms_service = ps
        # keep some status vales around so we don't have to look them up...
        self.init_status_done = False
        self.status_Located = None
        self.status_Completed = None
        self.status_New = None
        

    # h3. init_statuses
    def init_statuses(self, ctx):
        if self.init_status_done:
            return
        self.status_Located = ctx.db.query(SubmissionStatus.status_id).filter(SubmissionStatus.status == "Located").first()[0]
        self.status_Completed = ctx.db.query(SubmissionStatus.status_id).filter(SubmissionStatus.status == "Completed").first()[0]
        self.status_Removed = ctx.db.query(SubmissionStatus.status_id).filter(SubmissionStatus.status == "Removed").first()[0]
        self.status_New = ctx.db.query(SubmissionStatus.status_id).filter(SubmissionStatus.status == "New").first()[0]
        self.status_Failed = ctx.db.query(SubmissionStatus.status_id).filter(SubmissionStatus.status == "Failed").first()[0]
        self.init_status_done = True

    # h3. session_status_history
    def session_status_history(self, ctx, submission_id):
        """
           Return history rows
        """
        rows = []
        tuples = (
            ctx.db.query(SubmissionHistory, SubmissionStatus)
            .filter(SubmissionHistory.submission_id == submission_id)
            .filter(SubmissionStatus.status_id == SubmissionHistory.status_id)
            .order_by(SubmissionHistory.created)
        ).all()
        for row in tuples:
            submission_id = row.SubmissionHistory.submission_id
            created = row.SubmissionHistory.created.strftime("%Y-%m-%d %H:%M:%S")
            status = row.SubmissionStatus.status
            rows.append({"created": created, "status": status})
        return rows

    # h3. campaign_stage_datasets
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

    # h3. get_submissions_with_status
    def get_submissions_with_status(self, ctx, status_id, recheck_sids=None):
        self.init_statuses(ctx)
        sq = (
            ctx.db.query(SubmissionHistory.submission_id.label("l_sid"), func.max(SubmissionHistory.created).label("latest"))
            .group_by(SubmissionHistory.submission_id)
            .subquery()
        )

        query = (
            ctx.db.query(SubmissionHistory.submission_id, SubmissionHistory.status_id)
            .filter(SubmissionHistory.created == sq.c.latest)
            .filter(SubmissionHistory.submission_id == sq.c.l_sid)
            .filter(SubmissionHistory.created > datetime.now(utc) - timedelta(days=4))
            .group_by(SubmissionHistory.submission_id, SubmissionHistory.status_id)
            .having(SubmissionHistory.status_id == status_id)
        )
        if recheck_sids:
            query = query.filter(SubmissionHistory.submission_id.in_(recheck_sids))

        cpairs = query.all()
        completed_sids = [x[0] for x in cpairs]

        return completed_sids

    # h3. get_file_patterns
    def get_file_patterns(self, s):
        plist = []

        # try to get the file pattern list, either from the
        # dependencies that lead to this campaign_stage,
        # or from the job type

        for dcd in (
            self.ctx.db.query(CampaignDependency)
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
        return plist

    # h3. wrapup_tasks
    def wrapup_tasks(self, ctx):
        # this function call another function that is not in this module, it
        # use a poms_service object passed as an argument at the init.

        now = datetime.now(utc)
        res = ["wrapping up:"]

        self.checker = sam_project_checker(ctx)

        completed_projects = []
        finish_up_submissions = set()
        mark_located = []

        ctx.db.execute("SET SESSION lock_timeout = '450s';")
        ctx.db.execute("SET SESSION statement_timeout = '500s';")

        # get completed jobs, lock them, double check
        completed_sids = self.get_submissions_with_status(ctx, self.status_Completed)
        ctx.db.query(Submission).filter(Submission.submission_id.in_(completed_sids)).order_by(
            Submission.submission_id
        ).with_for_update(read=True).all()
        completed_sids = self.get_submissions_with_status(ctx, self.status_Completed, completed_sids)

        res.append("Completed submissions_ids: %s" % repr(list(completed_sids)))

        # now get the ones with completion_type "complete":
        # and put them in the finish_up_submissions list
        for s in (
            ctx.db.query(Submission)
            .join(
                CampaignStageSnapshot, Submission.campaign_stage_snapshot_id == CampaignStageSnapshot.campaign_stage_snapshot_id
            )
            .filter(Submission.submission_id.in_(completed_sids), CampaignStageSnapshot.completion_type == "complete")
            .all()
        ):
            res.append("completion type completed: %s" % s.submission_id)
            finish_up_submissions.add(s.submission_id)

        # now get the ones with completion_type "located":
        # and decide if they go on the finish_up_submissions list...
        n_project = 0
        for s in (
            ctx.db.query(Submission)
            .join(
                CampaignStageSnapshot, Submission.campaign_stage_snapshot_id == CampaignStageSnapshot.campaign_stage_snapshot_id
            )
            .filter(Submission.submission_id.in_(completed_sids))
            .all()
        ):

            res.append("completion type located: %s" % s.submission_id)
            # after two days, call it on time...
            if now - s.updated > timedelta(days=2) or (s.submission_params and s.submission_params.get("force_located", False)):
                finish_up_submissions.add(s.submission_id)
            elif s.project:
                self.checker.add_project_submission(s)
            else:
                self.checker.add_non_project_submission(s)

        finish_up_submissions, res = self.checker.check_added_submissions(finish_up_submissions, res)

        finish_up_submissions = list(finish_up_submissions)
        finish_up_submissions.sort()

        for s in finish_up_submissions:
            res.append("marking submission %s located " % s)
            self.update_submission_status(ctx, s, "Located")

        #
        # now commit having marked things located, to release database
        # locks.
        #
        if ctx.experiment == "borked":
            # testing hook
            logit.log("faking database error")
            res.append("faking database error")
            ctx.db.rollback()
        ctx.db.commit()

        #
        # now, after committing to clear locks, we run through the
        # job logs for the submissions and update process stats, and
        # launch any recovery jobs or jobs depending on us.
        # this way we don's keep the rows locked all day
        #

        res.append("finish_up_submissions: %s " % repr(finish_up_submissions))

        for submission_id in finish_up_submissions:
            submission = ctx.db.query(Submission).filter(Submission.submission_id == submission_id).one()
            # get logs for job for final cpu values, etc.
            msg = "Starting finish_up_submissions items for submission %s" % submission.submission_id
            logit.log(msg)
            res.append(msg)

            try:
                # take care of any recovery or dependency launches
                if not self.launch_recovery_if_needed(ctx, submission, None):
                    self.launch_dependents_if_needed(ctx, submission)
                # finish up any pending changes before the next try
                ctx.db.commit()
            except Exception as e:
                logit.logger.exception("exception %s during finish_up_submissions %s" % (e, submission))
                ctx.db.rollback()

        return res

    ###

    # h3. get_task_id_for
    def get_task_id_for(
        self,
        ctx,
        campaign=None,
        command_executed="",
        input_dataset="",
        parent_submission_id=None,
        submission_id=None,
        launch_time=None,
        task_id=None,
        user=None,
        campaign_stage_id=None,
        test=None,
    ):
        if submission_id == None and task_id != None:
            submission_id = task_id
        if campaign == None and campaign_stage_id != None:
            campaign = campaign_stage_id
        logit.log(
            "get_task_id_for(ctx.username='%s',experiment='%s',command_executed='%s',input_dataset='%s',parent_submission_id='%s',submission_id='%s'"
            % (ctx.username, ctx.experiment, command_executed, input_dataset, parent_submission_id, submission_id)
        )

        #
        # try to extract the project name from the launch command...
        #
        project = None
        config = configparser.ConfigParser()
        config.read("../webservice/poms.ini")
        launch_commands = config.get("launch_commands", "projre").split(",")
        for projre in launch_commands:
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

        cs = q.one()
        if launch_time:
            tim = launch_time
        else:
            tim = datetime.now(utc)

        if submission_id:

            s = ctx.db.query(Submission).filter(Submission.submission_id == submission_id).with_for_update(read=True).one()

            if command_executed and command_executed != s.command_executed:
                s.command_executed = command_executed

            if ctx.username != "poms" and s.creator != ctx.get_experimenter().experimenter_id:
                s.creator = ctx.get_experimenter().experimenter_id

            # do NOT update updated time here; only update it at creation
            # and final state change (Located, Failed, etc.)
            # because we will use this time for dependency dataset time range
            # s.updated = tim
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

        self.poms_service.miscPOMS.snapshot_parts(ctx, s, s.campaign_stage_id)

        ctx.db.add(s)
        ctx.db.flush()

        self.init_statuses(ctx)
        if not submission_id:
            sh = SubmissionHistory(submission_id=s.submission_id, status_id=self.status_New, created=tim)
            ctx.db.add(sh)
        logit.log("get_task_id_for: returning %s" % s.submission_id)
        ctx.db.commit()
        return s.submission_id

    # h3. get_last_history
    #
    #   query to find curent status of a submission; factored out
    #   because its easy to get wrong...
    #

    def get_last_history(self, ctx, submission_id):
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

        logit.log("get_last_history: sub_id %s returns %s" % (submission_id, lasthist))
        return lasthist

    # h3. update_submission_status
    def update_submission_status(self, ctx, submission_id, status, when=None):
        self.init_statuses(ctx)

        if when == None:
            when = datetime.now(utc)

        # always lock the submission first to prevent races

        s = (
            ctx.db.query(Submission)
            .filter(Submission.submission_id == submission_id)
            .order_by(Submission.submission_id)
            .with_for_update(read=True)
            .first()
        )

        # don't mark recovery jobs Failed -- they get just
        # the jobs that didn't pass the original submission,
        # the recovery is still a success even if they all fail again.
        if status == "Failed" and s.recovery_tasks_parent:
            status = "Completed"

        slist = ctx.db.query(SubmissionStatus.status_id).filter(SubmissionStatus.status == status).first()

        if slist:
            status_id = slist[0]
        else:
            # not a known status, just bail
            return

        lasthist = self.get_last_history(ctx, submission_id)

        logit.log(
            "update_submission_status: submission_id: %s  newstatus %s  lasthist: status %s created %s "
            % (submission_id, status_id, lasthist.status_id if lasthist else "", lasthist.created if lasthist else "")
        )

        # don't roll back Located, Failed, or Removed (final states)
        # note that we *intentionally don't* have LaunchFailed here, as we
        # *could*  have a launch that took a Really Long Time, and we might
        # have falsely concluded that the launch failed...
        final_states = (self.status_Located, self.status_Removed, self.status_Failed)
        if lasthist and lasthist.status_id in final_states and ctx.username == "poms":
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

        #
        # update Submission.updated *only* if this is a final state, as
        # this time will be used for the date range on the submission
        #
        if status_id in final_states:
            s.updated = sh.created
            ctx.db.add(s)

    # h3. mark_failed_submissions
    def mark_failed_submissions(self, ctx):
        """
            find all the recent submissions that are still "New" but more
            than two hours old, and mark them "LaunchFailed"
        """
        self.init_statuses(ctx)
        now = datetime.now(utc)

        cert = ctx.config_get("elasticsearch_cert")
        key = ctx.config_get("elasticsearch_key")

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
        for submission in (
            ctx.db.query(Submission)
            .filter(Submission.submission_id.in_(failed_sids))
            .order_by(Submission.submission_id)
            .with_for_update(read=True)
            .all()
        ):
            # sometimes jobs complete quickly, and we do not see them go by..
            # so if we got a jobsub_job_id, check for a job log to find
            # out what happened
            if submission.jobsub_job_id:
                logit.log("checking for log for %s:" % submission.jobsub_job_id)
                job_data = get_joblogs(
                    ctx.db,
                    submission.jobsub_job_id,
                    cert,
                    key,
                    submission.campaign_stage_obj.experiment,
                    submission.campaign_stage_obj.creator_role,
                )

                if job_data:
                    res.append("found job log for %s!" % submission_id)
                    logit.log("found job log for %s!" % submission_id)

                    if len(job_data["completed"]) == len(job_data["idle"]):
                        self.update_submission_status(ctx, submission_id, status="Completed")
                        res.append("submission %s Completed")
                        logit.log("submission %s Completed")

                continue
            res.append("failing launch for %s" % submission.submission_id)
            logit.log("failing launch for %s" % submission.submission_id)
            self.update_submission_status(ctx, submission.submission_id, status="LaunchFailed")
        ctx.db.commit()
        return "\n".join(res)

    # h3. submission_details
    def submission_details(self, ctx, submission_id):
        submission = (
            ctx.db.query(Submission)
            .options(joinedload(Submission.campaign_stage_snapshot_obj))
            .options(joinedload(Submission.login_setup_snapshot_obj))
            .options(joinedload(Submission.job_type_snapshot_obj))
            .filter(Submission.submission_id == submission_id)
            .one()
        )
        history = (
            ctx.db.query(SubmissionHistory)
            .filter(SubmissionHistory.submission_id == submission_id)
            .order_by(SubmissionHistory.created)
            .all()
        )

        qt = "select submission_id from submissions where submission_params->>'dataset' like 'poms_%s_%s_%%'"

        dq = text(qt % ("depends", submission_id)).columns(submission_id=Integer)
        depend_ids = [x[0] for x in ctx.db.execute(dq).fetchall()]

        rq = text(qt % ("recover", submission_id)).columns(submission_id=Integer)
        recovery_ids = [x[0] for x in ctx.db.execute(rq).fetchall()]

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
        elif submission and submission.command_executed.find("--dataset_definition") > 0:
            pos = submission.command_executed.find("--dataset_definition")
            dataset = submission.command_executed[pos + 21 :]
            pos = dataset.find(" ")
            dataset = dataset[:pos]
        elif submission and submission.command_executed.find("--dataset") > 0:
            pos = submission.command_executed.find("--dataset")
            dataset = submission.command_executed[pos + 10 :]
            pos = dataset.find(" ")
            dataset = dataset[:pos]
        elif submission and submission.project:
            dataset = sam_specifics(ctx).get_dataset_from_project(submission)
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
       
        statuses = []
        cs = submission.campaign_stage_snapshot_obj.campaign_stage
        listfiles = "%s/show_dimension_files/%s/%s?dims=%%s" % (cherrypy.request.app.root.path, cs.experiment, ctx.role)
        (
            summary_list,
            some_kids_decl_needed,
            some_kids_needed,
            base_dim_list,
            output_files,
            output_list,
            all_kids_decl_needed,
            some_kids_list,
            some_kids_decl_list,
            all_kids_decl_list,
        ) = sam_specifics(ctx).get_file_stats_for_submissions([submission], cs.experiment)
        
        i = 0
        psummary = summary_list[i]
        partpending = psummary.get("files_in_snapshot", 0) - some_kids_list[i]
        # pending = psummary.get('files_in_snapshot', 0) - all_kids_list[i]
        pending = partpending
      
        statuses = [
            ["Available output: ", output_list[i], listfiles % output_files[i]],
            ["Submitted: ",psummary.get("files_in_snapshot", 0), listfiles % base_dim_list[i]],
            ["Delivered to SAM: ",
                "%d"
                % (
                    psummary.get("tot_consumed", 0)
                    + psummary.get("tot_cancelled", 0)
                    + psummary.get("tot_failed", 0)
                    + psummary.get("tot_skipped", 0)
                    + psummary.get("tot_delivered", 0)
                ),
                listfiles % (base_dim_list[i] + " and consumed_status consumed,cancelled,completed,failed,skipped,delivered "),
            ],
            ["Unknown to SAM: ", "%d" % psummary.get("tot_unknown", 0), listfiles % base_dim_list[i] + " and consumed_status unknown"],
            ["Consumed: ", psummary.get("tot_consumed", 0), listfiles % base_dim_list[i] + " and consumed_status co%"],
            ["Cancelled: ", psummary.get("tot_cancelled", 0), listfiles % base_dim_list[i] + " and consumed_status cancelled"],
            ["Failed: ", psummary.get("tot_failed", 0), listfiles % base_dim_list[i] + " and consumed_status failed"],
            ["Skipped: ", psummary.get("tot_skipped", 0), listfiles % base_dim_list[i] + " and consumed_status skipped"],
            ["With some kids declared: ", some_kids_decl_list[i], listfiles % some_kids_decl_needed[i]],
            ["With all kids declared: ",all_kids_decl_list[i], listfiles % all_kids_decl_needed[i]],
            ["With kids located: ",some_kids_list[i], listfiles % some_kids_needed[i]],
            ["Pending: ", pending, listfiles % (base_dim_list[i] + " minus ( %s ) " % all_kids_decl_needed[i])],
        ]

        


        return submission, history, dataset, rmap, smap, ds, submission_log_format, recovery_ids, depend_ids, statuses

    # h3. running_submissions
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

        if cl and cl != "None":

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

        else:

            res = list(
                ctx.db.query(Submission.submission_id, Submission.jobsub_job_id, CampaignStage.experiment)
                .filter(CampaignStage.campaign_stage_id == Submission.campaign_stage_id)
                .filter(Submission.submission_id.in_(running_sids))
            )

        return res

    # h3. force_locate_submission
    def force_locate_submission(self, ctx, submission_id):
        # this now sets a flag in the submission_params rather than
        # backdating the time; we need the updated time for date ranges
        # for datasets, and setting it back here will make that goofy

        e = ctx.get_experimenter()
        s = ctx.db.query(Submission).filter(Submission.submission_id == submission_id).with_for_update(read=True).one()
        cs = s.campaign_stage_obj
        if s.submission_params:
            s.submission_params["force_located"] = True
            #
            # https://stackoverflow.com/questions/42559434/updates-to-json-field-dont-persist-to-db
            #
            flag_modified(s, "submission_params")
        else:
            s.submission_params = {"force_located": True}
        ctx.db.add(s)
        ctx.db.commit()
        return "Ok."

    # h3. update_submission
    def update_submission(self, ctx, submission_id, jobsub_job_id, pct_complete=None, status=None, project=None):

        #logit.log("submission_id=%s | Jobsub_job_id=%s" % (str(submission_id), str(jobsub_job_id)))
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
        if status == "Running" and pct_complete and float(pct_complete) >= s.campaign_stage_snapshot_obj.completion_pct:
            status = "Completed"

        if status is not None:
            self.update_submission_status(ctx, submission_id, status=status)

        ctx.db.commit()
        return "Ok."

    # h3. launch_dependents_if_needed
    def launch_dependents_if_needed(self, ctx, s):
        logit.log("Entering launch_dependents_if_needed(%s)" % s.submission_id)
        self.init_statuses(ctx)

        # if this is itself a recovery job, we go back to our parent
        # because dependants should use the parent, not the recovery job

        lasthist = self.get_last_history(ctx, s.submission_id)
        if lasthist.status_id != self.status_Located:
            logit.log("Not launching dependencies because submission is not marked Located")
            return

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

        launch_user = ctx.db.query(Experimenter).filter(Experimenter.experimenter_id == s.creator).one()
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
                    launch_user.experimenter_id,
                    test_launch=s.submission_params.get("test", False),
                )
            else:
                i = i + 1
                dname = sam_specifics(ctx).dependency_definition(s, cd, i)

                if s.submission_params and s.submission_params.get("test", False):
                    test_launch = s.submission_params.get("test", False)
                else:
                    test_launch = False

                logit.log("About to launch jobs, test_launch = %s" % test_launch)

                self.launch_jobs(ctx, cd.provides_campaign_stage_id, s.creator, dataset_override=dname, test_launch=test_launch)
        return 1

    # h3. launch_recovery_if_needed
    #  Note: assumes submission is already locked
    def launch_recovery_if_needed(self, ctx, s, recovery_type_override=None):
        logit.log("Entering launch_recovery_if_needed(%s)" % s.submission_id)
        self.init_statuses(ctx)
        if not ctx.config_get("poms.launch_recovery_jobs", False):
            logit.log("recovery launches disabled")
            return 1

        lasthist = self.get_last_history(ctx, s.submission_id)
        if lasthist.status_id != self.status_Located and not recovery_type_override:
            logit.log("Not launching recovery because submission is not marked Located")
            return

        # if this is itself a recovery job, we go back to our parent
        # to do all the work, because it has the counters, etc.
        current_s = s
        if s.parent_obj:
            s = s.parent_obj
        logit.log("launch_recovery_if_needed: current_s: %s" %repr(current_s))
        logit.log("launch_recovery_if_needed: s: %s" %repr(s))

        if s.recovery_position is None:
            s.recovery_position = 0

        if recovery_type_override is not None:
            rt = ctx.db.query(RecoveryType).filter(RecoveryType.recovery_type_id == int(recovery_type_override)).all()
            # need to make a temporary CampaignRecovery, put it far enough
            # out in rlist
            rlist = [None] * s.recovery_position + [
                CampaignRecovery(
                    job_type_id=s.campaign_stage_obj.job_type_id,
                    recovery_order=0,
                    param_overrides=[],
                    recovery_type_id=rt[0].recovery_type_id,
                    recovery_type=rt[0],
                )
            ]
        else:
            rlist = self.poms_service.miscPOMS.get_recovery_list_for_campaign_def(ctx, s.job_type_snapshot_obj)

        logit.log("recovery list %s" % rlist)

        while s.recovery_position is not None and s.recovery_position < len(rlist):
            logit.log("recovery position %d" % s.recovery_position)

            rtype = rlist[s.recovery_position].recovery_type
            param_overrides = rlist[s.recovery_position].param_overrides

            # if this is the first recovery, we use s (which may be the parent submission)
            # else we use the current submission as the project because we don't want to re-submit the same files
            # May want to add a secondary condition in the future to make sure that the recovery type is the same 
            # as the previous submission if choosing current_s rather than s
            if s.recovery_position == 0:
                nfiles, rname = sam_specifics(ctx).create_recovery_dataset(s, rtype, rlist)
            else:
                nfiles, rname = sam_specifics(ctx).create_recovery_dataset(current_s, rtype, rlist)

            s.recovery_position = s.recovery_position + 1

            if nfiles > 0:

                launch_user = ctx.db.query(Experimenter).filter(Experimenter.experimenter_id == s.creator).one()
                ctx.username = launch_user.username
                ctx.experimenter_cache = launch_user
                ctx.role = s.campaign_stage_obj.creator_role
                ctx.experiment = s.campaign_stage_obj.experiment

                res = self.launch_jobs(
                    ctx,
                    s.campaign_stage_snapshot_obj.campaign_stage_id,
                    launch_user.experimenter_id,
                    dataset_override=rname,
                    parent_submission_id=s.submission_id,
                    param_overrides=param_overrides,
                    test_launch=s.submission_params.get("test", False),
                )
                return res

        return None

    # h3. launch_recovery_for
    def launch_recovery_for(self, **kwargs):
        ctx = kwargs["ctx"]
        s = ctx.db.query(Submission).filter(Submission.submission_id == kwargs["submission_id"]).one()
        stime = datetime.now(utc)

        # return lcmd, cs, campaign_stage_id, outdir, outfile
        res = self.launch_recovery_if_needed(ctx, s, kwargs["recovery_type"])

        if res:
            return res[3], res[4], "%s/%s" % (res[3], res[4])
        else:
            raise AssertionError("No recovery needed, launch skipped.")

    # h3. set_job_launches
    def set_job_launches(self, ctx, hold):

        experimenter = ctx.get_experimenter()
        if hold not in ["hold", "allowed"]:
            return
        # keep held launches in campaign stage w/ campaign_stage_id == 0
        c = ctx.db.query(CampaignStage).with_for_update().filter(CampaignStage.campaign_stage_id == 0).one()
        if hold == "hold":
            c.hold_experimenter_id = experimenter.experimenter_id
            c.role_held_wtih = role
        else:
            c.hold_experimenter_id = None
            c.role_held_wtih = None
        ctx.db.add(c)
        ctx.db.commit()
        return

    # h3. get_job_launches
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

    # h3. launch_queued_job
    def launch_queued_job(self, ctx):
        if self.get_job_launches(ctx) == "hold":
            return "Held."

        hl = (
            ctx.db.query(HeldLaunch)
            .join(CampaignStage, HeldLaunch.campaign_stage_id == CampaignStage.campaign_stage_id)
            .filter(CampaignStage.hold_experimenter_id == None)
            .with_for_update(read=True)
            .order_by(HeldLaunch.created)
            .first()
        )
        if hl:
            launcher = hl.launcher
            campaign_stage_id = hl.campaign_stage_id
            dataset = hl.dataset
            parent_submission_id = hl.parent_submission_id
            param_overrides = hl.param_overrides
            launch_user = ctx.db.query(Experimenter).filter(Experimenter.experimenter_id == hl.launcher).one()
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
            ctx.experimenter_cache = launch_user

            self.launch_jobs(
                ctx,
                campaign_stage_id,
                launcher=launcher,
                dataset_override=dataset,
                parent_submission_id=parent_submission_id,
                param_overrides=param_overrides,
            )
            return "Launched."
        else:
            return "None."

    # h3. has_valid_proxy
    def has_valid_proxy(self, proxyfile):
        logit.log("Checking proxy: %s" % proxyfile)
        res = os.system("voms-proxy-info -exists -valid 0:10 -file %s" % proxyfile)
        logit.log("system(voms-proxy-info... returns %d" % res)
        return os.WIFEXITED(res) and os.WEXITSTATUS(res) == 0

    # h3. get_output_dir_file
    def get_output_dir_file(self, ctx, launch_time, username, campaign_stage_id=None, submission_id=None, test_login_setup=None):
        ds = launch_time.astimezone(utc).strftime("%Y%m%d_%H%M%S")

        if test_login_setup:
            subdir = "template_tests_%d" % int(test_login_setup)
        else:
            subdir = "campaign_%s" % campaign_stage_id
            assert submission_id

        outdir = "%s/private/logs/poms/launches/%s" % (os.environ["HOME"], subdir)
        outfile = "%s_%s" % (ds, username)

        if submission_id:
            outfile = "%s_%s" % (outfile, submission_id)

        outfullpath = "%s/%s" % (outdir, outfile)

        return outdir, outfile, outfullpath

    # h3. abort_launch
    def abort_launch(self, ctx, submission_id):
        """
            look in our output file to find if we have a pid and/or
            a completion message; then if we have the former and not
            the latter, ssh over and kill it.
        """
        submission = ctx.db.query(Submission).filter(Submission.submission_id == submission_id).one()
        outdir, outfile, outfullpath = self.get_output_dir_file(
            ctx, submission.created, submission.experimenter_creator_obj.username, submission.campaign_stage_id, submission_id
        )
        re1 = re.compile("== process_id: ([0-9]+) ==")
        re2 = re.compile("== completed: ([0-9]+) ==")
        pid = None
        finished = False
        with open(outfullpath, "r") as f:
            for line in f:
                m = re1.search(line)
                if m:
                    pid = m.group(1)
                m = re2.search(line)
                if m:
                    finished = True

        res = []
        if pid and not finished:
            cmd = "ssh %s@%s 'kill -9 -%s'" % (
                submission.login_setup_snapshot_obj.launch_account,
                submission.login_setup_snapshot_obj.launch_host,
                pid,
            )
            with os.popen(cmd, "r") as f:
                for line in f:
                    res.append(line)
        else:
            res.append("unable to abort launch")

        return "\n".join(res)

    # h3. launch_jobs
    def launch_jobs(
        self,
        ctx,
        campaign_stage_id,
        launcher=None,
        dataset_override=None,
        parent_submission_id=None,
        param_overrides=None,
        test_login_setup=None,
        test_launch=False,
        output_commands=False,
        parent=None,
        **kwargs,
    ):

        logit.log("Entering launch_jobs(%s, %s, %s)" % (campaign_stage_id, dataset_override, parent_submission_id))

        if launcher == None:
            launcher = ctx.username

        launch_time = datetime.now(utc)
        ds = launch_time.strftime("%Y%m%d_%H%M%S")
        e = ctx.get_experimenter()
        role = ctx.role
        

        # at the moment we're inconsistent about whether we pass
        # launcher as ausername or experimenter_id or if its a string
        # of the integer or  an integer... sigh
        if str(launcher)[0] in ("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"):
            launcher_experimenter = ctx.db.query(Experimenter).filter(Experimenter.experimenter_id == int(launcher)).one()
        else:
            launcher_experimenter = ctx.db.query(Experimenter).filter(Experimenter.username == launcher).one()

        if test_login_setup:
            lt = ctx.db.query(LoginSetup).filter(LoginSetup.login_setup_id == test_login_setup).first()
            dataset_override = "fake_test_dataset"
            cdid = "-"
            csid = "-"
            ccname = "-"
            cname = "-"
            csname = "-"
            cstype = "-"
            cid = "-"
            sid = "-"
            cs = None
            c_param_overrides = []
            test_launch_flag = False
            vers = "v0_0"
            dataset = "-"
            definition_parameters = []
            exp = ctx.experiment
            launch_script = """echo "Environment"; printenv; echo "jobsub is`which jobsub`;  echo "login_setup successful!"""
            do_tokens = (lt.launch_host == "fifeutilgpvm02.fnal.gov")

        else:
            if str(campaign_stage_id)[0] in "0123456789":
                cq = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id)
            else:
                cq = ctx.db.query(CampaignStage).filter(
                    CampaignStage.name == campaign_stage_id, CampaignStage.experiment == ctx.experiment
                )

            cs = cq.options(
                joinedload(CampaignStage.campaign_obj),
                joinedload(CampaignStage.login_setup_obj),
                joinedload(CampaignStage.job_type_obj),
            ).one()

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

            # handle merge_overrides behavior -- if set, start
            # with campaign default overrides.
            if cs.merge_overrides:
                c_param_overrides = cs.campaign_obj.defaults.get("defaults", {}).get("param_overrides", [])
                if isinstance(c_param_overrides,str):
                    c_param_overrides = json.loads(c_param_overrides)
            else:
                c_param_overrides = []

            c_param_overrides += cs.param_overrides

            # if it is a test launch, add in the test param overrides
            # and flag the task as a test (secretly relies on poms_client
            # v3_0_0)

        if not e and not (ra == "127.0.0.1" and xff is None):
            logit.log("launch_jobs -- experimenter not authorized")
            raise PermissionError("non experimenter launch not on localhost")

        if ctx.role == "production" and not lt.launch_host.find(exp) >= 0 and not lt.launch_host == "fifeutilgpvm02.fnal.gov" and not lt.launch_host == "fifeutilgpvm01.fnal.gov" and exp != "samdev":
            logit.log("launch_jobs -- {} is not a {} experiment node ".format(lt.launch_host, exp))
            output = "Not Authorized: {} is not a {} experiment node".format(lt.launch_host, exp)
            raise AssertionError(output)

        if ctx.role == "analysis" and not (
            lt.launch_host in ("pomsgpvm01.fnal.gov", "fermicloud210.fnal.gov", "poms-int.fnal.gov", "pomsint.fnal.gov", "fifeutilgpvm01.fnal.gov", "fifeutilgpvm02.fnal.gov")
        ):
            output = "Not Authorized: {} is not a analysis launch node for exp {}".format(lt.launch_host, exp)
            raise AssertionError(output)
        
        group = exp
        if ctx.role == "analysis":
            if group in ["samdev","accel","accelai", "icarus", "admx","annie","argoneut", "cdms","chips","cms","coupp","darksectorldrd","darkside","ebd","egp","emph","emphatic","fermilab","genie","lariat","larp","magis100","mars","minerva","miniboone","minos","next","noble","nova","numix","patriot","pip2","seaquest","spinquest","test","theory","uboone"]:
                credmon_group = "fermilab"
        if group == "samdev":
            group = "fermilab"
        
        
        uu = uuid.uuid4()  # random uuid -- shouldn't be guessable.
        
        experimenter_login = ctx.username
        if role == "analysis":
            vaultfilename = f"vt_{ctx.experiment}_analysis_{experimenter_login}"
        else:
            vaultfilename = f"vt_{ctx.experiment}_production_{experimenter_login}"
        if ctx.role == "analysis" and lt.launch_host == self.poms_service.hostname:
            sandbox = self.poms_service.filesPOMS.get_launch_sandbox(ctx)
            vaultfile = "%s/%s" % (sandbox, vaultfilename)
            proxyfile = "%s/x509up_voms_%s_Analysis_%s" % (sandbox, exp, experimenter_login)
        elif ctx.role == "analysis":
            sandbox = self.poms_service.filesPOMS.get_launch_sandbox(ctx)
            vaultfile = "%s/%s" % (sandbox, vaultfilename)
            proxyfile = "%s/x509up_voms_%s_Analysis_%s" % ("/home/poms/uploads/%s/%s" % (exp, experimenter_login), exp, experimenter_login)
        else:
            sandbox = "$HOME"
            proxyfile = "/opt/%spro/%spro.Production.proxy" % (exp, exp)
            if exp == "samdev":
                vaultfile = "/home/poms/uploads/%s/%s/%s" % (ctx.experiment, ctx.username, vaultfilename)
            #proxyfile = "/home/poms/cfg/samdevpro.Production.proxy"

        allheld = self.get_job_launches(ctx) == "hold"
        csheld = bool(cs and cs.hold_experimenter_id)
        
        # XXX
        # for the moment, using fifeutilgpvm02 is code for using
        # jobsub_lite and tokens.  This needs a flag on
        # the campaigns and/or experiments instead.
        #do_tokens = (lt.launch_host == "fifeutilgpvm02.fnal.gov")
        do_tokens = True
        
        proxyheld = ctx.role == "analysis" and not self.has_valid_proxy(proxyfile) and not do_tokens
        if allheld or csheld or proxyheld:

            errnum = 423
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
                ctx.db.add(cs)

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

            raise ctx.HTTPError(errnum, output)

        if dataset_override:
            dataset = dataset_override
        else:
            dataset = self.poms_service.stagesPOMS.get_dataset_for(ctx, cs, test_launch)


        if "poms" in self.poms_service.hostname:
            poms_test = ""
        elif "fermicloudmwm" in self.poms_service.hostname:
            poms_test = "int"
        else:
            poms_test = "1"

        # allocate task to set ownership
        if not test_login_setup:
            sid = self.get_task_id_for(ctx, campaign_stage_id, parent_submission_id=parent_submission_id, launch_time=launch_time)

            #
            # keep some bookkeeping flags
            #
            pdict = {}
            if dataset and dataset != "None":
                pdict["dataset"] = dataset
            if test_launch:
                pdict["test"] = 1
            if parent:
                pdict["parent"] = parent

            ctx.db.query(Submission).filter(Submission.submission_id == sid).update({Submission.submission_params: pdict})

        if cs and cs.campaign_stage_type == "approval":
            # special case for approval -- don't need to really launch...
            self.update_submission_status(ctx, sid, "Awaiting Approval")
            ctx.db.commit()

            sam_specifics(ctx).declare_approval_transfer_datasets(sid)

            outdir, outfile, outfullpath = self.get_output_dir_file(
                ctx, launch_time, ctx.username, campaign_stage_id, sid, test_login_setup=test_login_setup
            )
            lcmd = "await_approval"
            logit.log("trying to record launch in %s" % outfullpath)
            if not os.path.isdir(outdir):
                os.makedirs(outdir)
            f = open(outfullpath, "w")
            f.write("Set submission_id %s to status 'Awaiting Approval'" % sid)
            f.close()
            return lcmd, cs, campaign_stage_id, outdir, outfile

        if cs and cs.campaign_stage_type == "datatransfer":
            # also need "output" datasets for data transfer
            sam_specifics(ctx).declare_approval_transfer_datasets(sid)

        ctx.db.commit()
        
        # BEGIN TOKEN LOGIC
            
        """if os.path.exists("/home/poms/uploads/%s/%s/%s" % (ctx.experiment, ctx.username, vaultfilename)):
            vaultfile = "/home/poms/uploads/%s/%s/%s" % (ctx.experiment, ctx.username, vaultfilename)
        elif os.path.exists("/tmp/%s" % vaultfilename):
            vaultfile = "/tmp/%s" % vaultfilename
        """    
        # Sets read and write permissions for bearer token directory and vault tokens 
        # Securely copy vault token to external launch host prior to ssh'ing into the launch host
        if ctx.role == "analysis" or ctx.experiment == "samdev": 
            tok_permissions = "chmod +rw %s;" % (vaultfile)
            scp_command = "scp %s %s@%s:/tmp; " % (vaultfile, lt.launch_account, lt.launch_host)
            scp_command = scp_command + "scp %s %s@%s:/tmp;" % (proxyfile, lt.launch_account, lt.launch_host)
            #scp_command = "rsync -r %s %s@%s:%s" % (sandbox, lt.launch_account, lt.launch_host, sandbox)
            if lt.launch_host != self.poms_service.hostname:  
                vaultfile = "/tmp/%s" % vaultfilename
                proxyfile = "/tmp/x509up_voms_%s_Analysis_%s" % (exp, experimenter_login)
        else:
            tok_permissions = ""
            scp_command = ""
            vaultfile = ""
        
        # Declare where a bearer token should be stored when launch host calls htgettoken
        if ctx.role == "production" and ctx.experiment == "samdev": 
            # samdev doesn't have a managed token...
            htgettokenopts = "-a htvaultprod.fnal.gov -r default -i fermilab  --vaulttokeninfile=%s --credkey=%s" % (vaultfile, experimenter_login)
        elif ctx.role == "analysis":
             htgettokenopts = "-a htvaultprod.fnal.gov -r default -i %s --vaulttokeninfile=%s --credkey=%s" % (group, vaultfile, experimenter_login)
        else:
            htgettokenopts = "-a htvaultprod.fnal.gov -i %s -r %s --credkey=%spro/managedtokens/fifeutilgpvm01.fnal.gov " % (group, ctx.role, exp)

         # add token logic if not already in login_setup:
        tokens_defined_in_login_setup = "HTGETTOKENOPTS" in cs.login_setup_obj.launch_setup
        
        # token logic if not defined in launch script
        token_logic = [
            ("export USER=%s; " % experimenter_login) if ctx.role == "analysis" or ctx.experiment == "samdev" else "",
            "export XDG_RUNTIME_DIR=/tmp/;" if ctx.role == "analysis" or ctx.experiment == "samdev" else "",
            "export XDG_CACHE_HOME=/tmp/%s;" % experimenter_login if ctx.role == "analysis" or ctx.experiment == "samdev" else "",
            "export HTGETTOKENOPTS=\"%s\"; " %htgettokenopts,
            "export PATH=\"/usr/sbin:/usr/sbin/condor_store_cred:/usr/bin/condor_vault_storer:$PATH:/opt/puppetlabs/bin:/opt/jobsub_lite/bin\";",
            "export _condor_SEC_CREDENTIAL_STORER=/bin/true;",
            "export BEARER_TOKEN_FILE=/tmp/token%s; " % uu,
            "htgettoken %s; " % (htgettokenopts),
            ("condor_vault_storer -v %s_production; " % ctx.experiment) if ctx.role == "production" and ctx.experiment != "samdev" and not tokens_defined_in_login_setup
            else
            "(cat %s; echo  \"https://htvaultprod.fnal.gov:8200/v1/secret/oauth/creds/%s/%s:default\";)  | ( read TOK; read URL; echo \"{\"; echo \" \\\"vault_token\\\": \\\"$TOK\\\",\"; echo \"  \\\"vault_url\\\": \\\"$URL\\\"\"; echo \"}\"; ) | condor_store_cred add-oauth -s %s_default -i - > /dev/stdout" % (vaultfile, group, experimenter_login, group),
            "setup jobsub_client v_lite;"
        ]
        # END TOKEN LOGIC
        
        cmdl = [
            "exec 2>&1;",
            "set -x;",
            "export KRB5CCNAME=/tmp/krb5cc_poms_submit_%s;" % group,
            "kinit -kt $HOME/private/keytabs/poms.keytab `klist -kt $HOME/private/keytabs/poms.keytab | tail -1 | sed -e 's/.* //'`|| true;",
            scp_command if do_tokens and lt.launch_host != self.poms_service.hostname else "",
            tok_permissions if do_tokens else "",
            ("ssh -tx %s@%s '" % (lt.launch_account, lt.launch_host))
            % {
                "dataset": dataset,
                "parameter": dataset,
                "experiment": exp,
                "version": vers,
                "group": group,
                "experimenter": experimenter_login,
            },
            
            "echo == process_id: $$ ==;",
            "export UPLOADS='%s';" % sandbox,
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
            # also, for now we're conditionally doing different things
            # about authentication based on 'do_tokens'
            # this conditonal expression ugliness should go away once
            # we migrate to jobsub_lite for everyone...
            #
            # note we actually turn off running condor_vault_storer
            # (that's the _condor_SEC_CREDENTIAL_STORER=/bin/true)
            # because we assume that's getting done by
            #  * the production proxy service for production proxies and
            #  * by the analysis user uploading their vault token...
            #
            #
            
            "export X509_USER_PROXY=%s;" % proxyfile,
            # proxy file has to belong to us, apparently, so...
            "cp $X509_USER_PROXY /tmp/proxy%s; export X509_USER_PROXY=/tmp/proxy%s; chmod 0400 $X509_USER_PROXY; ls -l $X509_USER_PROXY;"
            % (uu, uu),
            "source /grid/fermiapp/products/common/etc/setups;",
            "setup poms_jobsub_wrapper -g poms41 -z /grid/fermiapp/products/common/db, ifdhc v2_6_10, ifdhc_config v2_6_16; export IFDH_TOKEN_ENABLE=1; export IFDH_PROXY_ENABLE=1;",
            ""
            if do_tokens
            else "setup poms_jobsub_wrapper -g poms41 -z /grid/fermiapp/products/common/db;",
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
            ).replace("'", """'"'"'""")
        ]
        
       
        if not tokens_defined_in_login_setup:
            cmdl.extend(token_logic)
            
        cmdl.extend([
            'UPS_OVERRIDE="" setup -j poms_jobsub_wrapper -g poms41 -z /grid/fermiapp/products/common/db, -j poms_client -g poms31 -z /grid/fermiapp/products/common/db, ifdhc v2_6_10, ifdhc_config v2_6_16; export IFDH_TOKEN_ENABLE=1; export IFDH_PROXY_ENABLE=1;',
            'setup jobsub_client v_lite;',
            "ups active;",
            # POMS4 'properly named' items for poms_jobsub_wrapper
            "export POMS4_CAMPAIGN_STAGE_ID=%s;" % csid,
            'export POMS4_CAMPAIGN_STAGE_NAME="%s";' % csname,
            "export POMS4_CAMPAIGN_STAGE_TYPE=%s;" % cstype,
            "export POMS4_CAMPAIGN_ID=%s;" % cid,
            'export POMS4_CAMPAIGN_NAME="%s";' % cname,
            "export POMS4_SUBMISSION_ID=%s;" % sid,
            "export POMS4_CAMPAIGN_ID=%s;" % cid,
            "export POMS4_TEST_LAUNCH=%s;" % test_launch_flag,
            "export POMS_CAMPAIGN_ID=%s;" % csid,
            'export POMS_CAMPAIGN_NAME="%s";' % ccname,
            "export POMS_PARENT_TASK_ID=%s;" % (parent_submission_id if parent_submission_id else ""),
            "export POMS_TASK_ID=%s;" % sid,
            "export POMS_LAUNCHER=%s;" % launcher_experimenter.username,
            "export POMS_TEST=%s;" % poms_test,
            "export POMS_TASK_DEFINITION_ID=%s;" % cdid,
            "export JOBSUB_GROUP=%s;" % group,
            "export GROUP=%s;" % group
        ])

        cleanup_cmdl = [
            # we made either a token or a proxy copy just for
            # authenticating this launch, so clean it up...
            #"rm -f $X509_USER_PROXY $BEARER_TOKEN_FILE"
            "rm -v -f /tmp/proxy%s; rm -v -f $BEARER_TOKEN_FILE; rm -v -f /tmp/token%s;" % (uu, uu),
            "rm -f %s;" % proxyfile if lt.launch_host != self.poms_service.hostname and ctx.role != "production" and ctx.experiment != "samdev" else "",
            "date +%H:%M:%S.%N;",
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
        formatdict = cs.campaign_obj.campaign_keywords if cs and cs.campaign_obj.campaign_keywords else {}
        formatdict.update(
            {
                "dataset": dataset,
                "parameter": dataset,
                "version": vers,
                "group": group,
                "experimenter": experimenter_login,
                "experiment": exp,
            }
        )

        lcmd = lcmd % formatdict
        lcmd = lcmd.replace("'", """'"'"'""")
        if output_commands:
            cmdl.append('echo "\n=========\nrun the following to launch the job:\n%s";' % lcmd)
            cmdl.append("/bin/bash -i")
        else:
            cmdl.append(lcmd)
        cmdl.extend(cleanup_cmdl)
        cmdl.append("echo == completed: $$ ==;")
        cmdl.append("exit")
        cmdl.append("' &")
        cmd = "\n".join(cmdl)

        cmd = cmd.replace("\r", "")

        if output_commands:
            cmd = cmd[cmd.find("ssh -tx") :]
            cmd = cmd[:-2]
            return "<pre>%s</pre>" % cmd


        outdir, outfile, outfullpath = self.get_output_dir_file(
            ctx, launch_time, ctx.username, campaign_stage_id=csid, submission_id=sid, test_login_setup=test_login_setup
        )

        logit.log("trying to record launch in %s" % outfullpath)

        if not os.path.isdir(outdir):
            os.makedirs(outdir)
        lf = open(outfullpath, "w")
        dn = open("/dev/null", "r")
        logit.log("actually starting launch ssh")
        pp = subprocess.Popen(cmd, shell=True, stdin=dn, stdout=lf, stderr=lf, close_fds=True)
        lf.close()
        dn.close()
        pp.wait()
        logit.log("started launch ssh")
   
        
        return lcmd, cs, campaign_stage_id, outdir, os.path.basename(outfile)

    def get_file_upload_path(self, ctx, filename):
        return "%s/uploads/%s/%s/%s" % (ctx.config_get("base_uploads_dir"), ctx.experiment, ctx.username, filename)

    
