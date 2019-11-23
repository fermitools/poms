#!/usr/bin/env python
"""

 This module contain the methods that handle the file status accounting
 List of methods: def list_task_logged_files, campaign_task_files, job_file_list, get_inflight,
 inflight_files, show_dimension_files, campaign_sheet
 ALso now includes file upload and analysis ctx.username launch sandbox code.
 Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions
 in poms_service.py written by Marc Mengel, Stephen White and Michael Gueith.
 October, 2016.
"""

from collections import deque, defaultdict
import os
import glob
import uuid
from datetime import datetime, timedelta
import time

from sqlalchemy.orm import joinedload

from . import logit
from .poms_model import Submission, CampaignStage, Experimenter, ExperimentsExperimenters, Campaign
from .utc import utc
from .SAMSpecifics import sam_specifics


class FilesStatus:
    """
        File related routines
    """

    # h3. __init__
    def __init__(self, ps):
        """ just hook it in """
        self.poms_service = ps

    # h3. campaign_task_files
    def campaign_task_files(self, ctx, campaign_stage_id=None, campaign_id=None):
        """
            Report of file counts for campaign stage with links to details
        """
        (tmin, tmax, tmins, tmaxs, nextlink, prevlink, time_range_string, tdays) = self.poms_service.utilsPOMS.handle_dates(
            ctx,
            "campaign_task_files/%s/%s?campaign_stage_id=%s&campaign_id=%s&"
            % (ctx.experiment, ctx.role, campaign_stage_id, campaign_id),
        )

        # inhale all the campaign related task info for the time window
        # in one fell swoop

        q = (
            ctx.db.query(Submission)  #
            .options(joinedload(Submission.campaign_stage_snapshot_obj))
            .filter(Submission.created >= tmin, Submission.created < tmax)
        )

        if campaign_stage_id not in ["", None, "None"]:
            q = q.filter(Submission.campaign_stage_id == campaign_stage_id)

        elif campaign_id not in ["", None, "None"]:
            q = q.join(CampaignStage, Submission.campaign_stage_id == CampaignStage.campaign_stage_id).filter(
                CampaignStage.campaign_id == campaign_id
            )
        else:
            return {}, {}, [], tmins, tmaxs, prevlink, nextlink, tdays

        tl = q.all()
        #
        # either get the campaign obj from above, or if we didn's
        # find any submissions in that window, look it up
        #
        if tl:
            cs = tl[0].campaign_stage_snapshot_obj.campaign_stage
            # cs = tl[0].campaign_stage_snapshot_obj
        else:
            if campaign_id not in ["", None, "None"]:
                cs = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_id == campaign_id).first()
            elif campaign_stage_id not in ["", None, "None"]:
                cs = ctx.db.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
            else:
                raise KeyError("need campaign_stage_id or campaign_id")
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
        ) = sam_specifics(ctx).get_file_stats_for_submissions(tl, cs.experiment)

        columns = [
            "campign<br>stage",
            "submission<br>jobsub_jobid",
            "project",
            "dataset",
            "date",
            "available<br>output",
            "submit-<br>ted",
            "deliv-<br>ered<br>SAM",
            "unknown<br>SAM",
            "con-<br>sumed",
            "failed",
            "skipped",
            "w/some kids<br>declared",
            "w/all kids<br>declared",
            "w/kids<br>located",
            "pending",
        ]

        listfiles = "../../show_dimension_files/%s/%s?dims=%%s" % (cs.experiment, ctx.role)
        datarows = deque()
        i = -1
        for s in tl:
            logit.log("task %d" % s.submission_id)
            i = i + 1
            psummary = summary_list[i]
            partpending = psummary.get("files_in_snapshot", 0) - some_kids_list[i]
            # pending = psummary.get('files_in_snapshot', 0) - all_kids_list[i]
            pending = partpending

            task_jobsub_job_id = s.jobsub_job_id
            if task_jobsub_job_id is None:
                task_jobsub_job_id = "s%s" % s.submission_id
            datarows.append(
                [
                    [
                        s.campaign_stage_obj.name,
                        "../../campaign_stage_info/%s/%s?campaign_stage_id=%s" % (ctx.experiment, ctx.role, s.campaign_stage_id),
                    ],
                    [
                        task_jobsub_job_id.replace("@", "@<br>"),
                        "../../submission_details/%s/%s/?submission_id=%s" % (ctx.experiment, ctx.role, s.submission_id),
                    ],
                    [
                        s.project,
                        "http://samweb.fnal.gov:8480/station_monitor/%s/stations/%s/projects/%s"
                        % (cs.experiment, cs.experiment, s.project),
                    ],
                    [s.submission_params and s.submission_params.get("dataset", "-") or "-"],
                    [s.created.strftime("%Y-%m-%d %H:%M"), None],
                    [output_list[i], listfiles % output_files[i]],
                    [psummary.get("files_in_snapshot", 0), listfiles % base_dim_list[i]],
                    [
                        "%d"
                        % (
                            psummary.get("tot_consumed", 0)
                            + psummary.get("tot_failed", 0)
                            + psummary.get("tot_skipped", 0)
                            + psummary.get("tot_delivered", 0)
                        ),
                        listfiles % (base_dim_list[i] + " and consumed_status consumed,failed,skipped,delivered "),
                    ],
                    ["%d" % psummary.get("tot_unknown", 0), listfiles % base_dim_list[i] + " and consumed_status unknown"],
                    [psummary.get("tot_consumed", 0), listfiles % base_dim_list[i] + " and consumed_status consumed"],
                    [psummary.get("tot_failed", 0), listfiles % base_dim_list[i] + " and consumed_status failed"],
                    [psummary.get("tot_skipped", 0), listfiles % base_dim_list[i] + " and consumed_status skipped"],
                    [some_kids_decl_list[i], listfiles % some_kids_decl_needed[i]],
                    [all_kids_decl_list[i], listfiles % all_kids_decl_needed[i]],
                    [some_kids_list[i], listfiles % some_kids_needed[i]],
                    [pending, listfiles % (base_dim_list[i] + " minus ( %s ) " % all_kids_decl_needed[i])],
                ]
            )
        return cs, columns, datarows, tmins, tmaxs, prevlink, nextlink, tdays

    # h3. show_dimension_files
    def show_dimension_files(self, ctx, dims):

        try:
            flist = sam_specifics(ctx).list_files(dims)
        except ValueError:
            flist = deque()
        return flist

    # h3. get_file_upload_path
    def get_file_upload_path(self, ctx, filename):
        return "%s/uploads/%s/%s/%s" % (ctx.config_get("base_uploads_dir"), ctx.experiment, ctx.username, filename)

    # h3. file_uploads
    def file_uploads(self, ctx, checkuser=None):
        ckuser = ctx.username
        if checkuser is not None:
            save = ctx.username
            ctx.username = checkuser
        flist = glob.glob(self.get_file_upload_path(ctx, "*"))
        if checkuser is not None:
            ctx.username = save
        file_stat_list = []
        total = 0
        for fname in flist:
            statout = os.stat(fname)
            uploaded = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(statout.st_mtime))
            file_stat_list.append([os.path.basename(fname), statout.st_size, uploaded])
            total += statout.st_size
        experimenters = (
            ctx.db.query(Experimenter, ExperimentsExperimenters)  #
            .join(ExperimentsExperimenters.experimenter_obj)
            .filter(ExperimentsExperimenters.experiment == ctx.experiment)
        ).all()
        quota = ctx.config_get("base_uploads_quota", 10485760)
        return file_stat_list, total, experimenters, quota

    # h3. download_file

    def download_file(self, ctx, filename):
        fname = self.get_file_upload_path(ctx, filename)
        f = open(fname, "r")
        data = f.read()
        f.close()
        return data

    # h3. upload_file
    def upload_file(self, ctx, filename):
        logit.log("upload_file: entry")

        quota = int(ctx.config_get("base_uploads_quota"))
        logit.log("upload_file: quota: %d" % quota)
        # if they pick multiple files, we get a list, otherwise just one
        # item, so if its not a list, make it a list of one item...
        if not isinstance(filename, list):
            filenames = [filename]
        else:
            filenames = filename

        logit.log("upload_file: files: %d" % len(filenames))

        for filename in filenames:
            logit.log("upload_file: filename: %s" % filename.filename)
            outf = self.get_file_upload_path(ctx, filename.filename)
            logit.log("upload_file: outf: %s" % outf)
            os.makedirs(os.path.dirname(outf), exist_ok=True)
            f = open(outf, "wb")
            size = 0
            while True:
                data = filename.file.read(8192)
                if not data:
                    break
                f.write(data)
                size += len(data)
            f.close()
            logit.log("upload_file: closed")
            fstatlist, total, experimenters, q = self.file_uploads(ctx)
            if total > quota:
                os.unlink(outf)
                raise ValueError("Upload exeeds quota of %d kbi" % (quota / 1024))

    # h3. remove_uploaded_files
    def remove_uploaded_files(self, ctx, filename, action=None, user=None):
        # if there's only one entry the web page will not send a list...
        if isinstance(filename, str):
            filename = [filename]

        for f in filename:
            outf = self.get_file_upload_path(ctx, f)
            try:
                os.unlink(outf)
            except FileNotFoundError:
                pass
        return "Ok."

    # h3. get_launch_sandbox
    def get_launch_sandbox(self, ctx):

        uploads = self.get_file_upload_path(ctx, "")
        uu = uuid.uuid4()  # random uuid -- shouldn't be guessable.
        sandbox = "%s/sandboxes/%s" % (ctx.config_get("base_uploads_dir"), str(uu))
        os.makedirs(sandbox, exist_ok=False)
        upload_path = self.get_file_upload_path(ctx, "*")
        logit.log("get_launch_sandbox linking items from upload_path %s into %s" % (upload_path, sandbox))
        flist = glob.glob(upload_path)
        for f in flist:
            os.link(f, "%s/%s" % (sandbox, os.path.basename(f)))
        return sandbox

    # h3. list_launch_file
    def list_launch_file(self, ctx, campaign_stage_id, fname, login_setup_id=None):
        """
            get launch output file and return the lines as a list
        """
        if campaign_stage_id and campaign_stage_id != 'None':
            q = (
                ctx.db.query(CampaignStage, Campaign)
                .filter(CampaignStage.campaign_stage_id == campaign_stage_id)
                .filter(CampaignStage.campaign_id == Campaign.campaign_id)
                .first()
            )
            campaign_name = q.Campaign.name
            stage_name = q.CampaignStage.name
        else:
            campaign_name = "-"
            stage_name = "-"
        if login_setup_id:
            dirname = "{}/private/logs/poms/launches/template_tests_{}".format(os.environ["HOME"], login_setup_id)
        else:
            dirname = "{}/private/logs/poms/launches/campaign_{}".format(os.environ["HOME"], campaign_stage_id)
        lf = open("{}/{}".format(dirname, fname), "r", encoding="utf-8", errors="replace")
        sb = os.fstat(lf.fileno())
        lines = lf.readlines()
        lf.close()
        # if file is recent set refresh to watch it
        if (time.time() - sb[8]) < 5:
            refresh = 3
        elif (time.time() - sb[8]) < 30:
            refresh = 10
        else:
            refresh = 0
        return lines, refresh, campaign_name, stage_name
