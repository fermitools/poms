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
import re
import json
import glob
import uuid
import base64
from filelock import FileLock
from datetime import datetime, timedelta
import time
from sqlalchemy.orm import joinedload

from . import logit
from .poms_model import Submission, CampaignStage, Experimenter, ExperimentsExperimenters, Campaign, DataDispatcherSubmission
from .utc import utc
from .SAMSpecifics import sam_specifics
import shutil

import toml
from urllib.parse import unquote
from cryptography.fernet import Fernet
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
        clear_campaign_column = False

        q = (
            ctx.db.query(Submission)  #
            .options(joinedload(Submission.campaign_stage_snapshot_obj))
            .filter(Submission.created >= tmin, Submission.created < tmax)
        )

        if campaign_stage_id not in ["", None, "None"]:
            q = q.filter(Submission.campaign_stage_id == campaign_stage_id)

            clear_campaign_column = True
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
            
        data_handling_service = cs.campaign_obj.data_handling_service
        if data_handling_service == "sam":
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
                "cancelled",
                "failed",
                "skipped",
                "w/some kids<br>declared",
                "w/all kids<br>declared",
                "w/kids<br>located",
                "pending",
            ]

            if clear_campaign_column:
                columns = columns[1:]

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
                row = [
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
                        "%s/station_monitor/%s/stations/%s/projects/%s"
                        % (ctx.web_config.get("SAM", "sam_base"), cs.experiment, cs.experiment, s.project),
                    ],
                    [s.submission_params and s.submission_params.get("dataset", "-") or "-"],
                    [s.created.strftime("%Y-%m-%d %H:%M"), None],
                    [output_list[i], listfiles % output_files[i]],
                    [psummary.get("files_in_snapshot", 0), listfiles % base_dim_list[i]],
                    [
                        "%d"
                        % (
                            psummary.get("tot_consumed", 0)
                            + psummary.get("tot_cancelled", 0)
                            + psummary.get("tot_failed", 0)
                            + psummary.get("tot_skipped", 0)
                            + psummary.get("tot_delivered", 0)
                        ),
                        listfiles % (base_dim_list[i] + " and consumed_status consumed,completed,failed,skipped,delivered "),
                    ],
                    ["%d" % psummary.get("tot_unknown", 0), listfiles % base_dim_list[i] + " and consumed_status unknown"],
                    [psummary.get("tot_consumed", 0), listfiles % base_dim_list[i] + " and consumed_status co%"],
                    [psummary.get("tot_cancelled", 0), listfiles % base_dim_list[i] + " and consumed_status cancelled"],
                    [psummary.get("tot_failed", 0), listfiles % base_dim_list[i] + " and consumed_status failed"],
                    [psummary.get("tot_skipped", 0), listfiles % base_dim_list[i] + " and consumed_status skipped"],
                    [some_kids_decl_list[i], listfiles % some_kids_decl_needed[i]],
                    [all_kids_decl_list[i], listfiles % all_kids_decl_needed[i]],
                    [some_kids_list[i], listfiles % some_kids_needed[i]],
                    [pending, listfiles % (base_dim_list[i] + " minus ( %s ) " % all_kids_decl_needed[i])],
                ]
                if clear_campaign_column:
                    row = row[1:]
                datarows.append(row)
                
        elif data_handling_service == "data_dispatcher":
            #dd_submissions = ctx.db.query(DataDispatcherSubmission).filter(DataDispatcherSubmission.archive == False,
            #        DataDispatcherSubmission.experiment == cs.experiment,
            #        DataDispatcherSubmission.submission_id.in_([sub.submission_id for sub in tl])
            #    ).all()
            dd_submissions =  (ctx.db.query(
                    DataDispatcherSubmission.data_dispatcher_project_idx.label("data_dispatcher_project_idx"),
                    DataDispatcherSubmission.project_id.label("project_id"),
                    DataDispatcherSubmission.project_id.label("project_name"),
                    DataDispatcherSubmission.created.label("created"),
                    DataDispatcherSubmission.submission_id.label("submission_id"),
                    DataDispatcherSubmission.named_dataset.label("named_dataset"),
                    DataDispatcherSubmission.jobsub_job_id.label("jobsub_job_id"),
                    CampaignStage.name.label("campaign_stage_name"), 
                    CampaignStage.campaign_stage_id.label("campaign_stage_id"), 
                    CampaignStage.output_ancestor_depth.label("output_ancestor_depth"),
                )
                .join(DataDispatcherSubmission.campaign_stage_obj)
                .join(DataDispatcherSubmission.submission_obj)
                .filter(DataDispatcherSubmission.submission_id.in_([sub.submission_id for sub in tl]))
                .all())
            
            submission_info = ctx.dmr_service.get_file_stats_for_submissions(dd_submissions)
            
            columns = [
                "campign<br>stage",
                "submission<br>jobsub_jobid",
                "project_id",
                "project name",
                "date",
                "percent<br>complete",
                "available<br>output",
                "submitted",
                "not submitted",
                "done",
                "failed",
                "reserved",
                "not located",
                "files in<br>project",
                "parents",
                "children",
            ]

            if clear_campaign_column:
                columns = columns[1:]

            datarows = deque()
            for s in dd_submissions:
                logit.log("task %d" % s.submission_id)
                task_jobsub_job_id = s.jobsub_job_id or None
                if task_jobsub_job_id is None:
                    task_jobsub_job_id = "s%s" % s.submission_id
                details = submission_info.get(s.submission_id, None)
                if "project_id" in details:
                    listfiles = "../../show_dimension_files/%s/%s?project_id=%s" % (cs.experiment, ctx.role, details["project_id"])
                else:
                    listfiles = "../../show_dimension_files/%s/%s?project_idx=%s" % (cs.experiment, ctx.role, details["project_idx"])
                if details:
                    row = [
                        [
                            s.campaign_stage_name,
                            "../../campaign_stage_info/%s/%s?campaign_stage_id=%s" % (ctx.experiment, ctx.role, s.campaign_stage_id),
                        ],
                        [ 
                            task_jobsub_job_id.replace("@", "@<br>"),
                            "../../submission_details/%s/%s/?submission_id=%s" % (ctx.experiment, ctx.role, s.submission_id),
                        ],
                        [ #TODO: is there a monitoring page?
                            s.project_id,listfiles
                        ],
                        [s.project_name or "N/A", listfiles + "&querying=all&mc_query=%s" % details["total"]],
                        [s.created.strftime("%Y-%m-%d %H:%M"), None],
                        [details["statistics"].get("pct_complete", "0%"), listfiles + "&querying=children&mc_query=%s" % details["children"]],
                        [details["statistics"].get("children", 0), listfiles + "&querying=output&mc_query=%s" % details["children"]],
                        [details["statistics"].get("submitted", 0), listfiles + "&querying=submitted&mc_query=%s" % details["submitted"]],
                        [details["statistics"].get("initial", 0), listfiles + "&querying=initial&mc_query=%s" % details["initial"]],
                        [details["statistics"].get("done", 0), listfiles + "&querying=done&mc_query=%s" % details["done"]],
                        [details["statistics"].get("failed", 0), listfiles + "&querying=failed&mc_query=%s" % details["failed"]],
                        [details["statistics"].get("reserved", 0), listfiles + "&querying=reserved&mc_query=%s" % details["reserved"]],
                        [details["statistics"].get("unknown", 0), listfiles + "&querying=unknown&mc_query=%s" % details["unknown"]],
                        [details["statistics"].get("total", 0), listfiles + "&querying=all&mc_query=%s" % details["total"]],
                        [details["statistics"].get("parents", 0), listfiles + "&querying=parents&mc_query=%s" %  details["parents"]],
                        [details["statistics"].get("children", 0), listfiles + "&querying=children&mc_query=%s" % details["children"]],
                    ]
            

                    if clear_campaign_column:
                        row = row[1:]
                    datarows.append(row)



        return cs, columns, datarows, tmins, tmaxs, prevlink, nextlink, tdays

    # h3. show_dimension_files
    def show_dimension_files(self, ctx, dims=None, project_id=None,project_idx=None,mc_query=None, querying=None):
        try:
            flist = None
            fdict = None
            data_handler = "sam"
            info = ""
            if querying and isinstance (querying, str):
                querying = querying.capitalize()
            if dims:
                flist = sam_specifics(ctx).list_files(dims)
            else:
                info = "%s files in project_id%s" % (querying, ": %s<br><code class='query-code' id='queryText'>%s</code><br><button id='copyButton'>Copy to Clipboard</button>" % (project_id, unquote(mc_query)) if project_id else "x:%s<br><code class='query-code' id='queryText'>%s</code><br><button id='copyButton'>Copy to Clipboard</button>" % (project_idx, unquote(mc_query)))
            if project_id:
                data_handler = "data_dispatcher"
                fdict, did_all = ctx.dmr_service.list_file_urls(project_id=project_id, mc_query=mc_query)
                if did_all:
                    info = "'%s' files <br><code class='query-code' id='queryText'>%s</code><br><button id='copyButton'>Copy to Clipboard</button><br>Returned with zero results. Displaying All" % (querying, unquote(mc_query))
            elif project_idx:
                data_handler = "data_dispatcher"
                fdict, did_all = ctx.dmr_service.list_file_urls(project_idx=project_idx, mc_query=mc_query)
                if did_all:
                    info = "'%s' files <br><code class='query-code' id='queryText'>%s</code><br><button id='copyButton'>Copy to Clipboard</button><br>Returned with zero results. Displaying All" % (querying, unquote(mc_query))
        except ValueError:
            flist = deque()
        return flist, data_handler, fdict, info

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
            print("upload_file: filename: %s" % filename.filename)
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

        try:
            #uploads = self.get_file_upload_path(ctx, "")
            uu = uuid.uuid4()  # random uuid -- shouldn't be guessable.
            sandbox = "%ssandboxes/%s" % (ctx.config_get("base_uploads_dir"), str(uu))
            os.makedirs(sandbox, exist_ok=False)
            upload_path = self.get_file_upload_path(ctx, "*")
            logit.log("get_launch_sandbox linking items from upload_path %s into %s" % (upload_path, sandbox))
            flist = glob.glob(upload_path)
            for f in flist:
                os.system("chmod +rw %s" %f)
                os.link(f, "%s/%s" % (sandbox, os.path.basename(f)))
            return sandbox
        except Exception as e: 
            logit.log("get_launch_sandbox failed to link items from upload_path %s into the sandbox: %s" % (upload_path, repr(e)))
            return ctx.config_get("base_uploads_dir")
            
            
            
    def is_date_older_than_six_months(self, date_string):
        # Convert the date string to a datetime object
        date_format = "%Y%m%d_%H%M%S"
        date = datetime.strptime(date_string, date_format)
        six_months_ago = datetime.now() - timedelta(days=6*30)
        if date < six_months_ago:
            return True
        else:
            return False

    
    def refactor_launch_directory(self, ctx):
        final_destinations = {}
        submission_paths = {}
        submission_dates = {}
        #files_to_remove = []
        doesnt_satisfy = []
        for root, dirs, ignore in os.walk("/home/poms/private/logs/poms/launches"):
            for dir in dirs:
                if "_" in dir:
                    for pwd, subdirs, files in os.walk(os.path.join(root, dir)):
                        for file in files:
                            splitFile = file.split("_")
                            if len(splitFile) < 3:
                                doesnt_satisfy.append(os.path.join(pwd, file))
                            #elif len(splitFile) > 1 and self.is_date_older_than_six_months("%s_%s" % (splitFile[0], splitFile[1])):
                            #    files_to_remove.append(file)
                            else:
                                if len(splitFile) == 4:
                                    submission_paths[splitFile[3]] = os.path.join(pwd, file)
                                    submission_dates[splitFile[3]] = datetime.strptime("%s_%s" % (splitFile[0], splitFile[1]), "%Y%m%d_%H%M%S")
        
        results = (
                ctx.db.query(Submission)
                #.filter(CampaignStage.campaign_stage_id in campaign_stage_submissions.keys())
                .filter(Submission.submission_id.in_(list(submission_dates.keys())))
                .all()
            )
        result_ids = {}
        for result in results:
            submission_id = '%s' % result.submission_id
            result_ids[submission_id] = submission_id in submission_dates
            # {{date_of_submission}}/{{experiment}}/{{campaign_id}}/{{campaign_stage_id}}/{{submission_id}}_{{submission_timestamp}}
            if submission_id in submission_dates and submission_id in submission_paths:
                # Create the new directory if it does not yet exist
                directory = "/home/poms/private/logs/poms/launches/%s/%s/%s/%s" % (
                    submission_dates[submission_id].date(), # Date of creation
                    result.campaign_stage_obj.experiment, # Name of experiment
                    result.campaign_stage_obj.campaign_id, # Campaign ID
                    result.campaign_stage_id, # Campaign Stage ID
                )
                # Create the directory
                try:
                     shutil.rmtree(directory)
                except:
                    pass
                os.makedirs(directory)
                # ensures correct new path for old files
                final_destinations[submission_paths[submission_id]] = "%s/%s_%s" % (
                    directory,
                    submission_id, # Submission ID
                    result.created.strftime('%Y%m%d_%H%M%S') # Submission Timestamp
                    )
                # Copy the file to the destination directory
                shutil.copy2(submission_paths[submission_id], final_destinations[submission_paths[submission_id]])

               
                            

    # h3. list_launch_file
    def list_launch_file(self, ctx, campaign_stage_id, fname=None, login_setup_id=None, submission_id=None):
        """
            get launch output file and return the lines as a list
        """
        #self.refactor_launch_directory(ctx)
        if campaign_stage_id and campaign_stage_id != "None":
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
        
        if submission_id:
            result:Submission = (
                ctx.db.query(Submission)
                .filter(Submission.submission_id == submission_id)
                .first()
            )
            base_dir = "{}/private/logs/poms/launches".format(os.environ["HOME"])
            directory = "%s/%s/%s/%s/%s/%s_%s" % (
                        base_dir,
                        result.created.astimezone(utc).date(), # Date of creation
                        result.campaign_stage_obj.experiment, # Name of experiment
                        result.campaign_stage_obj.campaign_id, # Campaign ID
                        result.campaign_stage_id, # Campaign Stage ID
                        result.submission_id, # Submission ID
                        result.created.astimezone(utc).strftime('%Y%m%d_%H%M%S')
                    )
            logit.log("Attempting to open log file at: %s" %directory)
            if os.path.exists(directory):
                lf = open(directory, "r", encoding="utf-8", errors="replace")
            else:
                lf = None
        else:
            result = None
            if login_setup_id:
                dirname = "{}/private/logs/poms/launches/template_tests_{}".format(os.environ["HOME"], login_setup_id)
            else:
                dirname = "{}/private/logs/poms/launches/campaign_{}".format(os.environ["HOME"], campaign_stage_id)
            lf = open("{}/{}".format(dirname, fname), "r", encoding="utf-8", errors="replace")
        
        if lf:
            file_path = lf.name
            
            sb = os.fstat(lf
                          .fileno())
            lines = lf.readlines()
            lf.close()
            # if file is recent set refresh to watch it
            if (time.time() - sb[8]) < 5:
                refresh = 3
            elif (time.time() - sb[8]) < 30:
                refresh = 10
            else:
                refresh = 0
            
            if not submission_id:
                submission_id = extract_job_info(lines, "task_id")
                result:Submission = (
                    ctx.db.query(Submission)
                    .filter(Submission.submission_id == submission_id)
                    .first()
                ) if not result and submission_id else None
                
            if result:
                if not result.jobsub_job_id:
                    job_id = extract_job_info(lines, "job_id", campaign_stage_id, submission_id)
                    if job_id:
                        self.poms_service.submissionsPOMS.update_submission(ctx, submission_id, job_id)
                        self.poms_service.submissionsPOMS.update_submission_status(ctx, submission_id, "Idle", datetime.now(utc))
                        ctx.db.commit()
                        
                        # This is a recently submitted job, we will initialize our new queue system which
                        # is monitored by the submission_agent. 
                        queue = LocalJsonQueue(ctx.web_config)
                        # Enqueue an item
                        queue.enqueue({"pomsTaskID": submission_id, "id": job_id, "group": ctx.experiment, "queued_at": str(datetime.now(utc))})
                        
                    else:
                        if len(result.status_history) < 2:
                            # Reopen after status check
                            lf = open(file_path, "r", encoding="utf-8", errors="replace")
                            status = extract_failure_status(lf)
                            if status and status == "fail":
                                last_modified_timestamp = os.path.getmtime(file_path)
                                last_modified_datetime = datetime.fromtimestamp(last_modified_timestamp, utc)
                                self.poms_service.submissionsPOMS.update_submission_status(ctx, submission_id, "LaunchFailed", last_modified_datetime)
                                ctx.db.commit()
        else:
            lines = ["No log records exist for this submission"]
            refresh = 0
        
        return lines, refresh, campaign_name, stage_name
    

def extract_job_info(lines, type="job_id", campaign_stage_id=None, submission_id=None):
    # Define the regex pattern for matching the job id
    # Adjust the pattern if the format varies
    if type == "job_id":
        job_id_pattern = r'Use job id (\d+\.0@jobsub[0-9]+\.fnal\.gov) to retrieve output'
        msg = "to extract job id from output"
        if campaign_stage_id:
            msg += f" | campaign_stage_id: {campaign_stage_id}"
        if submission_id:
            msg += f" | submission_id: {submission_id}"
        print(f"Attempting {msg}")
        try:
            job_id = None
            for line in lines:
                # Search for the job id in the file content
                match = re.search(job_id_pattern, line)
                if match:
                    # Extract the job id from the matched pattern
                    job_id = match.group(1)
                    print(f"Found job id: {job_id}")
                elif "Use job id" in line and " to retrieve output" in line:
                    print(f"Found job id: {job_id}")
                    job_id = line.replace("Use job id ", "").replace(" to retrieve output", "")
                if job_id:
                    return job_id
            print("Job id not found in the file.")
            return None
        except FileNotFoundError:
            return None
    elif type == "task_id":
        print("Attempting to extract submission_id from output")
        for line in lines:
            if  "export POMS_TASK_ID=" in line:
                submission_id = line.replace("export POMS_TASK_ID=", "").replace(";", "")
                if submission_id and submission_id.isnumeric():
                    print(f"submission_id found: {submission_id}")
                    return int(submission_id)
        print("submission_id not found")
        return None



def extract_failure_status(file):
    # Define the regex pattern to match the JSON string
    json_pattern = r'posting elasticsearch data: (\{.*?\})\s+% Total'
    complete_pattern = r'== completed: (\d+) =='
    try:
        with file:
            file_content = file.read()
            # Find all matches of JSON strings in the content
            matches_complete = re.findall(complete_pattern, file_content, re.DOTALL)
            if matches_complete:
                return "fail"
            
            matches_json = re.findall(json_pattern, file_content, re.DOTALL)
            for match in matches_json:
                # Parse the JSON string into a Python dictionary
                data = json.loads(match)
                # Check if the 'STATUS' key exists and its value indicates failure
                if 'STATUS' in data:
                    return data['STATUS'].strip().lower()  # Return True and the data dict if failure is found
            # If we get here, no failure status was found in any JSON string
            return None
    except FileNotFoundError:
        return None
    
    
class LocalJsonQueue:
    def __init__(self, cfg):
        self.queue_config = toml.load(cfg.get("POMS", "queue_file_path"))
        self.queue_path = self.queue_config["queue"]["queue_file"]
        self.queue_lock =  FileLock(self.queue_config["queue"]["queue_lock_file"])
        self.results_path =  self.queue_config["queue"]["results_file"] 
        self.results_lock =  FileLock(self.queue_config["queue"]["results_lock_file"])
        
        
    def enqueue(self, item):
        with self.queue_lock:
            if os.path.exists(self.queue_path):
                with open(self.queue_path, "r+", encoding="utf-8") as file:
                    data = json.load(file)
                    queue = data.get("queued", [])
                    queue.append(item)
                    file.seek(0)
                    json.dump(data, file, indent=4)
                    file.truncate()
            else:
                with open(self.queue_path, "w", encoding="utf-8") as file:
                    json.dump({"queued": [item]}, file)
    
    def dequeue(self):
        with self.queue_lock:
            if not os.path.exists(self.queue_path):
                return None
            with open(self.queue_path, "r+", encoding="utf-8") as file:
                data = json.load(file)
                if not data:
                    return None
                queue = data.get("queued", [])
                item = queue.pop(0) if queue and len(queue) > 0 else None
                file.seek(0)
                json.dump(data, file, indent=4)
                file.truncate()
                return item
            
    def fetch_session(self):
        file_session_data = {
            "queue": None,
            "results": None
        }
        for path, lock in { self.queue_path: self.queue_lock, self.results_path: self.results_lock}.items():
            with lock:
                if not os.path.exists(path):
                    raise AssertionError("Failed to set session info")
                try:
                    with open(path, "r+", encoding="utf-8") as file:
                        file_data = json.load(file)
                        if path == self.queue_path:
                            file_session_data["queue"] = file_data.get("_session", None)
                        elif path == self.results_path:
                            file_session_data["results"] = file_data.get("_session", None)
                        file.seek(0)
                        file.close()
                except Exception as e:
                    print("Submission Agent | set_instance | Exception: %s" % e)
                    exit(1)
        return file_session_data
    
    def authorize_agent(self, agent_header):
        keypath = self.queue_config["queue"]["agent_key"]
        host = self.queue_config["queue"]["server"]
        session = self.fetch_session()
        with open(keypath, 'rb') as keyfile:
            key = keyfile.read()
            cipher_suite = Fernet(key)
            if not (key and cipher_suite):
                return False
        
        agent_id = base64.b64decode(agent_header)
        agent_id = cipher_suite.decrypt(agent_id)
        agent_id = agent_id.decode('utf-8')
        
        secret = self.queue_config["queue"]["agent_secret"]
        evaluate = {
            "username": "submission_agent",
            "agent_id": agent_id,
            "host": host,
            "secret": secret
        }
        
        for key, val in session.items():
            
            session_info = base64.b64decode(val)
            session_info = cipher_suite.decrypt(session_info)
            session_info = session_info.decode('utf-8')
            session_info = json.loads(session_info)
            if "secret" in session_info:
                session_info["secret"] = base64.b64decode(session_info["secret"])
                session_info["secret"] = cipher_suite.decrypt(session_info["secret"])
                session_info["secret"] = session_info["secret"].decode('utf-8')
            for key, val in evaluate.items():
                if not (key in session_info and session_info[key] == val):
                    print("Failed to authorize agent")
                    return False
        
        return True
    
    