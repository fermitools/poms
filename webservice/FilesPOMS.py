#!/usr/bin/env python
"""

 This module contain the methods that handle the file status accounting
 List of methods: def list_task_logged_files, campaign_task_files, job_file_list, get_inflight,
 inflight_files, show_dimension_files, campaign_sheet
 ALso now includes file upload and analysis user launch sandbox code.
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
from .poms_model import Submission, CampaignStage, Experimenter, ExperimentsExperimenters
from .utc import utc
from .pomscache import pomscache


class FilesStatus:
    """
        File related routines
    """

    def __init__(self, ps):
        """ just hook it in """
        self.poms_service = ps

    def campaign_task_files(self, dbhandle, samhandle, experiment, role, campaign_stage_id=None,
                            campaign_id=None, tmin=None, tmax=None, tdays=1):
        '''
            Report of file counts for campaign stage with links to details
        '''
        (tmin, tmax,
         tmins, tmaxs,
         nextlink, prevlink,
         time_range_string, tdays
        ) = self.poms_service.utilsPOMS.handle_dates(
            tmin, tmax, tdays,
            'campaign_task_files/%s/%s?campaign_stage_id=%s&campaign_id=%s&' %(experiment, role, campaign_stage_id, campaign_id))

        # inhale all the campaign related task info for the time window
        # in one fell swoop

        q = (dbhandle.query(Submission)
             .options(joinedload(Submission.campaign_stage_snapshot_obj))
             .filter(Submission.created >= tmin, Submission.created < tmax))

        if campaign_stage_id not in ['', None, 'None']:
            q = q.filter(Submission.campaign_stage_id == campaign_stage_id)

        elif campaign_id not in ['', None, 'None']:
            q = (q.join(CampaignStage,
                        Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
                 .filter(CampaignStage.campaign_id == campaign_id))
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
            cs = dbhandle.query(CampaignStage).filter(
                CampaignStage.campaign_stage_id == campaign_stage_id).first()
            # cs = cs  # this is klugy -- does this work?
        #
        # fetch needed data in tandem
        # -- first build lists of stuff to fetch
        #
        base_dim_list = deque()
        summary_needed = deque()
        some_kids_needed = deque()
        some_kids_decl_needed = deque()
        all_kids_needed = deque()
        all_kids_decl_needed = deque()
        output_files = deque()
        # finished_flying_needed = deque()
        for s in tl:
            summary_needed.append(s)
            basedims = "snapshot_for_project_name %s " % s.project
            base_dim_list.append(basedims)

            somekiddims = "%s and isparentof: (version %s)" % (
                basedims, s.campaign_stage_snapshot_obj.software_version)
            some_kids_needed.append(somekiddims)

            somekidsdecldims = ("%s and isparentof: (version %s with availability anylocation )" %
                                (basedims, s.campaign_stage_snapshot_obj.software_version))
            some_kids_decl_needed.append(somekidsdecldims)

            allkiddecldims = basedims
            allkiddims = basedims
            for pat in str(
                    s.job_type_snapshot_obj.output_file_patterns).split(','):
                if pat == 'None':
                    pat = '%'
                if pat.find(' ') > 0:
                    dimbits = pat
                else:
                    dimbits = "file_name like '%s'" % pat

                allkiddims = ("%s and isparentof: ( %s and version '%s' ) " %
                              (allkiddims, dimbits,
                               s.campaign_stage_snapshot_obj.software_version))
                cdate = s.created.strftime("%Y-%m-%dT%H:%M:%S%z")
                allkiddecldims = ("%s and isparentof: "
                                  "( %s and version '%s' "
                                  "and create_date > '%s' "
                                  "with availability anylocation ) " %
                                  (allkiddecldims, dimbits,
                                   s.campaign_stage_snapshot_obj.software_version, cdate))
                outputfiledims = ("ischildof: ( %s ) and create_date > '%s' and  %s and version '%s'" %
                                  (basedims, s.created.strftime('%Y-%m-%d %H:%M:%S'), dimbits, s.campaign_stage_snapshot_obj.software_version))
            all_kids_needed.append(allkiddims)
            all_kids_decl_needed.append(allkiddecldims)
            output_files.append(outputfiledims)
        #
        # -- now call parallel fetches for items
        # samhandle = cherrypy.request.samweb_lite ####IMPORTANT
        summary_list = samhandle.fetch_info_list(summary_needed, dbhandle=dbhandle)
        output_list = samhandle.count_files_list(cs.experiment, output_files)
        some_kids_list = samhandle.count_files_list(cs.experiment, some_kids_needed)
        some_kids_decl_list = samhandle.count_files_list(cs.experiment, some_kids_decl_needed)
        all_kids_decl_list = samhandle.count_files_list(cs.experiment, all_kids_decl_needed)
        # all_kids_list = samhandle.count_files_list(cs.experiment, all_kids_needed)

        columns = ["campign<br>stage",
                   "submission<br>jobsub_jobid",
                   "project",
                   "date",
                   "available<br>output",
                   "submit-<br>ted",
                   "deliv-<br>ered<br>SAM",
                   "unknown<br>SAM",
                   "con-<br>sumed", "failed", "skipped",
                   "w/some kids<br>declared",
                   "w/all kids<br>declared",
                   "w/kids<br>located",
                   "pending"]

        listfiles = "../../show_dimension_files/%s/%s?dims=%%s" % (cs.experiment, role)
        datarows = deque()
        i = -1
        for s in tl:
            logit.log("task %d" % s.submission_id)
            i = i + 1
            psummary = summary_list[i]
            partpending = psummary.get(
                'files_in_snapshot', 0) - some_kids_list[i]
            #pending = psummary.get('files_in_snapshot', 0) - all_kids_list[i]
            pending = partpending

            task_jobsub_job_id = s.jobsub_job_id
            if task_jobsub_job_id is None:
                task_jobsub_job_id = "s%s" % s.submission_id
            datarows.append(
                [
                    [s.campaign_stage_obj.name, "../../campaign_stage_info/%s/%s?campaign_stage_id=%s" % (experiment, role, s.campaign_stage_id)],
                    [task_jobsub_job_id.replace('@', '@<br>'),
                     "../../submission_details/%s/%s/?submission_id=%s" % (experiment, role, s.submission_id)],
                    [
                        s.project,
                        "http://samweb.fnal.gov:8480/station_monitor/%s/stations/%s/projects/%s" %
                        (cs.experiment,
                         cs.experiment,
                         s.project)],
                    [s.created.strftime("%Y-%m-%d %H:%M"), None],
                    [output_list[i], listfiles % output_files[i]],
                    [psummary.get('files_in_snapshot', 0),
                     listfiles % base_dim_list[i]],
                    ["%d" % (psummary.get('tot_consumed', 0) +
                             psummary.get('tot_failed', 0) +
                             psummary.get('tot_skipped', 0) +
                             psummary.get('tot_delivered', 0)),
                     listfiles % base_dim_list[i] +
                     " and consumed_status consumed,failed,skipped,delivered "],
                    ["%d" % psummary.get('tot_unknown', 0),
                     listfiles % base_dim_list[i] + " and consumed_status unknown"],
                    [psummary.get('tot_consumed', 0), listfiles %
                     base_dim_list[i] +
                     " and consumed_status consumed"],
                    [psummary.get('tot_failed', 0), listfiles %
                     base_dim_list[i] +
                     " and consumed_status failed"],
                    [psummary.get('tot_skipped', 0), listfiles %
                     base_dim_list[i] +
                     " and consumed_status skipped"],
                    [some_kids_decl_list[i], listfiles %
                     some_kids_needed[i]],
                    [all_kids_decl_list[i], listfiles %
                     some_kids_decl_needed[i]],
                    [all_kids_decl_list[i], listfiles %
                     all_kids_decl_needed[i]],
                    [pending, listfiles %
                     base_dim_list[i] + "minus ( %s ) " %
                     all_kids_decl_needed[i]],
                ])
        return cs, columns, datarows, tmins, tmaxs, prevlink, nextlink, tdays

    def show_dimension_files(self, samhandle, experiment, dims, dbhandle=None):

        try:
            flist = samhandle.list_files(experiment, dims, dbhandle=dbhandle)
        except ValueError:
            flist = deque()
        return flist

    @pomscache.cache_on_arguments()
    def get_pending_dict_for_campaigns(
            self, dbhandle, samhandle, campaign_id_list, tmin, tmax):
        if isinstance(campaign_id_list, str):
            campaign_id_list = [
                cid for cid in campaign_id_list.split(',') if cid]
        dl, cl = self.get_pending_for_campaigns(
            dbhandle, samhandle, campaign_id_list, tmin, tmax)
        res = {cid: cs for cid, cs in zip(campaign_id_list, cl)}
        logit.log("get_pending_dict_for_campaigns returning: " + repr(res))
        return res

    def get_pending_for_campaigns(
            self, dbhandle, samhandle, campaign_id_list, tmin, tmax):

        task_list_list = deque()

        logit.log(
            "in get_pending_for_campaigns, tmin %s tmax %s" %
            (tmin, tmax))
        if isinstance(campaign_id_list, str):
            campaign_id_list = [
                cid for cid in campaign_id_list.split(',') if cid]

        task_list = (dbhandle.query(Submission).
                     options(joinedload(Submission.campaign_stage_snapshot_obj)).
                     options(joinedload(Submission.job_type_snapshot_obj)).
                     filter(Submission.campaign_stage_id.in_(campaign_id_list),
                            Submission.created >= tmin, Submission.created < tmax).
                     all())

        # To prepare the list of task lists
        tll = defaultdict(lambda: [])
        # Group submissions by campaign ids
        for task in task_list:
            tll[task.campaign_stage_id].append(task)
        # Build the list of task lists in original campaign order
        task_list_list = [tll[int(ci)] for ci in campaign_id_list]
        # logit.log("get_pending_for_campaigns: task_list_list (%d): %s" % (len(task_list_list), task_list_list))

        dl, cl = self.get_pending_for_task_lists(
            dbhandle, samhandle, task_list_list)

        return dl, cl

    @staticmethod
    def get_pending_dims_for_task_lists(dbhandle, samhandle, task_list_list):
        reason = 'no_project_info'
        now = datetime.now(utc)
        twodays = timedelta(days=2)
        dimlist = deque()
        explist = deque()
        # experiment = None
        logit.log(
            "get_pending_for_task_lists: task_list_list (%d): %s" %
            (len(task_list_list), task_list_list))
        for tl in task_list_list:
            diml = ["("]
            for task in tl:

                if task.project is None or task.status == 'Located':
                    if task.status == 'Located':
                        reason = 'all_located'
                        fakename = 'located'
                    else:
                        fakename = 'no_proj'
                    # no project/ old projects have no counts, so short-circuit
                    diml.append("(file_name __%s__ )" % fakename)
                    diml.append("union")
                    continue

                diml.append("(snapshot_for_project_name %s" % task.project)
                diml.append(
                    "minus ( snapshot_for_project_name %s and (" %
                    task.project)
                sep = ""
                for pat in str(
                        task.job_type_snapshot_obj.output_file_patterns).split(','):
                    if pat == "None":
                        pat = "%"
                    diml.append(sep)
                    diml.append("isparentof: ( file_name '%s' and version '%s' with availability physical )" %
                                (pat, task.campaign_stage_snapshot_obj.software_version))
                    sep = "or"
                diml.append(")")
                diml.append(")")
                diml.append(")")
                diml.append("union")

            diml[-1] = ")"

            if len(diml) <= 1:
                diml[0] = "project_name %s" % reason

            dimlist.append(" ".join(diml))

            if tl:
                explist.append(tl[0].job_type_snapshot_obj.experiment)
            else:
                explist.append("samdev")

        logit.log(
            "get_pending_for_task_lists: dimlist (%d): %s" %
            (len(dimlist), dimlist))
        return explist, dimlist

    def get_pending_for_task_lists(self, dbhandle, samhandle, task_list_list):
        explist, dimlist = self.get_pending_dims_for_task_lists(
            dbhandle, samhandle, task_list_list)
        count_list = samhandle.count_files_list(explist, dimlist)
        logit.log(
            "get_pending_for_task_lists: count_list (%d): %s" %
            (len(dimlist), count_list))

        return dimlist, count_list

    def get_file_upload_path(self, basedir, username, experiment, filename):
        return "%s/uploads/%s/%s/%s" % (basedir, experiment, username, filename)

    def file_uploads(self, basedir, experiment, user, dbhandle, checkuser=None):
        ckuser = user
        if checkuser is not None:
            ckuser = checkuser
        flist = glob.glob(self.get_file_upload_path(basedir, ckuser, experiment, '*'))
        file_stat_list = []
        total = 0
        for fname in flist:
            statout = os.stat(fname)
            uploaded = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(statout.st_mtime))
            file_stat_list.append([os.path.basename(fname), statout.st_size, uploaded])
            total += statout.st_size
        experimenters = (dbhandle.query(Experimenter, ExperimentsExperimenters)
                         .join(ExperimentsExperimenters.experimenter_obj)
                         .filter(ExperimentsExperimenters.experiment == experiment)
                        ).all()
        return file_stat_list, total, experimenters


    def upload_file(self, basedir, experiment, username, quota, filename, dbhandle):
        logit.log("upload_file: entry")

        # if they pick multiple files, we get a list, otherwise just one
        # item, so if its not a list, make it a list of one item...
        if not isinstance(filename, list):
            filenames = [filename]
        else:
            filenames = filename

        logit.log("upload_file: files: %d" % len(filenames))

        for filename in filenames:
            logit.log("upload_file: filename: %s" % filename.filename)
            outf = self.get_file_upload_path(basedir, username, experiment, filename.filename)
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
            fstatlist, total, experimenters = self.file_uploads(basedir, experiment, username, dbhandle)
            if total > quota:
                os.unlink(outf)
                raise ValueError("Upload exeeds quota of %d kbi" % quota/1024)


    def remove_uploaded_files(self, basedir, experiment, username, filename):
        # if there's only one entry the web page will not send a list...
        if isinstance(filename, str):
            filename = [filename]

        for f in filename:
            outf = self.get_file_upload_path(basedir, username, experiment, f)
            os.unlink(outf)

    def get_launch_sandbox(self, basedir, username, experiment):

        uploads = self.get_file_upload_path(basedir, username, experiment, '')
        uu = uuid.uuid4()  # random uuid -- shouldn't be guessable.
        sandbox = "%s/sandboxes/%s" % (basedir, str(uu))
        os.makedirs(sandbox, exist_ok=False)
        upload_path = self.get_file_upload_path(basedir, username, experiment, '*')
        logit.log("get_launch_sandbox linking items from upload_path %s into %s" % (upload_path, sandbox))
        flist = glob.glob(upload_path)
        for f in flist:
            os.link(f, "%s/%s" % (sandbox, os.path.basename(f)))
        return sandbox
