#!/usr/bin/env python
"""

 This module contain the methods that handle the file status accounting
 List of methods: def list_task_logged_files, campaign_task_files, job_file_list, get_inflight,
 inflight_files, show_dimension_files, campaign_sheet, actual_pending_files
 ALso now includes file upload and analysis user launch sandbox code.
 Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions
 in poms_service.py written by Marc Mengel, Stephen White and Michael Gueith.
 October, 2016.
"""

from collections import deque, defaultdict
import os
import shelve
import glob
import uuid
from datetime import datetime, timedelta

from sqlalchemy.orm import subqueryload, joinedload
from sqlalchemy import distinct, func

from . import logit
from .poms_model import Submission, CampaignStage
from .utc import utc
from .pomscache import pomscache


class Files_status:
    """
        File related routines
    """

    def __init__(self, ps):
        """ just hook it in """
        self.poms_service = ps

    def campaign_task_files(self, dbhandle, samhandle, campaign_stage_id=None,
                            campaign_id=None, tmin=None, tmax=None, tdays=1):
        (tmin, tmax,
         tmins, tmaxs,
         nextlink, prevlink,
         time_range_string, tdays
        ) = self.poms_service.utilsPOMS.handle_dates(
            tmin, tmax, tdays,
            'campaign_task_files?campaign_stage_id=%s&' %campaign_stage_id)

        # inhale all the campaign related task info for the time window
        # in one fell swoop

        q = (dbhandle.query(Submission)
             .options(joinedload(Submission.campaign_stage_snapshot_obj))
             .filter(Submission.created >= tmin, Submission.created < tmax))

        if campaign_stage_id:
            q = q.filter(Submission.campaign_stage_id == campaign_stage_id)

        elif campaign_id:
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
        if len(tl) > 0:
            cs = tl[0].campaign_stage_snapshot_obj
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
                allkiddecldims = ("%s and isparentof: "
                                  "( %s and version '%s' "
                                  "with availability anylocation ) " %
                                  (allkiddecldims, dimbits,
                                   s.campaign_stage_snapshot_obj.software_version))
                outputfiledims = ("ischildof: ( %s ) and create_date > '%s' and  %s and version '%s'" % 
                                   (basedims, s.created.strftime('%Y-%m-%d %H:%M:%S'), dimbits, s.campaign_stage_snapshot_obj.software_version))
            all_kids_needed.append(allkiddims)
            all_kids_decl_needed.append(allkiddecldims)
            output_files.append(outputfiledims)
        #
        # -- now call parallel fetches for items
        # samhandle = cherrypy.request.samweb_lite ####IMPORTANT
        summary_list = samhandle.fetch_info_list(summary_needed, dbhandle=dbhandle)
        output_list = samhandle.count_files_list( cs.experiment, output_files)
        some_kids_list = samhandle.count_files_list( cs.experiment, some_kids_needed)
        some_kids_decl_list = samhandle.count_files_list(cs.experiment, some_kids_decl_needed)
        all_kids_decl_list = samhandle.count_files_list(cs.experiment, all_kids_decl_needed)
        # all_kids_list = samhandle.count_files_list(cs.experiment, all_kids_needed)
        tids = [s.submission_id for s in tl]

        columns = ["submission<br>jobsub_jobid", "project", "date", 
                   "available<br>output",
                   "submit-<br>ted",
                   "deliv-<br>ered<br>SAM",
                   "unknown<br>SAM",
                   "con-<br>sumed", "failed", "skipped",
                   "w/some kids<br>declared",
                   "w/all kids<br>declared",
                   "w/kids<br>located",
                   "pending"]

        listfiles = "show_dimension_files?experiment=%s&dims=%%s" % cs.experiment
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
                    [task_jobsub_job_id.replace('@', '@<br>'),
                     "https://fermicloud045.fnal.gov/poms/submission_details?submission_id=%s" % s.submission_id],
                    [
                        s.project,
                        "http://samweb.fnal.gov:8480/station_monitor/%s/stations/%s/projects/%s" %
                        (cs.experiment,
                         cs.experiment,
                         s.project)],
                    [s.created.strftime("%Y-%m-%d %H:%M"), None],
                    [output_list[i], listfiles % output_files[i] ],
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

    def actual_pending_file_dims(
            self, dbhandle, samhandle, campaign_stage_id=None, tmin=None, tmax=None, tdays=1):
        (tmin, tmax,
         tmins, tmaxs,
         nextlink, prevlink,
         time_range_string, tdays
         ) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays,
                                                      'actual_pending_files?%s=%s&' %
                                                      ('campaign_stage_id', campaign_stage_id))

        tl = (dbhandle.query(Submission).
              options(joinedload(Submission.campaign_stage_obj)).
              options(joinedload(Submission.jobs).joinedload(Job.job_files)).
              filter(Submission.campaign_stage_id == campaign_stage_id,
                     Submission.created >= tmin, Submission.created < tmax).
              all())

        explist, dimlist = self.get_pending_dims_for_task_lists(
            dbhandle, samhandle, [tl])
        return explist, dimlist

    # maybe at the future for a  ReportsPOMS module
    def campaign_sheet(self, dbhandle, samhandle,
                       campaign_stage_id, tmin=None, tmax=None, tdays=7):

        daynames = [
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
            "Sunday"]

        (tmin, tmax,
         tmins, tmaxs,
         nextlink, prevlink,
         time_range_string, tdays
        ) = self.poms_service.utilsPOMS.handle_dates(
              tmin, tmax, tdays,
             'campaign_sheet?campaign_stage_id=%s&' % campaign_stage_id)

        el = (dbhandle.query( distinct(Job.user_exe_exit_code))
              .filter(
                  Job.updated >= tmin,
                  Job.updated <= tmax)
              .all())
        exitcodes = [e[0] for e in el]

        (experiment,) = (dbhandle.query(CampaignStage.experiment)
                         .filter(CampaignStage.campaign_stage_id == campaign_stage_id)
                         .one())

        #
        # get list of submissions
        #
        tl = (dbhandle.query(Submission)
              .filter(Submission.campaign_stage_id == campaign_stage_id, Submission.created > tmin, Submission.created < tmax)
              .order_by(Submission.created)
              .all())

        #
        # extract list of task ids
        #
        tids = [s.submission_id for s in tl]

        if len(tids) == 0:
            tjcl = []
            tjch = {}
            tjel = []
            tjcpuh = {}
            tjwallh = {}

        else:

            #
            # get job counts for each task, put in dict
            #
            tjcl = (dbhandle.query(Job.submission_id, func.count(Job.job_id))
                    .filter(Job.submission_id.in_(tids))
                    .group_by(Job.submission_id))

            tjch = dict(tjcl)
            logit.log("job counts:" + repr(tjch))

            #
            # get job efficiency for submissions
            #
            tjel = (dbhandle.query(
                        Job.submission_id,
                        func.sum(Job.wall_time),
                        func.sum(Job.cpu_time)
                    )
                    .filter(Job.submission_id.in_(tids))
                    .filter(
                        Job.cpu_time > 0.0,
                        Job.wall_time > 0,
                        Job.cpu_time < Job.wall_time * 10)
                    .group_by(Job.submission_id)
                    .all())

            tjcpuh = {}
            tjwallh = {}
            for row in tjel:
                tjcpuh[row[0]] = row[1]
                tjwallh[row[0]] = row[2]

            #
            # get input/output file counts
            #
            tjifl = (dbhandle.query(Job.submission_id, func.count(JobFile.file_name))
                     .filter(Job.submission_id.in_(tids))
                     .filter(JobFile.job_id == Job.job_id)
                     .filter(JobFile.file_type == "input")
                     .group_by(Job.submission_id)
                     .all())

            tjifh = dict(tjifl)

            tjofl = (dbhandle.query(Job.submission_id, func.count(JobFile.file_name))
                     .filter(Job.submission_id.in_(tids))
                     .filter(JobFile.job_id == Job.job_id)
                     .filter(JobFile.file_type == "output")
                     .group_by(Job.submission_id)
                     .all())

            tjofh = dict(tjofl)

        #
        # get exit code counts
        #
        ecc = {}
        for e in exitcodes:
            tjel = (dbhandle.query(Job.submission_id, func.count(Job.job_id))
                    .filter(Job.submission_id.in_(tids))
                    .filter(Job.user_exe_exit_code == e)
                    .group_by(Job.submission_id)
                    .all())
            ecc[e] = dict(tjel)

        # Get project summary list for a given task list in one parallel batch
        psl = self.poms_service.project_summary_for_tasks(tl)

        logit.log("got exitcodes: " + repr(exitcodes))
        day = -1
        date = None
        first = 1
        columns = [
            'day',
            'date',
            'requested files',
            'delivered files',
            'input<br>files',
            'jobs',
            'output<br>files',
            'pending',
            'efficiency%']
        exitcodes.sort(key=(lambda x: x if x else -1))
        for e in exitcodes:
            if e is not None:
                columns.append('exit(%d)' % (e))
            else:
                columns.append('No exitcode')

        outrows = deque()
        exitcounts = {e: 0 for e in exitcodes}
        totfiles = 0
        totdfiles = 0
        totjobs = 0
        outfiles = 0
        infiles = 0
        # pendfiles = 0
        tasklist = deque()
        totwall = 0.0

        daytasks = deque()
        for tno, task in enumerate(tl):
            if day != task.created.weekday():

                if not first:
                    # add a row to the table on the day boundary
                    daytasks.append(tasklist)
                    outrow = deque()
                    outrow.append(daynames[day])
                    outrow.append(date.isoformat()[:10])
                    outrow.append(str(totfiles if totfiles > 0 else infiles))
                    outrow.append(str(totdfiles))
                    outrow.append(str(infiles))
                    outrow.append(str(totjobs))
                    outrow.append(str(outfiles))
                    # we will get pending counts in a minute
                    outrow.append("...")
                    if totwall == 0.0 or totcpu == 0.0:     # totcpu undefined
                        outrow.append(-1)
                    else:
                        # totcpu undefined
                        outrow.append(int(totcpu * 100.0 / totwall))
                    for e in exitcodes:
                        outrow.append(exitcounts[e])

                    outrows.append(outrow)
                # clear counters for next days worth
                first = 0
                totfiles = 0
                totdfiles = 0
                totjobs = 0
                outfiles = 0
                infiles = 0
                totcpu = 0.0
                totwall = 0.0
                tasklist = deque()
                for e in exitcodes:
                    exitcounts[e] = 0
            tasklist.append(task)
            day = task.created.weekday()
            date = task.created
            #
            # ~ ps = self.project_summary_for_task(task.submission_id)
            ps = psl[tno]
            if ps:
                totdfiles += ps.get('tot_consumed', 0) + \
                    ps.get('tot_failed', 0)
                totfiles += ps.get('files_in_snapshot', 0)

            if tjch.get(task.submission_id, None):
                totjobs += tjch[task.submission_id]

            if tjcpuh.get(task.submission_id, None) and tjwallh.get(
                    task.submission_id, None):
                totwall += tjwallh[task.submission_id]
                totcpu += tjcpuh[task.submission_id]

            if tjofh.get(task.submission_id, None):
                outfiles += tjofh[task.submission_id]

            if tjifh.get(task.submission_id, None):
                infiles += tjifh[task.submission_id]

            for i, e in enumerate(exitcodes):
                if ecc.get(e, None) and ecc[e].get(task.submission_id, None):
                    exitcounts[e] += ecc[e][task.submission_id]

        # we *should* add another row here for the last set of totals, but
        # initially we just added a day to the query range, so we compute
        # a row of totals we don's use..
        # --- but that doesn's work on new projects...
        # add a row to the table on the day boundary
        daytasks.append(tasklist)
        outrow = deque()
        outrow.append(daynames[day])
        if date:
            outrow.append(date.isoformat()[:10])
        else:
            outrow.append('')
        outrow.append(str(totfiles if totfiles > 0 else infiles))
        outrow.append(str(totdfiles))
        outrow.append(str(infiles))
        outrow.append(str(totjobs))
        outrow.append(str(outfiles))
        outrow.append("...")   # we will get pending counts in a minute
        if totwall == 0.0 or totcpu == 0.0:
            outrow.append(-1)
        else:
            outrow.append(str(int(totcpu * 100.0 / totwall)))
        for e in exitcodes:
            outrow.append(exitcounts[e])
        outrows.append(outrow)

        #
        # --stubbed out , page template will make AJAX call to do this
        # get pending counts for the task list for each day
        # and fill in the 7th column...
        #
        dimlist = deque()

        if tl and tl[0]:
            name = tl[0].campaign_stage_snapshot_obj.name

        else:
            name = ''
        return (name, columns, outrows, dimlist, experiment, tmaxs,
                prevlink, nextlink, tdays, str(tmin)[:16], str(tmax)[:16])

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

            if len(tl):
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

    def get_file_upload_path(self, basedir, sesshandle_get, filename):
        username = sesshandle_get('experimenter').username
        experiment = sesshandle_get('experimenter').session_experiment
        return "%s/uploads/%s/%s/%s" % (basedir, experiment, username, filename)

    def file_uploads(self, basedir, sesshandle_get, quota):
        flist = glob.glob(self.get_file_upload_path(basedir, sesshandle_get, '*'))
        res = []
        total = 0
        for fname in flist:
            statout = os.stat(fname)
            res.append([os.path.basename(fname), statout])
            total += statout.st_size
        return res, total

    def upload_file(self, basedir, sesshandle_get, err_res, quota, filename):
        outf = self.get_file_upload_path(basedir, sesshandle_get, filename.filename)
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
        fstatlist, total = self.file_uploads(basedir, sesshandle_get, quota)
        if total > quota:
            unlink(outf)
            raise err_res("Upload exeeds quota of %d kbi" % quota/1024)
        return "Ok."


    def remove_uploaded_files(self, basedir, sesshandle_get, err_res, filename, actio):
        # if there's only one entry the web page will not send a list...
        if isinstance(filename,str):
            filename = [filename]

        for f in filename:
            outf = self.get_file_upload_path(basedir, sesshandle_get, f)
            os.unlink(outf)
        return "Ok."

    def get_launch_sandbox(self, basedir, sesshandle_get):
        uploads = self.get_file_upload_path(basedir, sesshandle_get, '')
        uu = uuid.uuid4()  # random uuid -- shouldn't be guessable.
        sandbox = "%s/sandboxes/%s" % (basedir, str(uu))
        os.makedirs(sandbox, exist_ok=False)
        flist = glob.glob(self.get_file_upload_path(basedir, sesshandle_get, '*'))
        for f in flist:
            os.link(f, "%s/%s" % (sandbox, os.path.basename(f)))
        return sandbox
