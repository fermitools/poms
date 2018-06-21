#!/usr/bin/env python

### This module contain the methods that handle the file status accounting
### List of methods: def list_task_logged_files, campaign_task_files, job_file_list, get_inflight,
# inflight_files, show_dimension_files, campaign_sheet, actual_pending_files
### Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions
# in poms_service.py written by Marc Mengel, Stephen White and Michael Gueith.
### October, 2016.

from collections import deque, defaultdict
import os
import shelve
from datetime import datetime, timedelta

from sqlalchemy.orm import subqueryload, joinedload
from sqlalchemy import distinct, func

from . import logit
from .poms_model import Job, Submission, CampaignStage, JobFile
from .utc import utc
from .pomscache import pomscache



class Files_status(object):

    def __init__(self, ps):
        self.poms_service = ps


    def list_task_logged_files(self, dbhandle, submission_id):
        s = dbhandle.query(Submission).filter(Submission.submission_id == submission_id).first()
        jobsub_job_id = s.jobsub_job_id
        fl = dbhandle.query(JobFile).join(Job).filter(Job.submission_id == submission_id, JobFile.job_id == Job.job_id).all()
        return fl, s, jobsub_job_id
        #DELETE: template = self.poms_service.jinja_env.get_template('list_task_logged_files.html')
        #return template.render(fl = fl, campaign = s.campaign_stage_snapshot_obj,  jobsub_job_id = jobsub_job_id, current_experimenter=cherrypy.session.get('experimenter'),  do_refresh = 0, pomspath=self.path, help_page="ListTaskLoggedFilesHelp", version=self.version)


    def campaign_task_files(self, dbhandle, samhandle, campaign_stage_id, tmin=None, tmax=None, tdays=1):
        (tmin, tmax,
         tmins, tmaxs,
         nextlink, prevlink,
         time_range_string, tdays) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays,
                                                                              'campaign_task_files?campaign_stage_id=%s&' % campaign_stage_id)
        # inhale all the campaign related task info for the time window
        # in one fell swoop
        tl = (dbhandle.query(Submission)
              .options(joinedload(Submission.campaign_stage_snapshot_obj))
              .filter(Submission.campaign_stage_id == campaign_stage_id,
                      Submission.created >= tmin, Submission.created < tmax)
              .all())
        #
        # either get the campaign obj from above, or if we didn's
        # find any submissions in that window, look it up
        #
        if len(tl) > 0:
            cs = tl[0].campaign_stage_snapshot_obj
            # cs = tl[0].campaign_stage_snapshot_obj
        else:
            cs = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
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
        # finished_flying_needed = deque()
        for s in tl:
            summary_needed.append(s)
            basedims = "snapshot_for_project_name %s " % s.project
            base_dim_list.append(basedims)

            somekiddims = "%s and isparentof: (version %s)" % (basedims, s.campaign_stage_snapshot_obj.software_version)
            some_kids_needed.append(somekiddims)

            somekidsdecldims = ("%s and isparentof: (version %s with availability anylocation )" %
                                (basedims, s.campaign_stage_snapshot_obj.software_version))
            some_kids_decl_needed.append(somekidsdecldims)

            allkiddecldims = basedims
            allkiddims = basedims
            for pat in str(s.job_type_snapshot_obj.output_file_patterns).split(','):
                if pat == 'None':
                    pat = '%'
                if pat.find(' ') > 0:
                    dimbits = pat
                else:
                    dimbits = "filename like '%s'" % pat

                allkiddims = ("%s and isparentof: ( file_name '%s' and version '%s' ) " %
                              (allkiddims, dimbits, s.campaign_stage_snapshot_obj.software_version))
                allkiddecldims = ("%s and isparentof: ( file_name '%s' and version '%s' with availability anylocation ) " %
                                  (allkiddecldims, dimbits, s.campaign_stage_snapshot_obj.software_version))
            all_kids_needed.append(allkiddims)
            all_kids_decl_needed.append(allkiddecldims)
        #
        # -- now call parallel fetches for items
        #samhandle = cherrypy.request.samweb_lite ####IMPORTANT
        summary_list = samhandle.fetch_info_list(summary_needed, dbhandle=dbhandle)
        some_kids_list = samhandle.count_files_list(cs.experiment, some_kids_needed)
        some_kids_decl_list = samhandle.count_files_list(cs.experiment, some_kids_decl_needed)
        all_kids_decl_list = samhandle.count_files_list(cs.experiment, all_kids_decl_needed)
        # all_kids_list = samhandle.count_files_list(cs.experiment, all_kids_needed)
        tids = [s.submission_id for s in tl]

        if (len(tids) == 0):
           tjifl = [] 
           tjifh = {}
           tjofl = []
           tjofh = {}
        else:
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


        columns = ["jobsub_jobid", "project", "date", "submit-<br>ted",
                   "deliv-<br>ered<br>SAM",
                   "unknown<br>SAM",
                   "deliv-<br>ered<br>logs",
                   "out-<br>put<br>logs",
                   "con-<br>sumed", "failed", "skipped",
                   "w/some kids<br>declared",
                   "w/all kids<br>declared",
                   "kids in<br>flight",
                   "w/kids<br>located",
                   "pending"]


        listfiles = "show_dimension_files?experiment=%s&dims=%%s" % cs.experiment
        datarows = deque()
        i = -1
        for s in tl:
            logit.log("task %d" % s.submission_id)
            i = i + 1
            psummary = summary_list[i]
            partpending = psummary.get('files_in_snapshot', 0) - some_kids_list[i]
            #pending = psummary.get('files_in_snapshot', 0) - all_kids_list[i]
            pending = partpending
            logdelivered = tjifh.get(s.submission_id,0)
            logoutput = tjofh.get(s.submission_id,0)

            task_jobsub_job_id = s.jobsub_job_id
            if task_jobsub_job_id is None:
                task_jobsub_job_id = "s%s" % s.submission_id
            datarows.append([
                            [task_jobsub_job_id.replace('@', '@<br>'), "https://fifemon.fnal.gov/monitor/d/000000188/dag-cluster-summary?var-cluster=%s&var-schedd=%s&from=now-2d&to=now&refresh=5m&orgId=1" % (task_jobsub_job_id[0:task_jobsub_job_id.find('@')],task_jobsub_job_id[task_jobsub_job_id.find('@')+1:])],
                            [s.project, "http://samweb.fnal.gov:8480/station_monitor/%s/stations/%s/projects/%s" % (cs.experiment, cs.experiment, s.project)],
                            [s.created.strftime("%Y-%m-%d %H:%M"), None],
                            [psummary.get('files_in_snapshot', 0), listfiles % base_dim_list[i]],
                            ["%d" % (psummary.get('tot_consumed', 0) + psummary.get('tot_failed', 0) + psummary.get('tot_skipped', 0) + psummary.get('tot_delivered', 0)),
                             listfiles % base_dim_list[i] + " and consumed_status consumed,failed,skipped,delivered "],
                            ["%d" % psummary.get('tot_unknown', 0),
                             listfiles % base_dim_list[i] + " and consumed_status unknown"],
                            ["%d" % logdelivered, "./list_task_logged_files?submission_id=%s" % s.submission_id],
                            ["%d" % logoutput, "./list_task_logged_files?submission_id=%s" % s.submission_id],
                            [psummary.get('tot_consumed', 0), listfiles % base_dim_list[i] + " and consumed_status consumed"],
                            [psummary.get('tot_failed', 0), listfiles % base_dim_list[i] + " and consumed_status failed"],
                            [psummary.get('tot_skipped', 0), listfiles % base_dim_list[i] + " and consumed_status skipped"],
                            [some_kids_decl_list[i], listfiles % some_kids_needed[i]],
                            [all_kids_decl_list[i], listfiles % some_kids_decl_needed[i]],
                            [len(self.poms_service.filesPOMS.get_inflight(dbhandle, campaign_stage_id, submission_id=s.submission_id)),
                             "./inflight_files?submission_id=%d" % s.submission_id],
                            [all_kids_decl_list[i], listfiles % all_kids_decl_needed[i]],
                            [pending, listfiles % base_dim_list[i] + "minus ( %s ) " % all_kids_decl_needed[i]],
                            ])
        return cs, columns, datarows, tmins, tmaxs, prevlink, nextlink, tdays


    @staticmethod
    def get_inflight(dbhandle, campaign_stage_id=None, submission_id=None):   # This method was deleted from the main script
        q = dbhandle.query(JobFile).join(Job).join(Submission).join(CampaignStage)
        q = q.filter(Submission.campaign_stage_id == CampaignStage.campaign_stage_id)
        q = q.filter(Submission.submission_id == Job.submission_id)
        q = q.filter(Job.job_id == JobFile.job_id)
        q = q.filter(JobFile.file_type == 'output')
        q = q.filter(JobFile.declared == None)
        if campaign_stage_id is not None:
            q = q.filter(Submission.campaign_stage_id == campaign_stage_id)
        if submission_id is not None:
            q = q.filter(Job.submission_id == submission_id)
        q = q.filter(Job.output_files_declared == False)
        return [jf.file_name for jf in q.all()]


    def inflight_files(self, dbhandle, status_response, getconfig, campaign_stage_id=None, submission_id=None):
        #status_response = cherrypy.response.status
        if campaign_stage_id:
            cs = dbhandle.query(CampaignStage).filter(CampaignStage.campaign_stage_id == campaign_stage_id).first()
        elif submission_id:
            cs = dbhandle.query(CampaignStage).join(Submission).filter(CampaignStage.campaign_stage_id == Submission.campaign_stage_id, Submission.submission_id == submission_id).first()
        else:
            # status_response = "404 Permission Denied."
            return "Neither CampaignStage nor Submission found"
        outlist = self.poms_service.filesPOMS.get_inflight(dbhandle, campaign_stage_id=campaign_stage_id, submission_id=submission_id)
        statusmap = {}
        if cs:
            fss_file = "%s/%s_files.db" % (getconfig("ftsscandir"), cs.experiment)
            if os.path.exists(fss_file):
                fss = shelve.open(fss_file, flag='r', protocol=3)
                for f in outlist:
                    try:
                        statusmap[f] = fss.get(f, '')
                    except KeyError:
                        statusmap[f] = ''
                fss.close()
        return outlist, statusmap, cs

    def show_dimension_files(self, samhandle, experiment, dims, dbhandle=None):

        try:
            flist = samhandle.list_files(experiment, dims, dbhandle=dbhandle)
        except ValueError:
            flist = deque()
        return flist


    def actual_pending_file_dims(self, dbhandle, samhandle, campaign_stage_id=None, tmin=None, tmax=None, tdays=1):
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

        explist, dimlist = self.get_pending_dims_for_task_lists(dbhandle, samhandle, [tl])
        return explist, dimlist

    def campaign_sheet(self, dbhandle, samhandle, campaign_stage_id, tmin=None, tmax=None, tdays=7):   # maybe at the future for a  ReportsPOMS module

        daynames = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        (tmin, tmax,
         tmins, tmaxs,
         nextlink, prevlink,
         time_range_string, tdays) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'campaign_sheet?campaign_stage_id=%s&' % campaign_stage_id)

        el = dbhandle.query(distinct(Job.user_exe_exit_code)).filter(Job.updated >= tmin, Job.updated <= tmax).all()
        exitcodes = [e[0] for e in el]

        (experiment,) = dbhandle.query(CampaignStage.experiment).filter(CampaignStage.campaign_stage_id == campaign_stage_id).one()

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
            logit.log("job counts:"+repr(tjch))

            #
            # get job efficiency for submissions
            #
            tjel = (dbhandle.query(Job.submission_id, func.sum(Job.wall_time), func.sum(Job.cpu_time))
                    .filter(Job.submission_id.in_(tids))
                    .filter(Job.cpu_time > 0.0, Job.wall_time > 0, Job.cpu_time < Job.wall_time * 10)
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

        psl = self.poms_service.project_summary_for_tasks(tl)        # Get project summary list for a given task list in one parallel batch

        logit.log("got exitcodes: " + repr(exitcodes))
        day = -1
        date = None
        first = 1
        columns = ['day', 'date', 'requested files', 'delivered files', 'input<br>files','jobs', 'output<br>files', 'pending', 'efficiency%']
        exitcodes.sort(key=(lambda x:  x if x else -1))
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
                    outrow.append("...")  # we will get pending counts in a minute
                    if totwall == 0.0 or totcpu == 0.0:     # totcpu undefined
                        outrow.append(-1)
                    else:
                        outrow.append(int(totcpu * 100.0 / totwall))   # totcpu undefined
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
            #~ ps = self.project_summary_for_task(task.submission_id)
            ps = psl[tno]
            if ps:
                totdfiles += ps.get('tot_consumed', 0) + ps.get('tot_failed', 0)
                totfiles += ps.get('files_in_snapshot', 0)

            if tjch.get(task.submission_id, None):
                totjobs += tjch[task.submission_id]

            if tjcpuh.get(task.submission_id, None) and tjwallh.get(task.submission_id, None):
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
        # initially we just added a day to the query range, so we compute a row of totals we don's use..
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
        #dimlist, pendings = self.poms_service.filesPOMS.get_pending_for_task_lists(dbhandle, samhandle, daytasks)
        #for i in range(len(pendings)):
        #    outrows[i][7] = pendings[i]

        if tl and tl[0]:
            name = tl[0].campaign_stage_snapshot_obj.name

        else:
            name = ''
        return name, columns, outrows, dimlist, experiment, tmaxs, prevlink, nextlink, tdays, str(tmin)[:16], str(tmax)[:16]


    @pomscache.cache_on_arguments()
    def get_pending_dict_for_campaigns(self, dbhandle, samhandle, campaign_id_list, tmin, tmax):
        if isinstance(campaign_id_list, str):
            campaign_id_list = [cid for cid in campaign_id_list.split(',') if cid]
        dl, cl = self.get_pending_for_campaigns(dbhandle, samhandle, campaign_id_list, tmin, tmax)
        res = {cid: cs for cid, cs in zip(campaign_id_list, cl)}
        logit.log("get_pending_dict_for_campaigns returning: " + repr(res))
        return res


    def get_pending_for_campaigns(self, dbhandle, samhandle, campaign_id_list, tmin, tmax):

        task_list_list = deque()

        logit.log("in get_pending_for_campaigns, tmin %s tmax %s" % (tmin, tmax))
        if isinstance(campaign_id_list, str):
            campaign_id_list = [cid for cid in campaign_id_list.split(',') if cid]

        task_list = (dbhandle.query(Submission).
                     options(joinedload(Submission.campaign_stage_snapshot_obj)).
                     options(joinedload(Submission.job_type_snapshot_obj)).
                     filter(Submission.campaign_stage_id.in_(campaign_id_list),
                            Submission.created >= tmin, Submission.created < tmax).
                     all())
        # logit.log("get_pending_for_campaigns: task_list (%d): %s" % (len(task_list), task_list))

        tll = defaultdict(lambda: [])                               # To prepare the list of task lists
        for task in task_list:                                      # Group submissions by campaign ids
            tll[task.campaign_stage_id].append(task)
        task_list_list = [tll[int(ci)] for ci in campaign_id_list]  # Build the list of task lists in original campaign order
        # logit.log("get_pending_for_campaigns: task_list_list (%d): %s" % (len(task_list_list), task_list_list))

        dl, cl = self.get_pending_for_task_lists(dbhandle, samhandle, task_list_list)

        return dl, cl


    @staticmethod
    def get_pending_dims_for_task_lists(dbhandle, samhandle, task_list_list):
        reason = 'no_project_info'
        now = datetime.now(utc)
        twodays = timedelta(days=2)
        dimlist = deque()
        explist = deque()
        # experiment = None
        logit.log("get_pending_for_task_lists: task_list_list (%d): %s" % (len(task_list_list), task_list_list))
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
                diml.append("minus ( snapshot_for_project_name %s and (" % task.project)
                sep = ""
                for pat in str(task.job_type_snapshot_obj.output_file_patterns).split(','):
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

        logit.log("get_pending_for_task_lists: dimlist (%d): %s" % (len(dimlist), dimlist))
        return explist, dimlist


    def get_pending_for_task_lists(self, dbhandle, samhandle, task_list_list):
        explist, dimlist = self.get_pending_dims_for_task_lists(dbhandle, samhandle, task_list_list)
        count_list = samhandle.count_files_list(explist, dimlist)
        logit.log("get_pending_for_task_lists: count_list (%d): %s" % (len(dimlist), count_list))

        return dimlist, count_list


    @staticmethod
    def report_declared_files(flist, dbhandle):
        now = datetime.now(utc)
        # the "extra" first query on Job is to make sure we get a shared lock
        # on Job before trying to get an update lock on JobFile, which will
        # then try to get a lock on Job, but can deadlock with someone
        # otherwise doing update_job()..
        dbhandle.query(Job, JobFile).with_for_update(of=Job, read=True).filter(JobFile.job_id == Job.job_id, JobFile.file_name.in_(flist)).order_by(Job.jobsub_job_id).all()
        dbhandle.query(Job, JobFile).with_for_update(of=JobFile, read=True).filter(JobFile.job_id == Job.job_id, JobFile.file_name.in_(flist)).order_by(JobFile.job_id, JobFile.file_name).all()
        dbhandle.query(JobFile).filter(JobFile.file_name.in_(flist)).update({JobFile.declared: now}, synchronize_session=False)
        dbhandle.commit()
