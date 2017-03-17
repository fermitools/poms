#!/usr/bin/env python

### This module contain the methods that handle the file status accounting
### List of methods: def list_task_logged_files, campaign_task_files, job_file_list, get_inflight,  inflight_files, show_dimension_files, campaign_sheet, actual_pending_files
### Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of functions in poms_service.py written by Marc Mengel, Stephen White and Michael Gueith.
### October, 2016.

import os
import logit

from model.poms_model import Job, Task, Campaign, JobFile
from sqlalchemy.orm import subqueryload, joinedload
from sqlalchemy import (desc,distinct)
from utc import utc
from datetime import datetime

class Files_status(object):

    def __init__(self, ps):
        self.poms_service = ps

    def list_task_logged_files(self, dbhandle, task_id):
        t = dbhandle.query(Task).filter(Task.task_id == task_id).first()
        jobsub_job_id = self.poms_service.taskPOMS.task_min_job(dbhandle, task_id)
        fl = dbhandle.query(JobFile).join(Job).filter(Job.task_id == task_id, JobFile.job_id == Job.job_id).all()
        return fl, t, jobsub_job_id
        #DELETE: template = self.poms_service.jinja_env.get_template('list_task_logged_files.html')
        #return template.render(fl = fl, campaign = t.campaign_snap_obj,  jobsub_job_id = jobsub_job_id, current_experimenter=cherrypy.session.get('experimenter'),  do_refresh = 0, pomspath=self.path, help_page="ListTaskLoggedFilesHelp", version=self.version)

    def campaign_task_files(self, dbhandle, samhandle, campaign_id, tmin=None, tmax=None, tdays=1):
        (tmin, tmax,
         tmins, tmaxs,
         nextlink, prevlink,
         time_range_string) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays,
                                                                       'campaign_task_files?campaign_id=%s&' % campaign_id)
        # inhale all the campaign related task info for the time window
        # in one fell swoop
        tl = (dbhandle.query(Task).
              options(joinedload(Task.campaign_snap_obj)).
              options(joinedload(Task.campaign_snap_obj)).
              options(joinedload(Task.jobs).joinedload(Job.job_files)).
              filter(Task.campaign_id == campaign_id,
                     Task.created >= tmin, Task.created < tmax).all()
              )
        #
        # either get the campaign obj from above, or if we didn't
        # find any tasks in that window, look it up
        #
        if len(tl) > 0:
            c = tl[0].campaign_snap_obj
            # cs = tl[0].campaign_snap_obj
        else:
            c = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
            # cs = c  # this is klugy -- does this work?
        #
        # fetch needed data in tandem
        # -- first build lists of stuff to fetch
        #
        base_dim_list = []
        summary_needed = []
        some_kids_needed = []
        some_kids_decl_needed = []
        all_kids_needed = []
        all_kids_decl_needed = []
        # finished_flying_needed = []
        for t in tl:
            summary_needed.append(t)
            basedims = "snapshot_for_project_name %s " % t.project
            base_dim_list.append(basedims)

            somekiddims = "%s and isparentof: (version %s)" % (basedims, t.campaign_snap_obj.software_version)
            some_kids_needed.append(somekiddims)

            somekidsdecldims = ("%s and isparentof: (version %s with availability anylocation )" %
                                (basedims, t.campaign_snap_obj.software_version))
            some_kids_decl_needed.append(somekidsdecldims)

            allkiddecldims = basedims
            allkiddims = basedims
            for pat in str(t.campaign_definition_snap_obj.output_file_patterns).split(','):
                if pat == 'None':
                    pat = '%'
                allkiddims = ("%s and isparentof: ( file_name '%s' and version '%s' ) " %
                              (allkiddims, pat, t.campaign_snap_obj.software_version))
                allkiddecldims = ("%s and isparentof: ( file_name '%s' and version '%s' with availability anylocation ) " %
                                  (allkiddecldims, pat, t.campaign_snap_obj.software_version))
            all_kids_needed.append(allkiddims)
            all_kids_decl_needed.append(allkiddecldims)
            logoutfiles = []
            for j in t.jobs:
                for f in j.job_files:
                    if f.file_type == "output":
                        logoutfiles.append(f.file_name)
        #
        # -- now call parallel fetches for items
        #samhandle = cherrypy.request.samweb_lite ####IMPORTANT
        summary_list = samhandle.fetch_info_list(summary_needed, dbhandle=dbhandle)
        some_kids_list = samhandle.count_files_list(c.experiment, some_kids_needed)
        some_kids_decl_list = samhandle.count_files_list(c.experiment, some_kids_decl_needed)
        all_kids_decl_list = samhandle.count_files_list(c.experiment, all_kids_decl_needed)
        # all_kids_list = samhandle.count_files_list(c.experiment, all_kids_needed)

        columns = ["jobsub_jobid", "project", "date", "submit-<br>ted",
                   "deliv-<br>ered<br>SAM",
                   "deliv-<br>ered<br> logs",
                   "con-<br>sumed", "failed", "skipped",
                   "w/some kids<br>declared",
                   "w/all kids<br>declared",
                   "kids in<br>flight",
                   "w/kids<br>located",
                   "pending"]

        listfiles = "show_dimension_files?experiment=%s&dims=%%s" % c.experiment
        datarows = []
        i = -1
        for t in tl:
            logit.log("task %d" % t.task_id)
            i = i + 1
            psummary = summary_list[i]
            partpending = psummary.get('files_in_snapshot', 0) - some_kids_list[i]
            #pending = psummary.get('files_in_snapshot', 0) - all_kids_list[i]
            pending = partpending
            logdelivered = 0
            # logwritten = 0
            logkids = 0
            for j in t.jobs:
                for f in j.job_files:
                    if f.file_type == "input":
                        logdelivered = logdelivered + 1
                    if f.file_type == "output":
                        logkids = logkids + 1
            task_jobsub_job_id = self.poms_service.taskPOMS.task_min_job(dbhandle, t.task_id)
            if task_jobsub_job_id is None:
                task_jobsub_job_id = "t%s" % t.task_id
            datarows.append([
                            [task_jobsub_job_id.replace('@', '@<br>'), "show_task_jobs?task_id=%d" % t.task_id],
                            [t.project, "http://samweb.fnal.gov:8480/station_monitor/%s/stations/%s/projects/%s" % (c.experiment, c.experiment, t.project)],
                            [t.created.strftime("%Y-%m-%d %H:%M"), None],
                            [psummary.get('files_in_snapshot', 0), listfiles % base_dim_list[i]],
                            ["%d" % (psummary.get('tot_consumed', 0) + psummary.get('tot_failed', 0) + psummary.get('tot_skipped', 0)),
                             listfiles % base_dim_list[i] + " and consumed_status consumed,failed,skipped "],
                            ["%d" % logdelivered, "./list_task_logged_files?task_id=%s" % t.task_id],
                            [psummary.get('tot_consumed', 0), listfiles % base_dim_list[i] + " and consumed_status consumed"],
                            [psummary.get('tot_failed', 0), listfiles % base_dim_list[i] + " and consumed_status failed"],
                            [psummary.get('tot_skipped', 0), listfiles % base_dim_list[i] + " and consumed_status skipped"],
                            [some_kids_decl_list[i], listfiles % some_kids_needed[i]],
                            [all_kids_decl_list[i], listfiles % some_kids_decl_needed[i]],
                            [len(self.poms_service.filesPOMS.get_inflight(dbhandle, campaign_id, task_id=t.task_id)),
                             "./inflight_files?task_id=%d" % t.task_id],
                            [all_kids_decl_list[i], listfiles % all_kids_decl_needed[i]],
                            [pending, listfiles % base_dim_list[i] + "minus ( %s ) " % all_kids_decl_needed[i]],
                            ])
        return c, columns, datarows, tmins, tmaxs, prevlink, nextlink, tdays

            ###I didn't include tdays, campaign_id, because it was passed as an argument, should I?????
            #DELETE template = self.jinja_env.get_template('campaign_task_files.html')
            #DELETE return template.render(name = c.name if c else "", columns = columns, datarows = datarows, tmin=tmins, tmax=tmaxs,  prev=prevlink, next=nextlink, days=tdays, current_experimenter=cherrypy.session.get('experimenter'),  campaign_id = campaign_id, pomspath=self.path,help_page="CampaignTaskFilesHelp", version=self.version)


    def job_file_list(self, dbhandle, jobhandle, job_id, force_reload=False):   # Should this funcion be here or at the main script ????
        j = dbhandle.query(Job).options(joinedload(Job.task_obj).joinedload(Task.campaign_snap_obj)).filter(Job.job_id == job_id).first()
        # find the job with the logs -- minimum jobsub_job_id for this task
        jobsub_job_id = self.poms_service.taskPOMS.task_min_job(dbhandle, j.task_id)
        role = j.task_obj.campaign_snap_obj.vo_role
        return jobhandle.index(jobsub_job_id, j.task_obj.campaign_snap_obj.experiment, role, force_reload)


    def job_file_contents(self, dbhandle, jobhandle, job_id, task_id, file, tmin=None, tmax=None, tdays=None):
        #jobhandle = cherrypy.request.jobsub_fetcher

        # we don't really use these for anything but we might want to
        # pass them into a template to set time ranges...
        (tmin, tmax,
         tmins, tmaxs,
         nextlink, prevlink,
         time_range_string) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'show_campaigns?')
        ### You don't use many of those arguments, is just because you need one of them then you call the whole method ???????
        j = dbhandle.query(Job).options(subqueryload(Job.task_obj).subqueryload(Task.campaign_snap_obj)).filter(Job.job_id == job_id).first()
        # find the job with the logs -- minimum jobsub_job_id for this task
        jobsub_job_id = self.poms_service.taskPOMS.task_min_job(dbhandle, j.task_id)
        logit.log("found job: %s " % jobsub_job_id)
        role = j.task_obj.campaign_snap_obj.vo_role
        job_file_contents = jobhandle.contents(file, j.jobsub_job_id, j.task_obj.campaign_snap_obj.experiment, role)
        return job_file_contents, tmin
        #DELETE template = self.jinja_env.get_template('job_file_contents.html')
        #DELETE return template.render(file=file, job_file_contents=job_file_contents, task_id=task_id, job_id=job_id, tmin=tmin, pomspath=self.path,help_page="JobFileContentsHelp", version=self.version)

    def format_job_counts(self, dbhandle, task_id=None, campaign_id=None, tmin=None, tmax=None, tdays=7, range_string=None): ##This method was deleted from the main script
        counts = self.poms_service.triagePOMS.job_counts(dbhandle, task_id=task_id, campaign_id=campaign_id, tmin=tmin, tmax=tmax, tdays=tdays)
        ck = counts.keys()
        res = ['<div><b>Job States</b><br>',
               '<table class="ui celled table unstackable">',
               '<tr><th>Total</th><th colspan=3>Active</th><th colspan=3>Completed In %s</th></tr>' % range_string,
               '<tr>']
        for k in ck:
            if k == "Completed Total":
                k = "Total"
            if k == "Completed":
                k = "Not Located"
            res.append("<th>%s</th>" % k)
        res.append("</tr>")
        res.append("<tr>")
        var = 'ignore_me'
        val = ''
        if campaign_id is not None:
            var = 'campaign_id'
            val = campaign_id
        if task_id is not None:
            var = 'task_id'
            val = task_id
        for k in ck:
            res.append('<td><a href="job_table?job_status=%s&%s=%s">%d</a></td>' % (k, var, val, counts[k]))
        res.append("</tr></table></div><br>")
        return "".join(res)


    def get_inflight(self, dbhandle, campaign_id=None, task_id=None):   # This method was deleted from the main script
        q = dbhandle.query(JobFile).join(Job).join(Task).join(Campaign)
        q = q.filter(Task.campaign_id == Campaign.campaign_id)
        q = q.filter(Task.task_id == Job.task_id)
        q = q.filter(Job.job_id == JobFile.job_id)
        q = q.filter(JobFile.file_type == 'output')
        q = q.filter(JobFile.declared is None)
        if campaign_id is not None:
            q = q.filter(Task.campaign_id == campaign_id)
        if task_id is not None:
            q = q.filter(Job.task_id == task_id)
        q = q.filter(Job.output_files_declared is False)
        outlist = []
        # jjid = "xxxxx"
        for jf in q.all():
            outlist.append(jf.file_name)

        return outlist


    def inflight_files(self, dbhandle, status_response, getconfig, campaign_id=None, task_id=None):
        #status_response = cherrypy.response.status
        if campaign_id:
            c = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        elif task_id:
            c = dbhandle.query(Campaign).join(Task).filter(Campaign.campaign_id == Task.campaign_id, Task.task_id == task_id).first()
        else:
            # status_response = "404 Permission Denied."
            return "Neither Campaign nor Task found"
        outlist = self.poms_service.filesPOMS.get_inflight(dbhandle, campaign_id=campaign_id, task_id=task_id)
        statusmap = {}
        if c:
            fss_file = "%s/%s_files.db" % (getconfig("ftsscandir"), c.experiment)
            if os.path.exists(fss_file):
                fss = shelve.open(fss_file, 'r')
                for f in outlist:
                    try:
                        statusmap[f] = fss.get(f.encode('ascii', 'ignore'), '')
                    except KeyError:
                        statusmap[f] = ''
                fss.close()
        return outlist, statusmap, c
        #template = self.jinja_env.get_template('inflight_files.html')
        #return template.render(flist = outlist,  current_experimenter=cherrypy.session.get('experimenter'),   statusmap = statusmap, c = c, jjid= self.task_min_job(task_id),campaign_id = campaign_id, task_id = task_id, pomspath=self.path,help_page="PendingFilesJobsHelp", version=self.version)


    def show_dimension_files(self, samhandle, experiment, dims, dbhandle=None):

        try:
            flist = samhandle.list_files(experiment, dims, dbhandle=dbhandle)
        except ValueError:
            flist = []
        return flist


    def actual_pending_files(self, dbhandle, count_or_list, task_id=None, campaign_id=None, tmin=None, tmax=None, tdays=1):
        (tmin, tmax,
         tmins, tmaxs,
         nextlink, prevlink,
         time_range_string
         ) = self.poms_services.utilsPOMS.handle_dates(tmin, tmax, tdays,
                                                       'actual_pending_files?count_or_list=%s&%s=%s&' %
                                                       (count_or_list, 'campaign_id', campaign_id) if campaign_id else (count_or_list, 'task_id', task_id))

        tl = (dbhandle.query(Task).
              options(joinedload(Task.campaign_obj)).
              options(joinedload(Task.jobs).joinedload(Job.job_files)).
              filter(Task.campaign_id == campaign_id,
              Task.created >= tmin, Task.created < tmax).
              all())

        c = None
        plist = []
        for t in tl:
            if not c:
                c = t.campaign_obj
            plist.append(t.project if t.project else 'None')

        if c:
            dims = "snapshot_for_project_name %s minus (" % ','.join(plist)
            sep = ""
            for pat in str(c.campaign_definition_obj.output_file_patterns).split(','):
                if pat == "None":
                    pat = "%"
                dims = "%s %s isparentof: ( file_name '%s' and version '%s' with availability physical ) " % (dims, sep, pat, t.campaign_obj.software_version)
                sep = "and"
                logit.log("dims now: %s" % dims)
            dims = dims + ")"
        else:
            c = dbhandle.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
            dims = None

        if dims is None or 'None' == dims:
            raise ValueError("None == dims in actual_pending_files method")
        else:

            logit.log("actual pending files: got dims %s" % dims)
            return c.experiment, dims


    def campaign_sheet(self, dbhandle, samhandle, campaign_id, tmin=None, tmax=None, tdays=7):   # maybe at the future for a  ReportsPOMS module

        daynames = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

        (tmin, tmax,
         tmins, tmaxs,
         nextlink, prevlink,
         time_range_string) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'campaign_sheet?campaign_id=%s&' % campaign_id)

        tl = (dbhandle.query(Task)
              .filter(Task.campaign_id == campaign_id, Task.created > tmin, Task.created < tmax)
              .order_by(desc(Task.created))
              .options(joinedload(Task.jobs))
              .all())

        psl = self.poms_service.project_summary_for_tasks(tl)        # Get project summary list for a given task list in one query
        # XXX should be based on Task create date, not job updated date..
        el = dbhandle.query(distinct(Job.user_exe_exit_code)).filter(Job.updated >= tmin, Job.updated <= tmax).all()
        (experiment,) = dbhandle.query(Campaign.experiment).filter(Campaign.campaign_id == campaign_id).one()
        exitcodes = []
        for e in el:
            exitcodes.append(e[0])

        logit.log("got exitcodes: " + repr(exitcodes))
        day = -1
        date = None
        first = 1
        columns = ['day', 'date', 'requested files', 'delivered files', 'jobs', 'failed', 'outfiles', 'pending', 'efficiency%']
        exitcodes.sort()
        for e in exitcodes:
            if e is not None:
                columns.append('exit(%d)' % (e))
            else:
                columns.append('No exitcode')

        outrows = []
        exitcounts = {}
        totfiles = 0
        totdfiles = 0
        totjobs = 0
        totjobfails = 0
        outfiles = 0
        infiles = 0
        # pendfiles = 0
        tasklist = []
        totwall = 0.0
        for e in exitcodes:
            exitcounts[e] = 0

        daytasks = []
        for tno, task in enumerate(tl):
            if day != task.created.weekday():
                if not first:
                    # add a row to the table on the day boundary
                    daytasks.append(tasklist)
                    outrow = []
                    outrow.append(daynames[day])
                    outrow.append(date.isoformat()[:10])
                    outrow.append(str(totfiles if totfiles > 0 else infiles))
                    outrow.append(str(totdfiles))
                    outrow.append(str(totjobs))
                    outrow.append(str(totjobfails))
                    outrow.append(str(outfiles))
                    outrow.append("")  # we will get pending counts in a minute
                    if totwall == 0.0 or totcpu == 0.0:     # totcpu undefined
                        outrow.append("-")
                    else:
                        outrow.append(str(int(totcpu * 100.0 / totwall)))   # totcpu undefined
                    for e in exitcodes:
                        outrow.append(exitcounts[e])
                    outrows.append(outrow)
                # clear counters for next days worth
                first = 0
                totfiles = 0
                totdfiles = 0
                totjobs = 0
                totjobfails = 0
                outfiles = 0
                infiles = 0
                totcpu = 0.0
                totwall = 0.0
                tasklist = []
                for e in exitcodes:
                    exitcounts[e] = 0
            tasklist.append(task)
            day = task.created.weekday()
            date = task.created
            #
            #~ ps = self.project_summary_for_task(task.task_id)
            ps = psl[tno]
            if ps:
                totdfiles += ps['tot_consumed'] + ps['tot_failed']
                totfiles += ps['files_in_snapshot']
                totjobfails += ps['tot_jobfails']

            totjobs += len(task.jobs)

            for job in list(task.jobs):

                if job.cpu_time and job.wall_time:
                    totcpu += job.cpu_time
                    totwall += job.wall_time

                exitcounts[job.user_exe_exit_code] = exitcounts.get(job.user_exe_exit_code, 0) + 1
                if job.job_files:
                    nout = len(job.job_files)
                    outfiles += nout

                if job.job_files:
                    nin = len([x for x in job.job_files if x.file_type == "input"])
                    infiles += nin
        # end 'for'
        # we *should* add another row here for the last set of totals, but
        # initially we just added a day to the query range, so we compute a row of totals we don't use..
        # --- but that doesn't work on new projects...
        # add a row to the table on the day boundary
        daytasks.append(tasklist)
        outrow = []
        outrow.append(daynames[day])
        if date:
            outrow.append(date.isoformat()[:10])
        else:
            outrow.append('')
        outrow.append(str(totfiles if totfiles > 0 else infiles))
        outrow.append(str(totdfiles))
        outrow.append(str(totjobs))
        outrow.append(str(totjobfails))
        outrow.append(str(outfiles))
        outrow.append("")   # we will get pending counts in a minute
        if totwall == 0.0 or totcpu == 0.0:
            outrow.append("-")
        else:
            outrow.append(str(int(totcpu * 100.0 / totwall)))
        for e in exitcodes:
            outrow.append(exitcounts[e])
        outrows.append(outrow)

        #
        # get pending counts for the task list for each day
        # and fill in the 7th column...
        #
        dimlist, pendings = self.poms_service.filesPOMS.get_pending_for_task_lists(dbhandle, samhandle, daytasks)
        for i in range(len(pendings)):
            outrows[i][7] = pendings[i]

        if tl and tl[0]:
            name = tl[0].campaign_snap_obj.name

        else:
            name = ''
        return name, columns, outrows, dimlist, experiment, tmaxs, prevlink, nextlink, tdays, str(tmin)[:16], str(tmax)[:16]


    def get_pending_for_campaigns(self, dbhandle, samhandle, campaign_list, tmin, tmax):

        task_list_list = []

        logit.log("in get_pending_for_campaigns, tmin %s tmax %s" % (tmin, tmax))

        for c in campaign_list:
            tl = (dbhandle.query(Task).
                  options(joinedload(Task.campaign_snap_obj)).
                  options(joinedload(Task.campaign_definition_snap_obj)).
                  filter(Task.campaign_id == c.campaign_id,
                         Task.created >= tmin, Task.created < tmax).
                  all())
            task_list_list.append(tl)

        return self.poms_service.filesPOMS.get_pending_for_task_lists(dbhandle, samhandle, task_list_list)


    def get_pending_for_task_lists(self, dbhandle, samhandle, task_list_list):
        dimlist = []
        explist = []
        # experiment = None
        logit.log("get_pending_for_task_lists: task_list_list (%d): %s" % (len(task_list_list), task_list_list))
        for tl in task_list_list:
            diml = ["("]
            for task in tl:
                #if task.project == None:
                #    continue
                diml.append("(snapshot_for_project_name %s" % task.project)
                diml.append("minus ( snapshot_for_project_name %s and (" % task.project)
                sep = ""
                for pat in str(task.campaign_definition_snap_obj.output_file_patterns).split(','):
                    if (pat == "None"):
                        pat = "%"
                    diml.append(sep)
                    diml.append("isparentof: ( file_name '%s' and version '%s' with availability physical )" %
                                (pat, task.campaign_snap_obj.software_version))
                    sep = "or"
                diml.append(")")
                diml.append(")")
                diml.append(")")
                diml.append("union")
            diml[-1] = ")"

            if len(diml) <= 1:
                diml[0] = "project_name no_project_info"

            dimlist.append(" ".join(diml))

            if len(tl):
                explist.append(tl[0].campaign_definition_snap_obj.experiment)
            else:
                explist.append("samdev")

        logit.log("get_pending_for_task_lists: dimlist (%d): %s" % (len(dimlist), dimlist))
        count_list = samhandle.count_files_list(explist, dimlist)
        logit.log("get_pending_for_task_lists: count_list (%d): %s" % (len(dimlist), count_list))
        return dimlist, count_list


    def report_declared_files(self, flist, dbhandle):
        now = datetime.now(utc)
        # the "extra" first query on Job is to make sure we get a shared lock
        # on Job before trying to get an update lock on JobFile, which will
        # then try to get a lock on Job, but can deadlock with someone
        # otherwise doing update_job()..
        dbhandle.query(Job, JobFile).with_for_update(of=Job, read=True).filter(JobFile.job_id == Job.job_id, JobFile.file_name.in_(flist)).all()
        dbhandle.query(JobFile).filter(JobFile.file_name.in_(flist)).update({JobFile.declared: now}, synchronize_session=False)
        dbhandle.commit()
