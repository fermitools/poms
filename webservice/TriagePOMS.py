#!/usr/bin/env python

### This module contain the methods that handle the
### List of methods:  job_counts, triage_job, job_table, failed_jobs_by_whatever
### Author: Felipe Alba ahandresf@gmail.com, This code is just a modify version of
### functions in poms_service.py written by Marc Mengel, Stephen White and Michael Gueith.
### October, 2016.
import urllib.request, urllib.parse, urllib.error
from . import logit

from .poms_model import JobHistory, Job, Task, Campaign, CampaignDefinition, ServiceDowntime, Service
from .elasticsearch import Elasticsearch
from sqlalchemy import func, desc, not_, and_
from collections import OrderedDict

from .pomscache import pomscache

class TriagePOMS(object):


    def __init__(self, ps):
        self.poms_service = ps


    def job_counts(self, dbhandle, task_id=None, campaign_id=None, tmin=None, tmax=None, tdays=None):
        ### This one method was deleted from the main script

        (tmin, tmax, tmins, tmaxs,
         nextlink, prevlink, time_range_string,tdays) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'job_counts')
        q = dbhandle.query(func.count(Job.status), Job.status).group_by(Job.status)
        if tmax is not None:
            q = q.filter(Job.updated <= tmax, Job.updated >= tmin)

        if task_id:
            q = q.filter(Job.task_id == task_id)

        if campaign_id:
            q = q.join(Task, Job.task_id == Task.task_id).filter(Task.campaign_id == campaign_id)

        out = OrderedDict([("All", 0), ("Idle", 0), ("Running", 0),
                           ("Held", 0), ("Total Completed", 0), ("Completed", 0), ("Located", 0), ("Removed", 0)])
        for row in q.all():
            # this rather bizzare hoseyness is because we want
            # "Running" to also match "running: copying files in", etc.
            # so we ignore the first character and do a match
            if row[1][1:7] == "unning":
                short = "Running"
            else:
                short = row[1]
            out[short] = out.get(short, 0) + int(row[0])
            out["All"] = out.get("All", 0) + int(row[0])
            out["Total Completed"] = out["Completed"] + out["Located"]

        return out


    @pomscache.cache_on_arguments()
    def triage_job(self, dbhandle, jobsub_fetcher, config, job_id, tmin=None, tmax=None, tdays=None, force_reload=False):
        # we don't really use these for anything but we might want to
        # pass them into a template to set time ranges...
        (tmin, tmax, tmins, tmaxs,
         nextlink, prevlink, time_range_string,tdays) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'show_campaigns?')
        job_file_list = self.poms_service.filesPOMS.job_file_list(dbhandle, jobsub_fetcher, job_id, force_reload)
        output_file_names_list = []
        job_info = (dbhandle.query(Job, Task, CampaignDefinition, Campaign)
                    .filter(Job.job_id == job_id)
                    .filter(Job.task_id == Task.task_id)
                    .filter(Campaign.campaign_definition_id == CampaignDefinition.campaign_definition_id)
                    .filter(Task.campaign_id == Campaign.campaign_id).first())
        job_history = dbhandle.query(JobHistory).filter(JobHistory.job_id == job_id).order_by(JobHistory.created).all()
        output_file_names_list = [x.file_name for x in job_info[0].job_files if x.file_type == "output"]

        #begins service downtimes
        first = job_history[0].created
        last = job_history[-1].created

        downtimes1 = (dbhandle.query(ServiceDowntime, Service)
                      .filter(ServiceDowntime.service_id == Service.service_id)
                      .filter(Service.name != "All").filter(Service.name != "DCache")
                      .filter(Service.name != "Enstore")
                      .filter(Service.name != "SAM")
                      .filter(~Service.name.endswith("sam"))
                      .filter(ServiceDowntime.downtime_started >= first)
                      .filter(ServiceDowntime.downtime_started < last)
                      .filter(ServiceDowntime.downtime_ended >= first)
                      .filter(ServiceDowntime.downtime_ended < last).all())

        # downtimes2 ?!?
        #downtimes = downtimes1 + downtimes2
        downtimes = downtimes1
        #ends service downtimes


        #begins condor event logs
        es = Elasticsearch(config)

        query = {
            'sort': [{'@timestamp': {'order': 'asc'}}],
            'size': 100,
            'query': {
                'term': {'jobid': job_info.Job.jobsub_job_id}
            }
        }

        try:
            es_response = es.search(index='fifebatch-logs-*', types=['condor_eventlog'], query=query)
        except:
            es_response = None

        #ends condor event logs


        #get cpu efficiency
        query = {
            'fields': ['efficiency'],
            'query': {
                'term': {'jobid': job_info.Job.jobsub_job_id}
            }
        }

        try:
            es_efficiency_response = es.search(index='fifebatch-jobs', types=['job'], query=query)
        except:
            es_efficiency_response = None

        try:
            if es_efficiency_response and "fields" in list(es_efficiency_response.get("hits").get("hits")[0].keys()):
                efficiency = int(es_efficiency_response.get('hits').get('hits')[0].get('fields').get('efficiency')[0] * 100)
            else:
                efficiency = None
        except:
            efficiency = None

        #ends get cpu efficiency

        task_jobsub_job_id = self.poms_service.taskPOMS.task_min_job(dbhandle, job_info.Job.task_id)
        return job_file_list, job_info, job_history, downtimes, output_file_names_list, es_response, efficiency, tmin, task_jobsub_job_id

        # return template.render(job_id=job_id, job_file_list=job_file_list, job_info=job_info,
        # job_history=job_history, downtimes=downtimes, output_file_names_list=output_file_names_list,
        # es_response=es_response, efficiency=efficiency, tmin=tmin, current_experimenter=cherrypy.session.get('experimenter'),
        # pomspath=self.path, help_page="TriageJobHelp", task_jobsub_job_id=task_jobsub_job_id, version=self.version)


    def job_table(self, dbhandle, sesshandle, tmin=None, tmax=None, tdays=1, **kwargs):

        session_experiment = sesshandle.get('experimenter').session_experiment
        (tmin, tmax, tmins, tmaxs,
         nextlink, prevlink, time_range_string,tdays) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays, 'job_table?')
        extra = ""
        filtered_fields = {}

        q = dbhandle.query(Job, Task, Campaign)
        q = q.execution_options(stream_results=True)
        q = q.filter(Job.task_id == Task.task_id, Task.campaign_id == Campaign.campaign_id, Campaign.experiment == session_experiment)
        q = q.filter(Job.updated >= tmin, Job.updated <= tmax)

        keyword = kwargs.get('keyword')
        if keyword:
            q = q.filter(Task.project.like("%{}%".format(keyword)))
            extra = extra + "with keyword %s" % keyword
            filtered_fields['keyword'] = keyword

        task_id = kwargs.get('task_id')
        if task_id:
            q = q.filter(Task.task_id == int(task_id))
            extra = extra + "in task id %s" % task_id
            filtered_fields['task_id'] = task_id

        campaign_id = kwargs.get('campaign_id')
        if campaign_id:
            q = q.filter(Task.campaign_id == int(campaign_id))
            extra = extra + "in campaign id %s" % campaign_id
            filtered_fields['campaign_id'] = campaign_id

        experiment = kwargs.get('experiment')
        if experiment:
            q = q.filter(Campaign.experiment == experiment)
            extra = extra + "in experiment %s" % experiment
            filtered_fields['experiment'] = experiment

        dataset = kwargs.get('dataset')
        if dataset:
            q = q.filter(Campaign.dataset == dataset)
            extra = extra + "in dataset %s" % dataset
            filtered_fields['dataset'] = dataset

        campaign_name = kwargs.get('campaign_name')
        if campaign_name:
            q = q.filter(Campaign.name == campaign_name)
            filtered_fields['campaign_name'] = campaign_name

        # alias for failed_jobs_by_whatever
        name = kwargs.get('name')
        if name:
            q = q.filter(Campaign.name == name)
            filtered_fields['campaign_name'] = name

        campaign_def_id = kwargs.get('campaign_def_id')
        if campaign_def_id:
            q = q.filter(Campaign.campaign_definition_id == campaign_def_id)
            filtered_fields['campaign_def_id'] = campaign_def_id

        vo_role = kwargs.get('vo_role')
        if vo_role:
            q = q.filter(Campaign.vo_role == vo_role)
            filtered_fields['vo_role'] = vo_role

        input_dataset = kwargs.get('input_dataset')
        if input_dataset:
            q = q.filter(Task.input_dataset == input_dataset)
            filtered_fields['input_dataset'] = input_dataset

        output_dataset = kwargs.get('output_dataset')
        if output_dataset:
            q = q.filter(Task.output_dataset == output_dataset)
            filtered_fields['output_dataset'] = output_dataset

        task_status = kwargs.get('task_status')
        if task_status and task_status != 'All' and task_status != 'Total Completed':
            q = q.filter(Task.status == task_status)
            filtered_fields['task_status'] = task_status

        project = kwargs.get('project')
        if project:
            q = q.filter(Task.project == project)
            filtered_fields['project'] = project

        #
        # this one for our effeciency percentage decile...
        # i.e. if you want jobs in the 80..90% eficiency range
        # you ask for eff_d == 8...
        # ... or with eff_d of -1, one where we don't have good cpu/wall time data
        #
        eff_d = kwargs.get('eff_d')
        if eff_d:
            if eff_d == "-1":
                q = q.filter(not_(and_(Job.cpu_time > 0.0, Job.wall_time > 0 , JOb.cpu_time < Job.wall_time * 10)))
            else:
                q = q.filter(Job.wall_time > 0.0, Job.cpu_time > 0.0, Job.cpu_time < Job.wall_time*10,  func.floor(Job.cpu_time * 10 / Job.wall_time) == eff_d)
            filtered_fields['eff_d'] = eff_d

        jobsub_job_id = kwargs.get('jobsub_job_id')
        if jobsub_job_id:
            q = q.filter(Job.jobsub_job_id == jobsub_job_id)
            filtered_fields['jobsub_job_id'] = jobsub_job_id

        node_name = kwargs.get('node_name')
        if node_name:
            q = q.filter(Job.node_name == node_name)
            filtered_fields['node_name'] = node_name

        cpu_type = kwargs.get('cpu_type')
        if cpu_type:
            q = q.filter(Job.cpu_type == cpu_type)
            filtered_fields['cpu_type'] = cpu_type

        host_site = kwargs.get('host_site')
        if host_site:
            q = q.filter(Job.host_site == host_site)
            filtered_fields['host_site'] = host_site

        job_status = kwargs.get('job_status')
        if job_status and job_status != 'All' and job_status != 'Total Completed':
            # this rather bizzare hoseyness is because we want
            # "Running" to also match "running: copying files in", etc.
            # so we ignore the first character and do a "like" match
            # on the rest...
            q = q.filter(Job.status.like('%' + job_status[1:] + '%'))
            filtered_fields['job_status'] = job_status

        user_exe_exit_code = kwargs.get('user_exe_exit_code')
        if user_exe_exit_code:
            q = q.filter(Job.user_exe_exit_code == int(user_exe_exit_code))
            extra = extra + "with exit code %s" % user_exe_exit_code
            filtered_fields['user_exe_exit_code'] = user_exe_exit_code

        output_files_declared = kwargs.get('output_files_declared')
        if output_files_declared:
            q = q.filter(Job.output_files_declared == output_files_declared)
            filtered_fields['output_files_declared'] = output_files_declared


        jl = q.all()


        if jl:
            jobcolumns = list(jl[0][0]._sa_instance_state.class_.__table__.columns.keys())
            taskcolumns = list(jl[0][1]._sa_instance_state.class_.__table__.columns.keys())
            campcolumns = list(jl[0][2]._sa_instance_state.class_.__table__.columns.keys())
        else:
            jobcolumns = []
            taskcolumns = []
            campcolumns = []

        sift = kwargs.get('sift')
        if sift:  # it was bool(sift)
            campaign_box = task_box = job_box = ""

            if kwargs.get('campaign_checkbox') == "on":
                campaign_box = "checked"
            else:
                campcolumns = []
            if kwargs.get('task_checkbox') == "on":
                task_box = "checked"
            else:
                taskcolumns = []
            if kwargs.get('job_checkbox') == "on":
                job_box = "checked"
            else:
                jobcolumns = []

            filtered_fields_checkboxes = {"campaign_checkbox": campaign_box, "task_checkbox": task_box, "job_checkbox": job_box}
            filtered_fields.update(filtered_fields_checkboxes)

            prevlink = prevlink + "&" + urllib.parse.urlencode(filtered_fields).replace("checked", "on") + "&sift=" + str(sift)
            nextlink = nextlink + "&" + urllib.parse.urlencode(filtered_fields).replace("checked", "on") + "&sift=" + str(sift)
        else:
            filtered_fields_checkboxes = {"campaign_checkbox": "checked",
                                          "task_checkbox": "checked",
                                          "job_checkbox": "checked"}  # setting this for initial page visit
            filtered_fields.update(filtered_fields_checkboxes)

        hidecolumns = ['task_id', 'campaign_id', 'created', 'creator', 'updated',
                       'updater', 'command_executed', 'task_parameters', 'depends_on', 'depend_threshold', 'task_order']

        #template = self.jinja_env.get_template('job_table.html')
        return jl, jobcolumns, taskcolumns, campcolumns, tmins, tmaxs, prevlink, nextlink, tdays, extra, hidecolumns, filtered_fields, time_range_string


    def failed_jobs_by_whatever(self, dbhandle, sesshandle, tmin=None, tmax=None, tdays=1, f=[], go=None):
        session_experiment = sesshandle.get('experimenter').session_experiment
        # deal with single/multiple argument silliness
        if isinstance(f, str):
            f = [f]

        if 'experiment' not in f:
            f.append('experiment')

        (tmin, tmax, tmins, tmaxs, nextlink, prevlink,
         time_range_string,tdays) = self.poms_service.utilsPOMS.handle_dates(tmin, tmax, tdays,
                                                                       'failed_jobs_by_whatever?%s&' % ('&'.join(['f=%s' % x for x in f])))

        #
        # build up:
        # * a group-by-list (gbl)
        # * a query-args-list (quargs)
        # * a columns list
        #
        gbl = []
        qargs = []
        columns = []


        for field in f:
            if f is None:
                continue
            columns.append(field)
            if hasattr(Job, field):
                gbl.append(getattr(Job, field))
                qargs.append(getattr(Job, field))
            elif hasattr(Campaign, field):
                gbl.append(getattr(Campaign, field))
                qargs.append(getattr(Campaign, field))

        possible_columns = [
                            # job fields
                            'node_name', 'cpu_type', 'host_site', 'user_exe_exit_code',
                            # campaign fields
                            'name', 'vo_role', 'dataset', 'software_version', 'experiment'
                            ]

        qargs.append(func.count(Job.job_id))
        columns.append("count")

        #
        #
        #
        q = dbhandle.query(*qargs)
        q = q.join(Task, Campaign)
        q = q.filter(Campaign.experiment == session_experiment)
        q = q.filter(Job.updated >= tmin, Job.updated <= tmax, Job.user_exe_exit_code != 0)
        q = q.group_by(*gbl).order_by(desc(func.count(Job.job_id)))

        jl = q.all()
        if jl:
            #logit.log( "got jobtable %s " % repr( jl[0].__dict__) )
            logit.log("got jobtable %s " % repr(jl[0]))

        return jl, possible_columns, columns, tmins, tmaxs, tdays, prevlink, nextlink, time_range_string, tdays
