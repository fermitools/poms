import cherrypy
from cherrypy.lib import sessions
import glob
import os
import time
import time_grid
import json
import urllib
import socket
import subprocess
import select
from collections import OrderedDict

from sqlalchemy import (Column, Integer, Sequence, String, DateTime, ForeignKey,
    and_, or_, not_,  create_engine, null, desc, text, func, exc, distinct
)
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from datetime import datetime, tzinfo,timedelta
from jinja2 import Environment, PackageLoader
import shelve
from poms.model.poms_model import (Service, ServiceDowntime, Experimenter, Experiment,
    ExperimentsExperimenters, Job, JobHistory, Task, CampaignDefinition,
    TaskHistory, Campaign, LaunchTemplate, Tag, CampaignsTags, JobFile,
    CampaignSnapshot, CampaignDefinitionSnapshot, LaunchTemplateSnapshot,
    CampaignRecovery,RecoveryType, CampaignDependency
)

from utc import utc
from crontab import CronTab
import gc
from elasticsearch import Elasticsearch
import pprint
import version


global_version="unknown"

import CalendarPOMS
import DBadminPOMS
import CampaignsPOMS
import JobsPOMS
import TaskPOMS
import UtilsPOMS
import TagsPOMS
import TriagePOMS
import FilesPOMS
import AccessPOMS
import TablesPOMS


def error_response():
    dump = ""
    if cherrypy.config.get("dump",True):
        dump = cherrypy._cperror.format_exc()
    message = dump.replace('\n','<br/>')

    jinja_env = Environment(loader=PackageLoader('poms.webservice','templates'))
    template = jinja_env.get_template('error_response.html')
    path = cherrypy.config.get("pomspath","/poms")
    body = template.render(message=message,pomspath=path,dump=dump,version=global_version)
    cherrypy.response.status = 500
    cherrypy.response.headers['content-type'] = 'text/html'
<<<<<<< HEAD
    cherrypy.response.body = body.encode()
=======
    cherrypy.response.body = str(body)
>>>>>>> release/v1_0_0a
    cherrypy.log(dump)



class poms_service:


    _cp_config = {'request.error_response': error_response,
                  'error_page.404': "%s/%s" % (os.path.abspath(os.getcwd()),'/templates/page_not_found.html')
                  }

    def __init__(self):
        self.jinja_env = Environment(loader=PackageLoader('poms.webservice','templates'))
        self.path = cherrypy.config.get("pomspath","/poms")
        self.hostname = socket.getfqdn()
        self.version = version.get_version()
        global_version = self.version
        self.calendarPOMS = CalendarPOMS.CalendarPOMS()
        self.dbadminPOMS = DBadminPOMS.DBadminPOMS()
        self.campaignsPOMS = CampaignsPOMS.CampaignsPOMS(self)
        self.jobsPOMS = JobsPOMS.JobsPOMS(self)
        self.taskPOMS = TaskPOMS.TaskPOMS(self)
        self.utilsPOMS = UtilsPOMS.UtilsPOMS(self)
        self.tagsPOMS = TagsPOMS.TagsPOMS(self)
        self.filesPOMS = FilesPOMS.Files_status(self)
        self.triagePOMS=TriagePOMS.TriagePOMS(self)
        self.accessPOMS=AccessPOMS.AccessPOMS()
        self.tablesPOMS = TablesPOMS.TablesPOMS(self, cherrypy.log)


    @cherrypy.expose
    def headers(self):
        return repr(cherrypy.request.headers)


    @cherrypy.expose
    def sign_out(self):
        cherrypy.lib.sessions.expire()
        log_out_url = "https://" + self.hostname + "/Shibboleth.sso/Logout"
        raise cherrypy.HTTPRedirect(log_out_url)


    @cherrypy.expose
    def index(self):
        template = self.jinja_env.get_template('index.html')
        return template.render(services=self.service_status_hier('All'),current_experimenter=cherrypy.session.get('experimenter'),

        launches = self.taskPOMS.get_job_launches(cherrypy.request.db),
                               do_refresh=1, pomspath=self.path, help_page="DashboardHelp", version=self.version)


    @cherrypy.expose
    def es(self):
        template = self.jinja_env.get_template('elasticsearch.html')

        es = Elasticsearch(config=cherrypy.config)

        query = {
            'sort' : [{ '@timestamp' : {'order' : 'asc'}}],
            'query' : {
                'term' : { 'jobid' : '17519748.0@fifebatch2.fnal.gov' }
            }
        }

        es_response= es.search(index='fifebatch-logs-*', types=['condor_eventlog'], query=query)
        pprint.pprint(es_response)
        return template.render(pomspath=self.path, es_response=es_response)


####################
### UtilsPOMS

    @cherrypy.expose
    def quick_search(self, search_term):
        self.utilsPOMS.quick_search(cherrypy.request.db, cherrypy.HTTPRedirect, search_term)


    @cherrypy.expose
    def jump_to_job(self, jobsub_job_id, **kwargs):
        self.utilsPOMS.jump_to_job(cherrypy.request.db, cherrypy.HTTPRedirect, jobsub_job_id, **kwargs)
#----------------

##############################
### CALENDAR
#Using CalendarPOMS.py module
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def calendar_json(self, start, end, timezone, _):
        return self.calendarPOMS.calendar_json(cherrypy.request.db, start, end, timezone, _)


    @cherrypy.expose
    def calendar(self):
        template = self.jinja_env.get_template('calendar.html')
        rows = self.calendarPOMS.calendar(dbhandle = cherrypy.request.db)
        return template.render(rows=rows,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                pomspath=self.path,
                                help_page="CalendarHelp")


    @cherrypy.expose
    def add_event(self, title, start, end):
        #title should be something like minos_sam:27 DCache:12 All:11 ...
        return self.calendarPOMS.add_event.calendar(cherrypy.request.db,title, start, end)

    @cherrypy.expose
    def edit_event(self, title, start, new_start, end, s_id):  #even though we pass in the s_id we should not rely on it because they can and will change the service name
        return self.calendarPOMS.edit_event(cherrypy.request.db, title, start, new_start, end, s_id)


    @cherrypy.expose
    def service_downtimes(self):
        template = self.jinja_env.get_template('service_downtimes.html')
        rows = self.calendarPOMS.service_downtimes(cherrypy.request.db)
        return template.render(rows=rows,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                pomspath=self.path,
                                help_page="ServiceDowntimesHelp")


    @cherrypy.expose
    def update_service(self, name, parent, status, host_site, total, failed, description):
        return self.calendarPOMS.update_service(cherrypy.request.db, cherrypy.log, name, parent, status, host_site, total, failed, description)


    @cherrypy.expose
    def service_status(self, under='All'):
        template = self.jinja_env.get_template('service_status.html')
        list=self.calendarPOMS.service_status (cherrypy.request.db, under)
        return template.render(list=list,
                                name=under,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                pomspath=self.path,
                                help_page="ServiceStatusHelp",
                                version=self.version)
#--------------------------


######
    #print "Check where should be this function."
    '''
    Apparently this function is not related with Calendar
    '''
    def service_status_hier(self, under='All', depth=0):
        p = cherrypy.request.db.query(Service).filter(Service.name==under).first()
        if depth == 0:
            res = '<div class="ui accordion styled">\n'
        else:
            res = ''
        active = ""
        for s in cherrypy.request.db.query(Service).filter(Service.parent_service_id==p.service_id).order_by(Service.name).all():
             posneg = {"good": "positive", "degraded": "orange", "bad": "negative"}.get(s.status, "")
             icon = {"good": "checkmark", "bad": "remove", "degraded": "warning sign"}.get(s.status,"help circle")
             if s.host_site:
                 res = res + """
                     <div class="title %s">
                      <i class="dropdown icon"></i>
                      <button class="ui button %s tbox_delayed" data-content="%s" data-variation="basic">
                         %s (%d/%d)
                         <i class="icon %s"></i>
                       </button>
                     </div>
                     <div  class="content %s">
                         <a target="_blank" href="%s">
                         <i class="icon external"></i>
                         source webpage
                         </a>
                     </div>
                  """ % (active, posneg, s.description, s.name, s.failed_items, s.items, icon, active, s.host_site)
             else:
                 res = res + """
                    <div class="title %s">
                      <i class="dropdown icon"></i>
                      <button class="ui button %s tbox_delayed" data-content="%s" data-variation="basic">
                       %s (%d/%d)
                      <i class="icon %s"></i>
                      </button>
                    </div>
                    <div class="content %s">
                      <p>components:</p>
                      %s
                    </div>
                 """ % (active, posneg, s.description, s.name, s.failed_items, s.items, icon, active,  self.service_status_hier(s.name, depth + 1))
             active = ""

        if depth == 0:
            res = res + "</div>"
        return res
#####
### DBadminPOMS
    @cherrypy.expose
    def raw_tables(self):
        if not self.accessPOMS.can_db_admin(cherrypy.request.headers.get, cherrypy.session.get):
            raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        template = self.jinja_env.get_template('raw_tables.html')
        return template.render(list=self.tablesPOMS.admin_map.keys(), current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path, help_page="RawTablesHelp", version=self.version)


    @cherrypy.expose
    def user_edit(self, *args, **kwargs):
        data = self.dbadminPOMS.user_edit(cherrypy.request.db, *args, **kwargs)
        template = self.jinja_env.get_template('user_edit.html')
        return template.render(data=data, current_experimenter=cherrypy.session.get('experimenter'),
                                pomspath=self.path, help_page="EditUsersHelp", version=self.version)


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def experiment_members(self, experiment, *args, **kwargs):
        trows = self.dbadminPOMS.experiment_members(cherrypy.request.db, experiment, *args, **kwargs)
        return trows


    @cherrypy.expose
#    @cherrypy.tools.json_out()
    def member_experiments(self, email, *args, **kwargs):
        trows = self.dbadminPOMS.member_experiments(cherrypy.request.db, email, *args, **kwargs)
        return trows


    @cherrypy.expose
    def experiment_edit(self, message=None):
        experiments=self.dbadminPOMS.experiment_edit(cherrypy.request.db)
        template = self.jinja_env.get_template('experiment_edit.html')
        return template.render(message=message, experiments=experiments, current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path, help_page="ExperimentEditHelp", version=self.version)


    @cherrypy.expose
    def experiment_authorize(self, *args, **kwargs):
        if not self.accessPOMS.can_db_admin(cherrypy.request.headers.get, cherrypy.session.get):
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        message = self.dbadminPOMS.experiment_authorize(cherrypy.request.db, cherrypy.log, *args, **kwargs)
        return self.experiment_edit(message)
#-----------------------------------------
#################################
### CampaignsPOMS
    @cherrypy.expose
    def launch_template_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.launch_template_edit(cherrypy.request.db, cherrypy.log, cherrypy.session.get, *args, **kwargs)
        template = self.jinja_env.get_template('launch_template_edit.html')
        return template.render(data=data, current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path, help_page="LaunchTemplateEditHelp", version=self.version)


    @cherrypy.expose
    def campaign_definition_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_definition_edit(cherrypy.request.db, cherrypy.log, cherrypy.session.get, *args, **kwargs)
        template = self.jinja_env.get_template('campaign_definition_edit.html')
        return template.render(data=data, current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path, help_page="CampaignDefinitionEditHelp", version=self.version)


    @cherrypy.expose
    def campaign_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_edit(cherrypy.request.db, cherrypy.log, cherrypy.session, *args, **kwargs)
        template = self.jinja_env.get_template('campaign_edit.html')
        return template.render(data=data, current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path, help_page="CampaignEditHelp", version=self.version)


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def campaign_edit_query(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_edit_query(cherrypy.request.db, *args, **kwargs)
        return data

<<<<<<< HEAD

    @cherrypy.expose
    def new_task_for_campaign(self, campaign_name, command_executed, experimenter_name, dataset_name = None):
        return self.campaignsPOMS.new_task_for_campaign(cherrypy.request.db, campaign_name, command_executed, experimenter_name, dataset_name)


    @cherrypy.expose
    def show_campaigns(self, experiment=None, tmin=None, tmax=None, tdays=1, active=True, **kwargs):
        (counts, counts_keys, clist, dimlist,
            tmin, tmax, tmins, tmaxs,
            nextlink, prevlink, time_range_string
        ) = self.campaignsPOMS.show_campaigns(cherrypy.request.db, cherrypy.log,
                                            cherrypy.request.samweb_lite, experiment=experiment,
                                            tmin=tmin, tmax=tmax, tdays=tdays, active=active)
=======
    @cherrypy.expose
    def wrapup_tasks(self):
        cherrypy.response.headers['Content-Type'] = "text/plain"
        now =  datetime.now(utc)
        res = ["wrapping up:"]

        #
        # make jobs which completed with no output files located.
        subq = cherrypy.request.db.query(func.count(JobFile.file_name)).filter(JobFile.job_id == Job.job_id, JobFile.file_type == 'output')
        cherrypy.request.db.query(Job).filter(subq == 0).update({'status':'Located'})
        for task in cherrypy.request.db.query(Task).options(subqueryload(Task.jobs)).filter(Task.status != "Completed", Task.status != "Located").all():
             total = 0
             running = 0
             for j in task.jobs:
                 total = total + 1
                 if j.status != "Completed" and j.status != "Located" and j.status != "Removed":
                     running = running + 1

             res.append("Task %d total %d running %d " % (task.task_id, total, running))

             if (total > 0 and running == 0) or (total == 0 and  now - task.created > timedelta(days= 2)):
                 task.status = "Completed"
                 task.updated = datetime.now(utc)
                 cherrypy.request.db.add(task)

        # mark them all completed, so we can look them over..
        cherrypy.request.db.commit()

        lookup_task_list = []
        lookup_dims_list = []
        lookup_exp_list = []
        n_completed = 0
        n_stale = 0
        n_project = 0
        n_located = 0
        # query with a with_for_update so we don't have two updates mark
        # it Located and possibly also launch jobs.
        for task in cherrypy.request.db.query(Task).with_for_update(of=Task).options(subqueryload(Task.jobs)).filter(Task.status == "Completed").all():
            n_completed = n_completed + 1
            # if it's been 2 days, just declare it located; its as 
            # located as its going to get...
            if (now - task.updated > timedelta(days=2)):
                 n_located = n_located + 1
                 n_stale = n_stale + 1
                 task.status = "Located"
                 for j in task.jobs:
                     j.status = "Located"
                     j.output_files_declared = True
                 task.updated = datetime.now(utc)
                 cherrypy.request.db.add(task)
                 if not self.launch_recovery_if_needed(cherrypy.request.db,task):
                     self.launch_dependents_if_needed(task)
            elif task.project:
                 # task had a sam project, add to the list to look
                 # up in sam
                 n_project = n_project + 1
                 basedims = "snapshot_for_project_name %s " % task.project
		 allkiddims = basedims
		 for pat in str(task.campaign_definition_snap_obj.output_file_patterns).split(','):
		     if pat == 'None':
			pat = '%'
		     allkiddims = "%s and isparentof: ( file_name '%s' and version '%s' with availability physical ) " % (allkiddims, pat, task.campaign_snap_obj.software_version)
                 lookup_exp_list.append(task.campaign_snap_obj.experiment)
                 lookup_task_list.append(task)
                 lookup_dims_list.append(allkiddims)
            else:
                 # we don't have a project, guess off of located jobs
                 locflag = True
                 for j in task.jobs:
                     if j.status != "Located":
                         locaflag = False
                 if locflag:
                     n_located = n_located + 1
                     task.status = "Located"
		     for j in task.jobs:
			 j.status = "Located"
			 j.output_files_declared = True
                     task.updated = datetime.now(utc)
                     cherrypy.request.db.add(task)
                     if not self.launch_recovery_if_needed(cherrypy.request.db, task):
                         self.launch_dependents_if_needed(task)

        cherrypy.request.db.commit()
        
        summary_list = cherrypy.request.samweb_lite.fetch_info_list(lookup_task_list)
        count_list = cherrypy.request.samweb_lite.count_files_list(lookup_exp_list,lookup_dims_list)
        thresholds = []
	cherrypy.log("wrapup_tasks: summary_list: %s" % repr(summary_list))
        for i in range(len(summary_list)):
            # XXX
            # this is using a 90% threshold, this ought to be
            # a tunable in the campaign_definition.  Basically we consider it
            # located if 90% of the files it consumed have suitable kids...
            # cfrac = lookup_task_list[i].campaign_definition_snap_obj.cfrac

            cfrac = 0.9
            threshold = (summary_list[i].get('tot_consumed',0) * cfrac)
            thresholds.append(threshold)
            if float(count_list[i]) >= threshold and threshold > 0:
                n_located = n_located + 1
                task = lookup_task_list[i]
                task.status = "Located"
                for j in task.jobs:
                    j.status = "Located"
                    j.output_files_declared = True
                task.updated = datetime.now(utc)
                cherrypy.request.db.add(task)

                if not self.launch_recovery_if_needed(task):
                    self.launch_dependents_if_needed(task)

        res.append("Counts: completed: %d stale: %d project %d: located %d" %
        	(n_completed, n_stale , n_project, n_located))
                
        res.append("count_list: %s" % count_list)
        res.append("thresholds: %s" % thresholds)
        res.append("lookup_dims_list: %s" % lookup_dims_list)

        cherrypy.request.db.commit()

        return "\n".join(res)

    def compute_status(self, task):
        st = self.job_counts(task_id = task.task_id)
        if task.status == "Located":
            return task.status
        res = "Idle"
        if (st['Held'] > 0):
            res = "Held"
        if (st['Running'] > 0):
            res = "Running"
        if (st['Completed'] > 0 and st['Idle'] == 0 and st['Held'] == 0 and st['Running'] == 0):
            res = "Completed"
            # no, not here we wait for "Located" status..
            #if task.status != "Completed":
            #    if not self.launch_recovery_if_needed(task):
            #        self.launch_dependents_if_needed(task)
        return res

    @cherrypy.expose
    def update_job(self, task_id = None, jobsub_job_id = 'unknown',  **kwargs):
         cherrypy.log("update_job( task_id %s, jobsub_job_id %s,  kwargs %s )" % (task_id, jobsub_job_id, repr(kwargs)))

         if not self.can_report_data():
              cherrypy.log("update_job: not allowed")
              return "Not Allowed"

         if task_id == "None":
             task_id = None

         if task_id:
             task_id = int(task_id)

         host_site = "%s_on_%s" % (jobsub_job_id, kwargs.get('slot','unknown'))

         jl = cherrypy.request.db.query(Job).options(subqueryload(Job.task_obj)).filter(Job.jobsub_job_id==jobsub_job_id).order_by(Job.job_id).all()
         first = True
         j = None
         for ji in jl:
             if first:
                j = ji
                first = False
             else:
                #
                # we somehow got multiple jobs with the sam jobsub_job_id
                #
                # mark the others as dups
                ji.jobsub_job_id="dup_"+ji.jobsub_job_id
                cherrypy.request.db.add(ji)
                # steal any job_files
	        files =  [x.file_name for x in j.job_files ]
                for jf in ji.job_files:
                    if jf.file_name not in files:
                        njf = JobFile(file_name = jf.file_name, file_type = jf.file_type, created =  jf.created, job_obj = j)
                        cherrypy.request.db.add(njf)

                cherrypy.request.db.delete(ji)
                cherrypy.request.db.flush()
                    
         if not j and task_id:
             t = cherrypy.request.db.query(Task).filter(Task.task_id==task_id).first()
             if t == None:
                 cherrypy.log("update_job -- no such task yet")
                 cherrypy.response.status="404 Task Not Found"
                 return "No such task"
             cherrypy.log("update_job: creating new job")
             j = Job()
             j.jobsub_job_id = jobsub_job_id.rstrip("\n")
             j.created = datetime.now(utc)
             j.updated = datetime.now(utc)
             j.task_id = task_id
             j.task_obj = t
             j.output_files_declared = False
             j.cpu_type = ''
             j.node_name = ''
             j.host_site = ''
             j.status = 'Idle'


         if j:
             cherrypy.log("update_job: updating job %d" % (j.job_id if j.job_id else -1))

             for field in ['cpu_type', 'node_name', 'host_site', 'status', 'user_exe_exit_code']:

                 if field == 'status' and j.status == "Located":
                     # stick at Located, don't roll back to Completed,etc.
                     continue

                 if kwargs.get(field, None):
                    setattr(j,field,kwargs[field].rstrip("\n"))
                 if not getattr(j,field, None):
                    if field != 'user_exe_exit_code':
                        setattr(j,field,'unknown')

             if kwargs.get('output_files_declared', None) == "True":
                 if j.status == "Completed" :
                     j.output_files_declared = True
                     j.status = "Located"

             for field in ['project','recovery_tasks_parent' ]:
                 if kwargs.get("task_%s" % field, None) and kwargs.get("task_%s" % field) != "None" and j.task_obj:
                    setattr(j.task_obj,field,kwargs["task_%s"%field].rstrip("\n"))
                    cherrypy.log("setting task %d %s to %s" % (j.task_obj.task_id, field, getattr(j.task_obj, field, kwargs["task_%s"%field])))


             for field in [ 'cpu_time', 'wall_time']:
                 if kwargs.get(field, None) and kwargs[field] != "None":
                    setattr(j,field,float(kwargs[field].rstrip("\n")))

             if kwargs.get('output_file_names', None):
                 cherrypy.log("saw output_file_names: %s" % kwargs['output_file_names'])
                 if j.job_files:
                     files =  [x.file_name for x in j.job_files if x.file_type == 'output']
                     # don't include metadata files
                     files =  [ f for f in files if f.find('.json') == -1 and f.find('.metadata') == -1]
                 else:
                     files = []

                 newfiles = kwargs['output_file_names'].split(' ')
                 for f in newfiles:
                     if not f in files:
                         if len(f) < 2 or f[0] == '-':  # ignore '0', '-D', etc...
                             continue
                         if f.find("log") >= 0:
                             ftype = "log"
                         else:
                             ftype = "output"

                         jf = JobFile(file_name = f, file_type = ftype, created =  datetime.now(utc), job_obj = j)
                         j.job_files.append(jf)
                         cherrypy.request.db.add(jf)

             if kwargs.get('input_file_names', None):
                 cherrypy.log("saw input_file_names: %s" % kwargs['input_file_names'])
                 if j.job_files:
                     files =  [x.file_name for x in j.job_files if x.file_type == 'input']
                 else:
                     files = []

                 newfiles = kwargs['input_file_names'].split(' ')
                 for f in newfiles:

                     if len(f) < 2 or f[0] == '-':  # ignore '0', '-D', etc...
                         continue

                     if not f in files:
                         jf = JobFile(file_name = f, file_type = "input", created =  datetime.now(utc), job_obj = j)
                         cherrypy.request.db.add(jf)


             if j.cpu_type == None:
                 j.cpu_type = ''

             cherrypy.log("update_job: db add/commit job status %s " %  j.status)

             j.updated =  datetime.now(utc)

             if j.task_obj:
                 newstatus = self.compute_status(j.task_obj)
                 if newstatus != j.task_obj.status:
                     j.task_obj.status = newstatus
                     j.task_obj.updated = datetime.now(utc)
                     j.task_obj.campaign_snap_obj.active = True

             cherrypy.request.db.add(j)
	     cherrypy.request.db.commit()

             cherrypy.log("update_job: done job_id %d" %  (j.job_id if j.job_id else -1))
 
         return "Ok."

              
    @cherrypy.expose
    def show_task_jobs(self, task_id, tmax = None, tmin = None, tdays = 1 ):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax,tdays,'show_task_jobs?task_id=%s' % task_id)

        jl = cherrypy.request.db.query(JobHistory,Job).filter(Job.job_id == JobHistory.job_id, Job.task_id==task_id ).order_by(JobHistory.job_id,JobHistory.created).all()
        tg = time_grid.time_grid()
        class fakerow:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)
        items = []
        extramap = {}
        laststatus = None
        lastjjid = None
        for jh, j in jl:
            if j.jobsub_job_id:
                jjid= j.jobsub_job_id.replace('fifebatch','').replace('.fnal.gov','')
            else:
                jjid= 'j' + str(jh.job_id)

            if j.status != "Completed" and j.status != "Located":
                extramap[jjid] = '<a href="%s/kill_jobs?job_id=%d"><i class="ui trash icon"></i></a>' % (self.path, jh.job_id)
            else:
                extramap[jjid] = '&nbsp; &nbsp; &nbsp; &nbsp;'
            if jh.status != laststatus or jjid != lastjjid:
                items.append(fakerow(job_id = jh.job_id,
                                  created = jh.created.replace(tzinfo=utc),
                                  status = jh.status,
                                  jobsub_job_id = jjid))
            laststatus = jh.status
            lastjjid = jjid

        job_counts = self.format_job_counts(task_id = task_id,tmin=tmins,tmax=tmaxs,tdays=tdays, range_string = time_range_string )
        key = tg.key(fancy=1)

        blob = tg.render_query_blob(tmin, tmax, items, 'jobsub_job_id', url_template=self.path + '/triage_job?job_id=%(job_id)s&tmin='+tmins, extramap = extramap)
        #screendata = screendata +  tg.render_query(tmin, tmax, items, 'jobsub_job_id', url_template=self.path + '/triage_job?job_id=%(job_id)s&tmin='+tmins, extramap = extramap)

        if len(jl) > 0:
            campaign_id = jl[0][1].task_obj.campaign_id
            cname = jl[0][1].task_obj.campaign_snap_obj.name
        else:
            campaign_id = 'unknown'
            cname = 'unknown'

        task_jobsub_id = self.task_min_job(task_id)

        template = self.jinja_env.get_template('show_task_jobs.html')
        return template.render( blob=blob, job_counts = job_counts,  taskid = task_id, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'),  extramap = extramap, do_refresh = 1, key = key, pomspath=self.path,help_page="ShowTaskJobsHelp", task_jobsub_id = task_jobsub_id, campaign_id = campaign_id,cname = cname, version=self.version)


    @cherrypy.expose
    def triage_job(self, job_id, tmin = None, tmax = None, tdays = None, force_reload = False):
        # we don't really use these for anything but we might want to
        # pass them into a template to set time ranges...
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin,tmax,tdays,'triage_job?job_id=%s' % job_id)

        job_file_list = self.job_file_list(job_id, force_reload)
        template = self.jinja_env.get_template('triage_job.html')

        output_file_names_list = []

        job_info = cherrypy.request.db.query(Job, Task, CampaignDefinition,  Campaign).filter(Job.job_id==job_id).filter(Job.task_id==Task.task_id).filter(Campaign.campaign_definition_id==CampaignDefinition.campaign_definition_id).filter(Task.campaign_id==Campaign.campaign_id).first()

        job_history = cherrypy.request.db.query(JobHistory).filter(JobHistory.job_id==job_id).order_by(JobHistory.created).all()

        output_file_names_list = [x.file_name for x in job_info[0].job_files if x.file_type == "output"]

        #begins service downtimes
        first = job_history[0].created
        last = job_history[len(job_history)-1].created

        downtimes1 = cherrypy.request.db.query(ServiceDowntime, Service).filter(ServiceDowntime.service_id == Service.service_id)\
        .filter(Service.name != "All").filter(Service.name != "DCache").filter(Service.name != "Enstore").filter(Service.name != "SAM").filter(~Service.name.endswith("sam"))\
        .filter(first >= ServiceDowntime.downtime_started).filter(first < ServiceDowntime.downtime_ended)\
        .filter(last >= ServiceDowntime.downtime_started).filter(last < ServiceDowntime.downtime_ended).all()

        downtimes2 = cherrypy.request.db.query(ServiceDowntime, Service).filter(ServiceDowntime.service_id == Service.service_id)\
        .filter(Service.name != "All").filter(Service.name != "DCache").filter(Service.name != "Enstore").filter(Service.name != "SAM").filter(~Service.name.endswith("sam"))\
        .filter(ServiceDowntime.downtime_started >= first).filter(ServiceDowntime.downtime_started < last)\
        .filter(ServiceDowntime.downtime_ended >= first).filter(ServiceDowntime.downtime_ended < last).all()

        downtimes = downtimes1 + downtimes2
        #ends service downtimes


        #begins condor event logs
        es = Elasticsearch()

        query = {
            'sort' : [{ '@timestamp' : {'order' : 'asc'}}],
            'size' : 100,
            'query' : {
                'term' : { 'jobid' : job_info.Job.jobsub_job_id }
            }
        }

        es_response= es.search(index='fifebatch-logs-*', types=['condor_eventlog'], query=query)
        #ends condor event logs


        #get cpu efficiency
        query = {
            'fields' : ['efficiency'],
            'query' : {
                'term' : { 'jobid' : job_info.Job.jobsub_job_id }
            }
        }

        es_efficiency_response = es.search(index='fifebatch-jobs', types=['job'], query=query)
        try:
            if es_efficiency_response and "fields" in es_efficiency_response.get("hits").get("hits")[0].keys():
                efficiency = int(es_efficiency_response.get('hits').get('hits')[0].get('fields').get('efficiency')[0] * 100)
            else:
                efficiency = None
        except:
	    efficiency = None
>>>>>>> release/v1_0_0a

        current_experimenter = cherrypy.session.get('experimenter')
        #~ cherrypy.log("current_experimenter.extra before: "+str(current_experimenter.extra))     # DEBUG
        if 'exp_selected' in kwargs:
            current_experimenter.extra = {'selected': kwargs['exp_selected']}
            cherrypy.session['experimenter'] = current_experimenter
            #~ cherrypy.log("current_experimenter.extra update... ")                               # DEBUG
        #~ cherrypy.log("current_experimenter.extra after: "+str(current_experimenter.extra))      # DEBUG

        experiments = self.dbadminPOMS.member_experiments(cherrypy.request.db, current_experimenter.email)

        template = self.jinja_env.get_template('show_campaigns.html')

        return template.render(In=("" if active == True else "In"), limit_experiment=experiment,
                                services=self.service_status_hier('All'), counts=counts, counts_keys=counts_keys,
                                cl=clist, tmins=tmins, tmaxs=tmaxs, tmin=str(tmin)[:16], tmax=str(tmax)[:16],
                                current_experimenter=current_experimenter, do_refresh=1,
                                next=nextlink, prev=prevlink, days=tdays, time_range_string=time_range_string,
                                key='', dimlist=dimlist, pomspath=self.path, help_page="ShowCampaignsHelp",
                                experiments=experiments,
                                dbg=kwargs,
                                version=self.version)


    @cherrypy.expose
    def campaign_info(self, campaign_id, tmin=None, tmax=None, tdays=None):
        (Campaign_info, time_range_string, tmins, tmaxs, Campaign_definition_info,
            Launch_template_info, tags, launched_campaigns, dimlist, cl, counts_keys,
            counts, launch_flist) = self.campaignsPOMS.campaign_info(cherrypy.request.db, cherrypy.log,
                                        cherrypy.request.samweb_lite, cherrypy.HTTPError, campaign_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_info.html')
        return template.render(Campaign_info=Campaign_info, time_range_string=time_range_string, tmins=tmins, tmaxs=tmaxs,
                        Campaign_definition_info=Campaign_definition_info, Launch_template_info=Launch_template_info,
                        tags=tags, launched_campaigns=launched_campaigns, dimlist=dimlist,
                        cl=cl, counts_keys=counts_keys, counts=counts, launch_flist=launch_flist,
                        current_experimenter=cherrypy.session.get('experimenter'),
                        do_refresh=0, pomspath=self.path,help_page="CampaignInfoHelp", version=self.version)


    @cherrypy.expose
    def campaign_time_bars(self, campaign_id=None, tag=None, tmin=None, tmax=None, tdays=1):
        (job_counts, blob, name, tmin, tmax,
            nextlink, prevlink, tdays,key, extramap) = self.campaignsPOMS.campaign_time_bars(cherrypy.request.db,
                                                            campaign_id = campaign_id, tag=tag, tmin=tmin, tmax=tmax, tdays=tdays)
        template = self.jinja_env.get_template('campaign_time_bars.html')
        return template.render(job_counts=job_counts, blob=blob, name=name, tmin=tmin, tmax=tmax,
                    current_experimenter=cherrypy.session.get('experimenter'),
                    do_refresh=1, next=nextlink, prev=prevlink, days=tdays, key=key,
                    pomspath=self.path, extramap=extramap, help_page="CampaignTimeBarsHelp", version=self.version)


    @cherrypy.expose
    def register_poms_campaign(self, experiment, campaign_name, version, user=None, campaign_definition=None, dataset="", role="Analysis", params=[]):
        campaign_id = self.campaignsPOMS.register_poms_campaign(cherrypy.request.db, cherrypy.log,
                            experiment, campaign_name, version, user, campaign_definition, dataset, role, params)
        return "Campaign=%d" % campaign_id


    @cherrypy.expose
    def list_launch_file(self, campaign_id, fname):
        lines = self.campaignsPOMS.list_launch_file(campaign_id, fname)
        output = "".join(lines)
        template = self.jinja_env.get_template('launch_jobs.html')
        res = template.render(command='', output=output,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                c=None, campaign_id=campaign_id, pomspath=self.path,
                                help_page="LaunchedJobsHelp", version=self.version)
        return res


    @cherrypy.expose
    def schedule_launch(self, campaign_id):
        c, job, launch_flist = self.campaignsPOMS.schedule_launch(cherrypy.request.db, campaign_id)
        template = self.jinja_env.get_template('schedule_launch.html')
        return template.render(c=c, campaign_id=campaign_id, job=job,
                        current_experimenter=cherrypy.session.get('experimenter'),
                        do_refresh=0, pomspath=self.path, help_page="ScheduleLaunchHelp",
                        launch_flist=launch_flist, version=self.version)


    @cherrypy.expose
    def mark_campaign_active(self, campaign_id, is_active):
        c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        if c and (cherrypy.session.get('experimenter').is_authorized(c.experiment) or self.accessPOMS.can_report_data( cherrypy.request.headers.get, cherrypy.log, cherrypy.session.get )()):
            c.active=(is_active == 'True')
            cherrypy.request.db.add(c)
            cherrypy.request.db.commit()
            raise cherrypy.HTTPRedirect("campaign_info?campaign_id=%s" % campaign_id)
=======
        # we don't really use these for anything but we might want to
        # pass them into a template to set time ranges...
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin,tmax,tdays,'job_file_contents')

        j = cherrypy.request.db.query(Job).options(subqueryload(Job.task_obj).subqueryload(Task.campaign_snap_obj)).filter(Job.job_id == job_id).first()
        # find the job with the logs -- minimum jobsub_job_id for this task
        jobsub_job_id = self.task_min_job(j.task_id)
        cherrypy.log("found job: %s " % jobsub_job_id)
        role = j.task_obj.campaign_snap_obj.vo_role
        job_file_contents = cherrypy.request.jobsub_fetcher.contents(file, j.jobsub_job_id,j.task_obj.campaign_snap_obj.experiment,role)
        template = self.jinja_env.get_template('job_file_contents.html')
        return template.render(file=file, job_file_contents=job_file_contents, task_id=task_id, job_id=job_id, tmin=tmin, pomspath=self.path,help_page="JobFileContentsHelp", version=self.version)

    @cherrypy.expose
    def test_job_counts(self, task_id = None, campaign_id = None):
        res = self.job_counts(task_id, campaign_id)
        return repr(res) + self.format_job_counts(task_id, campaign_id)

    def format_job_counts(self, task_id = None, campaign_id = None, tmin = None, tmax = None, tdays = 7, range_string = None):
        counts = self.job_counts(task_id, campaign_id, tmin, tmax, tdays)
        ck = counts.keys()
        res = [ '<div><b>Job States</b><br>',
                '<table class="ui celled table unstackable">',
                '<tr><th>Total</th><th colspan=3>Active</th><th colspan=2>In %s</th></tr>' % range_string,
                '<tr>' ]
        for k in ck:
            res.append( "<th>%s</th>" % k )
        res.append("</tr>")
        res.append("<tr>")
        var = 'ignore_me'
        val = ''
        if campaign_id != None:
             var = 'campaign_id'
             val = campaign_id
        if task_id != None:
             var = 'task_id'
             val = task_id
        for k in ck:
            res.append( '<td><a href="job_table?job_status=%s&%s=%s">%d</a></td>' % (k, var, val,  counts[k] ))
        res.append("</tr></table></div><br>")
        return "".join(res)

    def job_counts(self, task_id = None, campaign_id = None, tmin = None, tmax = None, tdays = None):
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax,tdays,'job_counts')

        q = cherrypy.request.db.query(func.count(Job.status),Job.status). group_by(Job.status)
        if tmax != None:
            q = q.filter(Job.updated <= tmax, Job.updated >= tmin)

        if task_id:
            q = q.filter(Job.task_id == task_id)

        if campaign_id:
            q = q.join(Task,Job.task_id == Task.task_id).filter( Task.campaign_id == campaign_id)

        out = OrderedDict([("All",0),("Idle",0),( "Running",0),( "Held",0),( "Completed",0), ("Located",0),("Removed",0)])
        for row in  q.all():
            # this rather bizzare hoseyness is because we want
            # "Running" to also match "running: copying files in", etc.
            # so we ignore the first character and do a match
            if row[1][1:7] == "unning":
                short = "Running"
            else:
                short = row[1]
            out[short] = out.get(short,0) + int(row[0])
            out["All"] = out.get("All",0) + int(row[0])

        return out


    @cherrypy.expose
    def job_table(self, tmin = None, tmax = None, tdays = 1, task_id = None, campaign_id = None , experiment = None, sift=False, campaign_name=None, name=None,campaign_def_id=None, vo_role=None, input_dataset=None, output_dataset=None, task_status=None, project=None, jobsub_job_id=None, node_name=None, cpu_type=None, host_site=None, job_status=None, user_exe_exit_code=None, output_files_declared=None, campaign_checkbox=None, task_checkbox=None, job_checkbox=None, ignore_me = None, keyword=None, dataset = None, eff_d = None):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax,tdays,'job_table?')
        extra = ""
        filtered_fields = {}

        q = cherrypy.request.db.query(Job,Task,Campaign)
        q = q.filter(Job.task_id == Task.task_id, Task.campaign_id == Campaign.campaign_id)
        q = q.filter(Job.updated >= tmin, Job.updated <= tmax)

        if keyword:
            q = q.filter( Task.project.like("%%%s%%" % keyword) )
            extra = extra + "with keyword %s" % keyword
            filtered_fields['keyword'] = keyword

        if task_id:
            q = q.filter( Task.task_id == int(task_id))
            extra = extra + "in task id %s" % task_id
            filtered_fields['task_id'] = task_id

        if campaign_id:
            q = q.filter( Task.campaign_id == int(campaign_id))
            extra = extra + "in campaign id %s" % campaign_id
            filtered_fields['campaign_id'] = campaign_id

        if experiment:
            q = q.filter( Campaign.experiment == experiment)
            extra = extra + "in experiment %s" % experiment
            filtered_fields['experiment'] = experiment

        if dataset:
            q = q.filter( Campaign.dataset == dataset)
            extra = extra + "in dataset %s" % dataset
            filtered_fields['dataset'] = dataset

        if campaign_name:
            q = q.filter(Campaign.name == campaign_name)
            filtered_fields['campaign_name'] = campaign_name

        # alias for failed_jobs_by_whatever
        if name:
            q = q.filter(Campaign.name == name)
            filtered_fields['campaign_name'] = name

        if campaign_def_id:
            q = q.filter(Campaign.campaign_definition_id == campaign_def_id)
            filtered_fields['campaign_def_id'] = campaign_def_id

        if vo_role:
            q = q.filter(Campaign.vo_role == vo_role)
            filtered_fields['vo_role'] = vo_role

        if input_dataset:
            q = q.filter(Task.input_dataset == input_dataset)
            filtered_fields['input_dataset'] = input_dataset

        if output_dataset:
            q = q.filter(Task.output_dataset == output_dataset)
            filtered_fields['output_dataset'] = output_dataset

        if task_status:
            q = q.filter(Task.status == task_status)
            filtered_fields['task_status'] = task_status

        if project:
            q = q.filter(Task.project == project)
            filtered_fields['project'] = project

        #
        # this one for our effeciency percentage decile...
        # i.e. if you want jobs in the 80..90% eficiency range
        # you ask for eff_d == 8...
        # ... or with eff_d of -1, one where we don't have good cpu/wall time data
        #
        if eff_d:
            if eff_d == "-1":
                q = q.filter(not_(and_(Job.cpu_time > 0.0, Job.wall_time >= Job.cpu_time)))
            else:
                q = q.filter(Job.wall_time != 0.0, func.floor(Job.cpu_time *10/Job.wall_time)== eff_d )
            filtered_fields['eff_d'] = eff_d

        if jobsub_job_id:
            q = q.filter(Job.jobsub_job_id == jobsub_job_id)
            filtered_fields['jobsub_job_id'] = jobsub_job_id

        if node_name:
            q = q.filter(Job.node_name == node_name)
            filtered_fields['node_name'] = node_name

        if cpu_type:
            q = q.filter(Job.cpu_type == cpu_type)
            filtered_fields['cpu_type'] = cpu_type

        if host_site:
            q = q.filter(Job.host_site == host_site)
            filtered_fields['host_site'] = host_site

        if job_status:
            # this rather bizzare hoseyness is because we want
            # "Running" to also match "running: copying files in", etc.
            # so we ignore the first character and do a "like" match
            # on the rest...
            q = q.filter(Job.status.like('%' + job_status[1:] + '%'))
            filtered_fields['job_status'] = job_status

        if user_exe_exit_code:
            q = q.filter(Job.user_exe_exit_code == int(user_exe_exit_code))
            extra = extra + "with exit code %s" % user_exe_exit_code
            filtered_fields['user_exe_exit_code'] = user_exe_exit_code

        if output_files_declared:
            q = q.filter(Job.output_files_declared == output_files_declared)
            filtered_fields['output_files_declared'] = output_files_declared


        jl = q.all()


        if jl:
            jobcolumns = jl[0][0]._sa_instance_state.class_.__table__.columns.keys()
            taskcolumns = jl[0][1]._sa_instance_state.class_.__table__.columns.keys()
            campcolumns = jl[0][2]._sa_instance_state.class_.__table__.columns.keys()
        else:
            jobcolumns = []
            taskcolumns = []
            campcolumns = []

        if bool(sift):
            campaign_box = task_box = job_box = ""

            if campaign_checkbox == "on":
                campaign_box = "checked"
            else:
                campcolumns = []
            if task_checkbox == "on":
                task_box = "checked"
            else:
                taskcolumns = []
            if job_checkbox == "on":
                job_box = "checked"
            else:
                jobcolumns = []

            filtered_fields_checkboxes = {"campaign_checkbox": campaign_box, "task_checkbox": task_box, "job_checkbox": job_box}
            filtered_fields.update(filtered_fields_checkboxes)

            prevlink = prevlink + "&" + urllib.urlencode(filtered_fields).replace("checked", "on") + "&sift=" + str(sift)
            nextlink = nextlink + "&" + urllib.urlencode(filtered_fields).replace("checked", "on") + "&sift=" + str(sift)
>>>>>>> release/v1_0_0a
        else:
            raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')


    @cherrypy.expose
    def make_stale_campaigns_inactive(self):
        if not self.accessPOMS.can_report_data(cherrypy.request.headers.get, cherrypy.log, cherrypy.session.get):
             raise err_res(401, 'You are not authorized to access this resource')
        res = self.campaignsPOMS.make_stale_campaigns_inactive(cherrypy.request.db, cherrypy.HTTPError)
        return "Marked inactive stale: " + ",".join(res)
#--------------------------------------
###############
### Tables
    @cherrypy.expose
    def list_generic(self, classname):
        l = self.tablesPOMS.list_generic(cherrypy.request.db, cherrypy.HTTPError, cherrypy.request.headers.get, cherrypy.session.get, classname)
        template = self.jinja_env.get_template('list_generic.html')
        return template.render(classname=classname,
                        list=l, edit_screen="edit_screen_generic",
                        primary_key='experimenter_id',
                        current_experimenter=cherrypy.session.get('experimenter'),
                        pomspath=self.path, help_page="ListGenericHelp", version=self.version)


    @cherrypy.expose
    def edit_screen_generic(self, classname, id=None):
        return self.tablesPOMS.edit_screen_generic(cherrypy.HTTPError, cherrypy.request.headers.get, cherrypy.session.get, classname, id)


    @cherrypy.expose
    def update_generic(self, classname, *args, **kwargs):
        return self.tablesPOMS.update_generic(cherrypy.request.db, cherrypy.request.headers.get, cherrypy.log, cherrypy.session.get, classname, *args, **kwargs)


    def edit_screen_for(self, classname, eclass, update_call, primkey, primval, valmap): ### Why this function is not expose
        screendata = self.tablesPOMS.edit_screen_for(cherrypy.request.db,
                            cherrypy.log, cherrypy.request.headers.get,
                            cherrypy.session.get, classname, eclass,
                            update_call, primkey, primval, valmap)
        template = self.jinja_env.get_template('edit_screen_for.html')
        return template.render(screendata=screendata, action="./"+update_call,
                        classname=classname,
                        current_experimenter=cherrypy.session.get('experimenter'),
                        pomspath=self.path, help_page="GenericEditHelp", version=self.version)
#----------------------------
#######
### JobPOMS
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def active_jobs(self):
        res = self.jobsPOMS.active_jobs(cherrypy.request.db)
        return res


    @cherrypy.expose
    def report_declared_files(self, flist):
        self.filesPOMS.report_declared_files(flist, cherrypy.request.db)
        return "Ok."


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def output_pending_jobs(self):
        res = self.jobsPOMS.output_pending_jobs(cherrypy.request.db)
        return res


    @cherrypy.expose
    def update_job(self, task_id, jobsub_job_id,  **kwargs):
        cherrypy.log("update_job( task_id %s, jobsub_job_id %s,  kwargs %s )" % (task_id, jobsub_job_id, repr(kwargs)))
        if not self.accessPOMS.can_report_data( cherrypy.request.headers.get, cherrypy.log, cherrypy.session.get ):
            cherrypy.log("update_job: not allowed")
            return "Not Allowed"
        return (self.jobsPOMS.update_job(cherrypy.request.db, cherrypy.log, cherrypy.response.status, cherrypy.request.samweb_lite, task_id, jobsub_job_id, **kwargs))


    @cherrypy.expose
    def test_job_counts(self, task_id=None, campaign_id=None):
        res = self.triagePOMS.job_counts(cherrypy.request.db, task_id, campaign_id)
        return repr(res) + self.filesPOMS.format_job_counts(task_id, campaign_id)


    @cherrypy.expose
    def kill_jobs(self, campaign_id=None, task_id=None, job_id=None, confirm=None):
        if confirm == None:
            jjil, t, campaign_id, task_id, job_id = self.jobsPOMS.kill_jobs(cherrypy.request.db, cherrypy.log, campaign_id, task_id, job_id, confirm)
            template = self.jinja_env.get_template('kill_jobs_confirm.html')
            return template.render(current_experimenter=cherrypy.session.get('experimenter'),
                                    jjil=jjil, task=t, campaign_id=campaign_id,
                                    task_id=task_id, job_id=job_id, pomspath=self.path,
                                    help_page="KilledJobsHelp", version=self.version)
        else:
            output, c, campaign_id, task_id, job_id = self.jobsPOMS.kill_jobs(cherrypy.request.db, cherrypy.log, campaign_id, task_id, job_id, confirm)
            template = self.jinja_env.get_template('kill_jobs.html')
            return template.render(output=output, current_experimenter=cherrypy.session.get('experimenter'),
                                    c=c, campaign_id=campaign_id, task_id=task_id,
                                    job_id=job_id, pomspath=self.path,
                                    help_page="KilledJobsHelp", version=self.version)


    @cherrypy.expose
    def jobs_eff_histo(self, campaign_id, tmax=None, tmin=None, tdays=1):
        (c, maxv, total, vals,
            tmaxs, campaign_id,
            tdays, tmin, tmax,
            nextlink, prevlink, tdays) = self.jobsPOMS.jobs_eff_histo(cherrypy.request.db, campaign_id, tmax, tmin, tdays)
        template = self.jinja_env.get_template('jobs_eff_histo.html')
        return template.render(c=c, maxv=maxv, total=total,
                                vals=vals, tmaxs=tmaxs,
                                campaign_id=campaign_id,
                                tdays=tdays, tmin=tmin, tmax=tmax,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                do_refresh=1, next=nextlink, prev=prevlink,
                                days=tdays, pomspath=self.path,
                                help_page="JobEfficiencyHistoHelp", version=self.version)


    @cherrypy.expose
    def set_job_launches(self, hold):
        self.taskPOMS.set_job_launches(cherrypy.request.db, hold)
        raise cherrypy.HTTPRedirect(self.path + "/")

    @cherrypy.expose
    def launch_queued_job(self):
        return self.taskPOMS.launch_queued_job(cherrypy.request.db,cherrypy.log, cherrypy.session.get, cherrypy.request.headers.get, cherrypy.session.get, cherrypy.response.status)

    @cherrypy.expose
    def launch_jobs(self, campaign_id, dataset_override=None, parent_task_id=None): ###needs to be analize in detail.
        vals = self.taskPOMS.launch_jobs(cherrypy.request.db,
                        cherrypy.log, cherrypy.session.get,
                        cherrypy.request.headers.get, cherrypy.session.get,
                        cherrypy.request.samweb_lite,
                        cherrypy.response.status, campaign_id, dataset_override, parent_task_id)
        cherrypy.log("Got vals: %s" % repr(vals))
        lcmd, output, c, campaign_id, outdir, outfile = vals
        template = self.jinja_env.get_template('launch_jobs.html')
        res = template.render(command=lcmd, output=output,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                c=c, campaign_id=campaign_id, pomspath=self.path,
                                help_page="LaunchedJobsHelp", version=self.version)
        return res
#----------------------
########################
### TaskPOMS
    @cherrypy.expose
    def create_task(self, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor):
         if not can_create_task():
             return "Not Allowed"
         return (self.taskPOMS.create_task(cherrypy.request.db, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor))

    @cherrypy.expose
    def wrapup_tasks(self):
        cherrypy.response.headers['Content-Type'] = "text/plain"
        return "\n".join(self.taskPOMS.wrapup_tasks(cherrypy.request.db, cherrypy.log, cherrypy.request.samweb_lite, cherrypy.config.get, cherrypy.request.headers.get, cherrypy.session.get, cherrypy.response.status))

    @cherrypy.expose
    def show_task_jobs(self, task_id, tmax=None, tmin=None, tdays=1): ### Need to be tested HERE
        (blob, job_counts,
            task_id, tmin, tmax,
            extramap, key, task_jobsub_id,
            campaign_id, cname) = self.taskPOMS.show_task_jobs( cherrypy.request.db, task_id, tmax, tmin, tdays)
        template = self.jinja_env.get_template('show_task_jobs.html')
        return template.render(blob=blob, job_counts=job_counts, taskid=task_id, tmin=tmin, tmax=tmax,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                extramap=extramap, do_refresh=1, key=key, pomspath=self.path, help_page="ShowTaskJobsHelp",
                                task_jobsub_id=task_jobsub_id,
                                campaign_id=campaign_id, cname=cname, version=self.version)


    @cherrypy.expose
    def get_task_id_for(self, campaign, user=None, experiment=None, command_executed="", input_dataset="", parent_task_id=None):
        task_id = self.taskPOMS.get_task_id_for(cherrypy.request.db, campaign, user, experiment, command_executed, input_dataset, parent_task_id)
        return "Task=%d" % task_id
#------------------------
#########################
### FilesPOMS
    @cherrypy.expose
    def list_task_logged_files(self, task_id):
        fl, t, jobsub_job_id = self.filesPOMS.list_task_logged_files(cherrypy.request.db, task_id)
        template = self.jinja_env.get_template('list_task_logged_files.html')
        return template.render(fl=fl, campaign=t.campaign_snap_obj, jobsub_job_id=jobsub_job_id,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                do_refresh=0, pomspath=self.path,
                                help_page="ListTaskLoggedFilesHelp", version=self.version)


    @cherrypy.expose
    def campaign_task_files(self, campaign_id, tmin=None, tmax=None, tdays=1):
        (c, columns, datarows,
            tmins, tmaxs,
            prevlink, nextlink, tdays) = self.filesPOMS.campaign_task_files(cherrypy.request.db, cherrypy.log, cherrypy.request.samweb_lite, campaign_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_task_files.html')
        return template.render(name = c.name if c else "",
                                columns=columns, datarows=datarows,
                                tmin=tmins, tmax=tmaxs,
                                prev=prevlink, next=nextlink, days=tdays,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                campaign_id=campaign_id, pomspath=self.path, help_page="CampaignTaskFilesHelp", version=self.version)


    @cherrypy.expose
    def job_file_list(self, job_id,force_reload = False): ##Ask Marc to check this in the module
        return self.filesPOMS.job_file_list(cherrypy.request.db, cherrypy.request.jobsub_fetcher, job_id, force_reload)


    @cherrypy.expose
    def job_file_contents(self, job_id, task_id, file, tmin=None, tmax=None, tdays=None):
        job_file_contents, tmin = self.filesPOMS.job_file_contents(cherrypy.request.db, cherrypy.log,
                                            cherrypy.request.jobsub_fetcher, job_id, task_id, file, tmin, tmax, tdays)
        template = self.jinja_env.get_template('job_file_contents.html')
        return template.render(file=file, job_file_contents=job_file_contents,
                                task_id=task_id, job_id=job_id, tmin=tmin,
                                pomspath=self.path, help_page="JobFileContentsHelp", version=self.version)


    @cherrypy.expose
    def inflight_files(self, campaign_id=None, task_id=None):
        outlist, statusmap, c = self.filesPOMS.inflight_files(cherrypy.request.db, cherrypy.response.status, cherrypy.config.get, campaign_id, task_id)
        template = self.jinja_env.get_template('inflight_files.html')
        return template.render(flist=outlist,
                               current_experimenter=cherrypy.session.get('experimenter'),
                               statusmap=statusmap, c=c,
                               jjid=self.taskPOMS.task_min_job(cherrypy.request.db, task_id),
                               campaign_id=campaign_id, task_id=task_id,
                               pomspath=self.path, help_page="PendingFilesJobsHelp", version=self.version)


    @cherrypy.expose
    def show_dimension_files(self, experiment, dims):
        flist = self.filesPOMS.show_dimension_files(cherrypy.request.samweb_lite, experiment, dims, dbhandle=cherrypy.request.db)
        template = self.jinja_env.get_template('show_dimension_files.html')
        return template.render(flist=flist, dims=dims,
                               current_experimenter=cherrypy.session.get('experimenter'), statusmap=[],
                               pomspath=self.path, help_page="ShowDimensionFilesHelp", version=self.version)


    @cherrypy.expose
    def actual_pending_files(self, count_or_list, task_id=None, campaign_id=None, tmin=None, tmax=None, tdays=1): ###??? Implementation of the exception.
        cherrypy.response.timeout = 600
        try:
            experiment, dims = self.filesPOMS.actual_pending_files(cherrypy.request.db, cherrypy.log, count_or_list, task_id, campaign_id, tmin, tmax, tdays)
            return self.show_dimension_files(experiment, dims)
        except ValueError:
            return "None == dims in actual_pending_files method"


    @cherrypy.expose
    def campaign_sheet(self, campaign_id, tmin=None, tmax=None, tdays=7):
        (name, columns, outrows, dimlist,
            experiment, tmaxs,
            prevlink, nextlink,
            tdays, tmin, tmax) = self.filesPOMS.campaign_sheet(cherrypy.request.db,
                                                               cherrypy.log,
                                                               cherrypy.request.samweb_lite,
                                                               campaign_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_sheet.html')
        return template.render(name=name,
                               columns=columns,
                               datarows=outrows,
                               dimlist=dimlist,
                               tmaxs=tmaxs,
                               prev=prevlink,
                               next=nextlink,
                               days=tdays,
                               tmin=tmin,
                               tmax=tmax,
                               current_experimenter=cherrypy.session.get('experimenter'),
                               campaign_id=campaign_id,
                               experiment=experiment,
                               pomspath=self.path,
                               help_page="CampaignSheetHelp",
                               version=self.version)


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def json_project_summary_for_task(self, task_id):
        return self.project_summary_for_task(task_id)


    def project_summary_for_task(self, task_id):
        t = cherrypy.request.db.query(Task).filter(Task.task_id == task_id).first()
        return cherrypy.request.samweb_lite.fetch_info(t.campaign_snap_obj.experiment, t.project, dbhandle=cherrypy.request.db)


    def project_summary_for_tasks(self, task_list):
        return cherrypy.request.samweb_lite.fetch_info_list(task_list, dbhandle=cherrypy.request.db)
        #~ return [ {"tot_consumed": 0, "tot_failed": 0, "tot_jobs": 0, "tot_jobfails": 0} ] * len(task_list)    #VP Debug
###Im here
#----------------------------
##########
### TriagePOMS
    @cherrypy.expose
    def triage_job(self, job_id, tmin=None, tmax=None, tdays=None, force_reload=False):
        (job_file_list, job_info, job_history,
            downtimes, output_file_names_list,
            es_response, efficiency,
            tmin, task_jobsub_job_id) = self.triagePOMS.triage_job(cherrypy.request.db, cherrypy.request.jobsub_fetcher, cherrypy.config, job_id, tmin, tmax, tdays, force_reload)
        template = self.jinja_env.get_template('triage_job.html')
        return template.render(job_id=job_id,
                                job_file_list=job_file_list,
                                job_info=job_info,
                                job_history=job_history,
                                downtimes=downtimes,
                                output_file_names_list=output_file_names_list,
                                es_response=es_response,
                                efficiency=efficiency,
                                tmin=tmin,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                pomspath=self.path,
                                help_page="TriageJobHelp",
                                task_jobsub_job_id=task_jobsub_job_id,
                                version=self.version)


    @cherrypy.expose
    def job_table(self, offset=0, **kwargs):
        ###The pass of the arguments is ugly we will fix that later.
        (jl, jobcolumns, taskcolumns,
            campcolumns, tmins, tmaxs,
            prevlink, nextlink, tdays,
            extra, hidecolumns, filtered_fields,
            time_range_string) = self.triagePOMS.job_table(cherrypy.request.db, **kwargs)

        template = self.jinja_env.get_template('job_table.html')

        return template.render(joblist=jl,
                                jobcolumns=jobcolumns,
                                taskcolumns=taskcolumns,
                                campcolumns=campcolumns,
                                tmin=tmins,
                                tmax=tmaxs,
                                prev=prevlink,
                                next=nextlink,
                                days=tdays,
                                extra=extra,
                                hidecolumns=hidecolumns,
                                filtered_fields=filtered_fields,
                                time_range_string=time_range_string,
                                offset=int(offset),
                                current_experimenter=cherrypy.session.get('experimenter'),
                                do_refresh=0,
                                pomspath=self.path,
                                help_page="JobTableHelp",
                                version=self.version)


    @cherrypy.expose
    def jobs_by_exitcode(self, tmin = None, tmax =  None, tdays = 1 ):
        raise cherrypy.HTTPRedirect("%s/failed_jobs_by_whatever?f=user_exe_exit_code&tdays=%s" % (self.path, tdays))


    @cherrypy.expose
    def failed_jobs_by_whatever(self, tmin=None, tmax=None, tdays=1 , f=[], go=None):
        (jl, possible_columns, columns,
            tmins, tmaxs, tdays,
            prevlink, nextlink,
            time_range_string, tdays) = self.triagePOMS.failed_jobs_by_whatever(cherrypy.request.db, cherrypy.log, tmin, tmax, tdays, f, go)
        template = self.jinja_env.get_template('failed_jobs_by_whatever.html')
        return template.render(joblist=jl,
                                possible_columns=possible_columns,
                                columns=columns,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                do_refresh=0,
                                tmin=tmins,
                                tmax=tmaxs,
                                tdays=tdays,
                                prev=prevlink,
                                next=nextlink,
                                time_range_string=time_range_string,
                                days=tdays,
                                pomspath=self.path,
                                help_page="JobsByExitcodeHelp",
                                version=self.version)
#-------------------
##############
### TagsPOMS
    @cherrypy.expose
    @cherrypy.tools.json_out()
    def link_tags(self, campaign_id, tag_name, experiment):
        return(self.tagsPOMS.link_tags(cherrypy.request.db, cherrypy.session.get, campaign_id, tag_name, experiment))


    @cherrypy.expose
    @cherrypy.tools.json_out()
    def delete_campaigns_tags(self, campaign_id, tag_id, experiment):
        return(self.tagsPOMS.delete_campaigns_tags( cherrypy.request.db, campaign_id, tag_id, experiment))


    @cherrypy.expose
    def search_tags(self, q):
        results, q_list = self.tagsPOMS.search_tags(cherrypy.request.db, q)
        template = self.jinja_env.get_template('search_tags.html')
        return template.render(results=results,
                                q_list=q_list,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                do_refresh=0,
                                pomspath=self.path,
                                help_page="SearchTagsHelp",
                                version=self.version)


    @cherrypy.expose
    def auto_complete_tags_search(self, experiment, q):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return(self.tagsPOMS.auto_complete_tags_search(cherrypy.request.db, experiment, q))
#-----------------------
