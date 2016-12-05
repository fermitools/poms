import cherrypy
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

from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey, and_, or_, not_,  create_engine, null, desc, text, func, exc, distinct
from sqlalchemy.orm  import subqueryload, joinedload, contains_eager
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from datetime import datetime, tzinfo,timedelta
from jinja2 import Environment, PackageLoader
import shelve
from model.poms_model import Service, ServiceDowntime, Experimenter, Experiment, ExperimentsExperimenters, Job, JobHistory, Task, CampaignDefinition, TaskHistory, Campaign, LaunchTemplate, Tag, CampaignsTags, JobFile, CampaignSnapshot, CampaignDefinitionSnapshot,LaunchTemplateSnapshot,CampaignRecovery,RecoveryType, CampaignDependency

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


def error_response():
    dump = ""
    if cherrypy.config.get("dump",True):
        dump = cherrypy._cperror.format_exc()
    message = dump.replace('\n','<br/>')

    jinja_env = Environment(loader=PackageLoader('webservice','templates'))
    template = jinja_env.get_template('error_response.html')
    path = cherrypy.config.get("pomspath","/poms")
    body = template.render(current_experimenter=cherrypy.session.get('experimenter'), message=message,pomspath=path,dump=dump, version=global_version)

    cherrypy.response.status = 500
    cherrypy.response.headers['content-type'] = 'text/html'
    cherrypy.response.body = body
    cherrypy.log(dump)

#
# utility function for running commands that don't run forever...
#
def popen_read_with_timeout(cmd, totaltime = 30):

    origtime = totaltime
    # start up keeping subprocess handle and pipe
    pp = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE)
    f = pp.stdout

    outlist = []
    block=" "

    # read the file, with select timeout of total time remaining
    while totaltime > 0 and len(block) > 0:
        t1 = time.time()
        r, w, e = select.select( [f],[],[], totaltime)
        if not f in r:
           outlist.append("\n[...timed out after %d seconds]\n" % origtime)
           # timed out!
           pp.kill()
           break
        block = os.read(f.fileno(), 512)
        t2 = time.time()
        totaltime = totaltime - (t2 - t1)
        outlist.append(block)

    pp.wait()
    output = ''.join(outlist)
    return output



class poms_service:

    _cp_config = {'request.error_response': error_response,
                  'error_page.404': "%s/%s" % (os.path.abspath(os.getcwd()),'/templates/page_not_found.html')
                  }

    def __init__(self):
        self.jinja_env = Environment(loader=PackageLoader('webservice','templates'))
        self.make_admin_map()
        self.task_min_job_cache = {}
        self.path = cherrypy.config.get("pomspath","/poms")
        cherrypy.config.update({'poms.launches': 'allowed'})
        self.hostname = socket.getfqdn()
        self.version = version.get_version()
        global_version = self.version
        self.calendarPOMS = CalendarPOMS.CalendarPOMS()
        self.dbadminPOMS = DBadminPOMS.DBadminPOMS()
        self.campaignsPOMS = CampaignsPOMS.CampaignsPOMS()
        self.jobsPOMS = JobsPOMS.JobsPOMS(self)
        self.taskPOMS = TaskPOMS.TaskPOMS(self)
        self.utilsPOMS = UtilsPOMS.UtilsPOMS(self)
        self.tagsPOMS = TagsPOMS.TagsPOMS(self)
        self.filesPOMS = FilesPOMS.Files_status(self)
        self.triagePOMS=TriagePOMS.TriagePOMS(self)

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

        launches = cherrypy.config.get("poms.launches","allowed"),
                               do_refresh = 1, pomspath=self.path,help_page="DashboardHelp", version=self.version)


    @cherrypy.expose
    def es(self):
        template = self.jinja_env.get_template('elasticsearch.html')

        es = Elasticsearch()

        query = {
            'sort' : [{ '@timestamp' : {'order' : 'asc'}}],
            'query' : {
                'term' : { 'jobid' : '9034906.0@fifebatch1.fnal.gov' }
            }
        }

        es_response= es.search(index='fifebatch-logs-*', types=['condor_eventlog'], query=query)
        pprint.pprint(es_response)
        return template.render(pomspath=self.path, es_response=es_response)


    def can_report_data(self):
        xff = cherrypy.request.headers.get('X-Forwarded-For', None)
        ra =  cherrypy.request.headers.get('Remote-Addr', None)
        user = cherrypy.request.headers.get('X-Shib-Userid', None)
        cherrypy.log("can_report_data: Remote-addr: %s" %  ra)
        if ra.startswith('131.225.67.'):
            return 1
        if ra.startswith('131.225.80.'):
            return 1
        if ra == '127.0.0.1' and xff and xff.startswith('131.225.67'):
             # case for fifelog agent..
             return 1
        if ra != '127.0.0.1' and xff and xff.startswith('131.225.80'):
             # case for jobsub_q agent (currently on bel-kwinith...)
             return 1
        if ra == '127.0.0.1' and xff == None:
             # case for local agents
             return 1
        if (cherrypy.session.get('experimenter')).is_root():
             # special admins
             return 1
        return 0


    def can_db_admin(self):
        xff = cherrypy.request.headers.get('X-Forwarded-For', None)
        ra =  cherrypy.request.headers.get('Remote-Addr', None)
        user = cherrypy.request.headers.get('X-Shib-Userid', None)
        if ra in ['127.0.0.1','131.225.80.97'] and xff == None:
             # case for local agents
             return 1
        if (cherrypy.session.get('experimenter')).is_root():
             # special admins
             return 1
        return 0


####################
#UtilsPOMS

    @cherrypy.expose
    def quick_search(self, search_term):
        self.utilsPOMS.quick_search(cherrypy.request.db, cherrypy.HTTPRedirect, search_term)


    @cherrypy.expose
    def jump_to_job(self, jobsub_job_id, **kwargs ):
        self.utilsPOMS.jump_to_job(cherrypy.request.db, cherrypy.HTTPRedirect, jobsub_job_id, **kwargs)
#----------------

##############################
#CALENDAR
#Using CalendarPOMS.py module
    @cherrypy.expose
    def calendar_json(self, start, end, timezone, _):
        cherrypy.response.headers['Content-Type'] = "application/json"
        return json.dumps(self.calendarPOMS.calendar_json(cherrypy.request.db, start, end, timezone, _))


    @cherrypy.expose
    def calendar(self):
        template = self.jinja_env.get_template('calendar.html')
        rows = self.calendarPOMS.calendar(dbhandle = cherrypy.request.db)
        return template.render(rows=rows, current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path,help_page="CalendarHelp")


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
        return template.render(rows = rows,current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path,help_page="ServiceDowntimesHelp")


    @cherrypy.expose
    def update_service(self, name, parent, status, host_site, total, failed, description):
       	return self.calendarPOMS.update_service(cherrypy.request.db, cherrypy.log, name, parent, status, host_site, total, failed, description)


    @cherrypy.expose
    def service_status(self, under = 'All'):
        template = self.jinja_env.get_template('service_status.html')
        list=self.calendarPOMS.service_status (cherrypy.request.db, under)
        return template.render(list=list, name=under,current_experimenter=cherrypy.session.get('experimenter'),  pomspath=self.path,help_page="ServiceStatusHelp", version=self.version)
#--------------------------


######
    #print "Check where should be this function."
    '''
    Apparently this function is not related with Calendar
    def service_status_hier(self, under = 'All', depth = 0):
        self.calendarPOMS.service_status_hier (cherrypy.request.db, under, depth)
    '''
    def service_status_hier(self, under = 'All', depth = 0):
        p = cherrypy.request.db.query(Service).filter(Service.name == under).first()
        if depth == 0:
            res = '<div class="ui accordion styled">\n'
        else:
            res = ''
        active = ""
        for s in cherrypy.request.db.query(Service).filter(Service.parent_service_id == p.service_id).order_by(Service.name).all():
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

####

    @cherrypy.expose
    def new_task_for_campaign(self, campaign_name, command_executed, experimenter_name, dataset_name = None):
        c = cherrypy.request.db.query(Campaign).filter(Campaign.name == campaign_name).first()
        e = cherrypy.request.db.query(Experimenter).filter(like_)(Experimenter.email,"%s@%%" % experimenter_name ).first()
        t = Task()
        t.campaign_id = c.campaign_id
        t.campaign_definition_id = c.campaign_definition_id
        t.task_order = 0
        t.input_dataset = "-"
        t.output_dataset = "-"
        t.status = 'started'
        t.created = datetime.now(utc)
        t.updated = datetime.now(utc)
        t.updater = e.experimenter_id
        t.creator = e.experimenter_id
        t.command_executed = command_executed
        if dataset_name:
            t.input_dataset = dataset_name
        cherrypy.request.db.add(t)
        cherrypy.request.db.commit()
        return "Task=%d" % t.task_id


    def make_admin_map(self):
        """
            make self.admin_map a map of strings to model class names
            and self.pk_map a map of primary keys for that class
        """
        cherrypy.log(" ---- make_admin_map: starting...")
        import model.poms_model
        self.admin_map = {}
        self.pk_map = {}
        for k in model.poms_model.__dict__.keys():
            if hasattr(model.poms_model.__dict__[k],'__module__') and model.poms_model.__dict__[k].__module__ == 'model.poms_model':
                self.admin_map[k] = model.poms_model.__dict__[k]
                found = self.admin_map[k]()
                columns = found._sa_instance_state.class_.__table__.columns
                for fieldname in columns.keys():
                    if columns[fieldname].primary_key:
                         self.pk_map[k] = fieldname
        cherrypy.log(" ---- admin map: %s " % repr(self.admin_map))
        cherrypy.log(" ---- pk_map: %s " % repr(self.pk_map))



#####
#DBadminPOMS
    @cherrypy.expose
    def raw_tables(self):
    	if not self.can_db_admin():
	    raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        template = self.jinja_env.get_template('raw_tables.html')
        return template.render(list = self.admin_map.keys(),current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path,help_page="RawTablesHelp", version=self.version)
    @cherrypy.expose
    def user_edit(self, *args, **kwargs):
        data = self.dbadminPOMS.user_edit(cherrypy.request.db, *args, **kwargs)
        template = self.jinja_env.get_template('user_edit.html')
        return template.render(data=data, current_experimenter=cherrypy.session.get('experimenter'),  pomspath=self.path, help_page="EditUsersHelp", version=self.version)

    @cherrypy.expose
    def experiment_members(self, *args, **kwargs):
        trows = self.dbadminPOMS.experiment_members(cherrypy.request.db, *args, **kwargs)
        return json.dumps(trows)

    @cherrypy.expose
    def experiment_edit(self, message=None):
	experiments=self.dbadminPOMS.experiment_edit(cherrypy.request.db)
        template = self.jinja_env.get_template('experiment_edit.html')
        return template.render(message=message, experiments=experiments, current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path,help_page="ExperimentEditHelp", version=self.version)

    @cherrypy.expose
    def experiment_authorize(self, *args, **kwargs):
        if not self.can_db_admin():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        message = self.dbadminPOMS.experiment_authorize(cherrypy.request.db, cherrypy.log, *args, **kwargs)
        return self.experiment_edit(message)



#################################
#CampaignsPOMS
    @cherrypy.expose
    def launch_template_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.launch_template_edit(cherrypy.request.db, cherrypy.log, cherrypy.session.get, *args, **kwargs)
        template = self.jinja_env.get_template('launch_template_edit.html')
        return template.render(data=data,current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path,help_page="LaunchTemplateEditHelp", version=self.version)

    @cherrypy.expose
    def campaign_definition_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_definition_edit(cherrypy.request.db, cherrypy.log, cherrypy.session.get, *args, **kwargs)
        template = self.jinja_env.get_template('campaign_definition_edit.html')
        return template.render(data=data,current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path,help_page="CampaignDefinitionEditHelp", version=self.version)


    @cherrypy.expose
    def campaign_edit(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_edit(cherrypy.request.db, cherrypy.log, cherrypy.session, *args, **kwargs)
        template = self.jinja_env.get_template('campaign_edit.html')
        return template.render(data=data,current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path,help_page="CampaignEditHelp", version=self.version)

    @cherrypy.expose
    def campaign_edit_query(self, *args, **kwargs):
        data = self.campaignsPOMS.campaign_edit_query(cherrypy.request.db, *args, **kwargs)
        return json.dumps(data)
#--------------------------------------


    @cherrypy.expose
    def list_generic(self, classname):
        if not self.can_db_admin():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        l = self.make_list_for(self.admin_map[classname],self.pk_map[classname])
        template = self.jinja_env.get_template('list_generic.html')
        return template.render( classname = classname, list = l, edit_screen="edit_screen_generic", primary_key='experimenter_id',current_experimenter=cherrypy.session.get('experimenter'),  pomspath=self.path,help_page="ListGenericHelp", version=self.version)


    @cherrypy.expose
    def edit_screen_generic(self, classname, id = None):
        if not self.can_db_admin():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        # XXX -- needs to get select lists for foreign key fields...
        return self.edit_screen_for(classname, self.admin_map[classname], 'update_generic', self.pk_map[classname], id, {})


    @cherrypy.expose
    def update_generic( self, classname, *args, **kwargs):
        if not self.can_report_data():
             return "Not allowed"
        return self.update_for(classname, self.admin_map[classname], self.pk_map[classname], *args, **kwargs)


    def update_for( self, classname, eclass, primkey,  *args , **kwargs):
        found = None
        kval = None
        if kwargs.get(primkey,'') != '':
            kval = kwargs.get(primkey,None)
            try:
               kval = int(kval)
               pred = "%s = %d" % (primkey, kval)
            except:
               pred = "%s = '%s'" % (primkey, kval)
            found = cherrypy.request.db.query(eclass).filter(text(pred)).first()
            cherrypy.log("update_for: found existing %s" % found )
        if found == None:
            cherrypy.log("update_for: making new %s" % eclass)
            found = eclass()
            if hasattr(found,'created'):
                setattr(found, 'created', datetime.now(utc))
        columns = found._sa_instance_state.class_.__table__.columns
        for fieldname in columns.keys():
            if not kwargs.get(fieldname,None):
                continue
            if columns[fieldname].type == Integer:
                setattr(found, fieldname, int(kwargs.get(fieldname,'')))
            elif columns[fieldname].type == DateTime:
                # special case created, updated fields; set created
                # if its null, and always set updated if we're updating
                if fieldname == "created" and getattr(found,fieldname,None) == None:
                    setattr(found, fieldname, datetime.now(utc))
                if fieldname == "updated" and kwargs.get(fieldname,None) == None:
                    setattr(found, fieldname, datetime.now(utc))
                if  kwargs.get(fieldname,None) != None:
                    setattr(found, fieldname, datetime.strptime(kwargs.get(fieldname,'')).replace(tzinfo = utc), "%Y-%m-%dT%H:%M")

            elif columns[fieldname].type == ForeignKey:
                kval = kwargs.get(fieldname,None)
                try:
                   kval = int(kval)
                except:
                   pass
                setattr(found, fieldname, kval)
            else:
                setattr(found, fieldname, kwargs.get(fieldname,None))
        cherrypy.log("update_for: found is now %s" % found )
        cherrypy.request.db.add(found)
        cherrypy.request.db.commit()
        if classname == "Task":
              self.snapshot_parts(found.campaign_id)
        return "%s=%s" % (classname, getattr(found,primkey))


    def edit_screen_for( self, classname, eclass, update_call, primkey, primval, valmap):
        if not self.can_db_admin():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')

        found = None
        sample = eclass()
        if primval != '':
            cherrypy.log("looking for %s in %s" % (primval, eclass))
            try:
                primval = int(primval)
                pred = "%s = %d" % (primkey,primval)
            except:
                pred = "%s = '%s'" % (primkey,primval)
                pass
            found = cherrypy.request.db.query(eclass).filter(text(pred)).first()
            cherrypy.log("found %s" % found)
        if not found:
            found = sample
        columns =  sample._sa_instance_state.class_.__table__.columns
        fieldnames = columns.keys()
        screendata = []
        for fn in fieldnames:
             screendata.append({
                  'name': fn,
                  'primary': columns[fn].primary_key,
                  'value': getattr(found, fn, ''),
                  'values' : valmap.get(fn, None)
              })
        template = self.jinja_env.get_template('edit_screen_for.html')
        return template.render( screendata = screendata, action="./"+update_call , classname = classname ,current_experimenter=cherrypy.session.get('experimenter'),  pomspath=self.path,help_page="GenericEditHelp", version=self.version)

    def make_list_for(self,eclass,primkey):
        res = []
        for i in cherrypy.request.db.query(eclass).order_by(primkey).all():
            res.append( {"key": getattr(i,primkey,''), "value": getattr(i,'name',getattr(i,'email','unknown'))})
        return res


#######
#JobPOMS
    @cherrypy.expose
    def active_jobs(self):
         cherrypy.response.headers['Content-Type']= 'application/json'
         #print "Im here"
	 return self.jobsPOMS.active_jobs(cherrypy.request.db)


    @cherrypy.expose
    def report_declared_files(self, flist):
        now =  datetime.now(utc)
        cherrypy.request.db.query(JobFile).filter(JobFile.file_name.in_(flist) ).update({JobFile.declared: now}, synchronize_session = False)
        cherrypy.request.db.commit()


    @cherrypy.expose
    def output_pending_jobs(self):
         cherrypy.response.headers['Content-Type']= 'application/json'
         return self.jobsPOMS.output_pending_jobs(cherrypy.request.db)


    @cherrypy.expose
    def update_job(self, task_id, jobsub_job_id,  **kwargs):
        cherrypy.log("update_job( task_id %s, jobsub_job_id %s,  kwargs %s )" % (task_id, jobsub_job_id, repr(kwargs)))
        if not self.can_report_data():
            cherrypy.log("update_job: not allowed")
            return "Not Allowed"
        return (self.jobsPOMS.update_job(cherrypy.request.db, cherrypy.log, cherrypy.response.status, task_id, jobsub_job_id, **kwargs))


    @cherrypy.expose
    def test_job_counts(self, task_id = None, campaign_id = None):
        res = self.triagePOMS.job_counts(cherrypy.request.db, task_id, campaign_id)
        return repr(res) + self.filesPOMS.format_job_counts(task_id, campaign_id)


########################
#TaskPOMS
    @cherrypy.expose
    def create_task(self, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor):
         if not can_create_task():
             return "Not Allowed"
         return (self.taskPOMS.create_task(cherrypy.request.db,experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor))

    @cherrypy.expose
    def wrapup_tasks(self):
        cherrypy.response.headers['Content-Type'] = "text/plain"
        return "\n".join(self.taskPOMS.wrapup_tasks(cherrypy.request.db, cherrypy.log, cherrypy.request.samweb_lite))


    @cherrypy.expose
    def show_task_jobs(self, task_id, tmax = None, tmin = None, tdays = 1 ): ### Need to be tested HERE
        blob, job_counts, task_id, tmin, tmax, extramap, key, task_jobsub_id, campaign_id, cname = self.taskPOMS.show_task_jobs( cherrypy.request.db, task_id, tmax, tmin, tdays)
        template = self.jinja_env.get_template('show_task_jobs.html')
        return template.render( blob = blob, job_counts = job_counts,  taskid = task_id, tmin = tmin, tmax = tmax, current_experimenter = cherrypy.session.get('experimenter'),
                               extramap = extramap, do_refresh = 1, key = key, pomspath=self.path, help_page="ShowTaskJobsHelp", task_jobsub_id = task_jobsub_id,
                               campaign_id = campaign_id,cname = cname, version=self.version)


    def task_min_job(self, task_id):
        return self.taskPOMS.task_min_job( cherrypy.request.db, task_id)
#------------------------


##########
#TriagePOMS


    @cherrypy.expose
    def triage_job(self, job_id, tmin = None, tmax = None, tdays = None, force_reload = False):
        job_file_list, job_info, job_history, downtimes, output_file_names_list, es_response, efficiency, tmin, task_jobsub_job_id = self.triagePOMS.triage_job(cherrypy.request.db, job_id, tmin, tmax, tdays, force_reload)
        template = self.jinja_env.get_template('triage_job.html')
        return template.render(job_id = job_id, job_file_list = job_file_list, job_info = job_info, job_history = job_history, downtimes=downtimes, output_file_names_list=output_file_names_list, es_response=es_response, efficiency=efficiency, tmin=tmin, current_experimenter=cherrypy.session.get('experimenter'),  pomspath=self.path, help_page="TriageJobHelp",task_jobsub_job_id = task_jobsub_job_id, version=self.version)


    @cherrypy.expose
    def show_campaigns(self,experiment = None, tmin = None, tmax = None, tdays = 1, active = True):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.utilsPOMS.handle_dates(tmin,tmax,tdays,'show_campaigns?')

        cq = cherrypy.request.db.query(Campaign).filter(Campaign.active == active ).order_by(Campaign.experiment)

        if experiment:
            cq = cq.filter(Campaign.experiment == experiment)

        cl = cq.all()

        counts = {}
        counts_keys = {}

        dimlist, pendings = self.get_pending_for_campaigns(cl, tmin, tmax)
        effs = self.get_efficiency(cl, tmin, tmax)

        i = 0
        for c in cl:
            counts[c.campaign_id] = self.triagePOMS.job_counts(cherrypy.request.db, tmax = tmax, tmin = tmin, tdays = tdays, campaign_id = c.campaign_id)
            counts[c.campaign_id]['efficiency'] = effs[i]
            counts[c.campaign_id]['pending'] = pendings[i]
            counts_keys[c.campaign_id] = counts[c.campaign_id].keys()
            i = i + 1

        template = self.jinja_env.get_template('show_campaigns.html')
        return template.render( In= ("" if active == True else "In"), limit_experiment = experiment, services=self.service_status_hier('All'), counts = counts, counts_keys = counts_keys, cl = cl, tmins = tmins, tmaxs = tmaxs, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'),  do_refresh = 1, next = nextlink, prev = prevlink, days = tdays, time_range_string = time_range_string, key = '', dimlist = dimlist, pomspath=self.path, help_page="ShowCampaignsHelp", version=self.version)

    @cherrypy.expose
    def campaign_info(self, campaign_id, tmin = None, tmax = None, tdays = None):
        campaign_id = int(campaign_id)

        Campaign_info = cherrypy.request.db.query(Campaign, Experimenter).filter(Campaign.campaign_id == campaign_id, Campaign.creator == Experimenter.experimenter_id).first()

        # default to time window of campaign
        if tmin == None and tdays == None and tdays == None:
            tmin = Campaign_info.Campaign.created
            tmax = datetime.now(utc)

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.utilsPOMS.handle_dates(tmin,tmax,tdays,'campaign_info?')

        Campaign_definition_info =  cherrypy.request.db.query(CampaignDefinition, Experimenter).filter(CampaignDefinition.campaign_definition_id == Campaign_info.Campaign.campaign_definition_id, CampaignDefinition.creator == Experimenter.experimenter_id ).first()
        Launch_template_info = cherrypy.request.db.query(LaunchTemplate, Experimenter).filter(LaunchTemplate.launch_id == Campaign_info.Campaign.launch_id, LaunchTemplate.creator == Experimenter.experimenter_id).first()
        tags = cherrypy.request.db.query(Tag).filter(CampaignsTags.campaign_id==campaign_id, CampaignsTags.tag_id==Tag.tag_id).all()

        launched_campaigns = cherrypy.request.db.query(CampaignSnapshot).filter(CampaignSnapshot.campaign_id == campaign_id).all()

        #
        # cloned from show_campaigns, but for a one row table..
        #
        cl = [Campaign_info[0]]
        counts = {}
        counts_keys = {}
        dimlist, pendings = self.get_pending_for_campaigns(cl, tmin, tmax)
        effs = self.get_efficiency(cl, tmin, tmax)
        counts[campaign_id] = self.triagePOMS.job_counts(cherrypy.request.db,tmax = tmax, tmin = tmin, tdays = tdays, campaign_id = campaign_id)
        counts[campaign_id]['efficiency'] = effs[0]
        counts[campaign_id]['pending'] = pendings[0]
        counts_keys[campaign_id] = counts[campaign_id].keys()
        #
        # any launch outputs to look at?
        #
        dirname="%s/private/logs/poms/launches/campaign_%s" % (
           os.environ['HOME'],campaign_id)
        launch_flist = glob.glob('%s/*' % dirname)
        launch_flist = map(os.path.basename, launch_flist)

        template = self.jinja_env.get_template('campaign_info.html')
        return template.render(  Campaign_info = Campaign_info, time_range_string = time_range_string, tmins = tmins, tmaxs = tmaxs, Campaign_definition_info = Campaign_definition_info, Launch_template_info = Launch_template_info, tags=tags, launched_campaigns=launched_campaigns, dimlist= dimlist, cl = cl, counts_keys = counts_keys, counts = counts, launch_flist = launch_flist, current_experimenter=cherrypy.session.get('experimenter'),  do_refresh = 0, pomspath=self.path,help_page="CampaignInfoHelp", version=self.version)


#########################
##FilesPOMS
    @cherrypy.expose
    def list_task_logged_files(self, task_id):
        fl, t, jobsub_job_id = self.filesPOMS.list_task_logged_files(cherrypy.request.db, task_id)
        template = self.jinja_env.get_template('list_task_logged_files.html')
        return template.render(fl = fl, campaign = t.campaign_snap_obj,  jobsub_job_id = jobsub_job_id, current_experimenter=cherrypy.session.get('experimenter'),  do_refresh = 0, pomspath=self.path, help_page="ListTaskLoggedFilesHelp", version=self.version)


    @cherrypy.expose
    def campaign_task_files(self, campaign_id, tmin = None, tmax = None, tdays = 1):
        c, columns, datarows, tmins, tmaxs, prevlink, nextlink, tdays = self.campaign_task_files(cherrypy.request.db, cherrypy.log, cherrypy.request.samweb_lite, campaign_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_task_files.html')
        return template.render(name = c.name if c else "", columns = columns, datarows = datarows, tmin=tmins, tmax=tmaxs,  prev=prevlink, next=nextlink, days=tdays, current_experimenter=cherrypy.session.get('experimenter'),  campaign_id = campaign_id, pomspath=self.path,help_page="CampaignTaskFilesHelp", version=self.version)


    @cherrypy.expose
    def job_file_list(self, job_id,force_reload = False): ##Ask Marc to check this in the module
        return self.filesPOMS.job_file_list(cherrypy.request.db, cherrypy.request.jobsub_fetcher, job_id, force_reload)


    @cherrypy.expose
    def job_file_contents(self, job_id, task_id, file, tmin = None, tmax = None, tdays = None):
        job_file_contents, tmin = self.filesPOMS.job_file_contents(cherrypy.request.db, cherrypy.log,  cherrypy.request.jobsub_fetcher, job_id, task_id, file, tmin, tmax, tdays)
        template = self.jinja_env.get_template('job_file_contents.html')
        return template.render(file=file, job_file_contents=job_file_contents, task_id=task_id, job_id=job_id, tmin=tmin, pomspath=self.path,help_page="JobFileContentsHelp", version=self.version)


    @cherrypy.expose
    def inflight_files(self, campaign_id=None, task_id=None):
        outlist, statusmap, c = self.filesPOMS.inflight_files( cherrypy.request.db, cherrypy.response.status, campaign_id, task_id)
        template = self.jinja_env.get_template('inflight_files.html')
        return template.render(flist = outlist,  current_experimenter=cherrypy.session.get('experimenter'),   statusmap = statusmap, c = c, jjid= self.task_min_job(task_id),campaign_id = campaign_id, task_id = task_id, pomspath=self.path,help_page="PendingFilesJobsHelp", version=self.version)


    @cherrypy.expose
    def show_dimension_files(self, experiment, dims):
        flist = self.filesPOMS.show_dimension_files(cherrypy.request.samweb_lite, experiment, dims)
        template = self.jinja_env.get_template('show_dimension_files.html')
        return template.render(flist = flist, dims = dims,  current_experimenter=cherrypy.session.get('experimenter'),   statusmap = [], pomspath=self.path,help_page="ShowDimensionFilesHelp", version=self.version)


    @cherrypy.expose
    def actual_pending_files(self, count_or_list, task_id = None, campaign_id = None, tmin = None, tmax= None, tdays = 1): ###??? Implementation of the exception.
        cherrypy.response.timeout = 600
        try:
            c.experiment, dims = self.filesPOMS.actual_pending_files(cherrypy.request.db, cherrypy.log, count_or_list, task_id, campaign_id, tmin, tmax, tdays)
            return self.show_dimension_files(c.experiment, dims)
        except ValueError:
            return "None == dims in actual_pending_files method"


    @cherrypy.expose
    def campaign_sheet(self, campaign_id, tmin = None, tmax = None , tdays = 7):
        name, columns, outrows, dimlist, tmaxs, prevlink, nextlink, tdays, tmin, tmax = campaign_sheet(cherrypy.request.db, cherrypy.log, campaign_id, tmin, tmax, tdays)
        template = self.jinja_env.get_template('campaign_sheet.html')
        return template.render(name=name,
                                columns=columns,
                                datarows=outrows,
                                dimlist=dimlist,
                                tmaxs=tmaxs,
                                prev=prevlink,
                                next=nextlink,
                                days=tdays,
                                tmin = tmin,
                                tmax = tmax,
                                current_experimenter=cherrypy.session.get('experimenter'),
                                campaign_id=campaign_id,
                                experiment=experiment,
                                pomspath=self.path,help_page="CampaignSheetHelp",
                                version=self.version)
###Im here
#----------------------------

    @cherrypy.expose
    def campaign_time_bars(self, campaign_id = None, tag = None, tmin = None, tmax = None, tdays = 1):
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.utilsPOMS.handle_dates(tmin, tmax,tdays,'campaign_time_bars?campaign_id=%s&'% campaign_id)

        tg = time_grid.time_grid()

        key = tg.key()

        class fakerow:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        sl = []
        # sl.append(self.filesPOMS.format_self.triagePOMS.job_counts(cherrypy.request.db,))

        q = cherrypy.request.db.query(Campaign)
        if campaign_id != None:
            q = q.filter(Campaign.campaign_id == campaign_id)
            cpl = q.all()
            name = cpl[0].name
        elif tag != None and tag != "":
            q = q.join(CampaignsTags,Tag).filter(Campaign.campaign_id == CampaignsTags.campaign_id,
Tag.tag_id == CampaignsTags.tag_id, Tag.tag_name == tag)
            cpl = q.all()
            name = tag
        else:
            cherrypy.response.status="404 Permission Denied."
            return "Neither Campaign nor Tag found"


        template = self.jinja_env.get_template('campaign_time_bars.html')

        job_counts_list = []
        cidl = []
        for cp in cpl:
             job_counts_list.append(cp.name)
             print "about to call format_job_counts( %s ,%s, %s, %s )" % (campaign_id, tmin, tmax, tdays)
             job_counts_list.append( self.filesPOMS.format_job_counts(cherrypy.request.db, campaign_id = cp.campaign_id, tmin = tmin, tmax = tmax, tdays = tdays, range_string = time_range_string))
             cidl.append(cp.campaign_id)

        job_counts = "\n".join(job_counts_list)

        qr = cherrypy.request.db.query(TaskHistory).join(Task).filter(Task.campaign_id.in_(cidl), TaskHistory.task_id == Task.task_id , or_(and_(Task.created > tmin, Task.created < tmax),and_(Task.updated > tmin, Task.updated < tmax)) ).order_by(TaskHistory.task_id,TaskHistory.created).all()
        items = []
        extramap = {}
        for th in qr:
            jjid = self.task_min_job(th.task_id)
            if not jjid:
                jjid= 't' + str(th.task_id)
            else:
                jjid = jjid.replace('fifebatch','').replace('.fnal.gov','')
            if th.status != "Completed" and th.status != "Located":
                extramap[jjid] = '<a href="%s/kill_jobs?task_id=%d"><i class="ui trash icon"></i></a>' % (self.path, th.task_id)
            else:
                extramap[jjid] = '&nbsp; &nbsp; &nbsp; &nbsp;'

            items.append(fakerow(task_id = th.task_id,
                                  created = th.created.replace(tzinfo = utc),
                                  tmin = th.task_obj.created - timedelta(minutes=15),
                                  tmax = th.task_obj.updated,
                                  status = th.status,
                                  jobsub_job_id = jjid))

        blob = tg.render_query_blob(tmin, tmax, items, 'jobsub_job_id', url_template = self.path + '/show_task_jobs?task_id=%(task_id)s&tmin=%(tmin)19.19s&tdays=1',extramap = extramap )

        return template.render( job_counts = job_counts, blob = blob, name = name, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'),  do_refresh = 1, next = nextlink, prev = prevlink, days = tdays, key = key, pomspath=self.path, extramap = extramap, help_page="CampaignTimeBarsHelp", version=self.version)


    @cherrypy.expose
    def job_table(self, tmin = None, tmax = None, tdays = 1, task_id = None, campaign_id = None , experiment = None, sift=False, campaign_name=None, name=None,campaign_def_id=None, vo_role=None, input_dataset=None, output_dataset=None, task_status=None, project=None, jobsub_job_id=None, node_name=None, cpu_type=None, host_site=None, job_status=None, user_exe_exit_code=None, output_files_declared=None, campaign_checkbox=None, task_checkbox=None, job_checkbox=None, ignore_me = None, keyword=None, dataset = None, eff_d = None):
        ###The pass of the arguments is ugly we will fix that later.
        jl, jobcolumns, taskcolumns, campcolumns, tmins, tmaxs, prevlink, nextlink, tdays, extra, hidecolumns, filtered_fields, time_range_string = self.triagePOMS.job_table(cherrypy.request.db, tmin, tmax, tdays, task_id, campaign_id, experimen, sift, campaign_name, name,campaign_def_id, vo_role, input_dataset, output_dataset, task_status, project, jobsub_job_id, node_name, cpu_type, host_site, job_status, user_exe_exit_code, output_files_declared, campaign_checkbox, task_checkbox, job_checkbox, ignore_me, keyword, dataset, eff_d)
        template = self.jinja_env.get_template('job_table.html')
        return template.render(joblist=jl, jobcolumns = jobcolumns, taskcolumns = taskcolumns, campcolumns = campcolumns, current_experimenter=cherrypy.session.get('experimenter'),  do_refresh = 0,  tmin=tmins, tmax =tmaxs,  prev= prevlink,  next = nextlink, days = tdays, extra = extra, hidecolumns = hidecolumns, filtered_fields=filtered_fields, time_range_string = time_range_string, pomspath=self.path,help_page="JobTableHelp", version=self.version)


    @cherrypy.expose
    def jobs_by_exitcode(self, tmin = None, tmax =  None, tdays = 1 ):
        raise cherrypy.HTTPRedirect("%s/failed_jobs_by_whatever?f=user_exe_exit_code&tdays=%s" % (self.path, tdays))


    @cherrypy.expose
    def failed_jobs_by_whatever(self, tmin = None, tmax =  None, tdays = 1 , f = [], go = None):
        jl, possible_columns, columns, tmins, tmaxs, tdays, prevlink, nextlink, time_range_string, tdays = self.triagePOMS.failed_jobs_by_whatever(cherrypy.request.db, cherrypy.log, tmin, tmax, tdays, f, go)
        template = self.jinja_env.get_template('failed_jobs_by_whatever.html')

        return template.render(joblist=jl, possible_columns = possible_columns, columns = columns, current_experimenter=cherrypy.session.get('experimenter'),  do_refresh = 0,  tmin=tmins, tmax =tmaxs,  tdays=tdays, prev= prevlink,  next = nextlink, time_range_string = time_range_string, days = tdays, pomspath=self.path,help_page="JobsByExitcodeHelp", version=self.version)
#-------------------


    @cherrypy.expose
    def get_task_id_for(self, campaign, user = None, experiment = None, command_executed = "", input_dataset = "", parent_task_id=None):
        if user == None:
             user = 4
        else:
             u = cherrypy.request.db.query(Experimenter).filter(Experimenter.email.like("%s@%%" % user)).first()
             if u:
                  user = u.experimenter_id
        q = cherrypy.request.db.query(Campaign)
        if campaign[0] in "0123456789":
            q = q.filter(Campaign.campaign_id == int(campaign))
        else:
            q = q.filter(Campaign.name.like("%%%s%%" % campaign))

        if experiment:
            q = q.filter(Campaign.experiment == experiment)

        c = q.first()
        tim = datetime.now(utc)
        t = Task(campaign_id = c.campaign_id,
                 task_order = 0,
                 input_dataset = input_dataset,
                 output_dataset = "",
                 status = "New",
                 task_parameters = "{}",
                 updater = 4,
                 creator = 4,
                 created = tim,
                 updated = tim,
                 command_executed = command_executed)

        if parent_task_id != None and parent_task_id != "None":
            t.recovery_tasks_parent = int(parent_task_id)

        self.snapshot_parts(t, t.campaign_id)

        cherrypy.request.db.add(t)
        cherrypy.request.db.commit()
        return "Task=%d" % t.task_id


    @cherrypy.expose
    def register_poms_campaign(self, experiment,  campaign_name, version, user = None, campaign_definition = None, dataset = "", role = "Analysis", params = []):
         if user == None:
              user = 4
         else:
              u = cherrypy.request.db.query(Experimenter).filter(Experimenter.email.like("%s@%%" % user)).first()
              if u:
                   user = u.experimenter_id


         if campaign_definition != None and campaign_definition != "None":
              cd = cherrypy.request.db.query(CampaignDefinition).filter(Campaign.name == campaign_definition, Campaign.experiment == experiment).first()
         else:
              cd = cherrypy.request.db.query(CampaignDefinition).filter(CampaignDefinition.name.like("%generic%"), Campaign.experiment == experiment).first()

         ld = cherrypy.request.db.query(LaunchTemplate).filter(LaunchTemplate.name.like("%generic%"), LaunchTemplate.experiment == experiment).first()

         cherrypy.log("campaign_definition = %s " % cd)

         c = cherrypy.request.db.query(Campaign).filter( Campaign.experiment == experiment, Campaign.name == campaign_name).first()
         if c:
             changed = False
         else:
             c = Campaign(experiment = experiment, name = campaign_name, creator = user, created = datetime.now(utc), software_version = version, campaign_definition_id=cd.campaign_definition_id, launch_id = ld.launch_id, vo_role = role)

         if version:
               c.software_verison = version
               changed = True

         if dataset:
               c.dataset = dataset
               changed = True

         if user:
               c.experimenter = user
               changed = True

         cherrypy.log("register_campaign -- campaign is %s" % c.__dict__)

         if changed:
                c.updated = datetime.now(utc)
                c.updator = user
                cherrypy.request.db.add(c)
                cherrypy.request.db.commit()

         return "Campaign=%d" % c.campaign_id





    @cherrypy.expose
    def json_project_summary_for_task(self, task_id):
        cherrypy.response.headers['Content-Type'] = "application/json"
        return json.dumps(self.project_summary_for_task(task_id))


    def project_summary_for_task(self, task_id):
        t = cherrypy.request.db.query(Task).filter(Task.task_id == task_id).first()
        return cherrypy.request.samweb_lite.fetch_info(t.campaign_snap_obj.experiment, t.project)


    def project_summary_for_tasks(self, task_list):
        return cherrypy.request.samweb_lite.fetch_info_list(task_list)
        #~ return [ {"tot_consumed": 0, "tot_failed": 0, "tot_jobs": 0, "tot_jobfails": 0} ] * len(task_list)    #VP Debug


    @cherrypy.expose
    def kill_jobs(self, campaign_id=None, task_id=None, job_id=None, confirm=None):
        jjil = []
        jql = None
        t = None
        if campaign_id != None or task_id != None:
            if campaign_id != None:
                tl = cherrypy.request.db.query(Task).filter(Task.campaign_id == campaign_id, Task.status != 'Completed', Task.status != 'Located').all()
            else:
                tl = cherrypy.request.db.query(Task).filter(Task.task_id == task_id).all()
            c = tl[0].campaign_snap_obj
            for t in tl:
                tjid = self.task_min_job(t.task_id)
                cherrypy.log("kill_jobs: task_id %s -> tjid %s" % (t.task_id, tjid))
                # for tasks/campaigns, kill the whole group of jobs
                # by getting the leader's jobsub_job_id and taking off
                # the '.0'.
                if tjid:
                    jjil.append(tjid.replace('.0',''))
        else:
            jql = cherrypy.request.db.query(Job).filter(Job.job_id == job_id, Job.status != 'Completed', Job.status != 'Located').all()
            c = jql[0].task_obj.campaign_snap_obj
            for j in jql:
                jjil.append(j.jobsub_job_id)

        if confirm == None:
            template = self.jinja_env.get_template('kill_jobs_confirm.html')
            return template.render(current_experimenter=cherrypy.session.get('experimenter'),  jjil = jjil, task = t, campaign_id = campaign_id, task_id = task_id, job_id = job_id, pomspath=self.path,help_page="KilledJobsHelp", version=self.version)
        else:
            group = c.experiment
            if group == 'samdev': group = 'fermilab'

            f = os.popen("jobsub_rm -G %s --role %s --jobid %s 2>&1" % (group, c.vo_role, ','.join(jjil)), "r")
            output = f.read()
            f.close()

            template = self.jinja_env.get_template('kill_jobs.html')
            return template.render(output = output, current_experimenter=cherrypy.session.get('experimenter'),  c = c, campaign_id = campaign_id, task_id = task_id, job_id = job_id, pomspath=self.path,help_page="KilledJobsHelp", version=self.version)


    def get_dataset_for(self, camp):
        res = None

        if camp.cs_split_type == None or camp.cs_split_type in [ '', 'draining','None' ]:
            # no split to do, it is a draining datset, etc.
            res =  camp.dataset

        elif camp.cs_split_type == 'list':
            j# we were given a list of datasets..
            l = camp.dataset.split(',')
            if camp.cs_last_split == '' or camp.cs_last_split == None:
                camp.cs_last_split = -1
            camp.cs_last_split += 1

            if camp.cs_last_split >= len(l):
                raise cherrypy.HTTPError(404, 'No more splits in this campaign')

            res = l[camp.cs_last_split]

            cherrypy.request.db.add(camp)
            cherrypy.request.db.commit()

        elif camp.cs_split_type.startswith('mod_'):
            m = int(camp.cs_split_type[4:])
            if camp.cs_last_split == '' or camp.cs_last_split == None:
                camp.cs_last_split = -1
            camp.cs_last_split += 1

            if camp.cs_last_split >= m:
                raise cherrypy.HTTPError(404, 'No more splits in this campaign')
            new = camp.dataset + "_slice%d" % camp.cs_last_split
            cherrypy.request.samweb_lite.create_definition(camp.campaign_definition_obj.experiment, new,  "defname: %s with stride %d offset %d" % (camp.dataset, m, camp.cs_last_split))

            res = new

            cherrypy.request.db.add(camp)
            cherrypy.request.db.commit()

        elif camp.cs_split_type == 'new':
            # save time *before* we define things, so we don't miss any
            # and knock off an estimated FTS delay
            est_fts_delay = 1800 # half an hour?
            t = time.time() - 1800

            if camp.cs_last_split == '' or camp.cs_last_split == None:
                new = camp.dataset
            else:
                new = camp.dataset + "_since_%s" % int(camp.cs_last_split)
                cherrypy.request.samweb_lite.create_definition(
                  camp.campaign_definition_obj.experiment,
                  new,
                  "defname: %s and end_time > '%s' and end_time <= '%s'" % (
                     camp.dataset,
                     time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(camp.cs_last_split)),
                     time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(t)))
                )

            # mark end time for start of next run
            camp.cs_last_split = t
            res = new

            cherrypy.request.db.add(camp)
            cherrypy.request.db.commit()

        elif camp.cs_split_type == 'new_local':
            # save time *before* we define things, so we don't miss any
            # and knock off an estimated FTS delay
            est_fts_delay = 1800 # half an hour?
            t = time.time() - 1800

            if camp.cs_last_split == '' or camp.cs_last_split == None:
                new = camp.dataset
            else:
                new = camp.dataset + "_since_%s" % int(camp.cs_last_split)
                cherrypy.request.samweb_lite.create_definition(
                  camp.campaign_definition_obj.experiment,
                  new,
                  "defname: %s and end_time > '%s' and end_time <= '%s'" % (
                     camp.dataset,
                     time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(camp.cs_last_split)),
                     time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(t)))
                )

            # mark end time for start of next run
            camp.cs_last_split = t
            res = new

            cherrypy.request.db.add(camp)
            cherrypy.request.db.commit()


        return res


    @cherrypy.expose
    def set_job_launches(self, hold):
        if hold in ["hold","allowed"]:
            cherrypy.config.update({'poms.launches': hold})
        raise cherrypy.HTTPRedirect(self.path + "/")


    def launch_dependents_if_needed(self, t):
        cherrypy.log("Entering launch_dependents_if_needed(%s)" % t.task_id)
	if not cherrypy.config.get("poms.launch_recovery_jobs",False):
            # XXX should queue for later?!?
            return 1
        cdlist = cherrypy.request.db.query(CampaignDependency).filter(CampaignDependency.needs_camp_id == t.campaign_snap_obj.campaign_id).all()

        i = 0
        for cd in cdlist:
           if cd.uses_camp_id == t.campaign_snap_obj.campaign_id:
              # self-reference, just do a normal launch
              self.launch_jobs(cd.uses_camp_id)
           else:
              i = i + 1
              dims = "ischildof: (snapshot_for_project %s) and version %s and file_name like '%s' " % (t.project, t.campaign_snap_obj.software_version, cd.file_patterns)
              dname = "poms_depends_%d_%d" % (t.task_id,i)

              cherrypy.request.samweb_lite.create_definition(t.campaign_snap_obj.experiment, dname, dims)
              self.launch_jobs(cd.uses_camp_id, dataset_override = dname)
        return 1


    def get_recovery_list_for_campaign_def(self, campaign_def):
        rlist = cherrypy.request.db.query(CampaignRecovery).options(joinedload(CampaignRecovery.recovery_type)).filter(CampaignRecovery.campaign_definition_id == campaign_def.campaign_definition_id).order_by(CampaignRecovery.recovery_order)

        # convert to a real list...
        l = []
        for r in rlist:
            l.append(r)
        rlist = l

        return rlist


    def launch_recovery_if_needed(self, t):
        cherrypy.log("Entering launch_recovery_if_needed(%s)" % t.task_id)
	if not cherrypy.config.get("poms.launch_recovery_jobs",False):
            # XXX should queue for later?!?
            return 1

        # if this is itself a recovery job, we go back to our parent
        # to do all the work, because it has the counters, etc.
        if t.parent_obj:
           t = t.parent_obj

        rlist = self.get_recovery_list_for_campaign_def(t.campaign_definition_snap_obj)

        if t.recovery_position == None:
           t.recovery_position = 0

        while t.recovery_position != None and t.recovery_position < len(rlist):
            rtype = rlist[t.recovery_position].recovery_type
            t.recovery_position = t.recovery_position + 1
            if rtype.name == 'consumed_status':
                 recovery_dims = "snapshot_for_project_name %s and consumed_status != 'consumed'" % t.project
            elif rtype.name == 'proj_status':
                 recovery_dims = "snapshot_for_project_name %s and process_status != 'ok'" % t.project
            elif rtype.name == 'pending_files':
                 recovery_dims = "snapshot_for_project_name %s " % t.project
                 if t.campaign_definition_snap_obj.output_file_types:
                     oftypelist = campaign_definition_snap_obj.output_file_types.split(",")
                 else:
                     oftypelist = ["%"]

                 for oft in oftypelist:
                     recovery_dims = recovery_dims + "minus isparent: ( version %s and file_name like %s) " % (t.campaign_snap_obj.software_version, oft)
            else:
                 # default to consumed status(?)
                 recovery_dims = "snapshot_for_project_name %s and consumed_status != 'consumed'" % t.project

            nfiles = cherrypy.request.samweb_lite.count_files(t.campaign_snap_obj.experiment,recovery_dims)

	    t.recovery_position = t.recovery_position + 1
            cherrypy.request.db.add(t)
            cherrypy.request.db.commit()

            if nfiles > 0:
                rname = "poms_recover_%d_%d" % (t.task_id,t.recovery_position)

                cherrypy.log("launch_recovery_if_needed: creating dataset for exp=%s name=%s dims=%s" % (t.campaign_snap_obj.experiment, rname, recovery_dims))

                cherrypy.request.samweb_lite.create_definition(t.campaign_snap_obj.experiment, rname, recovery_dims)


                self.launch_jobs(t.campaign_snap_obj.campaign_id, dataset_override=rname, parent_task_id = t.task_id)
                return 1

        return 0


    @cherrypy.expose
    def launch_jobs(self, campaign_id, dataset_override = None, parent_task_id = None):

        cherrypy.log("Entering launch_jobs(%s, %s, %s)" % (campaign_id, dataset_override, parent_task_id))
        if cherrypy.config.get("poms.launches","allowed") == "hold":
            return "Job launches currently held."

        c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).options(joinedload(Campaign.launch_template_obj),joinedload(Campaign.campaign_definition_obj)).first()
        cd = c.campaign_definition_obj
        lt = c.launch_template_obj

        e = cherrypy.session.get('experimenter')
        xff = cherrypy.request.headers.get('X-Forwarded-For', None)
        ra =  cherrypy.request.headers.get('Remote-Addr', None)
        if not e.is_authorized(c.experiment) and not ( ra == '127.0.0.1' and xff == None):
             cherrypy.log("launch_jobs -- experimenter not authorized")
             cherrypy.response.status="404 Permission Denied."
             return "Not Authorized: e: %s xff %s ra %s" % (e, xff, ra)
        experimenter_login = e.email[:e.email.find('@')]
        lt.launch_account = lt.launch_account % {
              "experimenter": experimenter_login,
        }

        if dataset_override:
            dataset = dataset_override
        else:
            dataset = self.get_dataset_for(c)

        group = c.experiment
        if group == 'samdev': group = 'fermilab'

        cmdl =  [
            "exec 2>&1",
            "export KRB5CCNAME=/tmp/krb5cc_poms_submit_%s" % group,
            "export POMS_PARENT_TASK_ID=%s" % (parent_task_id if parent_task_id else ""),
            "kinit -kt $HOME/private/keytabs/poms.keytab poms/cd/%s@FNAL.GOV || true" % self.hostname,
            "ssh -tx %s@%s <<'EOF'" % (lt.launch_account, lt.launch_host),
            lt.launch_setup % {
              "dataset":dataset,
              "version":c.software_version,
              "group": group,
              "experimenter": experimenter_login,
            },
            "setup poms_jobsub_wrapper v0_4 -z /grid/fermiapp/products/common/db",
            "export POMS_PARENT_TASK_ID=%s" % (parent_task_id if parent_task_id else ""),
            "export POMS_TEST=%s" % ("" if "poms" in self.hostname else "1"),
            "export POMS_CAMPAIGN_ID=%s" % c.campaign_id,
            "export POMS_TASK_DEFINITION_ID=%s" % c.campaign_definition_id,
            "export JOBSUB_GROUP=%s" % group,
        ]
        if cd.definition_parameters:
           params = OrderedDict(json.loads(cd.definition_parameters))
        else:
           params = OrderedDict([])

        if c.param_overrides != None and c.param_overrides != "":
            params.update(json.loads(c.param_overrides))

        lcmd = cd.launch_script + " " + ' '.join((x[0]+x[1]) for x in params.items())
        lcmd = lcmd % {
              "dataset":dataset,
              "version":c.software_version,
              "group": group,
              "experimenter": experimenter_login,
        }
        cmdl.append(lcmd)
        cmdl.append('exit')
        cmdl.append('EOF')
        cmd = '\n'.join(cmdl)

        cmd = cmd.replace('\r','')

        # make sure launch doesn't take more that half an hour...
        output = popen_read_with_timeout(cmd, 1800)

        template = self.jinja_env.get_template('launch_jobs.html')
        res = template.render(command = lcmd, output = output, current_experimenter=cherrypy.session.get('experimenter'),  c = c, campaign_id = campaign_id,  pomspath=self.path,help_page="LaunchedJobsHelp", version=self.version)
        # always record launch...
        ds = time.strftime("%Y%m%d_%H%M%S")
        outdir = "%s/private/logs/poms/launches/campaign_%s" % (os.environ["HOME"],campaign_id)
        outfile = "%s/%s" % (outdir, ds)
        cherrypy.log("trying to record launch in %s" % outfile)
        if not os.path.isdir(outdir):
            os.makedirs(outdir)
        lf = open(outfile,"w")
        lf.write(res)
        lf.close()
        return res


##############
#tagsPOMS
    @cherrypy.expose
    def link_tags(self, campaign_id, tag_name, experiment):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return(self.tagsPOMS.link_tags(cherrypy.request.db, cherrypy.session.get, campaign_id, tag_name, experiment))


    @cherrypy.expose
    def delete_campaigns_tags(self, campaign_id, tag_id, experiment):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return(self.tagsPOMS.delete_campaigns_tags( cherrypy.request.db, campaign_id, tag_id, experiment))


    @cherrypy.expose
    def search_tags(self, q):

        results, q_list = self.tagsPOMS.search_tags(cherrypy.request.db, q)
        template = self.jinja_env.get_template('search_tags.html')
        return template.render(results=results, q_list=q_list, current_experimenter=cherrypy.session.get('experimenter'),  do_refresh = 0,  pomspath=self.path, help_page="SearchTagsHelp", version=self.version)


    @cherrypy.expose
    def auto_complete_tags_search(self, experiment, q):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        return(self.tagsPOMS.auto_complete_tags_search(cherrypy.request.db, experiment, q))
#-----------------------


    @cherrypy.expose
    def jobs_eff_histo(self, campaign_id, tmax = None, tmin = None, tdays = 1 ):
        """  use
                  select count(job_id), floor(cpu_time * 10 / wall_time) as de
                     from jobs, tasks
                     where
                        jobs.task_id = tasks.task_id and
                        tasks.campaign_id=17 and
                        wall_time > 0 and
                        wall_time > cpu_time and
                        jobs.updated > '2016-03-10 00:00:00'
                        group by floor(cpu_time * 10 / wall_time)
                       order by de;
             to get height bars for a histogram, clicks through to
             jobs with a given efficiency...
             Need to add efficiency  (cpu_time/wall_time) as a param to
             jobs_table...

         """
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.utilsPOMS.handle_dates(tmin, tmax,tdays,'jobs_eff_histo?campaign_id=%s&' % campaign_id)

        q = cherrypy.request.db.query(func.count(Job.job_id), func.floor(Job.cpu_time *10/Job.wall_time))
        q = q.join(Job.task_obj)
        q = q.filter(Job.task_id == Task.task_id, Task.campaign_id == campaign_id)
        q = q.filter(Job.cpu_time > 0, Job.wall_time >= Job.cpu_time)
        q = q.filter(Task.created < tmax, Task.created >= tmin)
        q = q.group_by(func.floor(Job.cpu_time*10/Job.wall_time))
        q = q.order_by((func.floor(Job.cpu_time*10/Job.wall_time)))

        qz = cherrypy.request.db.query(func.count(Job.job_id))
        qz = qz.join(Job.task_obj)
        qz = qz.filter(Job.task_id == Task.task_id, Task.campaign_id == campaign_id)
        qz = qz.filter(not_(and_(Job.cpu_time > 0, Job.wall_time >= Job.cpu_time)))
        nodata = qz.first()

        total = 0
        vals = {-1: nodata[0]}
        maxv = 0.01
        if nodata[0] > maxv:
           maxv = nodata[0]
        for row in q.all():
            vals[row[1]] = row[0]
            if row[0] > maxv:
               maxv = row[0]
            total += row[0]

        c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        # return "total %d ; vals %s" % (total, vals)
        # return "Not yet implemented"

        template = self.jinja_env.get_template('jobs_eff_histo.html')
        return template.render(  c = c, maxv = maxv, total = total, vals = vals, tmaxs = tmaxs, campaign_id=campaign_id, tdays = tdays, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'),  do_refresh = 1, next = nextlink, prev = prevlink, days = tdays, pomspath=self.path, help_page="JobEfficiencyHistoHelp", version=self.version)


    @cherrypy.expose
    def list_launch_file(self, campaign_id, fname ):
        dirname="%s/private/logs/poms/launches/campaign_%s" % (
           os.environ['HOME'],campaign_id)
        lf = open("%s/%s" % (dirname, fname), "r")
        lines = lf.readlines()
        lf.close()
        return "".join(lines)


    @cherrypy.expose
    def schedule_launch(self, campaign_id ):
        c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        my_crontab = CronTab(user=True)
        citer = my_crontab.find_comment("POMS_CAMPAIGN_ID=%s" % campaign_id)
        # there should be only zero or one...
        job = None
        for job in citer:
            break

        # any launch outputs to look at?
        #
        dirname="%s/private/logs/poms/launches/campaign_%s" % (
           os.environ['HOME'],campaign_id)
        launch_flist = glob.glob('%s/*' % dirname)
        launch_flist = map(os.path.basename, launch_flist)

        template = self.jinja_env.get_template('schedule_launch.html')
        return template.render(  c = c, campaign_id = campaign_id, job = job, current_experimenter=cherrypy.session.get('experimenter'),  do_refresh = 0,  pomspath=self.path, help_page="ScheduleLaunchHelp", launch_flist= launch_flist, version=self.version)


    @cherrypy.expose
    def update_launch_schedule(self, campaign_id, dowlist = None,  domlist = None, monthly = None, month = None, hourlist = None, submit = None , minlist = None, delete = None):

        # deal with single item list silliness
        if isinstance(minlist, basestring):
           minlist = minlist.split(",")
        if isinstance(hourlist, basestring):
           hourlist = hourlist.split(",")
        if isinstance(dowlist, basestring):
           dowlist = dowlist.split(",")
        if isinstance(domlist, basestring):
           domlist = domlist.split(",")

        cherrypy.log("hourlist is %s " % hourlist)

        if minlist[0] == "*":
            minlist = None
        else:
            minlist = [int(x) for x in minlist if x != '']

        if hourlist[0] == "*":
            hourlist = None
        else:
            hourlist = [int(x) for x in hourlist if x != '']

        if dowlist[0] == "*":
            dowlist = None
        else:
            # dowlist[0] = [int(x) for x in dowlist if x != '']
            pass

        if domlist[0] == "*":
            domlist = None
        else:
            domlist = [int(x) for x in domlist if x != '']

        my_crontab = CronTab(user=True)
        # clean out old
        my_crontab.remove_all(comment="POMS_CAMPAIGN_ID=%s" % campaign_id)

        if not delete:

            # make job for new -- use current link for product
            pdir=os.environ.get("POMS_DIR","/etc/poms")
            pdir=pdir[:pdir.rfind("poms")+4] + "/current"
            job = my_crontab.new(command="%s/cron/launcher --campaign_id=%s" % (
                              pdir, campaign_id),
                              comment="POMS_CAMPAIGN_ID=%s" % campaign_id)

            # set timing...
            if dowlist:
                job.dow.on(*dowlist)

            if minlist:
                job.minute.on(*minlist)

            if hourlist:
                job.hour.on(*hourlist)

            if domlist:
                job.day.on(*domlist)

            job.enable()

        my_crontab.write()

        raise cherrypy.HTTPRedirect("schedule_launch?campaign_id=%s" % campaign_id )


    def snapshot_parts(self, t, campaign_id):
         c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
         for table, snaptable, field, sfield, tid , tfield in [
		[Campaign,CampaignSnapshot,Campaign.campaign_id,CampaignSnapshot.campaign_id,c.campaign_id, 'campaign_snap_obj' ],
		[CampaignDefinition, CampaignDefinitionSnapshot,CampaignDefinition.campaign_definition_id, CampaignDefinitionSnapshot.campaign_definition_id, c.campaign_definition_id, 'campaign_definition_snap_obj'],
                [LaunchTemplate ,LaunchTemplateSnapshot,LaunchTemplate.launch_id,LaunchTemplateSnapshot.launch_id,  c.launch_id, 'launch_template_snap_obj']]:

             i = cherrypy.request.db.query(func.max(snaptable.updated)).filter(sfield == tid).first()
             j = cherrypy.request.db.query(table).filter(field == tid).first()
             if (i[0] == None or j == None or j.updated == None or  i[0] < j.updated):
                newsnap = snaptable()
                columns = j._sa_instance_state.class_.__table__.columns
                for fieldname in columns.keys():
                     setattr(newsnap, fieldname, getattr(j,fieldname))
                cherrypy.request.db.add(newsnap)
             else:
                newsnap = cherrypy.request.db.query(snaptable).filter(snaptable.updated == i[0]).first()
             setattr(t, tfield, newsnap)
         cherrypy.request.db.add(t) #Felipe change HERE one tap + space to spaces indentation
         cherrypy.request.db.commit()


    @cherrypy.expose
    def mark_campaign_active(self, campaign_id, is_active):


        c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        if c and (cherrypy.session.get('experimenter').is_authorized(c.experiment) or self.can_report_data()):
            c.active=(is_active == 'True')
            cherrypy.request.db.add(c)
            cherrypy.request.db.commit()
            raise cherrypy.HTTPRedirect("campaign_info?campaign_id=%s" % campaign_id)
        else:
            raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')


    @cherrypy.expose
    def make_stale_campaigns_inactive(self):
        if not can_report_data():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        lastweek = datetime.now(utc) - timedelta(days=7)
        cp = cherrypy.request.db.query(Task.campaign_id).filter(Task.created > lastweek).group_by(Task.campaign_id).all()
        sc = []
        for cid in cp:
            sc.append(cid)

        stale =  cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id.notin_(sc), Campaign.active == True).all()
        res=[]
        for c in stale:
            res.append(c.name)
            c.active=False
            cherrypy.request.db.add(c)


        cherrypy.request.db.commit()

        return "Marked inactive stale: " + ",".join(res)
#----------------

    def get_efficiency(self, campaign_list, tmin, tmax):
        id_list = []
        for c in campaign_list:
            id_list.append(c.campaign_id)

        rows = (cherrypy.request.db.query( func.sum(Job.cpu_time), func.sum(Job.wall_time),Task.campaign_id).
                filter(Job.task_id == Task.task_id,
                       Task.campaign_id.in_(id_list),
                       Job.cpu_time > 0,
                       Job.wall_time > 0,
                       Task.created >= tmin, Task.created < tmax ).
                group_by(Task.campaign_id).all())

        cherrypy.log("got rows:")
        for r in rows:
            cherrypy.log("%s" % repr(r))

        mapem={}
        for totcpu, totwall, campaign_id in rows:
            if totcpu != None and totwall != None:
                mapem[campaign_id] = int(totcpu * 100.0 / totwall)
            else:
                mapem[campaign_id] = -1

        cherrypy.log("got map: %s" % repr(mapem))

        efflist = []
        for c in campaign_list:
            efflist.append(mapem.get(c.campaign_id, -2))

        cherrypy.log("got list: %s" % repr(efflist))
        return efflist


    def get_pending_for_campaigns(self, campaign_list, tmin, tmax):

        task_list_list = []

        cherrypy.log("in get_pending_for_campaigns, tmin %s tmax %s" % (tmin, tmax))

        for c in campaign_list:
	    tl = (cherrypy.request.db.query(Task).
		options( joinedload(Task.campaign_snap_obj)).
	        options( joinedload(Task.campaign_definition_snap_obj)).
                filter(Task.campaign_id == c.campaign_id,
                       Task.created >= tmin, Task.created < tmax ).
                all())
            task_list_list.append(tl)

        return self.get_pending_for_task_lists(task_list_list)


    def get_pending_for_task_lists(self, task_list_list):
        dimlist=[]
        explist=[]
        experiment = None
        cherrypy.log("get_pending_for_task_lists: task_list_list (%d): %s" % (len(task_list_list),task_list_list))
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
                     diml.append("isparentof: ( file_name '%s' and version '%s' with availability physical )" % (pat, task.campaign_snap_obj.software_version))
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

        cherrypy.log("get_pending_for_task_lists: dimlist (%d): %s" % (len(dimlist), dimlist))

	count_list = cherrypy.request.samweb_lite.count_files_list(explist,dimlist)
        cherrypy.log("get_pending_for_task_lists: count_list (%d): %s" % (len(dimlist), count_list))
        return dimlist, count_list
