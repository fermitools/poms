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

from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey, and_, or_, create_engine, null, desc, text, func, exc, distinct
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

def error_response():
    dump = ""
    if cherrypy.config.get("dump",True):
        dump = cherrypy._cperror.format_exc()
    message = dump.replace('\n','<br/>')

    jinja_env = Environment(loader=PackageLoader('webservice','templates'))
    template = jinja_env.get_template('error_response.html')
    path = cherrypy.config.get("pomspath","/poms")
    body = template.render(current_experimenter=cherrypy.session.get('experimenter'),message=message,pomspath=path,dump=dump)

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

    @cherrypy.expose
    def headers(self):
        return repr(cherrypy.request.headers)

    @cherrypy.expose
    def index(self):
        template = self.jinja_env.get_template('index.html')
        return template.render(services=self.service_status_hier('All'),current_experimenter=cherrypy.session.get('experimenter'),

        launches = cherrypy.config.get("poms.launches","allowed"),
                               do_refresh = 1, pomspath=self.path,help_page="DashboardHelp")



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

    def can_create_task(self):
        ra =  cherrypy.request.headers.get('Remote-Addr', None)
        cherrypy.log("can_create_task: Remote-addr: %s" %  ra)
        if ra.startswith('131.225.67.'):
            return 1
        return 0

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


    @cherrypy.expose
    def jump_to_job(self, jobsub_job_id, **kwargs ):

        job = cherrypy.request.db.query(Job).filter(Job.jobsub_job_id == jobsub_job_id).first()
        if job != None:
            tmins =  datetime.now(utc).strftime("%Y-%m-%d+%H:%M:%S")
            raise cherrypy.HTTPRedirect("triage_job?job_id=%d&tmin=%s" % (job.job_id, tmins))
        else:
            raise cherrypy.HTTPRedirect(".")

    @cherrypy.expose
    def calendar_json(self, start, end, timezone, _):
        cherrypy.response.headers['Content-Type'] = "application/json"
        list = []
        rows = cherrypy.request.db.query(ServiceDowntime, Service).filter(ServiceDowntime.service_id == Service.service_id).filter(ServiceDowntime.downtime_started.between(start, end)).filter(Service.name != "All").filter(Service.name != "DCache").filter(Service.name != "Enstore").filter(Service.name != "SAM").filter(~Service.name.endswith("sam")).all()
        for row in rows:

            if row.ServiceDowntime.downtime_type == 'scheduled':
                editable = 'true'
            else:
                editable = 'false'

            if row.Service.name.lower().find("sam") != -1:
                color = "#73ADA2"
            elif row.Service.name.lower().find("fts") != -1:
                color = "#5D8793"
            elif row.Service.name.lower().find("dcache") != -1:
                color = "#1BA8DD"
            elif row.Service.name.lower().find("enstore") != -1:
                color = "#2C7BE0"
            elif row.Service.name.lower().find("fifebatch") != -1:
                color = "#21A8BD"
            else:
                color = "red"


            list.append({'start_key': str(row.ServiceDowntime.downtime_started), 'title': row.Service.name, 's_id': row.ServiceDowntime.service_id, 'start': str(row.ServiceDowntime.downtime_started), 'end': str(row.ServiceDowntime.downtime_ended), 'editable': editable, 'color': color})
        return json.dumps(list)


    @cherrypy.expose
    def calendar(self):
        template = self.jinja_env.get_template('calendar.html')
        rows = cherrypy.request.db.query(Service).filter(Service.name != "All").filter(Service.name != "DCache").filter(Service.name != "Enstore").filter(Service.name != "SAM").filter(Service.name != "FifeBatch").filter(~Service.name.endswith("sam")).all()
        return template.render(rows=rows,current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path,help_page="CalendarHelp")



    @cherrypy.expose
    def add_event(self, title, start, end):
        #title should be something like minos_sam:27 DCache:12 All:11 ...

        start_dt = datetime.fromtimestamp(float(start), tz=utc)
        end_dt = datetime.fromtimestamp(float(end), tz=utc)

        s = cherrypy.request.db.query(Service).filter(Service.name == title).first()
        if s:
            try:
                #we got a service id
                d = ServiceDowntime()
                d.service_id = s.service_id
                d.downtime_started = start_dt
                d.downtime_ended = end_dt
                d.downtime_type = 'scheduled'
                cherrypy.request.db.add(d)
                cherrypy.request.db.commit()
                return "Ok."
            except exc.IntegrityError:
                return "This item already exists."

        else:
            #no service id
            return "Oops."



    @cherrypy.expose
    def edit_event(self, title, start, new_start, end, s_id):  #even though we pass in the s_id we should not rely on it because they can and will change the service name

        s = cherrypy.request.db.query(Service).filter(Service.name == title).first()

        new_start_dt = datetime.fromtimestamp(float(new_start), tz=utc)
        end_dt = datetime.fromtimestamp(float(end), tz=utc)

        record = cherrypy.request.db.query(ServiceDowntime, Service).filter(ServiceDowntime.downtime_started==start).filter(ServiceDowntime.service_id == s_id).first()
        if record and record.ServiceDowntime.downtime_type == 'scheduled':
            record.ServiceDowntime.service_id = s.service_id
            record.ServiceDowntime.downtime_started = new_start_dt
            record.ServiceDowntime.downtime_ended = end_dt
            cherrypy.request.db.commit()
            return "Ok."
        else:
            return "Oops."



    @cherrypy.expose
    def service_downtimes(self):
        template = self.jinja_env.get_template('service_downtimes.html')
        rows = cherrypy.request.db.query(ServiceDowntime, Service).filter(ServiceDowntime.service_id == Service.service_id).all()
        return template.render(rows=rows,current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path,help_page="ServiceDowntimesHelp")


    @cherrypy.expose
    def update_service(self, name, parent, status, host_site, total, failed, description):
        s = cherrypy.request.db.query(Service).filter(Service.name == name).first()


        if parent:
            p = cherrypy.request.db.query(Service).filter(Service.name == parent).first()
            cherrypy.log("got parent %s -> %s" % (parent, p))
            if not p:
                p = Service()
                p.name = parent
                p.status = "unknown"
                p.host_site = "unknown"
                p.updated = datetime.now(utc)
                cherrypy.request.db.add(p)
        else:
            p = None

        if not s:
            s = Service()
            s.name = name
            s.parent_service_obj = p
            s.updated =  datetime.now(utc)
            s.host_site = host_site
            s.status = "unknown"
            cherrypy.request.db.add(s)
            s = cherrypy.request.db.query(Service).filter(Service.name == name).first()

        if s.status != status and status == "bad" and s.service_id:
            # start downtime, if we aren't in one
            d = cherrypy.request.db.query(ServiceDowntime).filter(ServiceDowntime.service_id == s.service_id ).order_by(desc(ServiceDowntime.downtime_started)).first()
            if (d == None or d.downtime_ended != None):
                d = ServiceDowntime()
                d.service_id = s.service_id
                d.downtime_started = datetime.now(utc)
                d.downtime_ended = None
                d.downtime_type = 'actual'
                cherrypy.request.db.add(d)

        if s.status != status and status == "good":
            # end downtime, if we're in one
            d = cherrypy.request.db.query(ServiceDowntime).filter(ServiceDowntime.service_id == s.service_id ).order_by(desc(ServiceDowntime.downtime_started)).first()
            if d:
                if d.downtime_ended == None:
                    d.downtime_ended = datetime.now(utc)
                    cherrypy.request.db.add(d)

        s.parent_service_obj = p
        s.status = status
        s.host_site = host_site
        s.updated = datetime.now(utc)
        s.description = description
        s.items = total
        s.failed_items = failed
        cherrypy.request.db.add(s)
        cherrypy.request.db.commit()

        return "Ok."

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

    @cherrypy.expose
    def service_status(self, under = 'All'):
        prev = None
        prevparent = None
        p = cherrypy.request.db.query(Service).filter(Service.name == under).first()
        list = []
        for s in cherrypy.request.db.query(Service).filter(Service.parent_service_id == p.service_id).all():

            if s.host_site:
                 url = s.host_site
            else:
                 url = "./service_status?under=%s" % s.name

            list.append({'name': s.name,'status': s.status, 'url': url})

        template = self.jinja_env.get_template('service_status.html')
        return template.render(list=list, name=under,current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path,help_page="ServiceStatusHelp")

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

    experimentlist = [ ['nova','nova'],['minerva','minerva']]

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

    @cherrypy.expose
    def raw_tables(self):
        if not self.can_db_admin():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        template = self.jinja_env.get_template('raw_tables.html')
        return template.render(list = self.admin_map.keys(),current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path,help_page="RawTablesHelp")

    @cherrypy.expose
    def user_edit(self, *args, **kwargs):
        db = cherrypy.request.db
        message = None
        data = {}
        email = kwargs.pop('email',None)
        action = kwargs.pop('action',None)

        if action == 'membership':
            # To update memberships set all the tags to false and then reset the needed ones to true.
            e_id = kwargs.pop('experimenter_id',None)
            db.query(ExperimentsExperimenters).filter(ExperimentsExperimenters.experimenter_id==e_id).update({"active":False})
            for key,exp in kwargs.items():
                updated = (
                    db.query(ExperimentsExperimenters)
                    .filter(ExperimentsExperimenters.experimenter_id==e_id)
                    .filter(ExperimentsExperimenters.experiment==exp).update({"active":True})
                    )
                if updated==0:
                    EE = ExperimentsExperimenters()
                    EE.experimenter_id = e_id
                    EE.experiment = exp
                    EE.active = True
                    cherrypy.request.db.add( EE )
            db.commit()

        elif action == "add":
            if db.query(Experimenter).filter(Experimenter.email==email).one():
                message = "An experimenter with the email %s already exists" %  email
            else:
                experimenter = Experimenter()
                experimenter.first_name = kwargs.get('first_name')
                experimenter.last_name = kwargs.get('last_name')
                experimenter.email = email
                db.add( experimenter)
                db.commit()

        elif action == "edit":
            values = {"first_name" : kwargs.get('first_name'),
                      "last_name"  : kwargs.get('last_name'),
                      "email"      : email}
            db.query(Experimenter).filter(Experimenter.experimenter_id==kwargs.get('experimenter_id')).update(values)
            db.commit()

        if email:
            experimenter = db.query(Experimenter).filter(Experimenter.email == email ).first()
            if experimenter == None:
                message = "There is no experimenter with the email %s" % email
            else:
                data['experimenter'] = experimenter
                # Experiments that are members of an experiment can be active or inactive
                data['member_of_exp'] = db.query(ExperimentsExperimenters).filter(ExperimentsExperimenters.experimenter_id == experimenter.experimenter_id)
                # Experimenters that were never a member of an experiment will not have an entry in the experiments_experimenters table for that experiment
                subquery = db.query(ExperimentsExperimenters.experiment).filter(ExperimentsExperimenters.experimenter_id == experimenter.experimenter_id)
                data['not_member_of_exp'] = db.query(Experiment).filter(~Experiment.experiment.in_(subquery))

        db.commit()

        data['message'] = message
        template = self.jinja_env.get_template('user_edit.html')
        return template.render(data=data, current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path, help_page="EditUsersHelp")

    @cherrypy.expose
    def experiment_members(self, *args, **kwargs):
        db = cherrypy.request.db
        exp = kwargs['experiment']
        query = (db.query(Experiment,ExperimentsExperimenters,Experimenter)
                 .join(ExperimentsExperimenters).join(Experimenter)
                 .filter(Experiment.name==exp)
                 .order_by(ExperimentsExperimenters.active.desc(),Experimenter.last_name)
                 )
        trows=""
        for experiment, e2e, experimenter in query:
            active = "No"
            if e2e.active:
                active="Yes"
            trow = """<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>""" % (experimenter.first_name, experimenter.last_name, experimenter.email, active)
            trows = "%s%s" % (trows,trow)
        return json.dumps(trows)

    @cherrypy.expose
    def experiment_edit(self, message=None):
        db = cherrypy.request.db
        experiments = db.query(Experiment).order_by(Experiment.experiment)
        template = self.jinja_env.get_template('experiment_edit.html')
        return template.render(message=message, experiments=experiments, current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path,help_page="ExperimentEditHelp")

    @cherrypy.expose
    def experiment_authorize(self, *args, **kwargs):
        db = cherrypy.request.db
        if not self.can_db_admin():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')

        message = None
        # Add new experiment, if any
        try:
            experiment = kwargs.pop('experiment')
            name = kwargs.pop('name')
            try:
                db.query(Experiment).filter(Experiment.experiment==experiment).one()
                message = "Experiment, %s,  already exists." % experiment
            except NoResultFound:
                exp = Experiment(experiment=experiment, name=name)
                cherrypy.request.db.add(exp)
                cherrypy.request.db.commit()
        except KeyError:
            pass
        # Delete experiment(s), if any were selected
        try:
            experiment = None
            for experiment in kwargs:
                db.query(Experiment).filter(Experiment.experiment==experiment).delete()
            cherrypy.request.db.commit()
        except IntegrityError, e:
            message = "The experiment, %s, is used and may not be deleted." % experiment
            cherrypy.log(e.message)
            db.rollback()
        except SQLAlchemyError, e:
            cherrypy.request.db.rollback()
            message = "SqlAlchemy error - %s" % e.message
            cherrypy.log(e.message)

        return self.experiment_edit(message)

    @cherrypy.expose
    def launch_template_edit(self, *args, **kwargs):
        db = cherrypy.request.db
        data = {}
        message = None
        data['exp_selections'] = db.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)

        action = kwargs.pop('action',None)
        exp = kwargs.pop('experiment',None)

        if action == 'delete':
            name = kwargs.pop('name')
            try:
                db.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).filter(LaunchTemplate.name==name).delete()
                db.commit()
            except:
                db.rollback()
                message = "The template, %s, is in use and may not be deleted." % name
                cherrypy.log(e.message)

        if action == 'add' or action == 'edit':
            ae_launch_id = kwargs.pop('ae_launch_id')
            ae_launch_name = kwargs.pop('ae_launch_name')
            ae_launch_host = kwargs.pop('ae_launch_host')
            ae_launch_account = kwargs.pop('ae_launch_account')
            ae_launch_setup = kwargs.pop('ae_launch_setup')
            experimenter_id = kwargs.pop('experimenter_id')
            try:
                if action == 'add':
                    template = LaunchTemplate(experiment=exp, name=ae_launch_name, launch_host=ae_launch_host, launch_account=ae_launch_account,
                                              launch_setup=ae_launch_setup,creator = experimenter_id, created = datetime.now(utc))
                    db.add(template)
                else:
                    columns = {"name":           ae_launch_name,
                               "launch_host":    ae_launch_host,
                               "launch_account": ae_launch_account,
                               "launch_setup":   ae_launch_setup,
                               "updated":        datetime.now(utc),
                               "updater":        experimenter_id
                               }
                    template = db.query(LaunchTemplate).filter(LaunchTemplate.launch_id==ae_launch_id).update(columns)
            except IntegrityError, e:
                message = "Integrity error - you are most likely using a name which already exists in database."
                cherrypy.log(e.message)
                db.rollback()
            except SQLAlchemyError, e:
                message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % e.message
                cherrypy.log(e.message)
                db.rollback()
            else:
                db.commit()

        # Find templates
        if exp: # cuz the default is find
            data['curr_experiment'] = exp
            data['authorized'] = cherrypy.session.get('experimenter').is_authorized(exp)
            data['templates'] = db.query(LaunchTemplate,Experiment).join(Experiment).filter(LaunchTemplate.experiment==exp).order_by(LaunchTemplate.name)

        data['message'] = message
        template = self.jinja_env.get_template('launch_template_edit.html')
        return template.render(data=data,current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path,help_page="LaunchTemplateEditHelp")

    @cherrypy.expose
    def campaign_definition_edit(self, *args, **kwargs):
        db = cherrypy.request.db
        data = {}
        message = None
        data['exp_selections'] = db.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)

        action = kwargs.pop('action',None)
        exp = kwargs.pop('experiment',None)

        if action == 'delete':
            name = kwargs.pop('name')
            try:
                db.query(CampaignDefinition).filter(CampaignDefinition.experiment==exp).filter(CampaignDefinition.name==name).delete()
                db.commit()
            except:
                db.rollback()
                message = "The campaign definition, %s, is in use and may not be deleted." % name
                cherrypy.log(message)

        if action == 'add' or action == 'edit':
            campaign_definition_id = kwargs.pop('ae_campaign_definition_id')
            name = kwargs.pop('ae_definition_name')
            input_files_per_job = kwargs.pop('ae_input_files_per_job')
            output_files_per_job = kwargs.pop('ae_output_files_per_job')
            output_file_patterns = kwargs.pop('ae_output_file_patterns')
            launch_script = kwargs.pop('ae_launch_script')
            definition_parameters = kwargs.pop('ae_definition_parameters')
            recoveries = kwargs.pop('ae_definition_recovery')
            experimenter_id = kwargs.pop('experimenter_id')
            try:
                if action == 'add':
                    cd = CampaignDefinition( name=name, experiment=exp,
                                            input_files_per_job=input_files_per_job, output_files_per_job = output_files_per_job,
                                            output_file_patterns = output_file_patterns,
                                            launch_script=launch_script, definition_parameters=definition_parameters,
                                            creator=experimenter_id, created=datetime.now(utc))

                    db.add(cd)
                    db.flush()
                    campaign_definition_id = cd.campaign_definition_id
                else:
                    columns = {"name":                  name,
                               "input_files_per_job":   input_files_per_job,
                               "output_files_per_job":  output_files_per_job,
                               "output_file_patterns":  output_file_patterns,
                               "launch_script":         launch_script,
                               "definition_parameters": definition_parameters,
                               "updated":               datetime.now(utc),
                               "updater":               experimenter_id
                               }
                    cd = db.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==campaign_definition_id).update(columns)

                # now fixup recoveries -- clean out existing ones, and
                # add listed ones.
                db.query(CampaignRecovery).filter(CampaignRecovery.campaign_definition_id == campaign_definition_id).delete()
                i = 0
                for rtn in json.loads(recoveries):
                    rt = db.query(RecoveryType).filter(RecoveryType.name==rtn).first()
                    cr = CampaignRecovery(campaign_definition_id = campaign_definition_id, recovery_order = i, recovery_type = rt)
                    db.add(cr)
                db.commit()
            except IntegrityError, e:
                message = "Integrity error - you are most likely using a name which already exists in database."
                cherrypy.log(e.message)
                db.rollback()
            except SQLAlchemyError, e:
                message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % e.message
                cherrypy.log(e.message)
                db.rollback()
            else:
                db.commit()

        # Find definitions
        if exp: # cuz the default is find
            data['curr_experiment'] = exp
            data['authorized'] = cherrypy.session.get('experimenter').is_authorized(exp)
            # for testing ui...
            #data['authorized'] = True
            data['definitions'] = (db.query(CampaignDefinition,Experiment)
                                   .join(Experiment)
                                   .filter(CampaignDefinition.experiment==exp)
                                   .order_by(CampaignDefinition.name)
                                   )
            # Build the recoveries for each campaign.
            cids = [row[0].campaign_definition_id for row in data['definitions'].all()]
            recs_dict = {}
            for cid in cids:
                recs = (cherrypy.request.db.query(CampaignRecovery).join(CampaignDefinition).options(joinedload(CampaignRecovery.recovery_type))
                        .filter(CampaignRecovery.campaign_definition_id == cid,CampaignDefinition.experiment == exp)
                        .order_by(CampaignRecovery.campaign_definition_id, CampaignRecovery.recovery_order))

                rec_list  = []
                for rec in recs:
                    rec_list.append(rec.recovery_type.name )
                recs_dict[cid] = json.dumps(rec_list)
            data['recoveries'] = recs_dict

            data['rtypes'] = (cherrypy.request.db.query(RecoveryType.name,RecoveryType.description).order_by(RecoveryType.name).all())
        data['message'] = message
        template = self.jinja_env.get_template('campaign_definition_edit.html')
        return template.render(data=data,current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path,help_page="CampaignDefinitionEditHelp")


    @cherrypy.expose
    def campaign_edit(self, *args, **kwargs):
        db = cherrypy.request.db
        data = {}
        message = None
        data['exp_selections'] = db.query(Experiment).filter(~Experiment.experiment.in_(["root","public"])).order_by(Experiment.experiment)

        action = kwargs.pop('action',None)
        exp = kwargs.pop('experiment',None)

        if action == 'delete':
            name = kwargs.pop('name')
            try:
                db.query(Campaign).filter(Campaign.experiment==exp).filter(Campaign.name==name).delete()
                db.commit()
            except:
                db.rollback()
                message = "The campaign definition, %s, has been used and may not be deleted." % name
                cherrypy.log(message)

        if action == 'add' or action == 'edit':
            campaign_id = kwargs.pop('ae_campaign_id')
            name = kwargs.pop('ae_campaign_name')
            active = kwargs.pop('ae_active')
            split_type = kwargs.pop('ae_split_type')
            vo_role = kwargs.pop('ae_vo_role')
            software_version = kwargs.pop('ae_software_version')
            dataset = kwargs.pop('ae_dataset')
            param_overrides = kwargs.pop('ae_param_overrides')
            campaign_definition_id = kwargs.pop('ae_campaign_definition_id')
            launch_id = kwargs.pop('ae_launch_id')
            experimenter_id = kwargs.pop('experimenter_id')
            depends = kwargs.pop('ae_depends')
            if depends and depends != "[]":
                depends = json.loads(depends)
            else:
                depends = {"campaigns": [], "file_patterns": []}

            try:
                if action == 'add':
                    c = Campaign(name=name, experiment=exp,vo_role=vo_role,
                                 active=active, cs_split_type = split_type,
                                 software_version=software_version, dataset=dataset,
                                 param_overrides=param_overrides, launch_id=launch_id,
                                 campaign_definition_id=campaign_definition_id,
                                 creator=experimenter_id, created=datetime.now(utc))

                    db.add(c)
                else:
                    columns = {"name":                  name,
                               "vo_role":               vo_role,
                               "software_version":      software_version,
                               "dataset" :              dataset,
                               "param_overrides":       param_overrides,
                               "campaign_definition_id":campaign_definition_id,
                               "launch_id":             launch_id,
                               "updated":               datetime.now(utc),
                               "updater":               experimenter_id
                               }
                    cd = db.query(Campaign).filter(Campaign.campaign_id==campaign_id).update(columns)
                # now redo dependencies
                db.query(CampaignDependency).filter(CampaignDependency.uses_camp_id == campaign_id).delete()
                cherrypy.log("depends for %s are: %s" % (campaign_id, depends))
                depcamps = db.query(Campaign).filter(Campaign.name.in_(depends['campaigns'])).all()
                
                for i in range(len(depcamps)):
                      
                    cherrypy.log("trying to add dependency for: %s" % depcamps[i].name)
                    d = CampaignDependency(uses_camp_id = campaign_id, needs_camp_id = depcamps[i].campaign_id, file_patterns=depends['file_patterns'][i])
                    db.add(d)
                db.commit()

            except IntegrityError, e:
                message = "Integrity error - you are most likely using a name which already exists in database."
                cherrypy.log(e.message)
                db.rollback()
            except SQLAlchemyError, e:
                message = "SQLAlchemyError.  Please report this to the administrator.   Message: %s" % e.message
                cherrypy.log(e.message)
                db.rollback()
            else:
                db.commit()

        # Find campaigns
        if exp: # cuz the default is find
            data['curr_experiment'] = exp
            data['authorized'] = cherrypy.session.get('experimenter').is_authorized(exp)
            # for testing ui...
            #data['authorized'] = True

            data['campaigns'] = db.query(Campaign).filter(Campaign.experiment==exp).order_by(Campaign.name)
            data['definitions'] = db.query(CampaignDefinition).filter(CampaignDefinition.experiment==exp).order_by(CampaignDefinition.name)
            data['templates'] = db.query(LaunchTemplate).filter(LaunchTemplate.experiment==exp).order_by(LaunchTemplate.name)
            cids = [c.campaign_id for c in data['campaigns'].all()]
            depends = {}
            for cid in cids:
                sql = (db.query(CampaignDependency.uses_camp_id, Campaign.name, CampaignDependency.file_patterns )
                       .filter(CampaignDependency.uses_camp_id == cid,
                               Campaign.campaign_id == CampaignDependency.needs_camp_id))
                deps = {"campaigns"     : [row[1] for row  in sql.all()],
                        "file_patterns" : [row[2] for row  in sql.all()]
                        }
                depends[cid] = json.dumps(deps)
            data['depends'] = depends

        data['message'] = message
        template = self.jinja_env.get_template('campaign_edit.html')
        return template.render(data=data,current_experimenter=cherrypy.session.get('experimenter'),
                               pomspath=self.path,help_page="CampaignEditHelp")

    @cherrypy.expose
    def campaign_edit_query(self, *args, **kwargs):
        db = cherrypy.request.db
        data = {}
        ae_launch_id = kwargs.pop('ae_launch_id',None)
        ae_campaign_definition_id = kwargs.pop('ae_campaign_definition_id',None)

        if ae_launch_id:
            template = {}
            temp = db.query(LaunchTemplate).filter(LaunchTemplate.launch_id==ae_launch_id).first()
            template['launch_host'] = temp.launch_host
            template['launch_account'] = temp.launch_account
            template['launch_setup'] = temp.launch_setup
            data['template'] = template

        if ae_campaign_definition_id:
            definition = {}
            cdef = db.query(CampaignDefinition).filter(CampaignDefinition.campaign_definition_id==ae_campaign_definition_id).first()
            definition['input_files_per_job'] = cdef.input_files_per_job
            definition['output_files_per_job'] = cdef.output_files_per_job
            definition['launch_script'] = cdef.launch_script
            definition['definition_parameters'] = cdef.definition_parameters
            data['definition'] = definition
        return json.dumps(data)

    @cherrypy.expose
    def list_generic(self, classname):
        if not self.can_db_admin():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        l = self.make_list_for(self.admin_map[classname],self.pk_map[classname])
        template = self.jinja_env.get_template('list_generic.html')
        return template.render( classname = classname, list = l, edit_screen="edit_screen_generic", primary_key='experimenter_id',current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path,help_page="ListGenericHelp")

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
        return template.render( screendata = screendata, action="./"+update_call , classname = classname ,current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path,help_page="GenericEditHelp")

    def make_list_for(self,eclass,primkey):
        res = []
        for i in cherrypy.request.db.query(eclass).order_by(primkey).all():
            res.append( {"key": getattr(i,primkey,''), "value": getattr(i,'name',getattr(i,'email','unknown'))})
        return res

    @cherrypy.expose
    def create_task(self, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor = None ):
         if not can_create_task():
             return "Not Allowed"
         first,last,email = creator.split(' ')
         creator = self.get_or_add_experimenter(first, last, email)
         exp = self.get_or_add_experiment(experiment)
         td = self.get_or_add_taskdef(taskdef, creator, exp)
         camp = self.get_or_add_campaign(exp,td,creator)
         t = Task()
         t.campaign_id = camp.campaign_id
         #t.campaign_definition_id = td.campaign_definition_id
         t.task_order = 0
         t.input_dataset = input_dataset
         t.output_dataset = output_dataset
         t.waitingfor = waitingfor
         t.order = 0
         t.creator = creator.experimenter_id
         t.created = datetime.now(utc)
         t.status = "created"
         t.task_parameters = params
         t.waiting_threshold = 5
         t.updater = creator.experimenter_id
         t.updated = datetime.now(utc)

         cherrypy.request.db.add(t)
         cherrypy.request.db.commit()
         return str(t.task_id)

    @cherrypy.expose
    def active_jobs(self):
         cherrypy.response.headers['Content-Type']= 'application/json'
         res = [ "[" ]
         sep=""
         for job in cherrypy.request.db.query(Job).filter(Job.status != "Completed", Job.status != "Located", Job.status != "Removed").all():
              if job.jobsub_job_id == "unknown":
                   continue
              res.append( '%s "%s"' % (sep, job.jobsub_job_id))
              sep = ","
         res.append( "]" )
         res = "".join(res)
         gc.collect(2)
         return res

    @cherrypy.expose
    def report_declared_files(self, flist):
        now =  datetime.now(utc)
        cherrypy.request.db.query(JobFile).filter(JobFile.file_name.in_(flist) ).update({JobFile.declared: now}, synchronize_session = False)
        cherrypy.request.db.commit()

    @cherrypy.expose
    def output_pending_jobs(self):
         cherrypy.response.headers['Content-Type']= 'application/json'
         res = {}
         sep=""
         preve = None
         prevj = None
         for e, jobsub_job_id, fname  in cherrypy.request.db.query(Campaign.experiment,Job.jobsub_job_id,JobFile.file_name).join(Task).filter(Task.campaign_id == Campaign.campaign_id, Job.jobsub_job_id != "unknown", Job.task_id == Task.task_id, Job.job_id == JobFile.job_id, Job.status == "Completed", JobFile.declared == None, JobFile.file_type == 'output').order_by(Campaign.experiment,Job.jobsub_job_id).all():
              if preve != e:
                  preve = e
                  res[e] = {}
              if prevj != jobsub_job_id:
                  prevj = jobsub_job_id
                  res[e][jobsub_job_id] = []
              res[e][jobsub_job_id].append(fname)
         sres =  json.dumps(res)
         res = None
         return sres

    @cherrypy.expose
    def wrapup_tasks(self):
        cherrypy.response.headers['Content-Type'] = "text/plain"
        now =  datetime.now(utc)
        res = ["wrapping up:"]
        for task in cherrypy.request.db.query(Task).options(subqueryload(Task.jobs)).filter(Task.status != "Completed", Task.status != "Located").all():
             total = 0
             running = 0
             for j in task.jobs:
                 total = total + 1
                 if j.status != "Completed" and j.status != "Located":
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
        n_completed = 0
        n_stale = 0
        n_project = 0
        n_located = 0
        for task in cherrypy.request.db.query(Task).options(subqueryload(Task.jobs)).options(subqueryload(Task.campaign_obj,Campaign.campaign_definition_obj)).filter(Task.status == "Completed").all():
            n_completed = n_completed + 1
            # if it's been 2 days, just declare it located; its as 
            # located as its going to get...
            if (now - task.updated > timedelta(days=2)):
                 n_located = n_located + 1
                 n_stale = n_stale + 1
                 task.status = "Located"
                 task.updated = datetime.now(utc)
                 cherrypy.request.db.add(task)
                 if not self.launch_recovery_if_needed(task.task_id):
                     self.launch_dependents_if_needed(task.task_id)
            elif task.project:
                 # task had a sam project, add to the list to look
                 # up in sam
                 n_project = n_project + 1
                 lookup_task_list.append(task)
                 basedims = "snapshot_for_project_name %s " % task.project
		 allkiddims = basedims
		 for pat in str(task.campaign_obj.campaign_definition_obj.output_file_patterns).split(','):
		     if pat == 'None':
			pat = '%'
		     allkiddims = "%s and isparentof: ( file_name '%s' and version '%s' ) " % (allkiddims, pat, task.campaign_obj.software_version)
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
                     task.updated = datetime.now(utc)
                     cherrypy.request.db.add(task)
                     if not self.launch_recovery_if_needed(task.task_id):
                         self.launch_dependents_if_needed(task.task_id)

        cherrypy.request.db.commit()
        
        summary_list = cherrypy.request.samweb_lite.fetch_info_list(lookup_task_list)
        count_list = cherrypy.request.samweb_lite.count_files_list(task.campaign_obj.experiment,lookup_dims_list)
        thresholds = []
        for i in range(len(summary_list)):
            # XXX
            # this is using a 90% threshold, this ought to be
            # a tunable in the campaign_definition.  Basically we consider it
            # located if 90% of the files it consumed have suitable kids...
            cfrac = lookup_task_list[i].campaign_obj.campaign_def_obj.cfrac
            threshold = (summary_list[i].get('tot_consumed',0) * cfrac)
            thresholds.append(threshold)
            if float(count_list[i]) >= threshold:
                n_located = n_located + 1
                task = lookup_task_list[i]
                task.status = "Located"
                task.updated = datetime.now(utc)
                cherrypy.request.db.add(task)

                if not self.launch_recovery_if_needed(task.task_id):
                    self.launch_dependents_if_needed(task.task_id)

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
        if (st['Completed'] > 0 and st['Idle'] == 0 and st['Held'] == 0):
            res = "Completed"
            # no, not here we wait for "Located" status..
            #if task.status != "Completed":
            #    if not self.launch_recovery_if_needed(task.task_id):
            #        self.launch_dependents_if_needed(task.task_id)
        return res

    @cherrypy.expose
    def update_job(self, task_id = None, jobsub_job_id = 'unknown',  **kwargs):
         cherrypy.log("update_job( task_id %s, jobsub_job_id %s,  kwargs %s )" % (task_id, jobsub_job_id, repr(kwargs)))

         if not self.can_report_data():
              cherrypy.log("update_job: not allowed")
              return "Not Allowed"

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
                     j.task_obj.campaign_obj.active = True

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
            cname = jl[0][1].task_obj.campaign_obj.name
        else:
            campaign_id = 'unknown'
            cname = 'unknown'

        task_jobsub_id = self.task_min_job(task_id)

        template = self.jinja_env.get_template('show_task_jobs.html')
        return template.render( blob=blob, job_counts = job_counts,  taskid = task_id, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'), extramap = extramap, do_refresh = 1, key = key, pomspath=self.path,help_page="ShowTaskJobsHelp", task_jobsub_id = task_jobsub_id, campaign_id = campaign_id,cname = cname)


    @cherrypy.expose
    def triage_job(self, job_id, tmin = None, tmax = None, tdays = None, force_reload = False):

        # we don't really use these for anything but we might want to
        # pass them into a template to set time ranges...
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin,tmax,tdays,'show_campaigns?')

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

        task_jobsub_job_id = self.task_min_job(job_info.Job.task_id)

        return template.render(job_id = job_id, job_file_list = job_file_list, job_info = job_info, job_history = job_history, downtimes=downtimes, output_file_names_list=output_file_names_list, es_response=es_response, tmin=tmin, current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path, help_page="TriageJobHelp",task_jobsub_job_id = task_jobsub_job_id)

    def handle_dates(self,tmin, tmax, tdays, baseurl):
        """
            tmin,tmax,tmins,tmaxs,nextlink,prevlink,tranges = self.handle_dates(tmax, tdays, name)
            assuming tmin, tmax, are date strings or None, and tdays is
            an integer width in days, come up with real datetimes for
            tmin, tmax, and string versions, and next ane previous links
            and a string describing the date range.  Use everywhere.
        """

        # set a flag to remind us to set tdays from max and min if
        # they are both set coming in.
        set_tdays =  (tmax != None and tmax != '') and (tmin != None and tmin!= '')

        if tmax == None or tmax == '':
            if tmin != None and tmin != '' and tdays != None and tdays != '':
                if isinstance(tmin, basestring):
                    tmin = datetime.strptime(tmin[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)
                tmax = tmin + timedelta(days=float(tdays))
            else:
                # if we're not given a max, pick now
                tmax = datetime.now(utc)
        elif isinstance(tmax, basestring):
            tmax = datetime.strptime(tmax[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)

        if tdays == None or tdays == '':  # default to one day
            tdays = 1

        tdays = float(tdays)

        if tmin == None or tmin == '':
            tmin = tmax - timedelta(days = tdays)

        elif isinstance(tmin, basestring):
            tmin = datetime.strptime(tmin[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)

        if set_tdays:
            # if we're given tmax and tmin, compute tdays
            tdays = (tmax - tmin).total_seconds() / 86400.0

        tsprev = tmin.strftime("%Y-%m-%d+%H:%M:%S")
        tsnext = (tmax + timedelta(days = tdays)).strftime("%Y-%m-%d+%H:%M:%S")
        tmaxs =  tmax.strftime("%Y-%m-%d %H:%M:%S")
        tmins =  tmin.strftime("%Y-%m-%d %H:%M:%S")
        prevlink="%s/%stmax=%s&tdays=%d" % (self.path,baseurl,tsprev, tdays)
        nextlink="%s/%stmax=%s&tdays=%d" % (self.path,baseurl,tsnext, tdays)
        # if we want to handle hours / weeks nicely, we should do
        # it here.
        plural =  's' if tdays > 1.0 else ''
        tranges = '%f day%s ending <span class="tmax">%s</span>' % (tdays, plural, tmaxs)

        # redundant, but trying to rule out tz woes here...
        tmin = tmin.replace(tzinfo = utc)
        tmax = tmax.replace(tzinfo = utc)


        return tmin,tmax,tmins,tmaxs,nextlink,prevlink,tranges

       
    @cherrypy.expose
    def show_campaigns(self,tmin = None, tmax = None, tdays = 1, active = True):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin,tmax,tdays,'show_campaigns?')

        cl = cherrypy.request.db.query(Campaign).filter(Campaign.active == active ).order_by(Campaign.experiment).all()

        counts = {}
        counts_keys = {}
        for c in cl:
            counts[c.campaign_id] = self.job_counts(tmax = tmax, tmin = tmin, tdays = tdays, campaign_id = c.campaign_id)
            counts_keys[c.campaign_id] = counts[c.campaign_id].keys()

        template = self.jinja_env.get_template('show_campaigns.html')
        return template.render( In= ("" if active == True else "In"), services=self.service_status_hier('All'), counts = counts, counts_keys = counts_keys, cl = cl, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 1, next = nextlink, prev = prevlink, days = tdays, time_range_string = time_range_string, key = '', pomspath=self.path, help_page="ShowCampaignsHelp")

    @cherrypy.expose
    def campaign_info(self, campaign_id ):

        Campaign_info = cherrypy.request.db.query(Campaign, Experimenter).filter(Campaign.campaign_id == campaign_id, Campaign.creator == Experimenter.experimenter_id).first()
        Campaign_definition_info =  cherrypy.request.db.query(CampaignDefinition, Experimenter).filter(CampaignDefinition.campaign_definition_id == Campaign_info.Campaign.campaign_definition_id, CampaignDefinition.creator == Experimenter.experimenter_id ).first()
        Launch_template_info = cherrypy.request.db.query(LaunchTemplate, Experimenter).filter(LaunchTemplate.launch_id == Campaign_info.Campaign.launch_id, LaunchTemplate.creator == Experimenter.experimenter_id).first()
        tags = cherrypy.request.db.query(Tag).filter(CampaignsTags.campaign_id==campaign_id, CampaignsTags.tag_id==Tag.tag_id).all()

        launched_campaigns = cherrypy.request.db.query(CampaignSnapshot).filter(CampaignSnapshot.campaign_id == campaign_id).all()

        # any launch outputs to look at?
        #
        dirname="%s/private/logs/poms/launches/campaign_%s" % (
           os.environ['HOME'],campaign_id)
        launch_flist = glob.glob('%s/*' % dirname)
        launch_flist = map(os.path.basename, launch_flist)

        template = self.jinja_env.get_template('campaign_info.html')
        return template.render(  Campaign_info = Campaign_info, Campaign_definition_info = Campaign_definition_info, Launch_template_info = Launch_template_info, tags=tags, launched_campaigns=launched_campaigns, launch_flist = launch_flist, current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 0, pomspath=self.path,help_page="CampaignInfoHelp")

    @cherrypy.expose
    def list_task_logged_files(self, task_id):
        t =  cherrypy.request.db.query(Task).filter(Task.task_id== task_id).first()
        jobsub_job_id = self.task_min_job(task_id)
        fl = cherrypy.request.db.query(JobFile).join(Job).filter(Job.task_id == task_id, JobFile.job_id == Job.job_id).all()
        template = self.jinja_env.get_template('list_task_logged_files.html')
        return template.render(fl = fl, campaign = t.campaign_obj,  jobsub_job_id = jobsub_job_id, current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 0, pomspath=self.path, help_page="ListTaskLoggedFilesHelp")

         
    @cherrypy.expose
    def campaign_task_files(self,campaign_id, tmin = None, tmax = None, tdays = 1):
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin,tmax,tdays,'campaign_task_files?campaign_id=%s&' % campaign_id)
 
        #
        # inhale all the campaign related task info for the time window
        # in one fell swoop
        #

	tl = (cherrypy.request.db.query(Task).
		options(joinedload(Task.campaign_obj)).
                options(joinedload(Task.jobs).joinedload(Job.job_files)).
                filter(Task.campaign_id == campaign_id, 
                       Task.created >= tmin, Task.created < tmax ).
                all())
 
        #
        # either get the campaign obj from above, or if we didn't
        # find any tasks in that window, look it up
        #
        if len(tl) > 0:
            c = tl[0].campaign_obj
        else:
            c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()


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
        finished_flying_needed = []
        for t in tl:
             summary_needed.append(t)
             basedims = "snapshot_for_project_name %s " % t.project
             base_dim_list.append(basedims)

             somekiddims = "%s and isparentof: (version %s)" % (basedims, t.campaign_obj.software_version)
             some_kids_needed.append(somekiddims)

             somekidsdecldims = "%s and isparentof: (version %s with availability anylocation )" % (basedims, t.campaign_obj.software_version)
             some_kids_decl_needed.append(somekidsdecldims)

             allkiddecldims = basedims
             allkiddims = basedims
             for pat in str(t.campaign_obj.campaign_definition_obj.output_file_patterns).split(','):
                 if pat == 'None':
                    pat = '%'
                 allkiddims = "%s and isparentof: ( file_name '%s' and version '%s' ) " % (allkiddims, pat, t.campaign_obj.software_version)
                 allkiddecldims = "%s and isparentof: ( file_name '%s' and version '%s' with availability anylocation ) " % (allkiddecldims, pat, t.campaign_obj.software_version)
             all_kids_needed.append(allkiddims)
             all_kids_decl_needed.append(allkiddecldims)

             logoutfiles = []
             for j in t.jobs:           
                 for f in j.job_files:
                     if f.file_type == "output":
                         logoutfiles.append(f.file_name)

        #
        # -- now call parallel fetches for items
        #
        summary_list = cherrypy.request.samweb_lite.fetch_info_list(summary_needed)
        some_kids_list = cherrypy.request.samweb_lite.count_files_list(c.experiment, some_kids_needed)
        some_kids_decl_list = cherrypy.request.samweb_lite.count_files_list(c.experiment, some_kids_decl_needed)
        all_kids_decl_list = cherrypy.request.samweb_lite.count_files_list( c.experiment, all_kids_decl_needed)
        all_kids_list = cherrypy.request.samweb_lite.count_files_list(c.experiment, all_kids_needed)

        columns=["jobsub_jobid", "project", "date", "submit-<br>ted",
                 "deliv-<br>ered<br>SAM",
                 "deliv-<br>ered<br> logs",
                 "con-<br>sumed","failed", "skipped",
                 "w/some kids<br>declared",
                 "w/all kids<br>declared",
                 "kids in<br>flight",
                 "w/kids<br>located",
                 "pending"]

        listfiles = "show_dimension_files?experiment=%s&dims=%%s" % c.experiment
        datarows = []
        i = -1
        for t in tl:
             cherrypy.log("task %d" % t.task_id)
             i = i + 1
             psummary = summary_list[i]
             partpending = psummary.get('files_in_snapshot', 0) - some_kids_list[i]
             #pending = psummary.get('files_in_snapshot', 0) - all_kids_list[i]
             pending = partpending
             logdelivered = 0
             logwritten = 0
             logkids = 0
             for j in t.jobs:
                 for f in j.job_files:
                     if f.file_type == "input":
                         logdelivered = logdelivered + 1
                     if f.file_type == "output":
                         logkids = logkids + 1
             task_jobsub_job_id = self.task_min_job(t.task_id)
             if task_jobsub_job_id == None:
                 task_jobsub_job_id = "t%s" % t.task_id
             datarows.append([
                           [task_jobsub_job_id.replace('@','@<br>'), "show_task_jobs?task_id=%d" % t.task_id],
                           [t.project,"http://samweb.fnal.gov:8480/station_monitor/%s/stations/%s/projects/%s" % (c.experiment, c.experiment, t.project)],
                           [t.created.strftime("%Y-%m-%d %H:%M"),None], 
                           [psummary.get('files_in_snapshot',0), listfiles % base_dim_list[i]],
                           ["%d" % (psummary.get('tot_consumed',0) + psummary.get('tot_failed',0) + psummary.get('tot_skipped',0)), listfiles % base_dim_list[i] + " and consumed_status consumed,failed,skipped "],
                           ["%d" % logdelivered, "./list_task_logged_files?task_id=%s" % t.task_id ],
                           [psummary.get('tot_consumed',0), listfiles % base_dim_list[i] + " and consumed_status consumed"],
                           [ psummary.get('tot_failed',0),  listfiles % base_dim_list[i] + " and consumed_status failed"],
                           [ psummary.get('tot_skipped',0),  listfiles % base_dim_list[i] + " and consumed_status skipped"],
                           [some_kids_decl_list[i], listfiles % some_kids_needed[i] ],
                           [all_kids_decl_list[i], listfiles % some_kids_decl_needed[i]],
                           [len(self.get_inflight(task_id=t.task_id)), "./inflight_files?task_id=%d" % t.task_id],
                           [all_kids_decl_list[i], listfiles % all_kids_decl_needed[i]],
                           [pending, listfiles % base_dim_list[i] + "minus ( %s ) " % all_kids_decl_needed[i]],
                ])
        template = self.jinja_env.get_template('campaign_task_files.html')
        return template.render(name = c.name if c else "", columns = columns, datarows = datarows, tmin=tmins, tmax=tmaxs,  prev=prevlink, next=nextlink, days=tdays, current_experimenter=cherrypy.session.get('experimenter'), campaign_id = campaign_id, pomspath=self.path,help_page="CampaignTaskFilesHelp")
                           

    @cherrypy.expose
    def campaign_time_bars(self, campaign_id, tmin = None, tmax = None, tdays = 1):
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax,tdays,'campaign_time_bars?campaign_id=%s&'% campaign_id)

        tg = time_grid.time_grid()

        key = tg.key()

        class fakerow:
            def __init__(self, **kwargs):
                self.__dict__.update(kwargs)

        sl = []
        # sl.append(self.format_job_counts())

        cp = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        name = cp.name

        template = self.jinja_env.get_template('campaign_time_bars.html')

        job_counts = self.format_job_counts(campaign_id = cp.campaign_id, tmin = tmin, tmax = tmax, tdays = tdays, range_string = time_range_string)

        qr = cherrypy.request.db.query(TaskHistory).join(Task).filter(Task.campaign_id == campaign_id, TaskHistory.task_id == Task.task_id , or_(and_(Task.created > tmin, Task.created < tmax),and_(Task.updated > tmin, Task.updated < tmax)) ).order_by(TaskHistory.task_id,TaskHistory.created).all()
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
                                  tmax = th.task_obj.updated + timedelta(hours=6.5),
                                  status = th.status,
                                  jobsub_job_id = jjid))

        blob = tg.render_query_blob(tmin, tmax, items, 'jobsub_job_id', url_template = self.path + '/show_task_jobs?task_id=%(task_id)s&tmin=%(tmin)19.19s&tmax=%(tmax)19.19s',extramap = extramap )

        return template.render( job_counts = job_counts, blob = blob, name = name, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 1, next = nextlink, prev = prevlink, days = tdays, key = key, pomspath=self.path, extramap = extramap, help_page="CampaignTimeBarsHelp")

    def task_min_job(self, task_id):
        # find the job with the logs -- minimum jobsub_job_id for this task
        # also will be nickname for the task...
        if ( self.task_min_job_cache.has_key(task_id) ):
           return self.task_min_job_cache.get(task_id)
        j = cherrypy.request.db.query(Job).filter( Job.task_id == task_id ).order_by(Job.jobsub_job_id).first()
        if j:
            self.task_min_job_cache[task_id] = j.jobsub_job_id
            return j.jobsub_job_id
        else:
            return None

    @cherrypy.expose
    def job_file_list(self, job_id,force_reload = False):
        j = cherrypy.request.db.query(Job).options(joinedload(Job.task_obj).joinedload(Task.campaign_obj)).filter(Job.job_id == job_id).first()
        # find the job with the logs -- minimum jobsub_job_id for this task
        jobsub_job_id = self.task_min_job(j.task_id)
        role = j.task_obj.campaign_obj.vo_role
        return cherrypy.request.jobsub_fetcher.index(jobsub_job_id,j.task_obj.campaign_obj.experiment ,role, force_reload)

    @cherrypy.expose
    def job_file_contents(self, job_id, task_id, file, tmin = None, tmax = None, tdays = None):

        # we don't really use these for anything but we might want to
        # pass them into a template to set time ranges...
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin,tmax,tdays,'show_campaigns?')

        j = cherrypy.request.db.query(Job).options(subqueryload(Job.task_obj).subqueryload(Task.campaign_obj)).filter(Job.job_id == job_id).first()
        # find the job with the logs -- minimum jobsub_job_id for this task
        jobsub_job_id = self.task_min_job(j.task_id)
        cherrypy.log("found job: %s " % jobsub_job_id)
        role = j.task_obj.campaign_obj.vo_role
        job_file_contents = cherrypy.request.jobsub_fetcher.contents(file, j.jobsub_job_id,j.task_obj.campaign_obj.experiment,role)
        template = self.jinja_env.get_template('job_file_contents.html')
        return template.render(file=file, job_file_contents=job_file_contents, task_id=task_id, job_id=job_id, tmin=tmin, pomspath=self.path,help_page="JobFileContentsHelp")

    @cherrypy.expose
    def test_job_counts(self, task_id = None, campaign_id = None):
        res = self.job_counts(task_id, campaign_id)
        return repr(res) + self.format_job_counts(task_id, campaign_id)

    def format_job_counts(self, task_id = None, campaign_id = None, tmin = None, tmax = None, tdays = 7, range_string = None):
        counts = self.job_counts(task_id, campaign_id, tmin, tmax, tdays)
        ck = counts.keys()
        res = [ '<div><b>Job States</b><br>',
                '<table class="ui celled table unstackable">',
                '<tr><th colspan=3>Active</th><th colspan=2>In %s</th></tr>' % range_string,
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

        out = OrderedDict([("Idle",0),( "Running",0),( "Held",0),( "Completed",0), ("Located",0),("Removed",0)])
        for row in  q.all():
            # this rather bizzare hoseyness is because we want
            # "Running" to also match "running: copying files in", etc.
            # so we ignore the first character and do a match
            if row[1][1:7] == "unning":
                short = "Running"
            else:
                short = row[1]
            out[short] = out.get(short,0) + int(row[0])

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
        #
        if eff_d:
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
        else:
            filtered_fields_checkboxes = {"campaign_checkbox": "checked", "task_checkbox": "checked", "job_checkbox": "checked"}  #setting this for initial page visit
            filtered_fields.update(filtered_fields_checkboxes)

        hidecolumns = [ 'task_id', 'campaign_id', 'created', 'creator', 'updated', 'updater', 'command_executed', 'task_parameters', 'depends_on', 'depend_threshold', 'task_order']

        template = self.jinja_env.get_template('job_table.html')
        return template.render(joblist=jl, jobcolumns = jobcolumns, taskcolumns = taskcolumns, campcolumns = campcolumns, current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 0,  tmin=tmins, tmax =tmaxs,  prev= prevlink,  next = nextlink, days = tdays, extra = extra, hidecolumns = hidecolumns, filtered_fields=filtered_fields, time_range_string = time_range_string, pomspath=self.path,help_page="JobTableHelp")

    @cherrypy.expose
    def jobs_by_exitcode(self, tmin = None, tmax =  None, tdays = 1 ):
        raise cherrypy.HTTPRedirect("%s/failed_jobs_by_whatever?f=user_exe_exit_code&tdays=%s" % (self.path, tdays))

    @cherrypy.expose
    def failed_jobs_by_whatever(self, tmin = None, tmax =  None, tdays = 1 , f = [], go = None):
        # deal with single/multiple argument silliness
        if isinstance(f, basestring):
            f = [f]

        if not 'experiment' in f:
            f.append('experiment')

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax,tdays,'failed_jobs_by_whatever?%s&' % ('&'.join(['f=%s'%x for x in f] )))

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
            if f == None:
                continue
            columns.append(field)
            if hasattr(Job,field):
               gbl.append(getattr(Job, field))
               qargs.append(getattr(Job, field))
            elif hasattr(Campaign,field):
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
        q = cherrypy.request.db.query(*qargs)
        q = q.join(Task,Campaign)
        q = q.filter(Job.updated >= tmin, Job.updated <= tmax, Job.user_exe_exit_code != 0)
        q = q.group_by(*gbl).order_by(desc(func.count(Job.job_id)))

        jl = q.all()
        if jl:
            cherrypy.log( "got jobtable %s " % repr( jl[0].__dict__) )

        template = self.jinja_env.get_template('failed_jobs_by_whatever.html')

        return template.render(joblist=jl, possible_columns = possible_columns, columns = columns, current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 0,  tmin=tmins, tmax =tmaxs,  tdays=tdays, prev= prevlink,  next = nextlink, time_range_string = time_range_string, days = tdays, pomspath=self.path,help_page="JobsByExitcodeHelp")

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

        cherrypy.request.db.add(t)
        cherrypy.request.db.commit()
        self.snapshot_parts(t.campaign_id)
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

         ld = cherrypy.request.db.query(LaunchTemplate).filter(LaunchTemplate.name.like("%generic%")).first()

         if cd == None:
              # pick *any* generic one...
              cd = cherrypy.request.db.query(CampaignDefinition).filter(Campaign.name.like("%generic%")).first()
          
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
    def quick_search(self, search_term):
        search_term = search_term.strip()
        job_info = cherrypy.request.db.query(Job).filter(Job.jobsub_job_id == search_term).first()
        if job_info:
            tmins =  datetime.now(utc).strftime("%Y-%m-%d+%H:%M:%S")
            raise cherrypy.HTTPRedirect("%s/triage_job?job_id=%s&tmin=%s" % (self.path,str(job_info.job_id),tmins))
        else:
            search_term = search_term.replace("+", " ")
            query = urllib.urlencode({'q' : search_term})
            raise cherrypy.HTTPRedirect("%s/search_tags?%s" % (self.path, query))

    @cherrypy.expose
    def json_project_summary_for_task(self, task_id):
        cherrypy.response.headers['Content-Type'] = "application/json"
        return json.dumps(self.project_summary_for_task(task_id))

    def project_summary_for_task(self, task_id):
        t = cherrypy.request.db.query(Task).filter(Task.task_id == task_id).first()
        return cherrypy.request.samweb_lite.fetch_info(t.campaign_obj.experiment, t.project)

    def project_summary_for_tasks(self, task_list):
        return cherrypy.request.samweb_lite.fetch_info_list(task_list)
        #~ return [ {"tot_consumed": 0, "tot_failed": 0, "tot_jobs": 0, "tot_jobfails": 0} ] * len(task_list)    #VP Debug


    def get_inflight(self, campaign_id=None, task_id=None):
        q = cherrypy.request.db.query(JobFile).join(Job).join(Task).join(Campaign)
        q = q.filter(Task.campaign_id == Campaign.campaign_id)
        q = q.filter(Task.task_id == Job.task_id)
        q = q.filter(Job.job_id == JobFile.job_id)
        q = q.filter(JobFile.file_type == 'output' )
        q = q.filter(JobFile.declared == None )
        if campaign_id != None:
            q = q.filter(Task.campaign_id == campaign_id)
        if task_id != None:
            q = q.filter(Job.task_id == task_id)
        q = q.filter(Job.output_files_declared == False)
        outlist = []
        jjid = "xxxxx"
        for jf in q.all():
            outlist.append(jf.file_name)

        return outlist

    @cherrypy.expose
    def show_dimension_files(self, experiment, dims):

        flist = cherrypy.request.samweb_lite.list_files(experiment, dims)

        template = self.jinja_env.get_template('show_dimension_files.html')
        return template.render(flist = flist, dims = dims,  current_experimenter=cherrypy.session.get('experimenter'),  statusmap = [], pomspath=self.path,help_page="ShowDimensionFilesHelp")

    @cherrypy.expose
    def inflight_files(self, campaign_id=None, task_id=None):
        if campaign_id:
            c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        elif task_id:
            c = cherrypy.request.db.query(Campaign).join(Task).filter(Campaign.campaign_id == Task.campaign_id, Task.task_id == task_id).first()
        else: 
            cherrypy.response.status="404 Permission Denied."
            return "Neither Campaign nor Task found"
        outlist = self.get_inflight(campaign_id=campaign_id, task_id= task_id)
        statusmap = {}
        if c:
	    fss_file = "%s/%s_files.db" % (cherrypy.config.get("ftsscandir"), c.experiment)
	    if os.path.exists(fss_file):
		fss = shelve.open(fss_file, 'r')
		for f in outlist:
		    try:
			statusmap[f] = fss.get(f.encode('ascii','ignore'),'')
		    except KeyError:
			statusmap[f] = ''
		fss.close()

        template = self.jinja_env.get_template('inflight_files.html')

        return template.render(flist = outlist,  current_experimenter=cherrypy.session.get('experimenter'),  statusmap = statusmap, c = c, jjid= self.task_min_job(task_id),campaign_id = campaign_id, task_id = task_id, pomspath=self.path,help_page="PendingFilesJobsHelp")


    @cherrypy.expose
    def campaign_sheet(self, campaign_id, tmin = None, tmax = None , tdays = 7):

        daynames = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday", "Sunday"]

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax, tdays, 'campaign_sheet?campaign_id=%s&' % campaign_id)

        tl = (cherrypy.request.db.query(Task)
                .filter(Task.campaign_id==campaign_id, Task.created > tmin, Task.created < tmax)
                .order_by(desc(Task.created))
                .options(joinedload(Task.jobs))
                .all())
        psl = self.project_summary_for_tasks(tl)        # Get project summary list for a given task list in one query

        el = cherrypy.request.db.query(distinct(Job.user_exe_exit_code)).filter(Job.updated >= tmin, Job.updated <= tmax).all()
        
	experiment, = cherrypy.request.db.query(Campaign.experiment).filter(Campaign.campaign_id == campaign_id).one()

	exitcodes = []
        for e in el:
            exitcodes.append(e[0])

        cherrypy.log("got exitcodes: " + repr(exitcodes))

        day = -1
        date = None
        first = 1
        columns = ['day','date','requested files','delivered files','jobs','failed','outfiles','pending']
        exitcodes.sort()
        for e in exitcodes:
            if e != None:
                columns.append('exit(%d)'%(e))
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
        pendfiles = 0
        for e in exitcodes:
            exitcounts[e] = 0

        for tno, task in enumerate(tl):
            if day != task.created.weekday():
                if not first:
                     # add a row to the table on the day boundary
                     outrow = []
                     outrow.append(daynames[day])
                     outrow.append(date.isoformat()[:10])
                     outrow.append(str(totfiles if totfiles > 0 else infiles))
                     outrow.append(str(totdfiles))
                     outrow.append(str(totjobs))
                     outrow.append(str(totjobfails))
                     outrow.append(str(outfiles))
                     outrow.append(str(pendfiles))
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
                pendfiles = 0
                for e in exitcodes:
                    exitcounts[e] = 0
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

            for job in task.jobs:
                # BZZT -- wrong.  consider all jobs from this submission/task
                # dont consider jobs outside of our window even if the task is
                #if job.updated < tmin or job.updated > tmax:
                #    continue

                exitcounts[job.user_exe_exit_code] = exitcounts.get(job.user_exe_exit_code, 0) + 1
                if job.job_files:
                    nout = len(job.job_files)
                    outfiles += nout
                    if not job.output_files_declared:
                        # a bit of a lie, we don't know they're *all* pending, just some of them
                        # but its close, and we don't want to re-poll SAM here..
                        pendfiles += nout

                if job.job_files:
                    nin = len([x for x in job.job_files if x.file_type == "input"])
                    infiles += nin
        # end 'for'
        # we *should* add another row here for the last set of totals, but
        # initially we just added a day to the query range, so we compute a row of totals we don't use..
        # --- but that doesn't work on new projects...
        # add a row to the table on the day boundary
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
        outrow.append(str(pendfiles))
        for e in exitcodes:
            outrow.append(exitcounts[e])
        outrows.append(outrow)

        template = self.jinja_env.get_template('campaign_sheet.html')
        if tl and tl[0]:
            name = tl[0].campaign_obj.name
        else:
            name = ''
        return template.render(name=name,
                                columns=columns,
                                datarows=outrows,
                                tmaxs=tmaxs,
                                prev=prevlink,
                                next=nextlink,
                                days=tdays,
                                tmin = str(tmin)[:16],
                                tmax = str(tmax)[:16],
                                current_experimenter=cherrypy.session.get('experimenter'),
                                campaign_id=campaign_id,
                                experiment=experiment,
				pomspath=self.path,help_page="CampaignSheetHelp")


    @cherrypy.expose
    def kill_jobs(self, campaign_id=None, task_id=None, job_id=None, confirm=None):
        jjil = []
        jql = None
        if campaign_id != None or task_id != None:
            if campaign_id != None:
                tl = cherrypy.request.db.query(Task).filter(Task.campaign_id == campaign_id, Task.status != 'Completed', Task.status != 'Located').all()
            else:
                tl = cherrypy.request.db.query(Task).filter(Task.task_id == task_id).all()
            c = tl[0].campaign_obj
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
            c = jql[0].task_obj.campaign_obj
            for j in jql:
                jjil.append(j.jobsub_job_id)

        if confirm == None:
            template = self.jinja_env.get_template('kill_jobs_confirm.html')
            return template.render(current_experimenter=cherrypy.session.get('experimenter'), jjil = jjil, task = t, campaign_id = campaign_id, task_id = task_id, job_id = job_id, pomspath=self.path,help_page="KilledJobsHelp")
        else:
            group = c.experiment
            if group == 'samdev': group = 'fermilab'

            f = os.popen("jobsub_rm -G %s --role %s --jobid %s 2>&1" % (group, c.vo_role, ','.join(jjil)), "r")
            output = f.read()
            f.close()

            template = self.jinja_env.get_template('kill_jobs.html')
            return template.render(output = output, current_experimenter=cherrypy.session.get('experimenter'), c = c, campaign_id = campaign_id, task_id = task_id, job_id = job_id, pomspath=self.path,help_page="KilledJobsHelp")

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
            cherrypy.request.samweb_lite.create_definition(camp.campaign_definition_obj.experiment, new,  "defname: %s with stride %d skip %d" % (camp.dataset, m, camp.cs_last_split))

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

    def launch_dependents_if_needed(self, task_id):
        cherrypy.log("Entering launch_dependents_if_needed(%s)" % task_id)
	if not cherrypy.config.get("poms.launch_recovery_jobs",False):
            # XXX should queue for later?!?
            return 1
        t = cherrypy.request.db.query(Task).options(joinedload(Task.campaign_obj).joinedload(Campaign.campaign_definition_obj)).filter(Task.task_id == task_id).first()
        cdlist = cherrypy.request.db.query(CampaignDependency).filter(CampaignDependency.needs_camp_id == t.campaign_obj.campaign_id).all()

        i = 0
        for cd in cdlist:
           if cd.uses_camp_id == t.campaign_obj.campaign_id:
              # self-reference, just do a normal launch
              self.launch_jobs(cd.uses_camp_id)
           else:
              i = i + 1
              dims = "ischildof: (snapshot_for_project %s and version %s and file_name like '%s'" % (t.project, t.campaign_obj.software_version, cd.file_patterns)
              dname = "poms_depends_%d_%d" % (t.task_id,i)
 
              cherrypy.request.samweb_lite.create_definition(t.campaign_obj.experiment, dname, dims)
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

    def launch_recovery_if_needed(self, task_id):
        cherrypy.log("Entering launch_recovery_if_needed(%s)" % task_id)
	if not cherrypy.config.get("poms.launch_recovery_jobs",False):
            # XXX should queue for later?!?
            return 1

        t = cherrypy.request.db.query(Task).options(joinedload(Task.campaign_obj).joinedload(Campaign.campaign_definition_obj)).filter(Task.task_id == task_id).first()
        # if this is itself a recovery job, we go back to our parent
        # to do all the work, because it has the counters, etc.
        if t.parent_obj:
           t = t.parent_obj

        rlist = self.get_recovery_list_for_campaign_def(t.campaign_obj.campaign_definition_obj)

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
                 if t.campaign_obj.campaign_definition_obj.output_file_types:
                     oftypelist = campaign_obj.campaign_definition_obj.output_file_types.split(",") 
                 else:
                     oftypelist = ["%"]

                 for oft in oftypelist:
                     recovery_dims = recovery_dims + "minus isparent: ( version %s and file_name like %s) " % (t.campaign_obj.software_version, oft)
            else:
                 # default to consumed status(?)
                 recovery_dims = "snapshot_for_project_name %s and consumed_status != 'consumed'" % t.project

            nfiles = cherrypy.request.samweb_lite.count_files(t.campaign_obj.experiment,recovery_dims)

	    t.recovery_position = t.recovery_position + 1
            cherrypy.request.db.add(t)
            cherrypy.request.db.commit()
 
            if nfiles > 0:
                rname = "poms_recover_%d_%d" % (t.task_id,t.recovery_position)

                cherrypy.log("launch_recovery_if_needed: creating dataset for exp=%s name=%s dims=%s" % (t.campaign_obj.experiment, rname, recovery_dims))

                cherrypy.request.samweb_lite.create_definition(t.campaign_obj.experiment, rname, recovery_dims)

            
                self.launch_jobs(t.campaign_obj.campaign_id, dataset_override=rname, parent_task_id = t.task_id)
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
            "setup poms_jobsub_wrapper v0_5 -z /grid/fermiapp/products/common/db",
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
        res = template.render(command = lcmd, output = output, current_experimenter=cherrypy.session.get('experimenter'), c = c, campaign_id = campaign_id,  pomspath=self.path,help_page="LaunchedJobsHelp")
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


    @cherrypy.expose
    def link_tags(self, campaign_id, tag_name, experiment):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        response = {}

        if cherrypy.session.get('experimenter').is_authorized(experiment):

            tag = cherrypy.request.db.query(Tag).filter(Tag.tag_name == tag_name, Tag.experiment == experiment).first()

            if tag:  #we have a tag in the db for this experiment so go ahead and do the linking
                try:
                    ct = CampaignsTags()
                    ct.campaign_id = campaign_id
                    ct.tag_id = tag.tag_id
                    cherrypy.request.db.add(ct)
                    cherrypy.request.db.commit()
                    response = {"campaign_id": ct.campaign_id, "tag_id": ct.tag_id, "tag_name": tag.tag_name, "msg": "OK"}
                    return json.dumps(response)
                except exc.IntegrityError:
                    response = {"msg": "This tag already exists."}
                    return json.dumps(response)
            else:  #we do not have a tag in the db for this experiment so create the tag and then do the linking
                try:
                    t = Tag()
                    t.tag_name = tag_name
                    t.experiment = experiment
                    cherrypy.request.db.add(t)
                    cherrypy.request.db.commit()

                    ct = CampaignsTags()
                    ct.campaign_id = campaign_id
                    ct.tag_id = t.tag_id
                    cherrypy.request.db.add(ct)
                    cherrypy.request.db.commit()
                    response = {"campaign_id": ct.campaign_id, "tag_id": ct.tag_id, "tag_name": t.tag_name, "msg": "OK"}
                    return json.dumps(response)
                except exc.IntegrityError:
                    response = {"msg": "This tag already exists."}
                    return json.dumps(response)
        else:
            response = {"msg": "You are not authorized to add tags."}
            return json.dumps(response)




    @cherrypy.expose
    def delete_campaigns_tags(self, campaign_id, tag_id, experiment):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        if cherrypy.session.get('experimenter').is_authorized(experiment):
            cherrypy.request.db.query(CampaignsTags).filter(CampaignsTags.campaign_id == campaign_id, CampaignsTags.tag_id == tag_id).delete()
            cherrypy.request.db.commit()
            response = {"msg": "OK"}
        else:
            response = {"msg": "You are not authorized to delete tags."}
        return json.dumps(response)




    @cherrypy.expose
    def search_tags(self, q):

        q_list = q.split(" ")

        query = cherrypy.request.db.query(Campaign).filter(CampaignsTags.tag_id == Tag.tag_id, Tag.tag_name.in_(q_list), Campaign.campaign_id == CampaignsTags.campaign_id).group_by(Campaign.campaign_id).having(func.count(Campaign.campaign_id) == len(q_list))
        results = query.all()

        template = self.jinja_env.get_template('search_tags.html')

        return template.render(results=results, q_list=q_list, current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 0,  pomspath=self.path, help_page="SearchTagsHelp")







    @cherrypy.expose
    def auto_complete_tags_search(self, experiment, q):
        cherrypy.response.headers['Content-Type'] = 'application/json'
        response = {}
        results = []
        rows = cherrypy.request.db.query(Tag).filter(Tag.tag_name.like('%'+q+'%'), Tag.experiment == experiment).order_by(desc(Tag.tag_name)).all()
        for row in rows:
            results.append({"tag_name": row.tag_name})

        response["results"] = results
        return json.dumps(response)











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
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax,tdays,'jobs_eff_histo?campaign_id=%s&' % campaign_id)

        q = cherrypy.request.db.query(func.count(Job.job_id), func.floor(Job.cpu_time *10/Job.wall_time))
        q = q.join(Job.task_obj)
        q = q.filter(Job.task_id == Task.task_id, Task.campaign_id == campaign_id)
        q = q.filter(Job.wall_time > 0, Job.wall_time > Job.cpu_time)
        q = q.filter(Job.updated <= tmax, Job.updated >= tmin)
        q = q.group_by(func.floor(Job.cpu_time*10/Job.wall_time))
        q = q.order_by((func.floor(Job.cpu_time*10/Job.wall_time)))

        total = 0
        vals = {}
        for row in q.all():
            vals[row[1]] = row[0]
            total += row[0]

        c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
        # return "total %d ; vals %s" % (total, vals)
        # return "Not yet implemented"

        template = self.jinja_env.get_template('jobs_eff_histo.html')
        return template.render(  c = c, total = total, vals = vals, tmaxs = tmaxs, campaign_id=campaign_id, tdays = tdays, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 1, next = nextlink, prev = prevlink, days = tdays, pomspath=self.path, help_page="JobEfficiencyHistoHelp")

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
        return template.render(  c = c, campaign_id = campaign_id, job = job, current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 0,  pomspath=self.path, help_page="ScheduleLaunchHelp", launch_flist= launch_flist)

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

    def snapshot_parts(self, campaign_id):
         c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
         for table, snaptable, field, sfield, tid in [
		[Campaign,CampaignSnapshot,Campaign.campaign_id,CampaignSnapshot.campaign_id,c.campaign_id], 
		[CampaignDefinition, CampaignDefinitionSnapshot,CampaignDefinition.campaign_definition_id, CampaignDefinitionSnapshot.campaign_definition_id, c.campaign_definition_id], 
                [LaunchTemplate ,LaunchTemplateSnapshot,LaunchTemplate.launch_id,LaunchTemplateSnapshot.launch_id,  c.launch_id]]:
              
             i = cherrypy.request.db.query(func.max(snaptable.updated)).filter(sfield == tid).first()
             j = cherrypy.request.db.query(table).filter(field == tid).first()
             if (i[0] == None or j == None or j.updated == None or  i[0] < j.updated):
                newsnap = snaptable()
                columns = j._sa_instance_state.class_.__table__.columns
                for fieldname in columns.keys():
                     setattr(newsnap, fieldname, getattr(j,fieldname))     
                cherrypy.request.db.add(newsnap)
             cherrypy.request.db.commit()

    @cherrypy.expose
    def make_stale_campaigns_inactive(self):
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

    @cherrypy.expose
    def actual_pending_files(self, count_or_list, task_id = None, campaign_id = None, tmin = None, tmax= None, tdays = 1):
        cherrypy.response.timeout = 600
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax,tdays,'actual_pending_files?count_or_list=%s&%s=%s&' % (count_or_list,'campaign_id',campaign_id) if campaign_id else (count_or_list,'task_id',task_id))

	tl = (cherrypy.request.db.query(Task).
		options(joinedload(Task.campaign_obj)).
                options(joinedload(Task.jobs).joinedload(Job.job_files)).
                filter(Task.campaign_id == campaign_id, 
                       Task.created >= tmin, Task.created < tmax ).
                all())

        c = None
        plist = []
        for t in tl:
            if not c:
                c = t.campaign_obj
            plist.append(t.project if t.project else 'None')
 
        if c:
            dims = "snapshot_for_project_name %s minus (" %  ','.join(plist)
            sep = ""
            for pat in str(c.campaign_definition_obj.output_file_patterns).split(','):
                if pat == "None":
                   pat = "%"
                dims = "%s %s isparentof: ( file_name '%s' and version '%s' ) " % (dims, sep, pat, t.campaign_obj.software_version)
                sep = "and"
                cherrypy.log("dims now: %s" % dims)
            dims = dims + ")"
        else:
            c = cherrypy.request.db.query(Campaign).filter(Campaign.campaign_id == campaign_id ).first()
            dims = None

        if None == dims or 'None' == dims:
            return "Ouch"

        cherrypy.log("actual pending files: got dims %s" % dims)

        return self.show_dimension_files(c.experiment, dims)



