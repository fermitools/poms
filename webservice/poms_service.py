import cherrypy
import os
import time_grid
import json
import urllib
from collections import OrderedDict

from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey, and_, or_, create_engine, null, desc, text, func, exc, distinct
from sqlalchemy.orm  import subqueryload, contains_eager
from sqlalchemy.orm.exc import NoResultFound
from datetime import datetime, tzinfo,timedelta
from jinja2 import Environment, PackageLoader
from model.poms_model import Service, ServiceDowntime, Experimenter, Experiment, Job, JobHistory, Task, TaskDefinition, TaskHistory, Campaign

ZERO = timedelta(0)

class UTC(tzinfo):
    """UTC"""

    def utcoffset(self, dt):
        return ZERO

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return ZERO

utc = UTC()

def error_response():
# We should be able to make a better page for errors with this handle
# see sams exc.error_response
#    typ, value, trace = sys.exc_info()
#    for typ, handler in default_error_map.iteritems():
#        if isinstance(value, typ):
#            do stuff 
    dump = cherrypy._cperror.format_exc()
    message = '<html><body><b><hl>POMS -- Krash!</h1></b><br><br>Make some nice page for this. <br><br>'
    message = "%s%s" % (message, '<br>%s<br><pre>%s</pre></body></html>' % (cherrypy.url(), dump.replace('\n','<br/>') ))
    cherrypy.response.status = 500
    cherrypy.response.headers['content-type'] = 'text/html'
    cherrypy.response.body = [message]
    cherrypy.log(dump)

class poms_service:
    
    _cp_config = {'request.error_response': error_response,
                  'error_page.404': "%s/%s" % (os.path.abspath(os.getcwd()),'/templates/page_not_found.html')
                  }

    def __init__(self):
        self.jinja_env = Environment(loader=PackageLoader('webservice','templates'))
        self.make_admin_map()
        self.task_min_job_cache = {}
        self.path = cherrypy.config.get("pomspath","/poms")


    @cherrypy.expose
    def headers(self):
        return repr(cherrypy.request.headers)

    @cherrypy.expose
    def index(self):
        template = self.jinja_env.get_template('service_statuses.html')
        return template.render(services=self.service_status_hier('All'),current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 1, pomspath=self.path)

    @cherrypy.expose
    def test(self):
        cherrypy.response.headers['content-type'] = 'text/plain'
        html = """<html>
          <head></head>
          <body>
          session._id:       %s<br>
          session.get('id'): %s<br>
          session.get('experimenter'): %s<br>
          </body>
        </html>""" % (cherrypy.session._id,cherrypy.session.get('id'),cherrypy.session.get('experimenter'))
        return html

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
        if ra == '127.0.0.1' and xff == None:
             # case for local agents
             return 1
        if (cherrypy.session.get('experimenter')).is_root():
             # special admins
             return 1
        return 0

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
                color = "#92D3F3"
            elif row.Service.name.lower().find("fts") != -1:
                color = "#92D3F3"
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
        return template.render(rows=rows,current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path)



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
        return template.render(rows=rows,current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path)


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
            s.parent = p
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

        s.parent_service = p
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
        return template.render(list=list, name=under,current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path)

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
        return template.render(list = self.admin_map.keys(),current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path)
        
    @cherrypy.expose
    def experiment_edit(self, message=None):
        experiments = cherrypy.request.db.query(Experiment).order_by(Experiment.experiment)
        template = self.jinja_env.get_template('experiment_edit.html')
        return template.render(message=message, experiments=experiments, current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path)
        
    @cherrypy.expose
    def experiment_authorize(self, *args, **kwargs):
        if not self.can_db_admin():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')

        message = None
        # Add new experiment, if any
        try:
            experiment = kwargs.pop('experiment')
            name = kwargs.pop('name')
            try:
                cherrypy.request.db.query(Experiment).filter(Experiment.experiment==experiment).one()
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
                cherrypy.request.db.query(Experiment).filter(Experiment.experiment==experiment).delete()
                pass
            cherrypy.request.db.commit()
        except:
            cherrypy.request.db.rollback()
            message = "The experiment, %s, is used and may not be deleted." % experiment

        return self.experiment_edit(message)

    @cherrypy.expose
    def list_generic(self, classname):
        if not self.can_db_admin():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        l = self.make_list_for(self.admin_map[classname],self.pk_map[classname])
        template = self.jinja_env.get_template('list_screen.html')
        return template.render( classname = classname, list = l, edit_screen="edit_screen_generic", primary_key='experimenter_id',current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path)

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
        template = self.jinja_env.get_template('edit_screen.html')
        return template.render( screendata = screendata, action="./"+update_call , classname = classname ,current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path)

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
         #t.task_definition_id = td.task_definition_id
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
         for job in cherrypy.request.db.query(Job).filter(Job.status != "Completed").all():
              if job.jobsub_job_id == "unknown":
                   continue
              res.append( '%s "%s"' % (sep, job.jobsub_job_id))
              sep = ","
         res.append( "]" )
         return "".join(res)

    @cherrypy.expose
    def output_pending_jobs(self):
         cherrypy.response.headers['Content-Type']= 'application/json'
         res = [ "{" ]
         sep=""
         for job in cherrypy.request.db.query(Job).filter(Job.status == "Completed", Job.output_file_names != "").all():
              if job.jobsub_job_id == "unknown":
                   continue
              res.append( '%s "%s" : {"output_file_names":"%s", "experiment":"%s"}' % (sep, job.jobsub_job_id, job.output_file_names, job.task_obj.campaign_obj.experiment))
              sep = ","
         res.append( "}" )
         return "".join(res)

    @cherrypy.expose
    def wrapup_tasks(self):
        cherrypy.response.headers['Content-Type'] = "application/json"
        now =  datetime.now(utc)
        res = ["wrapping up:"]
        for task in cherrypy.request.db.query(Task).options(subqueryload(Task.jobs)).filter(Task.status != "Completed", Task.status != "Located").all():
             total = 0
             running = 0
             for j in task.jobs:
                 total = total + 1
                 if j.status != "Completed":
                     running = running + 1    

             res.append("Task %d total %d running %d " % (task.task_id, total, running))

             if (total > 0 and running == 0) or (total == 0 and  now - task.created > timedelta(days= 2)):
                 task.status = "Completed"
                 task.updated = datetime.now(utc)
	         cherrypy.request.db.add(task)

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
        if res == "Completed":
            dcount = cherrypy.request.db.query(func.count(Job.job_id)).filter(Job.output_files_declared).scalar()
            if dcount == st["Completed"]:
                #all completed jobs have files declared
                res = "Located"
        return res
         
    @cherrypy.expose
    def update_job(self, task_id = None, jobsub_job_id = 'unknown',  **kwargs):
	 cherrypy.log("update_job( task_id %s, jobsub_job_id %s,  kwargs %s )" % (task_id, jobsub_job_id, repr(kwargs)))

         if not self.can_report_data():
              return "Not Allowed"

         if task_id:
             task_id = int(task_id)

         host_site = "%s_on_%s" % (jobsub_job_id, kwargs.get('slot','unknown'))
         j = cherrypy.request.db.query(Job).options(subqueryload(Job.task_obj)).filter(Job.jobsub_job_id==jobsub_job_id).first()

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
             j.task_id = task_id
             j.task_obj = t
             j.output_files_declared = False
             j.node_name = ''

         if j:
	     cherrypy.log("update_job: updating job %d" % (j.job_id if j.job_id else -1)) 
             if kwargs.get('output_files_declared', None) == "True":
                 if j.status == "Completed":
                     j.output_files_declared = True
                     j.status = "Located"

	     for field in ['cpu_type', 'node_name', 'host_site', 'status', 'user_exe_exit_code']:
                 if field == 'status' and j.status == "Located":
                     # stick at Located, don't roll back to Completed,etc.
                     continue
		 if kwargs.get(field, None):
		    setattr(j,field,kwargs[field].rstrip("\n"))
		 if not getattr(j,field, None):
		    if field == 'user_exe_exit_code':
			setattr(j,field,0)
		    else:
			setattr(j,field,'unknown')

	     for field in ['project', ]:
		 if kwargs.get("task_%s" % field, None) and j.task_obj:
		    setattr(j.task_obj,field,kwargs["task_%s"%field].rstrip("\n"))
                  
             if kwargs.get('output_file_names', None):
                 cherrypy.log("saw output_file_names: %s" % kwargs['output_file_names'])
                 if j.output_file_names:
                     files =  j.output_file_names.split(' ')
                 else:
                     files = []

                 newfiles = kwargs['output_file_names'].split(' ')
                 for f in newfiles:
                     if not f in files:
                         files.append(f)
                 j.output_file_names = ' '.join(files)

             if kwargs.get('input_file_names', None):
                 cherrypy.log("saw input_file_names: %s" % kwargs['input_file_names'])
                 if j.input_file_names:
                     files =  j.input_file_names.split(' ')
                 else:
                     files = []

                 newfiles = kwargs['input_file_names'].split(' ')
                 for f in newfiles:
                     if not f in files:
                         files.append(f)
                 j.input_file_names = ' '.join(files)
    
	     j.updated =  datetime.now(utc)

	     if j.task_obj:
                 j.task_obj.status = self.compute_status(j.task_obj)
		 j.task_obj.updated =  datetime.now(utc)
		 cherrypy.request.db.add(j.task_obj)

	     cherrypy.log("update_job: db add/commit job ") 
	     cherrypy.request.db.add(j)
	     cherrypy.request.db.commit()
	     cherrypy.log("update_job: done job_id %d" %  (j.job_id if j.job_id else -1))
         return "Ok."

    @cherrypy.expose
    def check_output_files_declared(self):
        #
        # Completed means jobs are done
        # Declared means all output files are declared
        # we try to make this transition here.
        # we just got there if our output_files_per_job == 0
        #
        tl = cherrypy.request.db.query(Task).filter(Task.status == "Completed").all()
        for t in tl:
            if t.campaign_obj.task_definition_obj.output_files_per_job == 0:
                t.status = "Located"
            else:
                all_all_declared = 1
                for j in t.jobs:
                    if not j.output_files_declared:
                        all_all_declared = 0
                        break
                if all_all_declared:
                    t.status = "Located"

    @cherrypy.expose
    def show_task_jobs(self, task_id, tmax = None, tmin = None, tdays = 1 ):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax,tdays,'show_task_jobs?task_id=%s' % task_id)

        jl = cherrypy.request.db.query(JobHistory,Job).filter(Job.job_id == JobHistory.job_id, Job.task_id==task_id, JobHistory.created >= tmin - timedelta(hours=4), JobHistory.created <= tmax).order_by(JobHistory.job_id,JobHistory.created).all()
        tg = time_grid.time_grid()
	class fakerow:
	    def __init__(self, **kwargs):
	        self.__dict__.update(kwargs)
	items = []
	for jh, j in jl:
	    if j.jobsub_job_id: 
		jjid= j.jobsub_job_id.replace('fifebatch','').replace('.fnal.gov','')
	    else: 
		jjid= 'j' + str(jh.job_id)

	    items.append(fakerow(job_id = jh.job_id,
				  created = jh.created.replace(tzinfo=utc),
				  status = jh.status,
				  jobsub_job_id = jjid))

        screendata = self.format_job_counts(task_id = task_id,tmin=tmins,tmax=tmaxs,tdays=tdays, range_string = time_range_string )
        key = tg.key(fancy=1)
        screendata = screendata +  tg.render_query(tmin, tmax, items, 'jobsub_job_id', url_template=self.path + '/triage_job?job_id=%(job_id)s&tmin='+tmins)         

        template = self.jinja_env.get_template('job_grid.html')
        return template.render( taskid = task_id, screendata = screendata, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 1, key = key, pomspath=self.path)



    @cherrypy.expose
    def triage_job(self, job_id, tmin = None, tmax = None, tdays = None, force_reload = False):

	# we don't really use these for anything but we might want to
        # pass them into a template to set time ranges...
        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin,tmax,tdays,'show_campaigns?')
 
        job_file_list = self.job_file_list(job_id, force_reload)
        template = self.jinja_env.get_template('triage_job.html')

        output_file_names_list = []

        job_info = cherrypy.request.db.query(Job, Task, TaskDefinition,  Campaign).filter(Job.job_id==job_id).filter(Job.task_id==Task.task_id).filter(Campaign.task_definition_id==TaskDefinition.task_definition_id).filter(Task.campaign_id==Campaign.campaign_id).first()

        job_history = cherrypy.request.db.query(JobHistory).filter(JobHistory.job_id==job_id).order_by(JobHistory.created).all()

        if job_info.Job.output_file_names:
            output_file_names_list = job_info.Job.output_file_names.split(" ")

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
        
        return template.render(job_id = job_id, job_file_list = job_file_list, job_info = job_info, job_history = job_history, downtimes=downtimes, output_file_names_list=output_file_names_list, tmin=tmin, current_experimenter=cherrypy.session.get('experimenter'), pomspath=self.path)

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
        set_tdays =  tmax != None and tmin != None
          

        if tdays == None:  # default to one day
            tdays = 1

        if tmax == None:
            # if we're not given a max, pick now
            tmax = datetime.now(utc)
        elif isinstance(tmax, basestring):
            tmax = datetime.strptime(tmax[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)
        
        tdays = float(tdays)

        if tmin == None:
            tmin = tmax - timedelta(days = tdays)

        elif isinstance(tmin, basestring):
            tmin = datetime.strptime(tmin[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)

        tsprev = tmin.strftime("%Y-%m-%d+%H:%M:%S")
        tsnext = (tmax + timedelta(days = tdays)).strftime("%Y-%m-%d+%H:%M:%S")
        tmaxs =  tmax.strftime("%Y-%m-%d %H:%M:%S")
        tmins =  tmin.strftime("%Y-%m-%d %H:%M:%S")
        prevlink="%s/%stmax=%s&tdays=%d" % (self.path,baseurl,tsprev, tdays)
        nextlink="%s/%stmax=%s&tdays=%d" % (self.path,baseurl,tsnext, tdays)
        # if we want to handle hours / weeks nicely, we should do
        # it here.
        plural =  's' if tdays > 1.0 else ''
        tranges = '%d day%s ending <span class="tmax">%s</span>' % (tdays, plural, tmaxs)

        # redundant, but trying to rule out tz woes here...
        tmin = tmin.replace(tzinfo = utc)
        tmax = tmax.replace(tzinfo = utc)

        if set_tdays:
            # if we're given tmax and tmin, compute tdays
            tdays = (tmax - tmin).total_seconds() / 86400.0

        return tmin,tmax,tmins,tmaxs,nextlink,prevlink,tranges

    @cherrypy.expose
    def show_campaigns(self,tmin = None, tmax = None, tdays = 1):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin,tmax,tdays,'show_campaigns?')

        cl = cherrypy.request.db.query(Campaign).filter(Task.campaign_id == Campaign.campaign_id).order_by(Campaign.experiment).all()

        res = []
        res.append( '<table class="ui celled table unstackable">')
        res.append( '<tr><th colspan=2></th><th colspan=3>Active Jobs</th><th colspan=2>Jobs in %s</th></tr>' % time_range_string),
        res.append( '<tr><th>Exp</th><th>Name</th><th>Idle</th><th>Running</th><th>Held</th><th>Completed</th><th>Located</th></tr>')

        for c in cl:
            res.append('<tr>')
            res.append('<td>%s</td>' % c.experiment)
            res.append('<td>%s' % c.name)
            res.append('<a href="%s/campaign_sheet?campaign_id=%d&tmax=%s"><i class="external share icon"></i></a>' % ( self.path, c.campaign_id, tmaxs))
            res.append('<a href="%s/campaign_time_bars?campaign_id=%d&tmax=%s"><i class="external graph icon"></i></a>' % ( self.path, c.campaign_id, tmaxs))
            res.append('</td>')
            counts = self.job_counts(tmax = tmax, tdays = tdays, campaign_id = c.campaign_id)
            for k in counts.keys():
                res.append('<td><a href="job_table?campaign_id=%s&job_status=%s">%d</a></td>' % (c.campaign_id, k, counts[k]))
            res.append("</tr>")
            
        res.append("</table>")
        screendata = "\n".join(res)
              
        template = self.jinja_env.get_template('campaign_grid.html')
        return template.render(  screendata = screendata, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 1, next = nextlink, prev = prevlink, days = tdays, time_range_string = time_range_string, key = '', pomspath=self.path)

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

        cl = cherrypy.request.db.query(Campaign).join(Task).filter(Campaign.campaign_id == campaign_id, Task.campaign_id == Campaign.campaign_id , Task.created > tmin - timedelta(hours= 12), Task.created < tmax ).all()
        
        for c in cl:
              sl.append('<h2 class="ui row dividing header">%s Tasks' % c.name )
              sl.append('</h2>' )
              sl.append('<div class="ui row">')
              sl.append(self.format_job_counts(campaign_id = c.campaign_id, tmin = tmin, tmax = tmax, tdays = tdays, range_string = time_range_string))
              sl.append('</div>')
	      sl.append('<div class="ui row">')
              sl.append('<b>Key</b>')
	      sl.append(key)
	      sl.append('</div>')

              qr = cherrypy.request.db.query(TaskHistory).join(Task).filter(Task.campaign_id == c.campaign_id, TaskHistory.task_id == Task.task_id , Task.created > tmin - timedelta(hours=12), Task.created < tmax ).order_by(TaskHistory.task_id,TaskHistory.created).all()
              items = []
              for th in qr:
                  jjid = self.task_min_job(th.task_id)
                  if not jjid: 
                      jjid= 't' + str(th.task_id)
                  else:
                      jjid = jjid.replace('fifebatch','').replace('.fnal.gov','')
                  items.append(fakerow(task_id = th.task_id,
 					created = th.created.replace(tzinfo = utc),
 					status = th.status,
                 			jobsub_job_id = jjid))

              sl.append( tg.render_query(tmin, tmax, items, 'jobsub_job_id', url_template = self.path + '/show_task_jobs?task_id=%(task_id)s&tmin=%(created)19.19s' ))
        screendata = "\n".join(sl)
              
        template = self.jinja_env.get_template('campaign_bars.html')
        return template.render(  screendata = screendata, name = name, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 1, next = nextlink, prev = prevlink, days = tdays, key = key, pomspath=self.path)

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
        j = cherrypy.request.db.query(Job).options(subqueryload(Job.task_obj).subqueryload(Task.campaign_obj)).filter(Job.job_id == job_id).first()
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
        return template.render(file=file, job_file_contents=job_file_contents, task_id=task_id, job_id=job_id, tmin=tmin, pomspath=self.path)

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

        out = OrderedDict([("Idle",0),( "Running",0),( "Held",0),( "Completed",0), ("Located",0)])
        for row in  q.all():
            if row[1][1:7] == "unning":
                short = "Running"
            else:
                short = row[1]
            out[short] = out.get(short,0) + int(row[0])

        return out

    
    @cherrypy.expose
    def job_table(self, tmin = None, tmax = None, tdays = 1, task_id = None, campaign_id = None , experiment = None, sift=False, campaign_name=None, task_def_id=None, vo_role=None, input_dataset=None, output_dataset=None, task_status=None, project=None, jobsub_job_id=None, node_name=None, cpu_type=None, host_site=None, job_status=None, user_exe_exit_code=None, output_files_declared=None, campaign_checkbox=None, task_checkbox=None, job_checkbox=None, ignore_me = None, keyword=None):
           
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

        if campaign_name:
            q = q.filter(Campaign.name == campaign_name)
            filtered_fields['campaign_name'] = campaign_name

        if task_def_id:
            q = q.filter(Campaign.task_definition_id == task_def_id)
            filtered_fields['task_def_id'] = task_def_id

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
            q = q.filter(Job.status == job_status)
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
        return template.render(joblist=jl, jobcolumns = jobcolumns, taskcolumns = taskcolumns, campcolumns = campcolumns, current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 0,  tmin=tmins, tmax =tmaxs,  prev= prevlink,  next = nextlink, days = tdays, extra = extra, hidecolumns = hidecolumns, filtered_fields=filtered_fields, time_range_string = time_range_string, pomspath=self.path)

    @cherrypy.expose
    def jobs_by_exitcode(self, tmin = None, tmax =  None, tdays = 1 ):

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax,tdays,'jobs_by_exitcode?')
 
        q = cherrypy.request.db.query(Job.user_exe_exit_code,func.count(Job.job_id)).filter(Job.updated >= tmin, Job.updated <= tmax).group_by(Job.user_exe_exit_code).order_by(Job.user_exe_exit_code)
 
        jl = q.all()
        cherrypy.log( "got jobtable %s " % repr( jl[0].__dict__) )
        columns = [ "exit_code","count"]
        
        template = self.jinja_env.get_template('job_count_table.html')

        return template.render(joblist=jl, columns = columns, current_experimenter=cherrypy.session.get('experimenter'), do_refresh = 0,  tmin=tmins, tmax =tmaxs,  prev= prevlink,  next = nextlink, time_range_string = time_range_string, days = tdays, pomspath=self.path)


    @cherrypy.expose
    def quick_search(self, jobsub_job_id):
        job_info = cherrypy.request.db.query(Job).filter(Job.jobsub_job_id == jobsub_job_id).first()
        if job_info:
            raise cherrypy.HTTPRedirect("%s/triage_job?job_id=%s&tmin=%s" % (self.path,str(job_info.job_id), "somekindofdate"))
        else:
            raise cherrypy.HTTPRedirect(self.path + "/")

    @cherrypy.expose
    def json_project_summary_for_task(self, task_id):
        cherrypy.response.headers['Content-Type'] = "application/json"
        return json.dumps(self.project_summary_for_task( task_id))

    def project_summary_for_task(self, task_id):
        t = cherrypy.request.db.query(Task).filter(Task.task_id == task_id).first()
        return cherrypy.request.project_fetcher.fetch_info( t.campaign_obj.experiment, t.project)


    @cherrypy.expose
    def campaign_sheet(self, campaign_id, tmin = None, tmax = None , tdays = 14):

        daynames=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday", "Sunday"]

        tmin,tmax,tmins,tmaxs,nextlink,prevlink,time_range_string = self.handle_dates(tmin, tmax,tdays,'campaign_sheet?campaign_id=%s' % campaign_id)

        tl = cherrypy.request.db.query(Task).filter(Task.campaign_id == campaign_id , Task.created > tmin, Task.created < tmax ).order_by(desc(Task.created)).all()
        el = cherrypy.request.db.query(distinct(Job.user_exe_exit_code)).filter(Job.updated >= tmin, Job.updated <= tmax).all()

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
            columns.append('exit(%d)'%e)
        outrows = []
        exitcounts = {}

        for task in tl:
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
            ps = self.project_summary_for_task(task.task_id)
            if ps:
		totdfiles = totdfiles + ps['tot_consumed'] + ps['tot_failed']
		totfiles = totfiles + ps['files_in_snapshot']
		totjobfails = totjobfails + ps['tot_jobfails']

	    totjobs = totjobs + len(task.jobs)
	    for job in task.jobs:
		exitcounts[job.user_exe_exit_code] = exitcounts[job.user_exe_exit_code] + 1
		if job.output_file_names:
		    nout = len(job.output_file_names.split(' '))
		    outfiles += nout
		    if not job.output_files_declared:
			# a bit of a lie, we don't know they're *all* pending, just some of them
			# but its close, and we don't want to re-poll SAM here..
			pendfiles += nout

		if job.input_file_names:
		    nin = len(job.input_file_names.split(' '))
		    infiles += nin

        # it looks like we should add another row here for the last set of totals, but
        # instead we added a day to the query range, so we compute a row of totals we don't use..
    
        template = self.jinja_env.get_template('campaign_sheet.html')
        if tl and tl[0]:
            name = tl[0].campaign_obj.name 
        else:
            name = ''
        return template.render(name = name,columns = columns, datarows = outrows, prevlink=prevlink, nextlink=nextlink,current_experimenter=cherrypy.session.get('experimenter'), campaign_id = campaign_id, pomspath=self.path)

