import cherrypy
import os
import time_grid
import json

from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey, and_, or_, create_engine, null, desc, text, func, exc, distinct
from sqlalchemy.orm  import subqueryload, contains_eager
from datetime import datetime, tzinfo,timedelta
from jinja2 import Environment, PackageLoader
from model.poms_model import Service, ServiceDowntime, Experimenter, Job, JobHistory, Task, TaskDefinition, TaskHistory, Campaign

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

    @cherrypy.expose
    def headers(self):
        return repr(cherrypy.request.headers)

    def get_current_experimenter(self):
        if cherrypy.request.headers.get('X-Shib-Email',None):
            experimenter = cherrypy.request.db.query(Experimenter).filter(Experimenter.email == cherrypy.request.headers['X-Shib-Email'] ).first()
        else:
            experimenter = None

        if not experimenter and cherrypy.request.headers.get('X-Shib-Email',None):
             experimenter = Experimenter(
		   first_name = cherrypy.request.headers['X-Shib-Name-First'],
		   last_name =  cherrypy.request.headers['X-Shib-Name-Last'],
		   email =  cherrypy.request.headers['X-Shib-Email'])
	     cherrypy.request.db.add(experimenter)
             cherrypy.request.db.commit()

             experimenter = cherrypy.request.db.query(Experimenter).filter(Experimenter.email == cherrypy.request.headers['X-Shib-Email'] ).first()
        return experimenter

    @cherrypy.expose
    def index(self):
        template = self.jinja_env.get_template('service_statuses.html')
        return template.render(services=self.service_status_hier('All'),current_experimenter=self.get_current_experimenter(), do_refresh = 1)

    @cherrypy.expose
    def test(self):
        cherrypy.response.headers['content-type'] = 'text/plain'
        html = """<html>
          <head></head>
          <body>
          session._id:       %s<br>
          session.get('id'): %s
          </body>
        </html>""" % (cherrypy.session._id,cherrypy.session.get('id'))
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
        if user in ['mengel','illingwo','mgheith','swhite']:
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
        if user in ['mengel','illingwo','mgheith','swhite']:
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
        return template.render(rows=rows,current_experimenter=self.get_current_experimenter())



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
        return template.render(rows=rows,current_experimenter=self.get_current_experimenter())


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
        return template.render(list=list, name=under,current_experimenter=self.get_current_experimenter())

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
                      <button class="ui button %s" title="%s">
                         %s (%d/%d)
                       </button>
                       <i class="icon %s"></i>
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
                      <button class="ui button %s" title="%s">
                        %s (%d/%d)
                      </button>
                      <i class="icon %s"></i>
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
    def admin_screen(self):
        if not self.can_db_admin():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        template = self.jinja_env.get_template('admin_screen.html')
        return template.render(list = self.admin_map.keys(),current_experimenter=self.get_current_experimenter())
        
    @cherrypy.expose
    def list_generic(self, classname):
        if not self.can_db_admin():
             raise cherrypy.HTTPError(401, 'You are not authorized to access this resource')
        l = self.make_list_for(self.admin_map[classname],self.pk_map[classname])
        template = self.jinja_env.get_template('list_screen.html')
        return template.render( classname = classname, list = l, edit_screen="edit_screen_generic", primary_key='experimenter_id',current_experimenter=self.get_current_experimenter())

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
        return template.render( screendata = screendata, action="./"+update_call , classname = classname ,current_experimenter=self.get_current_experimenter())

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
         t.task_definition_id = td.task_definition_id
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
	     cherrypy.log("update_job: creating new job") 
             j = Job()
             j.jobsub_job_id = jobsub_job_id.rstrip("\n")
             j.created = datetime.now(utc)
             j.task_id = task_id
             j.output_files_declared = False
             j.node_name = ''

         if j:
	     cherrypy.log("update_job: updating job %d" % (j.job_id if j.job_id else -1)) 
	     for field in ['cpu_type', 'node_name', 'host_site', 'status', 'user_exe_exit_code']:
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
                  
             if kwargs.get('output_files_declared', None) == "True":
                 j.output_files_declared = True

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
    def show_task_jobs(self, task_id, tmin, tmax = None ):
        tmin = datetime.strptime(tmin, "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)
        if tmax != None:
            tmax = datetime.strptime(tmax, "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)
        else:
            tmax = tmin + timedelta(days=1)

        jl = cherrypy.request.db.query(JobHistory).join(Job).filter(Job.task_id==task_id, JobHistory.created >= tmin, JobHistory.created <= tmax).order_by(JobHistory.job_id,JobHistory.created).all()
        tg = time_grid.time_grid()
        screendata = self.format_job_counts(task_id = task_id)
        screendata = screendata +  tg.render_query(tmin, tmax, jl, 'job_id', url_template='/poms/triage_job?job_id=%(job_id)s&tmin='+str(tmin).split('+')[0])         

        template = self.jinja_env.get_template('job_grid.html')
        return template.render( taskid = task_id, screendata = screendata, tmin = str(tmin)[:16], tmax = str(tmax)[:16],current_experimenter=self.get_current_experimenter(), do_refresh = 1)



    @cherrypy.expose
    def triage_job(self, job_id, tmin, force_reload = False):
        job_file_list = self.job_file_list(job_id, force_reload)
        template = self.jinja_env.get_template('triage_job.html')

        job_info = cherrypy.request.db.query(Job, Task, TaskDefinition,  Campaign).filter(Job.job_id==job_id).filter(Job.task_id==Task.task_id).filter(Task.task_definition_id==TaskDefinition.task_definition_id).filter(Task.campaign_id==Campaign.campaign_id).first()

        job_history = cherrypy.request.db.query(JobHistory).filter(JobHistory.job_id==job_id).order_by(JobHistory.created).all()
        
        return template.render(job_id = job_id, job_file_list = job_file_list, job_info = job_info, job_history = job_history, tmin=tmin, current_experimenter=self.get_current_experimenter())


    @cherrypy.expose
    def show_campaigns(self,tmax = None, tdays = 1):

        if tmax == None:
            tmax = datetime.now(utc)
        else:
            tmax = datetime.strptime(tmax, "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)

        tdays = int(tdays)
        tminscreen = tmax - timedelta(days = tdays)
        tmin = tminscreen - timedelta(days = 1)
        tsprev = tmin.strftime("%Y-%m-%d+%H:%M:%S")
        tsnext = (tmax + timedelta(days = tdays)).strftime("%Y-%m-%d+%H:%M:%S")
        tminscreens =  tmin.strftime("%Y-%m-%d %H:%M:%S")
        tmaxs =  tmax.strftime("%Y-%m-%d %H:%M:%S")
        prevlink="/poms/show_campaigns?tmax=%s&tdays=%d" % (tsprev, tdays)
        nextlink="/poms/show_campaigns?tmax=%s&tdays=%d" % (tsnext, tdays)


        tg = time_grid.time_grid()

	class fakerow:
	    def __init__(self, **kwargs):
	        self.__dict__.update(kwargs)

        sl = []
        sl.append(self.format_job_counts())

        cl = cherrypy.request.db.query(Campaign).join(Task).filter(Task.campaign_id == Campaign.campaign_id , Task.created > tmin, Task.created < tmax ).all()
        
        for c in cl:
              sl.append('<h2 class="ui dividing header">%s Tasks</h2>' % c.name )
              sl.append(self.format_job_counts(campaign_id = c.campaign_id))

              items = cherrypy.request.db.query(TaskHistory).join(Task).filter(Task.campaign_id == c.campaign_id, TaskHistory.task_id == Task.task_id , Task.created > tmin, Task.created < tmax ).order_by(TaskHistory.task_id,TaskHistory.created).all()
              sl.append( tg.render_query(tminscreen, tmax, items, 'task_id', url_template = '/poms/show_task_jobs?task_id=%(task_id)s&tmin=%(created)19.19s' ))

        screendata = "\n".join(sl)

        allcounts =  self.format_job_counts()
              
        template = self.jinja_env.get_template('campaign_grid.html')
        return template.render(  screendata = screendata, tmin = str(tminscreen)[:16], tmax = str(tmax)[:16],current_experimenter=self.get_current_experimenter(), do_refresh = 1, next = nextlink, prev = prevlink, days = tdays)

    
    @cherrypy.expose
    def job_file_list(self, job_id,force_reload = False):
        j = cherrypy.request.db.query(Job).filter(Job.job_id == job_id).first()
        # find the job with the logs -- minimum jobsub_job_id for this task
        j = cherrypy.request.db.query(Job).filter( Job.task_id == j.task_id ).order_by(Job.jobsub_job_id).first()
        cherrypy.log("found job: %s " % j.jobsub_job_id)
        role = j.task_obj.campaign_obj.vo_role
        return cherrypy.request.jobsub_fetcher.index(j.jobsub_job_id,j.task_obj.campaign_obj.experiment ,role, force_reload)

    @cherrypy.expose
    def job_file_contents(self, job_id, task_id, file, tmin):
        j = cherrypy.request.db.query(Job).filter(Job.job_id == job_id).first()
        # find the job with the logs -- minimum jobsub_job_id for this task
        j = cherrypy.request.db.query(Job).filter( Job.task_id == j.task_id ).order_by(Job.jobsub_job_id).first()
        cherrypy.log("found job: %s " % j.jobsub_job_id)
        role = j.task_obj.campaign_obj.vo_role
        job_file_contents = cherrypy.request.jobsub_fetcher.contents(file, j.jobsub_job_id,j.task_obj.campaign_obj.experiment,role)
        template = self.jinja_env.get_template('job_file_contents.html')
        return template.render(file=file, job_file_contents=job_file_contents, task_id=task_id, job_id=job_id, tmin=tmin)

    @cherrypy.expose
    def test_job_counts(self, task_id = None, campaign_id = None):
        res = self.job_counts(task_id, campaign_id)
        return repr(res) + self.format_job_counts(task_id, campaign_id)

    def format_job_counts(self, task_id = None, campaign_id = None):
        counts = self.job_counts(task_id, campaign_id)
        ck = counts.keys()
        ck.sort()
        res = [ '<table class="ui celled table unstackable"><tr><td span=4>Job states</td></tr><tr>' ]
        for k in ck:
            res.append( "<th>%s</th>" % k )
        res.append("</tr><tr>")
        for k in ck:
            res.append( "<td>%d</td>" % counts[k] )
        res.append("</tr></table>")
        return "".join(res)

    def job_counts(self, task_id = None, campaign_id = None):
        q = cherrypy.request.db.query(func.count(Job.status),Job.status).group_by(Job.status) 
        if task_id:
            q = q.filter(Job.task_id == task_id)

        if campaign_id:
            q = q.join(Task,Job.task_id == Task.task_id).filter( Task.campaign_id == campaign_id)

        out = {"Idle":0, "Running":0, "Held":0, "Completed":0}
        for row in  q.all():
            if row[1][1:7] == "unning":
                short = "Running"
            else:
                short = row[1]
            out[short] = out.get(short,0) + int(row[0])
        return out





    
    @cherrypy.expose
    def job_table(self, tmax =  None, tdays = 1, exitcode = None, campaign_id = None , experiment = None):
        if tmax == None:
            tmax = datetime.now(utc)
        else:
            tmax = datetime.strptime(tmax, "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)
        tdays = int(tdays)
        tmin = tmax - timedelta(days = tdays)
        tsprev = tmin.strftime("%Y-%m-%d+%H:%M:%S")
        tsnext = (tmax + timedelta(days = tdays)).strftime("%Y-%m-%d+%H:%M:%S")
        tmins =  tmin.strftime("%Y-%m-%d %H:%M:%S")
        tmaxs =  tmax.strftime("%Y-%m-%d %H:%M:%S")
        prevlink="/poms/job_table?tmax=%s&tdays=%d" % (tsprev, tdays)
        nextlink="/poms/job_table?tmax=%s&tdays=%d" % (tsnext, tdays)
        extra = ""

        q = cherrypy.request.db.query(Job,Task,Campaign)
        q = q.filter(Job.task_id == Task.task_id, Task.campaign_id == Campaign.campaign_id)
        q = q.filter(Job.updated >= tmin, Job.updated <= tmax)

        extra = ""
        if exitcode != None:
            q = q.filter(Job.user_exe_exit_code == int(exitcode))
            extra = extra + "with exit code %s" % exitcode

        if campaign_id != None:
            q = q.filter( Job.task_obj.campaign_id == int(campaign_id))
            extra = extra + "in campaign id %d" % campaign_id

        if experiment != None:
            q = q.filter( task_obj.campaign_obj.experiment == experiment)
            extra = extra + "in experiment %s" % experiment

        jl = q.all()

        if jl:
            jobcolumns = jl[0][0]._sa_instance_state.class_.__table__.columns.keys() 
            taskcolumns = jl[0][1]._sa_instance_state.class_.__table__.columns.keys() 
            campcolumns = jl[0][2]._sa_instance_state.class_.__table__.columns.keys() 
        else:
            jobcolumns = []
            taskcolumns = []
            campcolumns = []

        hidecolumns = [ 'task_id', 'campaign_id', 'created', 'creator', 'updated', 'updater', 'command_executed', 'task_definition_id', 'task_parameters']
        
        template = self.jinja_env.get_template('job_table.html')
        return template.render(joblist=jl, jobcolumns = jobcolumns, taskcolumns = taskcolumns, campcolumns = campcolumns, current_experimenter=self.get_current_experimenter(), do_refresh = 0,  tmin=tmins, tmax =tmaxs,  prev= prevlink,  next = nextlink, days = tdays, extra = extra, hidecolumns = hidecolumns)

    @cherrypy.expose
    def jobs_by_exitcode(self, tmax =  None, tdays = 1 ):

        if tmax == None:
            tmax = datetime.now(utc)
        else:
            tmax = datetime.strptime(tmax, "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)

        tdays = int(tdays)
        tmin = tmax - timedelta(days = tdays)
        tsprev = tmin.strftime("%Y-%m-%d+%H:%M:%S")
        tsnext = (tmax + timedelta(days = tdays)).strftime("%Y-%m-%d+%H:%M:%S")
        tmins =  tmin.strftime("%Y-%m-%d %H:%M:%S")
        tmaxs =  tmax.strftime("%Y-%m-%d %H:%M:%S")
        prevlink="/poms/jobs_by_exitcode?tmax=%s&tdays=%d" % (tsprev, tdays)
        nextlink="/poms/jobs_by_exitcode?tmax=%s&tdays=%d" % (tsnext, tdays)

        q = cherrypy.request.db.query(Job.user_exe_exit_code,func.count(Job.job_id)).filter(Job.updated >= tmin, Job.updated <= tmax).group_by(Job.user_exe_exit_code).order_by(Job.user_exe_exit_code)

        jl = q.all()
        cherrypy.log( "got jobtable %s " % repr( jl[0].__dict__) )
        columns = [ "exit_code","count"]
        
        template = self.jinja_env.get_template('job_count_table.html')
        return template.render(joblist=jl, columns = columns, current_experimenter=self.get_current_experimenter(), do_refresh = 0,  tmin=tmins, tmax =tmaxs,  prev= prevlink,  next = nextlink, days = tdays)

    @cherrypy.expose
    def quick_search(self, jobsub_job_id):
        job_info = cherrypy.request.db.query(Job).filter(Job.jobsub_job_id == jobsub_job_id).first()
        raise cherrypy.HTTPRedirect("/poms/triage_job?job_id=%s&tmin=%s" % (str(job_info.job_id), "somekindofdate"))

    @cherrypy.expose
    def json_project_summary_for_task(self, task_id):
        cherrypy.response.headers['Content-Type'] = "application/json"
        return json.dumps(self.project_summary_for_task( task_id))

    def project_summary_for_task(self, task_id):
        t = cherrypy.request.db.query(Task).filter(Task.task_id == task_id).first()
        return cherrypy.request.project_fetcher.fetch_info( t.campaign_obj.experiment, t.project)


    @cherrypy.expose
    def campaign_sheet(self, campaign_id, tmax = None , tdays = 14):

        daynames=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday", "Sunday"]

        if tmax == None:
            tmax = datetime.now(utc)
        else:
            tmax = datetime.strptime(tmax, "%Y-%m-%d %H:%M:%S").replace(tzinfo = utc)
        tdays = int(tdays)
        tmin = tmax - timedelta(days = tdays+1)  # extra day, see below...

        tsprev = tmin.strftime("%Y-%m-%d+%H:%M:%S")
        tsnext = (tmax + timedelta(days = tdays)).strftime("%Y-%m-%d+%H:%M:%S")
        tmins =  tmin.strftime("%Y-%m-%d %H:%M:%S")
        tmaxs =  tmax.strftime("%Y-%m-%d %H:%M:%S")
        prevlink="/poms/campaign_sheet?campaign_id=%s&tmax=%s&tdays=%d" % (campaign_id,tsprev, tdays)
        nextlink="/poms/campaign_sheet?campaign_id=%s&tmax=%s&tdays=%d" % (campaign_id,tsnext, tdays)


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
                     outrow.append(date.isoformat())
                     outrow.append(str(totfiles))
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
		totjobs = totjobs + len(task.jobs)
		totjobfails = totjobfails + ps['tot_jobfails']
		for job in task.jobs:
                    exitcounts[job.user_exe_exit_code] = exitcounts[job.user_exe_exit_code] + 1
		    if job.output_file_names:
			nout = len(job.output_file_names.split(' '))
			outfiles += nout
			if not job.output_files_delcared:
			    # a bit of a lie, we don't know they're *all* pending, just some of them
			    # but its close, and we don't want to re-poll SAM here..
			    pendingfiles += nout

        # it looks like we should add another row here for the last set of totals, but
        # instead we added a day to the query range, so we compute a row of totals we don't use..
    
        template = self.jinja_env.get_template('campaign_sheet.html')
        return template.render(name = tl[0].campaign_obj.name ,columns = columns, datarows = outrows, prevlink=prevlink, nextlink=nextlink,current_experimenter=self.get_current_experimenter())

