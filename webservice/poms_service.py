import cherrypy
import os

from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey, and_, or_, create_engine, null, desc, text
from datetime import datetime, tzinfo,timedelta
from jinja2 import Environment, PackageLoader
from model.poms_model import Service, ServiceDowntime, Experimenter

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
        return template.render(services=self.service_status_hier('All'),current_experimenter=self.get_current_experimenter())

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


    @cherrypy.expose
    def calendar(self):
        template = self.jinja_env.get_template('calendar.html')
        return template.render()


    @cherrypy.expose
    def service_downtimes(self):
        template = self.jinja_env.get_template('service_downtimes.html')
        rows = cherrypy.request.db.query(ServiceDowntime, Service).filter(ServiceDowntime.service_id == Service.service_id).all()
        return template.render(rows=rows)


    @cherrypy.expose
    def update_service(self, name, parent, status, host_site):
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
        return template.render(list=list, name=under)

    def service_status_hier(self, under = 'All', depth = 0):
        p = cherrypy.request.db.query(Service).filter(Service.name == under).first()
        if depth == 0:
            res = '<div class="ui accordion styled">\n'
        else:
            res = ''
        active = ""
        for s in cherrypy.request.db.query(Service).filter(Service.parent_service_id == p.service_id).order_by(desc(Service.name)).all():
             posneg = "positive" if s.status == "good" else "negative" if s.status == "bad" else ""
             icon =  "checkmark" if s.status == "good" else "remove" if s.status == "bad" else "help circle"
             if s.host_site:
                 res = res + """
                     <div class="title %s">
		      <i class="dropdown icon"></i>
                      <button class="ui button %s">
                         %s
                       </button>
                       <i class="icon %s"></i>
                     </div>
                     <div  class="content %s">
                         <a target="_blank" href="%s"</a>
                         <i class="icon external"></i> 
                         source webpage
                         </a>
                     </div>
                  """ % (active, posneg, s.name,  icon, active, s.host_site) 
             else:
                 res = res + """
                    <div class="title %s">
		      <i class="dropdown icon"></i>
                      <button class="ui button %s">
                        %s
                      </button>
                      <i class="icon %s"></i>
                    </div>
                    <div class="content %s">
                      <p>components:</p>
                      %s
                    </div>
                 """ % (active, posneg, s.name, icon, active,  self.service_status_hier(s.name, depth + 1))
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
        template = self.jinja_env.get_template('admin_screen.html')
        return template.render(list = self.admin_map.keys())
        
    @cherrypy.expose
    def list_generic(self, classname):
        l = self.make_list_for(self.admin_map[classname],self.pk_map[classname])
        template = self.jinja_env.get_template('list_screen.html')
        return template.render( classname = classname, list = l, edit_screen="edit_screen_generic", primary_key='experimenter_id')

    @cherrypy.expose
    def edit_screen_generic(self, classname, id = None):
        # XXX -- needs to get select lists for foreign key fields...
        return self.edit_screen_for(classname, self.admin_map[classname], 'update_generic', self.pk_map[classname], id, {})
         
    @cherrypy.expose
    def update_generic( self, classname, *args, **kwargs):
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
                    setattr(found, fieldname, datetime.strptime(kwargs.get(fieldname,'')), "%Y-%m-%dT%H:%M")
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
        return template.render( screendata = screendata, action="./"+update_call , classname = classname )

    def make_list_for(self,eclass,primkey):
        res = []
        for i in cherrypy.request.db.query(eclass).order_by(primkey).all():
            res.append( {"key": getattr(i,primkey,''), "value": getattr(i,'name',getattr(i,'email','unknown'))})
        return res

    @cherrypy.expose
    def create_task(self, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor = None ):
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
    def update_job(self, task_id, jobsubjobid, slot , cputype, status):
         host_site = "%s_on_%s" % (jobsubjobid, slot)
         j = cherrypy.request.db.query(Job).filter(Job.host_site==host_site, Job.task_id==task_id).first()
         if not j:
             j = Job()
         j.task_id = task_id
         j.node_name = slot[slot.find('@')+1:]
         j.cpu_type = cputype
         j.host_site = host_site
         j.status = status
         j.updated = datetime.now(utc)
         cherrypy.request.db.add(j)
         cherrypy.request.db.commit()

