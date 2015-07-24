import cherrypy
import sys
import json
from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey, and_, or_, create_engine, null, desc
from sqlalchemy.orm import sessionmaker, scoped_session, joinedload, aliased
from model.poms_model import *
import logging
import logging.handlers
from datetime import datetime, tzinfo,timedelta
import traceback
from jinja2 import Environment, PackageLoader

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

class dbparts:
    # keep our database connection/session bits here
    engine = None
    SessionMaker = None

    def __init__(self, dbpath = None) :
        if dbpath == None:
           dbpath = "postgres://cspgsdev.fnal.gov:5437/pomsdev"
        cherrypy.log("Starting with dbpath of %s" % dbpath)
        dbparts.engine = create_engine(dbpath, echo = True)
        dbparts.SessionMaker = scoped_session(sessionmaker(bind = dbparts.engine))

def withsession(func):
    ''' decorator to call a function with a scoped-session with cleanup'''
    def newfunc(*args, **kwargs):
        try:
            session = dbparts.SessionMaker()
            kwargs['session'] = session
            res = func(*args, **kwargs)
        except Exception as e:
            cherrypy.log(traceback.format_exc())
            if session:
                # probably a database error needing rollback
                session.rollback()
                raise 
        finally:
            dbparts.SessionMaker.remove()
        return res
    return newfunc

class poms_service:
    def __init__(self):
        self.jinja_env = Environment(loader=PackageLoader('webservice','templates'))

    @cherrypy.expose
    def hello(self):
        return "<html><body>Hello</body></html>"

    @cherrypy.expose
    @withsession
    def update_service(self, name, parent, status, host_site, session = None):
        s = session.query(Service).filter(Service.name == name).first()

        if not s:
            s = Service()
            s.name = name

        if parent:
	    p = session.query(Service).filter(Service.name == parent).first()
            cherrypy.log("got parent %s -> %s" % (parent, p))
	    if not p:
		p = Service()
		p.name = parent
		p.status = "unknown"
		p.updated = datetime.now(utc)
		session.add(p)
        else:
            p = None

        if s.status != status and status == "bad":
            # start downtime, if we aren't in one
            d = session.query(ServiceDowntime).filter(ServiceDowntime.service_id == s.service_id ).order_by(desc(ServiceDowntime.downtime_started)).first()
            if d == None or d.downtime_ended != None:
	        d = ServiceDowntime()
	        d.service = s
	        d.downtime_started = datetime.now(utc)
		d.downtime_ended = None
		session.add(d)

        if s.status != status and status == "good":
            # end downtime, if we're in one
            d = session.query(ServiceDowntime).filter(ServiceDowntime.service_id == s.service_id ).order_by(desc(ServiceDowntime.downtime_started)).first()
            if d:
                if d.downtime_ended == None:
                    d.downtime_ended = datetime.now(utc)
                    session.add(d)

        s.parent_service = p
        s.status = status
        s.host_site = host_site
        s.updated = datetime.now(utc)
        session.add(s)
        session.commit()

        return "Ok."
    @cherrypy.expose
    @withsession
    def service_status(self, under = 'All', session = None):
        prev = None
        prevparent = None
        p = session.query(Service).filter(Service.name == under).first()
        list = []
        for s in session.query(Service).filter(Service.parent_service_id == p.service_id).all():

            if s.host_site:
                 url = s.host_site
            else:
                 url = "./service_status?under=%s" % s.name

            list.append({'name': s.name,'status': s.status, 'url': url})

        template = self.jinja_env.get_template('service_status.html')
        return template.render(list=list, name=under)

    experimentlist = [ ['nova','nova'],['minerva','minerva']]

    @cherrypy.expose
    @withsession
    def list_experimenters(self, session):
        l = self.make_list_for(Experimenter,'experimenter_id',session)
        template = self.jinja_env.get_template('list_screen.html')
        return template.render( list = l, edit_screen="edit_screen_experimenter", primary_key='experimenter_id')
         
    @cherrypy.expose
    @withsession
    def edit_screen_experimenter( self, experimenter_id, session = None ):
        return self.edit_screen_for(Experimenter, 'update_experimenter',  'experimenter_id', experimenter_id, {}, session = session)

    @cherrypy.expose
    @withsession
    def update_experimenter( self, *args, **kwargs):
        return self.update_for(Experimenter, 'experimenter_id', *args, **kwargs)

    @cherrypy.expose
    @withsession
    def edit_screen_campaign( self, campaign_id, session = None):
        return self.edit_screen_for(Campaign, 'update_campaign',  'campaign_id', campaign_id, {}, session = session)

    @cherrypy.expose
    @withsession
    def update_campaign( self, *args, **kwargs):
        return self.update_for(Campaign, 'campaign_id', *args, **kwargs)

    @cherrypy.expose
    @withsession
    def update_for( self, eclass, primkey,  *args , **kwargs):
        session = kwargs.get('session',None)
        found = None
        if kwargs.get(primkey,'') != '':
            found = session.query(eclass).filter(text("%s = %d" % (primkey,int(kwargs.get(primkey,'0'))))).first()
            cherrypy.log("update_for: found existing %s" % found )
        if found == None:
            cherrypy.log("update_for: making new %s" % eclass)
            found = eclass()
        columns = found._sa_instance_state.class_.__table__.columns
        for fieldname in columns.keys():
            if columns[fieldname].primary_key:
                continue
            if columns[fieldname].type == Integer:
                setattr(found, fieldname, int(kwargs.get(fieldname,'')))
            else:
                setattr(found, fieldname, kwargs.get(fieldname,''))
        cherrypy.log("update_for: found is now %s" % found )
        session.add(found)
        session.commit()
        return "Ok."
  
    def edit_screen_for( self, eclass, update_call, primkey, primval, valmap, session ):
        found = None
        sample = eclass()
        if primval != '':
            cherrypy.log("looking for %s in %s" % (primval, eclass))
            found = session.query(eclass).filter(text("%s = %d" % (primkey,int(primval)))).first()
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
        return template.render( screendata = screendata, action="./"+update_call )
    def make_list_for(self,eclass,primkey,session):
        res = []
        for i in session.query(eclass).order_by(primkey).all():
            res.append( {"key": getattr(i,primkey,''), "value": getattr(i,'name',getattr(i,'email','unknown'))})
        return res

    @cherrypy.expose
    @withsession
    def create_task(self, experiment, taskdef, params, input_dataset, output_dataset, creator, waitingfor = None , session = None):
         first,last,email = creator.split(' ')
         creator = self.get_or_add_experimenter(first, last, email, session = session)
         exp = self.get_or_add_experiment(experiment,session = session)
         td = self.get_or_add_taskdef(taskdef, creator, exp, session = session)
         camp = self.get_or_add_campaign(exp,td,creator, session = session)
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

         session.add(t)
         session.commit()
         return str(t.task_id)

    @cherrypy.expose
    @withsession
    def update_job(self, task_id, jobsubjobid, slot , cputype, status, session = None):
         host_site = "%s_on_%s" % (jobsubjobid, slot)
         j = session.query(Job).filter(Job.host_site==host_site, Job.task_id==task_id).first()
         if not j:
             j = Job()
         j.task_id = task_id
         j.node_name = slot[slot.find('@')+1:]
         j.cpu_type = cputype
         j.host_site = host_site
         j.status = status
         j.updated = datetime.now(utc)
         session.add(j)
         session.commit()

def set_rotating_log(app):
     ''' recipe  for a rotating log file...'''
     maxBytes=100000000
     keepcount=10
     for x in 'error', 'access':
         fname = '%s.log' % x
         h = logging.handlers.RotatingFileHandler(fname, 'a',maxBytes,keepcount)
         h.setLevel(logging.DEBUG)
         h.setFormatter(cherrypy._cplogging.logfmt)
         getattr(app.log, '%s_log' % x).addHandler(h)

if __name__ == '__main__':

    configfile = "poms.ini"

    if len(sys.argv) > 1 and sys.argv[1] == '-c':
        configfile = sys.argv[2]
        sys.argv = sys.argv[2:]

    cherrypy.config.update(configfile)
    cherrypy.config.update("passwd.ini")

    # normal operating mode:
    db = cherrypy.config.get("db")
    path = cherrypy.config.get("path")
    if path == None:
       path = "/poms"
    dbp = dbparts(db)
    #print "got db of %s and path of %s" % (db, path)
    app = cherrypy.tree.mount(poms_service(), path , configfile)
    set_rotating_log(app)
    cherrypy.engine.start()
    cherrypy.engine.block()

