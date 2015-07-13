import cherrypy
import sys
import json
from sqlalchemy import Column, Integer, Sequence, String, DateTime, ForeignKey, and_, or_, create_engine, null
from sqlalchemy.orm import sessionmaker, scoped_session, joinedload, aliased
from model.poms_model import *
import logging
import logging.handlers
from datetime import datetime, tzinfo,timedelta
import traceback

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
            res = func(*args, session = session, **kwargs)
        except Exception as e:
            cherrypy.log(traceback.format_exc())
            if session:
                # probably a database error needing rollback
                session.rollback()
                raise cherrypy.HTTPError(502,"Database error: %s " % e)
        finally:
            dbparts.SessionMaker.remove()
        return res
    return newfunc

class poms_service:

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
            # start downtime
            d = ServiceDowntime()
            d.service = s
            d.downtime_started = datetime.now(utc)
            d.downtime_ended = None
            session.add(d)

        if s.status != status and status == "ok":
            # end downtime
            d = session.query(ServiceDowntime).filter(SessionDowntime.service_id == s.id , Session.downtime_ended == null()).first()
            if d:
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
        res = ["<ul>"]
        prev = None
        prevparent = None
        p = session.query(Service).filter(Service.name == under).first()
        for s in session.query(Service).filter(Service.parent_service_id == p.service_id).all():

            if s.host_site:
                 url = s.host_site
            else:
                 url = "./service_status?under=%s" % s.name

            res.append("<li> %s -- %s via <a href='%s'>here</a>" % (s.name,s.status, url))
        res.append("</ul>")
        return "\n".join(res)

    @cherrypy.expose
    @withsession
    def create_task(self, experiment, taskdef, params, input_dataset, output_dataset, waitingfor = None , session = None):
         td = session.query(TaskDefinition).filter(TaskDefinition.name == taskdef).first()
         c = session.query(Campaign).filter(Campaign.task_definition_id == td.task_definition_id).first()
         t = Task()
         t.task_definition_id = td.task_definition_id
         t.campaign_id = c.campaign_id
         t.task_order = 0
         t.input_datset = input_dataset
         t.output_dataset = output_datset
         t.waitingfor = waitingfor
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
         j.updated = datetime.now(uct)
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

