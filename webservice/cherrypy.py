import cherrypy
import sys
import json
from sqlalchemy import engine
from sqlalchemy.orm import sessionmaker, scoped_session
from model.poms_model import *
import logging
import logging.handlers

class dbparts:
    # keep our database connection/session bits here
    Engine = None
    SessionMaker = None

    def __init__(self,dbpath = "postgres://user:pass@cspgsdev.fnal.gov:5437/pomsdev"):
        dbparts.Engine = engine(dbpath)
        dbparts.SessionMaker = scoped_session(sessionmaker(bind = dbparts.Engine))

def withsession(func):
    ''' decorator to call a function with a scoped-session with cleanup'''
    def newfunc(*args, **kwargs):
        try:
            session = dbparts.SessionMaker()
            res = func(*args, session = session, **kwargs)
        except Exception as e:
            if session:
                # probably a database error needing rollback
                session.rollback()
                raise cherrypy.HTTPError(502,"Database error: %s " % e)
        finally:
            SessionMaker.remove()
        return res
    return newfunc

class poms_service:

    @cherrypy.expose
    @withsession
    def update_service(self, name, parent, status, session = None):
        s = session.query(Service).filter(Service.name = name).one()
        if not s:
            s = Service()
            s.name = name
        p = session.query(Service).filter(Service.name = parent).one()
        if not p:
            p = Service()
            p.name = parent
            session.add(p)
        s.parent = p
        if s.status == "ok" and status == "bad":
            # start downtime
            d = ServiceDowntime()
            d.service = s
            d.downtime_started = DateTime.now()
            d.downtime_ended = None
            session.add(d)
        if s.status == "bad" and status == "ok":
            # end downtime
            d = session.query(ServiceDowntime).filter(SessionDowntime.service = s,Session.downtime_ended = None).one()
            d.downtime_ended = DateTime.now()
            session.add(d)
        s.status = status
        session.add(s)
        session.commit()
        return "Ok."
        
    @cherrypy.expose
    @withsession
    def service_status(self, session = None):
        res = ["<ul>"]
        for s in session.query(Service).all():
            res.append("<li> %s -- %s" % (s.name,s.status)
        res.append("</ul>")
        return "\n".join(res)

def set_rotating_log(app):
     ''' recipe  for a rotating log file...'''
     maxBytes=100000000
     keepcount=10
     for x in 'error', 'access':
         fname = getattr(app.log, '%s_file' % x , None)
         if not fname:
             fname = '%s.log' % x
         h = logging.handlers.RotatingFileHandler(fname, 'a',maxBytes,keepcount)
         h.setLevel(logging.DEBUG)
         h.setFormatter(cherrypy._cplogging.logfmt)
         getattr(app.log, '%s_log' % x).addHandler(h)

if __name__ == '__main__':

    configfile = "registry.conf"

    if len(sys.argv) > 1 and sys.argv[1] == '-c':
        configfile = sys.argv[2]
        sys.argv = sys.argv[2:]

    cherrypy.config.update(configfile)


    # normal operating mode:
    db = cherrypy.config.get("db")
    path = cherrypy.config.get("path")
    dbp = dbparts(db)
    #print "got db of %s and path of %s" % (db, path)
    app = cherrypy.tree.mount(service_registry(db), path , configfile)
    set_rotating_log(app)
    cherrypy.engine.start()
    cherrypy.engine.block()

