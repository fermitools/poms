import os, os.path
import argparse
from logging import handlers, DEBUG

import cherrypy
from cherrypy.process import wspbus, plugins
 
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

import poms_service
 
class SAEnginePlugin(plugins.SimplePlugin):
    def __init__(self, bus):
        """
        The plugin is registered to the CherryPy engine and therefore
        is part of the bus (the engine *is* a bus) registery.
 
        We use this plugin to create the SA engine. At the same time,
        when the plugin starts we create the tables into the database
        using the mapped class of the global metadata.
 
        Finally we create a new 'bind' channel that the SA tool
        will use to map a session to the SA engine at request time.
        """
        plugins.SimplePlugin.__init__(self, bus)
        self.sa_engine = None
        self.bus.subscribe("bind", self.bind)
 
    def start(self):
        db = cherrypy.config.get("db")
        dbuser = cherrypy.config.get("dbuser")
        dbpass = cherrypy.config.get("dbpass")
        dbhost = cherrypy.config.get("dbhost")
        dbport = cherrypy.config.get("dbport")
        db_path = "postgresql://%s:%s@%s:%s/%s" % (dbuser, dbpass, dbhost, dbport, db)
        self.sa_engine = create_engine(db_path, echo=True)
 
    def stop(self):
        if self.sa_engine:
            self.sa_engine.dispose()
            self.sa_engine = None
 
    def bind(self, session):
        session.configure(bind=self.sa_engine)
 
class SATool(cherrypy.Tool):
    def __init__(self):
        """
        The SA tool is responsible for associating a SA session
        to the SA engine and attaching it to the current request.
        Since we are running in a multithreaded application,
        we use the scoped_session that will create a session
        on a per thread basis so that you don't worry about
        concurrency on the session object itself.
 
        This tools binds a session to the engine each time
        a requests starts and commits/rollbacks whenever
        the request terminates.
        """
        cherrypy.Tool.__init__(self, 'on_start_resource',
                               self.bind_session,
                               priority=20)
        self.session = scoped_session(sessionmaker(autoflush=True,
                                                  autocommit=False))
 
    def _setup(self):
        cherrypy.Tool._setup(self)
        cherrypy.request.hooks.attach('on_end_resource',
                                      self.release_session,
                                      priority=80)
    def bind_session(self):
        cherrypy.engine.publish('bind', self.session)
        cherrypy.request.db = self.session
 
    def release_session(self):
        cherrypy.request.db = None
        self.session.remove()

class SessionTool(cherrypy.Tool):
        # Something must be set in the sessionotherwise a unique session 
        # will be created for each request. 
    def __init__(self):
        cherrypy.Tool.__init__(self, 'before_request_body',
                               self.force_session,
                               priority=50)
        
    def force_session(self):
        cherrypy.session['id'] = cherrypy.session._id
 
def set_rotating_log(app):
    ''' recipe  for a rotating log file...'''
    # Remove the regular file handlers
    app.log.error_file = "" 
    app.log.access_file = ""

    maxBytes = cherrypy.config.get("log.rot_maxBytes",10000000)
    backupCount = cherrypy.config.get("log.rot_backupCount", 1000)

    # Create and add rotating file handlers
    for x in ['error', 'access']:
        fname = getattr(cherrypy.log,"rot_%s_file" % x, "error.log")
        h = handlers.RotatingFileHandler(fname, 'a',maxBytes,backupCount)
        h.setLevel(DEBUG)
        h.setFormatter(cherrypy._cplogging.logfmt)
        getattr(cherrypy.log, '%s_log' % x).addHandler(h)

def pidfile():
    pidfile = cherrypy.config.get("log.pidfile",None)
    pid = os.getpid()
    cherrypy.log("PID: %s" % pid)
    if pidfile:
        fd = open(pidfile,'w')
        fd.write("%s" % pid )
        fd.close()
        cherrypy.log("Pid File: %s" % pidfile)

def parse_command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help="Filespec for POMS config file.")
    parser.add_argument('-p', '--password', help="Filespec for POMS password file.")
    args = parser.parse_args()
    return parser,args

if __name__ == '__main__':

    config = { '/' : {
                      'tools.db.on': True,
                      'tools.psess.on': True,
                      'tools.staticdir.root': os.path.abspath(os.getcwd()),
		      'tools.sessions.on': True,
                      'tools.sessions.timeout': 60,
                     },
               '/static' : {
                      'tools.staticdir.on': True,
                      'tools.staticdir.dir': './static'
                      },
               }

    configfile = "poms.ini"
    dbasefile  = "passwd.ini"
    parser,args = parse_command_line()
    if args.config:
        configfile = args.config
    if args.password:
        dbasefile = args.password
    try:
        cherrypy.config.update(configfile)
        cherrypy.config.update(dbasefile)
    except IOError, mess:
        print mess
        parser.print_help()
        raise SystemExit
    path = cherrypy.config.get("path")
    if path == None:
       path = "/poms"

    pidfile()
    SAEnginePlugin(cherrypy.engine).subscribe()
    cherrypy.tools.db = SATool()
    cherrypy.tools.psess = SessionTool()
    print "*"*80
    print dir(cherrypy.tools)
    print "*"*80
    app = cherrypy.tree.mount(poms_service.poms_service(), path, configfile)
    app.merge(config)
    set_rotating_log(app)

    # Start SSL Server if in config file
    ssl_host = cherrypy.config.get("server.socket_host",None)
    ssl_port = cherrypy.config.get("sslserver.socket_port",None)
    ssl_certificate = cherrypy.config.get("sslserver.ssl_certificate",None)
    ssl_private_key = cherrypy.config.get("sslserver.ssl_private_key",None)
    if ssl_port is None or ssl_certificate is None or ssl_private_key is None:
       cherrypy.log("**** SSL Server is not configured for running.")
    else:
        sslserver = cherrypy._cpserver.Server()
        sslserver.ssl_module = 'builtin'
        sslserver._socket_host = cherrypy.config.get("server.socket_host",None)
        sslserver.socket_port = cherrypy.config.get("sslserver.socket_port",None)
        sslserver.ssl_certificate = cherrypy.config.get("sslserver.ssl_certificate",None)
        sslserver.ssl_private_key = cherrypy.config.get("sslserver.ssl_private_key",None)
        sslserver.subscribe()
    
    cherrypy.engine.start()
    cherrypy.engine.block()
