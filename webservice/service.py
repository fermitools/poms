#!/usr/bin/env python

import sys
import os
from datetime import datetime
from utc import utc
import atexit
from textwrap import dedent
from io import StringIO
import dowser
import poms.webservice.pomscache as pomscache

#
#if os.environ.get("SETUP_POMS", "") == "":
#    sys.path.insert(0, os.environ.get('SETUPS_DIR', os.environ.get('HOME') + '/products'))
#    import setups
#    ups = setups.setups()
#    ups.use_package("poms", "", "SETUP_POMS")

from poms.webservice.poms_model import Experimenter, ExperimentsExperimenters
# from sqlalchemy.orm import subqueryload, joinedload, contains_eager
import os.path
import argparse
import logging
import logging.config
import cherrypy
from cherrypy.process import wspbus, plugins

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from poms.webservice import poms_service

from poms.webservice import jobsub_fetcher
from poms.webservice import samweb_lite
from poms.webservice import logging_conf
from poms.webservice import logit


class SAEnginePlugin(plugins.SimplePlugin):
    def __init__(self, bus, app):
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
        self.app = app
        self.sa_engine = None
        self.bus.subscribe("bind", self.bind)


    def destroy(self):
        cherrypy.engine.exit()
        print("destroy worker")


    def start(self):
        section = self.app.config['Databases']
        db = section["db"]
        dbuser = section["dbuser"]
        dbhost = section["dbhost"]
        dbport = section["dbport"]
        db_path = "postgresql://%s:@%s:%s/%s" % (dbuser, dbhost, dbport, db)
        self.sa_engine = create_engine(db_path, echo=False, echo_pool=False)
        atexit.register(self.destroy)

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
        self.session = scoped_session(sessionmaker(autoflush=True, autocommit=False))
        self.jobsub_fetcher = jobsub_fetcher.jobsub_fetcher()
        self.samweb_lite = samweb_lite.samweb_lite()

    def _setup(self):
        cherrypy.Tool._setup(self)
        cherrypy.request.hooks.attach('on_end_resource',
                                      self.release_session,
                                      priority=80)

    def bind_session(self):
        cherrypy.engine.publish('bind', self.session)
        cherrypy.request.db = self.session
        cherrypy.request.jobsub_fetcher = self.jobsub_fetcher
        cherrypy.request.samweb_lite = self.samweb_lite
        self.session.execute("SET SESSION lock_timeout = '60s';")
        self.session.execute("SET SESSION statement_timeout = '120s';")
        self.session.commit()

    def release_session(self):
        # flushing here deletes it too soon...
        #cherrypy.request.jobsub_fetcher.flush()
        cherrypy.request.samweb_lite.flush()
        cherrypy.request.db.close()
        cherrypy.request.db = None
        cherrypy.request.jobsub_fetcher = None
        cherrypy.request.samweb_lite = None
        self.session.remove()


class SessionExperimenter(object):

    def __init__(self, experimenter_id=None, first_name=None, last_name=None, username=None, authorized_for=None, **kwargs):
        self.experimenter_id = experimenter_id
        self.first_name = first_name
        self.last_name = last_name
        self.username = username
        self.authorized_for = authorized_for
        self.extra = kwargs
        self.valid_ip_list = []     # FIXME

    def _is_valid_ip(self, ip, iplist):
        for x in iplist:
            if ip.startswith(x):
                return True
        return False

    def is_authorized(self, experiment):
        # Root is authorized for all experiments
        if self.is_root():
            return True

        ra  = cherrypy.session['Remote-Addr']
        xff = cherrypy.session['X-Forwarded-For']
        if self._is_valid_ip(ra, self.valid_ip_list):
            return 1
        if ra.startswith('131.225.80.'):
            return 1
        if ra == '127.0.0.1' and xff and xff.startswith('131.225.67'):
            # case for fifelog agent..
            return 1
        if ra != '127.0.0.1' and xff and xff.startswith('131.225.80'):
            # case for jobsub_q agent (currently on bel-kwinith...)
            return 1
        if ra == '127.0.0.1' and xff is None:
            # case for local agents
            return 1

        return self.authorized_for.get(experiment, False)

    def is_root(self):
        if cherrypy.session['Remote-Addr'] in ['127.0.0.1', '131.225.80.97'] and cherrypy.session['X-Forwarded-For'] is None:
            # case for local agents
            return True
        return self.authorized_for.get('root', False)

    def __str__(self):
        return "%s %s %s" % (self.first_name, self.last_name, self.username)


class SessionTool(cherrypy.Tool):
        # Something must be set in the sessionotherwise a unique session
        # will be created for each request.
    def __init__(self):
        cherrypy.Tool.__init__(self, 'before_request_body',
                               self.establish_session,
                               priority=90)

    # Here is how to add aditional hooks. Left as example
    #def _setup(self):
    #    cherrypy.Tool._setup(self)
    #    cherrypy.request.hooks.attach('before_request_body',
    #                                  self.your_method,
    #                                  priority=90)

    def establish_session(self):
        logit.log("mengel -- in establish_session")

        if cherrypy.session.get('id', None):
            logit.log("mengel -- bailing no session id")
            #logit.log("EXISTING SESSION: %s" % str(cherrypy.session['experimenter']))
            return

        cherrypy.session['id']              = cherrypy.session.originalid  #The session ID from the users cookie.
        cherrypy.session['X-Forwarded-For'] = cherrypy.request.headers.get('X-Forwarded-For', None)
        # someone had all these SHIB- headers mixed case, which is not
        # how they are on fermicloud045 or on pomsgpvm01...
        cherrypy.session['Remote-Addr']     = cherrypy.request.headers.get('Remote-Addr', None)
        cherrypy.session['X-Shib-Userid']   = cherrypy.request.headers.get('X-Shib-Userid', None)

        username = None
        logit.log("mengel -- checking Shib-Userid")
        if cherrypy.request.headers.get('X-Shib-Userid', None):
            logit.log("mengel --have Shib-Userid")
            username = cherrypy.request.headers['X-Shib-Userid']
            experimenter = None
            experimenter = (cherrypy.request.db.query(Experimenter)
                            .filter(ExperimentsExperimenters.active == True)
                            .filter(Experimenter.username == username)
                            .first()
                            )
        else:
            # being me for testing...
            
            logit.log("faking user...")
            experimenter = cherrypy.request.db.query(Experimenter).filter(Experimenter.username == os.environ['USER']).first()
            # experimenter = None

        if not experimenter and cherrypy.request.headers.get('X-Shib-Userid', None):
            username = cherrypy.request.headers['X-Shib-Userid']
            experimenter = Experimenter(
                                first_name=cherrypy.request.headers['X-Shib-Name-First'],
                                last_name=cherrypy.request.headers['X-Shib-Name-Last'],
                                username=username
                            )
            cherrypy.request.db.add(experimenter)
            cherrypy.request.db.flush()
            e2e = ExperimentsExperimenters(
                        experimenter_id=experimenter.experimenter_id,
                        experiment='public',
                        active=True
                    )
            cherrypy.request.db.add(e2e)
            cherrypy.request.db.commit()

        e = cherrypy.request.db.query(Experimenter).filter(Experimenter.username == username).all()
        if len(e):
            e2e = cherrypy.request.db.query(ExperimentsExperimenters).filter(ExperimentsExperimenters.experimenter_id==e[0].experimenter_id)
        else:
            e2e = []
        exps = {}
        for row in e2e:
            exps[row.experiment] = row.active
        if len(e):
            extra = {'selected': list(exps.keys())}
            cherrypy.session['experimenter'] = SessionExperimenter(e[0].experimenter_id,
                                                                   e[0].first_name, e[0].last_name, e[0].username, exps, **extra)
        else:
            cherrypy.session['experimenter'] = SessionExperimenter("anonymous", "", "", "", {})
        cherrypy.request.db.query(Experimenter).filter(Experimenter.username == username).update({'last_login': datetime.now(utc)})
        cherrypy.request.db.commit()
        cherrypy.log.error("NEW SESSION: %s %s %s %s %s" % (cherrypy.request.headers.get('X-Forwarded-For', 'Unknown'),
                                                            cherrypy.session['id'],
                                                            experimenter.username if experimenter else 'none',
                                                            experimenter.first_name if experimenter else 'none',
                                                            experimenter.last_name if experimenter else 'none'))


def pidfile():
    pidfile = cherrypy.config.get("log.pidfile", None)
    pid = os.getpid()
    cherrypy.log.error("PID: %s" % pid)
    if pidfile:
        fd = open(pidfile, 'w')
        fd.write("%s" % pid)
        fd.close()
        cherrypy.log.error("Pid File: %s" % pidfile)


def parse_command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help="Filepath for POMS config file.")
    parser.add_argument('--use-wsgi', dest='use_wsgi', action='store_true', help="Run behind WSGI. (Default)")
    parser.add_argument('--no-wsgi', dest='use_wsgi', action='store_false', help="Run without WSGI.")
    parser.set_defaults(use_wsgi=True)
    args = parser.parse_args()
    return parser, args

# if __name__ == '__main__':
if True:

    configfile = "poms.ini"
    parser, args = parse_command_line()
    if args.config:
        configfile = args.config

    #
    # make %(HOME) and %(POMS_DIR) work in various sections
    #
    confs = dedent("""
       [DEFAULT]
       HOME="%(HOME)s"
       POMS_DIR="%(POMS_DIR)s"
    """ % os.environ)

    cf = open(configfile,"r")
    confs = confs + cf.read()
    cf.close

    try:
        cherrypy.config.update(StringIO(confs))
        #cherrypy.config.update(configfile)
    except IOError as mess:
        print(mess, file=sys.stderr)
        parser.print_help()
        raise SystemExit

    # add dowser in to monitor memory...
    dapp = cherrypy.tree.mount(dowser.Root(), '/dowser')

    pomsInstance = poms_service.poms_service()
    app = cherrypy.tree.mount(pomsInstance, pomsInstance.path, StringIO(confs))
    #app = cherrypy.tree.mount(pomsInstance, pomsInstance.path, configfile)


    SAEnginePlugin(cherrypy.engine, app).subscribe()
    cherrypy.tools.db = SATool()
    cherrypy.tools.psess = SessionTool()

    cherrypy.engine.unsubscribe('graceful', cherrypy.log.reopen_files)

    logging.config.dictConfig(logging_conf.LOG_CONF)
    section = app.config['POMS']
    log_level = section["log_level"]
    logit.setlevel(log_level)
    logit.log("POMSPATH: %s" % pomsInstance.path)
    pidfile()

    pomsInstance.post_initalize()

    if args.use_wsgi:
        cherrypy.server.unsubscribe()

    cherrypy.engine.start()

    if not args.use_wsgi:
        cherrypy.engine.block()		# Disable built-in HTTP server when behind wsgi
        logit.log("Starting Cherrypy HTTP")

    application = cherrypy.tree
    if 0:
        # from paste.exceptions.errormiddleware import ErrorMiddleware
        # application = ErrorMiddleware(application, debug=True)
        pass
    if 0:
        # from repoze.errorlog import ErrorLog
        # application = ErrorLog(application, channel=None, keep=20, path='/__error_log__', ignored_exceptions=())
        pass
    # END
