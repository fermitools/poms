#!/usr/bin/env python
import sys
import os
import os.path
import socket
import atexit
from textwrap import dedent
import io
import urllib.parse
import argparse
import logging
import logging.config
from markupsafe import Markup

import cherrypy
from cherrypy.process import plugins

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
import sqlalchemy.exc

from prometheus_client import make_wsgi_app

from poms.webservice.poms_model import Experimenter, ExperimentsExperimenters, Experiment
from poms.webservice.get_user import get_user
from poms.webservice import poms_service
from poms.webservice import jobsub_fetcher
from poms.webservice import samweb_lite
from poms.webservice import DMRService
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
        section = self.app.config["Databases"]
        db = section["db"]
        dbuser = section["dbuser"]
        dbhost = section["dbhost"]
        dbport = section["dbport"]
        db_path = "postgresql://%s:@%s:%s/%s" % (dbuser, dbhost, dbport, db)
        self.sa_engine = create_engine(
            db_path,
            echo=False,
            echo_pool=False,
            pool_size=40,  # 40 connections total in pool
            pool_recycle=600,  # recycle after 10 minutes
            pool_pre_ping=True,  # check connections before using
        )
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
        on a per thread basis so that you don's worry about
        concurrency on the session object itself.

        This tools binds a session to the engine each time
        a requests starts and commits/rollbacks whenever
        the request terminates.
        """
        cherrypy.Tool.__init__(self, "on_start_resource", self.bind_session, priority=20)
        self.session = scoped_session(sessionmaker(autoflush=True, autocommit=False))
        self.jobsub_fetcher = jobsub_fetcher.jobsub_fetcher(
            cherrypy.config.get("Elasticsearch", "cert"), cherrypy.config.get("Elasticsearch", "key")
        )
        self.samweb_lite = samweb_lite.samweb_lite()
        self.dmr_service = DMRService.DMRService() # Data-Dispatcher/Metacat/Rucio

    def _setup(self):
        cherrypy.Tool._setup(self)
        cherrypy.request.hooks.attach("on_end_resource", self.release_session, priority=80)

    def bind_session(self):
        cherrypy.engine.publish("bind", self.session)
        cherrypy.request.db = self.session
        cherrypy.request.jobsub_fetcher = self.jobsub_fetcher
        cherrypy.request.samweb_lite = self.samweb_lite
        cherrypy.request.dmr_service = self.dmr_service
        try:
            # Disabiling pylint false positives
            self.session.execute("SET SESSION lock_timeout = '300s';")  # pylint: disable=E1101
            self.session.execute("SET SESSION statement_timeout = '400s';")  # pylint: disable=E1101
            self.session.commit()  # pylint: disable=E1101
        except sqlalchemy.exc.UnboundExecutionError:
            # restart database connection
            cherrypy.engine.stop()
            cherrypy.engine.start()
            cherrypy.engine.publish("bind", self.session)
            cherrypy.request.db = self.session
            self.session = scoped_session(sessionmaker(autoflush=True, autocommit=False))
            self.session.execute("SET SESSION lock_timeout = '300s';")  # pylint: disable=E1101
            self.session.execute("SET SESSION statement_timeout = '400s';")  # pylint: disable=E1101
            self.session.commit()  # pylint: disable=E1101

    def release_session(self):
        # flushing here deletes it too soon...
        # cherrypy.request.jobsub_fetcher.flush()
        cherrypy.request.samweb_lite.flush()
        cherrypy.request.dmr_service.flush()
        cherrypy.request.db.close()
        cherrypy.request.db = None
        cherrypy.request.jobsub_fetcher = None
        cherrypy.request.samweb_lite = None
        cherrypy.request.dmr_service = None
        self.session.remove()


class SessionTool(cherrypy.Tool):
    # will be created for each request.

    def __init__(self):
        cherrypy.Tool.__init__(self, "before_request_body", self.establish_session, priority=90)

    # Here is how to add aditional hooks. Left as example

    def _setup(self):
        cherrypy.Tool._setup(self)
        cherrypy.request.hooks.attach("before_finalize", self.finalize_session, priority=10)

    def finalize_session(self):
        pass

    def establish_session(self):
        pass


def urlencode_filter(s):
    if isinstance(s, Markup):
        s = s.unescape()
    s = s.encode("utf8")
    s = urllib.parse.quote(s)
    return Markup(s)


def augment_params():
    e = cherrypy.request.db.query(Experimenter).filter(Experimenter.username == get_user()).first()
    
    if not e:
        exp =  "this experiment" 
        if len(cherrypy.url().split("/")) > 5:
            exp = cherrypy.url().split("/")[5]
        raise cherrypy.HTTPError(
            401, "%s is not a registered user for %s in the POMS database" % (get_user(), exp)
        )

    roles = ["analysis", "production-shifter", "production", "superuser"]

    root = cherrypy.request.app.root
    root.jinja_env.globals.update(
        dict(
            version=root.version,
            pomspath=root.path,
            docspath=root.docspath,
            sam_base = root.sam_base,
            landscape_base = root.landscape_base,
            fifemon_base = root.fifemon_base,
            hostname=socket.gethostname(),
            all_roles=roles,
            ExperimentsExperimenters=ExperimentsExperimenters,
        )
    )

    # logit.log("jinja_env.globals: {}".format(str(root.jinja_env.globals)))
    # # DEBUG
    root.jinja_env.filters["urlencode"] = urlencode_filter


def pidfile():
    pidfile = cherrypy.config.get("log.pidfile", None)
    pid = os.getpid()
    cherrypy.log.error("PID: %s" % pid)
    if pidfile:
        fd = open(pidfile, "w")
        fd.write("%s" % pid)
        fd.close()
        cherrypy.log.error("Pid File: %s" % pidfile)


def parse_command_line():
    parser = argparse.ArgumentParser()
    parser.add_argument("-cs", "--config", help="Filepath for POMS config file.")
    parser.add_argument("--use-wsgi", dest="use_wsgi", action="store_true", help="Run behind WSGI. (Default)")
    parser.add_argument("--no-wsgi", dest="use_wsgi", action="store_false", help="Run without WSGI.")
    parser.set_defaults(use_wsgi=True)
    args = parser.parse_args()
    return parser, args


# if __name__ == '__main__':
run_it = True
if run_it:

    configfile = "poms.ini"
    parser, args = parse_command_line()
    if args.config:
        configfile = args.config

    #
    # make %(HOME) and %(POMS_DIR) work in various sections
    #
    confs = dedent(
        """
       [DEFAULT]
       HOME="%(HOME)s"
       POMS_DIR="%(POMS_DIR)s"
    """
        % os.environ
    )

    cf = open(configfile, "r")
    confs = confs + cf.read()
    cf.close()

    try:
        cherrypy.config.update(io.StringIO(confs))
        # cherrypy.config.update(configfile)
    except IOError as mess:
        print(mess, file=sys.stderr)
        parser.print_help()
        raise SystemExit

    # add dowser in to monitor memory...
    # dapp = cherrypy.tree.mount(dowser.Root(), '/dowser')

    poms_instance = poms_service.PomsService()
    app = cherrypy.tree.mount(poms_instance, poms_instance.path, io.StringIO(confs))
    cherrypy.tree.graft(make_wsgi_app(), poms_instance.path + "/metrics")

    SAEnginePlugin(cherrypy.engine, app).subscribe()
    cherrypy.tools.db = SATool()
    cherrypy.tools.psess = SessionTool()

    cherrypy.tools.augment_params = cherrypy.Tool("before_handler", augment_params, None, priority=30)

    cherrypy.engine.unsubscribe("graceful", cherrypy.log.reopen_files)

    logging.config.dictConfig(logging_conf.LOG_CONF)
    section = app.config["POMS"]
    log_level = section["log_level"]
    logit.setlevel(log_level)
    logit.log("POMSPATH: %s" % poms_instance.path)
    pidfile()

    poms_instance.post_initialize()

    if args.use_wsgi:
        cherrypy.server.unsubscribe()

    cherrypy.engine.start()

    if not args.use_wsgi:
        cherrypy.engine.block()  # Disable built-in HTTP server when behind wsgi
        logit.log("Starting Cherrypy HTTP")

    application = cherrypy.tree

    # END
