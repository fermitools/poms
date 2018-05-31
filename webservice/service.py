#!/usr/bin/env python
from collections import deque

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

from poms.webservice.poms_model import Experimenter, ExperimentsExperimenters, Experiment
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
        on a per thread basis so that you don's worry about
        concurrency on the session object itself.

        This tools binds a session to the engine each time
        a requests starts and commits/rollbacks whenever
        the request terminates.
        """
        cherrypy.Tool.__init__(self, 'on_start_resource',
                               self.bind_session,
                               priority=20)
        self.session = scoped_session(sessionmaker(autoflush=True, autocommit=False))
        self.jobsub_fetcher = jobsub_fetcher.jobsub_fetcher(cherrypy.config.get('elasticsearch_cert'),
                                                            cherrypy.config.get('elasticsearch_key'))
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
        self.session.execute("SET SESSION lock_timeout = '360s';")
        self.session.execute("SET SESSION statement_timeout = '240s';")
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

    def __init__(self, experimenter_id=None, first_name=None, last_name=None,
                 username=None, authorization=None, session_experiment=None, session_role=None, root=None):
        self.experimenter_id = experimenter_id
        self.first_name = first_name
        self.last_name = last_name
        self.username = username
        self.authorization = authorization
        self.session_role = session_role
        self.session_experiment = session_experiment
        self.root = root or False

    def get_allowed_roles(self):
        """
        Returns the list of allowed roles for the user/experiment in the session
        """
        exp = self.authorization.get(self.session_experiment, {'roles':[]})
        return exp.get('roles')

    def is_authorized(self, campaign=None):
        """
        Who can change a campagin/any object with properties fo experiment, creator and creator_role :
                The creator can change her/his own campaign_stages with the same role used to create the campaign.
                The root can change any campaign_stages.
                The coordinator can change any campaign_stages that in the same experiment as the coordinator.
                Anyone with a production role can change a campaign created with a production role.
        :param campaign: Name of the campaign.
        :return: True or False
        """
        if not campaign:
            return False
        if self.is_root():
            return True
        elif self.is_coordinator() and self.session_experiment == campaign.experiment:
            return True
        elif self.is_production and self.session_experiment == campaign.experiment \
                and campaign.creator_role == "production":
            return True
        elif campaign.creator == self.experimenter_id and campaign.experiment == self.session_experiment \
                and campaign.creator_role == self.session_role:
            return True
        else:
            return False

    def is_root(self):
        return self.root

    def is_coordinator(self):
        if self.session_role == 'coordinator':
            return True
        return False

    def is_production(self):
        if self.session_role == "production":
            return True
        return False

    def is_analysis(self):
        if self.session_role == "analysis":
            return True
        return False

    def user_authorization(self):
        """
        Returns a dictionary of dictionaries.  Where:
          {'experiment':
            {'roles': [analysis,production,coordinator,root]   # Ordered list of roles the user plays in the experiment
            },
          }
        """
        return self.authorization

    def roles(self):
        """
        Returns a list of roles for this user/experiment
        """
        return self.authorization.get(self.session_experiment).get('role')

    def __str__(self):
        return "%s %s %s" % (self.first_name, self.last_name, self.username)


class SessionTool(cherrypy.Tool):
        # will be created for each request.

    def __init__(self):
        cherrypy.Tool.__init__(self, 'before_request_body',
                               self.establish_session,
                               priority=90)


    # Here is how to add aditional hooks. Left as example
    def _setup(self):
        cherrypy.Tool._setup(self)
        cherrypy.request.hooks.attach('before_finalize',
                                      self.finalize_session,
                                      priority=10)

    def finalize_session(self):
        pass

    def establish_session(self):

        if cherrypy.session.get('id', None):
            #logit.log("EXISTING SESSION: %s" % str(cherrypy.session['experimenter']))
            return

        logit.log("establish_session startup -- mengel")


        cherrypy.session['id']              = cherrypy.session.originalid  #The session ID from the users cookie.
        cherrypy.session['X-Forwarded-For'] = cherrypy.request.headers.get('X-Forwarded-For', None)
        # someone had all these SHIB- headers mixed case, which is not
        # how they are on fermicloud045 or on pomsgpvm01...
        cherrypy.session['Remote-Addr']     = cherrypy.request.headers.get('Remote-Addr', None)
        cherrypy.session['X-Shib-Userid']   = cherrypy.request.headers.get('X-Shib-Userid', None)

        experimenter = None
        username = None

        if cherrypy.request.headers.get('X-Shib-Userid', None):
            logit.log("Shib-Userid case")
            username = cherrypy.request.headers['X-Shib-Userid']
            experimenter = None
            experimenter = (cherrypy.request.db.query(Experimenter)
                            .filter(ExperimentsExperimenters.active == True)
                            .filter(Experimenter.username == username)
                            .first()
                           )

        elif cherrypy.config.get('standalone_test_user', None):
            logit.log("standalone_test_user case")
            username = cherrypy.config.get('standalone_test_user', None)
            experimenter = (cherrypy.request.db.query(Experimenter)
                            .filter(ExperimentsExperimenters.active == True)
                            .filter(Experimenter.username == username)
                            .first()
                           )

        if not experimenter:
            raise cherrypy.HTTPError(401, 'POMS account does not exist.  To be added you must registered in VOMS.')

        e = cherrypy.request.db.query(Experimenter).filter(Experimenter.username == username).one()

        # Retrieve what experiments a user is ACTIVE in and the level of access right to each experiment.
        # and construct security role data on each active experiment
        # Not that root is a FLAG not a ROLE. Take care not to make it a role.
        roles = ['analysis', 'production', 'coordinator']  #Ordered by how they will appear in the form dropdown.
        exps = {}
        e2e = None
        if e.root is True:
            e2e = (cherrypy.request.db.query(Experiment))
            for row in e2e:
                exps[row.experiment] = {'roles':roles}
        else:
            e2e = (cherrypy.request.db.query(ExperimentsExperimenters)
                   .filter(ExperimentsExperimenters.experimenter_id == e.experimenter_id)
                   .filter(ExperimentsExperimenters.active == True))
            for row in e2e:
                position = 0
                if e.root is True:
                    position = 3
                elif row.role == 'coordinator':
                    position = 3
                elif row.role == 'production':
                    position = 2
                else: #analysis
                    position = 1
                exps[row.experiment] = {'roles':roles[:position]}

        if e.session_experiment == "":
            # don's choke on blank session_experiment, just pick one...
            e.session_experiment = next(iter(exps.keys()))

        cherrypy.session['experimenter'] = SessionExperimenter(e.experimenter_id,
                                                               e.first_name, e.last_name, e.username, exps,
                                                               e.session_experiment, e.session_role, e.root)

        cherrypy.session.save()
        cherrypy.request.db.query(Experimenter).filter(Experimenter.username == username).update({'last_login': datetime.now(utc)})
        cherrypy.request.db.commit()
        cherrypy.log.error("New Session: %s %s %s %s %s" % (cherrypy.request.headers.get('X-Forwarded-For', 'Unknown'),
                                                            cherrypy.session['id'],
                                                            experimenter.username if experimenter else 'none',
                                                            experimenter.first_name if experimenter else 'none',
                                                            experimenter.last_name if experimenter else 'none'))


#
# non ORM class to cache an experiment
#
class SessionExperiment():
    def __init__(self, exp):
        self.experiment = exp.experiment
        self.name = exp.name
        self.logbook = exp.logbook
        self.snow_url = exp.snow_url
        self.restricted = exp.restricted


def augment_params():
    experiment = cherrypy.session.get('experimenter').session_experiment
    exp_obj = cherrypy.request.db.query(Experiment).filter(Experiment.experiment == experiment).first()
    current_experimenter = cherrypy.session.get('experimenter')
    root = cherrypy.request.app.root
    root.jinja_env.globals.update(dict(exp_obj=SessionExperiment(exp_obj),
                                       current_experimenter=current_experimenter,
                                       user_authorization=current_experimenter.user_authorization(),
                                       session_experiment=current_experimenter.session_experiment,
                                       session_role=current_experimenter.session_role,
                                       allowed_roles=current_experimenter.get_allowed_roles(),
                                       version=root.version,
                                       pomspath=root.path))
    # logit.log("jinja_env.globals: {}".format(str(root.jinja_env.globals)))   # DEBUG


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
    parser.add_argument('-cs', '--config', help="Filepath for POMS config file.")
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

    poms_instance = poms_service.PomsService()
    app = cherrypy.tree.mount(poms_instance, poms_instance.path, StringIO(confs))
    # app = cherrypy.tree.mount(pomsInstance, pomsInstance.path, configfile)


    SAEnginePlugin(cherrypy.engine, app).subscribe()
    cherrypy.tools.db = SATool()
    cherrypy.tools.psess = SessionTool()

    cherrypy.tools.augment_params = cherrypy.Tool('before_handler', augment_params, None, priority=30)

    cherrypy.engine.unsubscribe('graceful', cherrypy.log.reopen_files)

    logging.config.dictConfig(logging_conf.LOG_CONF)
    section = app.config['POMS']
    log_level = section["log_level"]
    logit.setlevel(log_level)
    logit.log("POMSPATH: %s" % poms_instance.path)
    pidfile()

    poms_instance.post_initialize()

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
