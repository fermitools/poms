import cherrypy
import os
from .get_user import get_user
from .poms_model import Experimenter
from sqlalchemy import text
from configparser import ConfigParser
from . import DMRService

# h2. Ctx "Context" class


class Ctx:
    """
        Class to bundle up commonly used parameters into one "context"
        object: in 99% of cases the defaulted parameters will give
        correct values, but you can construct one overriding, say,
        username, role, and experiment if needed (i.e in wrapup_tasks
        when calling launch_jobs on behalf of users...) 

    """

    def __init__(
        self,
        username=None,
        role=None,
        experiment=None,
        db=None,
        config_get=None,
        headers_get=None,
        sam=None,
        HTTPError=cherrypy.HTTPError,
        HTTPRedirect=cherrypy.HTTPRedirect,
        tmin=None,
        tmax=None,
        tdays=None,
        dmr=None,
        web_config=None,
        function=None
    ):

        # functions take experiment and role, but we steal them out
        # of the url if they don't pass them in using pathv...

        pathv = cherrypy.request.path_info.split("/")

        self.db = db if db else cherrypy.request.db
        for pid in self.db.execute(text("select pg_backend_pid();")):
            self.backend_pid = pid
        self.config_get = config_get if config_get else cherrypy.config.get
        if not os.environ.get("WEB_CONFIG", None):
            os.environ["WEB_CONFIG"] = "/home/poms/poms/webservice/poms.ini"
        self.web_config = web_config if web_config else ConfigParser()
        self.web_config.read(os.environ["WEB_CONFIG"])
        self.headers_get = headers_get if headers_get else cherrypy.request.headers.get
        self.sam = sam if sam else cherrypy.request.samweb_lite
        self.experiment = (
            experiment if experiment else pathv[2] if len(pathv) >= 4 else cherrypy.request.params.get("experiment", None)
        )
        self.role = role if role else pathv[3] if len(pathv) >= 4 else cherrypy.request.params.get("role", None)

        self.username = username if username else get_user()
        rows = self.db.execute("select txid_current();")
        for r in rows:
            self.dbtransaction = r[0]
        rows.close()
        self.HTTPError = cherrypy.HTTPError
        self.HTTPRedirect = cherrypy.HTTPRedirect
        self.tmin = tmin
        self.tmax = tmax
        self.tdays = tdays
        self.experimenter_cache = None
            
        if self.experiment == None or self.role == None:
            e = self.get_experimenter()
            self.experiment = e.session_experiment
            self.role = e.session_role
            
        self.dmr_service = dmr if dmr else cherrypy.request.dmr_service
        self.dmr_service.update_config_if_needed(self.db,self.experiment, self.username, self.role)
        services = self.dmr_service.services_logged_in
        if services:
            if not services["data_dispatcher"]:
                self.dmr_service.begin_services("data_dispatcher")
            if not services["metacat"]:
                self.dmr_service.begin_services("metacat")
        else:
            self.dmr_service.begin_services()
        cherrypy.request.dmr_service = self.dmr_service

    def get_experimenter(self):
        if not self.experimenter_cache:
            self.experimenter_cache = self.db.query(Experimenter).filter(Experimenter.username == self.username).first()
        return self.experimenter_cache
    
    def get_experimenter_id(self):
        if not self.experimenter_cache:
            self.experimenter_cache = self.db.query(Experimenter).filter(Experimenter.username == self.username).first()
        return self.experimenter_cache.experimenter_id

    def __repr__(self):
        res = ["<Ctx:"]
        for k in self.__dict__:
            if k in ("db", "config_get", "headers_get", "sam", "HTTPError", "HTTPRedirect"):
                res.append("'%s': '...'," % k)
            else:
                res.append("'%s': '%s'," % (k, self.__dict__[k]))
        res.append(">")
        return " ".join(res)
