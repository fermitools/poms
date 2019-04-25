import cherrypy
from .get_user import get_user
from .poms_model import Experimenter


# h2. Ctx "Context" class

class Ctx:
    '''
        Class to bundle up commonly used parameters into one "context"
        object: in 99% of cases the defaulted parameters will give
        correct values, but you can construct one overriding, say,
        username, role, and experiment if needed (i.e in wrapup_tasks
        when calling launch_jobs on behalf of users...) 

    '''

    def __init__(self,
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
            tdays=None
        ):

        # functions take experiment and role, but we steal them out
        # of the url if they don't pass them in using pathv...

        pathv = cherrypy.request.path_info.split('/')

        self.db = db if db else cherrypy.request.db
        self.config_get = config_get if config_get else cherrypy.config.get
        self.headers_get = headers_get if headers_get else cherrypy.request.headers.get
        self.sam = sam if sam else cherrypy.request.samweb_lite
        self.experiment = experiment if experiment else pathv[2] if len(pathv) >=4 else None
        self.role = role if role else pathv[3] if len(pathv) >= 4 else None
        self.username = username if username else get_user()
        self.HTTPError = cherrypy.HTTPError
        self.HTTPRedirect = cherrypy.HTTPRedirect
        self.tmin = tmin
        self.tmax = tmax
        self.tdays = tdays
        self.experimenter_cache = None

    def get_experimenter(self):
        if not self.experimenter_cache:
            self.experimenter_cache = self.db.query(Experimenter).filter(Experimenter.username == self.username).first()
        return self.experimenter_cache

