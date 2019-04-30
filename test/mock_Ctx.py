"""
   mocked up version of Ctx class from webservice
   used in testing
"""
from webservice.poms_model import Experimenter
import DBHandle
import utils


def get_user():
    return "poms"


class mockHTTPError(Exception):
    def __init__(self, **kwargs):
        pass


class mockHTTPRedirect(Exception):
    def __init__(self, **kwargs):
        pass


# h2. Ctx "Context" class

fakeheaders = {
    "Remote-Addr": "127.0.0.1",
    "Host": "127.0.0.1:8080",
    "Cache-Control": "max-age=0",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML,like Gecko) Chrome/74.0.3729.108 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
    "Referer": "https://pingprod.fnal.gov:9031/",
    "Accept-Encoding": "gzip, deflate, br",
    "X-Shib-Userid": "mengel",
    "X-Shib-Email": "mengel@fnal.gov",
    "X-Shib-Name-Last": "Mengel",
    "X-Shib-Name-First": "Marc",
    "X-Forwarded-Proto": "https",
    "X-Forwarded-For": "131.225.80.97",
    "X-Forwarded-Host": "pomsgpvm01.fnal.gov",
    "X-Forwarded-Server": "pomsgpvm01.fnal.gov",
    "Connection": "Keep-Alive",
}


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
        HTTPError=mockHTTPError,
        HTTPRedirect=mockHTTPRedirect,
        tmin=None,
        tmax=None,
        tdays=None,
    ):

        # functions take experiment and role, but we steal them out
        # of the url if they don't pass them in using pathv...

        pathv = "/poms/samdev/production/op"

        self.db = db if db else DBHandle.DBHandle().get()
        self.config_get = config_get if config_get else utils.get_config().get
        self.headers_get = headers_get if headers_get else fakeheaders.get
        self.sam = sam if sam else None
        self.experiment = experiment if experiment else pathv[2] if len(pathv) >= 4 else None
        self.role = role if role else pathv[3] if len(pathv) >= 4 else None
        self.username = username if username else get_user()
        self.HTTPError = mockHTTPError
        self.HTTPRedirect = mockHTTPRedirect
        self.tmin = tmin
        self.tmax = tmax
        self.tdays = tdays
        self.experimenter_cache = None
        print("Ctx.__init__(): self:", repr(self.__dict__))

    def get_experimenter(self):
        if not self.experimenter_cache:
            self.experimenter_cache = self.db.query(Experimenter).filter(Experimenter.username == self.username).first()
        return self.experimenter_cache
