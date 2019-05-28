from .Ctx import Ctx
from .poms_model import CampaignStage, Submission, Experiment, LoginSetup, Base, Experimenter
from jinja2 import Environment, PackageLoader
import cherrypy
import jinja2.exceptions
import json
import logging
import sqlalchemy.exc

# mostly so we can pass them to page templates...
import datetime
import time

from . import (
    CampaignsPOMS,
    DBadminPOMS,
    FilesPOMS,
    JobsPOMS,
    TablesPOMS,
    TagsPOMS,
    TaskPOMS,
    UtilsPOMS,
    Permissions,
    logit,
    version,
)


def error_rewrite(f):
    def wrapper(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except PermissionError as e:
            logging.exception("rewriting:")
            raise cherrypy.HTTPError(401, repr(e))
        except TypeError as e:
            logging.exception("rewriting:")
            raise cherrypy.HTTPError(400, repr(e))
        except KeyError as e:
            logging.exception("rewriting:")
            raise cherrypy.HTTPError(400, "Missing form field: %s" % repr(e))
        except sqlalchemy.exc.DataError as e:
            logging.exception("rewriting:")
            raise cherrypy.HTTPError(400, "Invalid argument: %s" % repr(e))
        except ValueError as e:
            logging.exception("rewriting:")
            raise cherrypy.HTTPError(400, "Invalid argument: %s" % repr(e))
        except jinja2.exceptions.UndefinedError as e:
            logging.exception("rewriting:")
            raise cherrypy.HTTPError(400, "Missing arguments")
        except:
            raise

    return wrapper


class JSONORMEncoder(json.JSONEncoder):
    # This will show up as an error in pylint.   Appears to be a bug in pylint, so its disabled:
    #    pylint #89092 @property.setter raises an E0202 when attribute is set
    def default(self, obj):  # pylint: disable=E0202

        if obj == datetime:
            return "datetime"

        if isinstance(obj, Base):
            # smash ORM objects into dictionaries
            res = {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}
            # first put in relationship keys, but not loaded
            res.update({c.key: None for c in inspect(obj).mapper.relationships})
            # load specific relationships that won't cause cycles
            res.update({c.key: getattr(obj, c.key) for c in inspect(obj).mapper.relationships if "experimenter" in c.key})
            res.update({c.key: getattr(obj, c.key) for c in inspect(obj).mapper.relationships if "snapshot_obj" in c.key})
            res.update({c.key: getattr(obj, c.key) for c in inspect(obj).mapper.relationships if c.key == "campaign_stage_obj"})
            # Limit to the name only for campaign_obj to prevent circular reference error
            res.update(
                {c.key: {"name": getattr(obj, c.key).name} for c in inspect(obj).mapper.relationships if c.key == "campaign_obj"}
            )
            res.update({c.key: list(getattr(obj, c.key)) for c in inspect(obj).mapper.relationships if c.key == "stages"})

            return res

        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%dT%H:%M:%S")

        return super(JSONORMEncoder, self).default(obj)


def poms_method(
    p=[], t=None, help_page="POMS_User_Documentation", rtype="html", redirect=None, u=[], confirm=None, call_args=None
):
    """
    This is a decorator that will do most of the repeated things in poms_service.py  
    use as:
       ---------------
         @poms_method(p=[{"p":"can_view","t":"Campaign", "item_id":"campaign_id"}], 
                      t="show_campaign_stages.html"
         ) 
         def show_campaign_stages(**kwargs): 
              return self.campaignPOMS.show_campaign_stages(**kwargs)
       ------------------
       for most of our methods.  

    The parameters are:
      p = our permissions checks, which can be a list, and refer to kwargs.
      t = jinja template filename
      help_page = name of help page in wiki
      rtype = return type: "html", "json", or "redirect"
      redirect = where to redirect to -- % formatted against dict = kwargs + poms_path, etc.
      confirm = True to convert template.html to template_confirm.html if there is no confirm parameter.
    It then
      - automatically pulls out args that go into Ctx() block
      - picks out the plist stuff and calls permission checks
      - calls the underlying method
      - formats the result either with jinja template, or as json, or does redirect

    Note that we also subsume the cherrypy.expose and logit.logstartstop decorators.
    """

    def decorator(func):
        def method(self, *args, **kwargs):
            if not call_args:
                for i in range(len(args)):
                    kwargs[["experiment", "role", "user"][i]] = args[i]
            # make context with any params in the list
            cargs = {k: kwargs.get(k, None) for k in ("experiment", "role", "tmin", "tmax", "tdays")}
            ctx = Ctx(**cargs)
            # clean context args out of kwargs
            for k in cargs:
                if k in kwargs and not k in ("experiment", "role"):
                    del kwargs[k]

            # make permission checks
            for perm in p:
                pargs = {"ctx": ctx}
                for k in perm:
                    if k == "t":
                        pargs[k] = perm[k]
                    elif k == "p":
                        pmethod = perm[k]
                    else:
                        # look for permission values in kwargs or ctx...
                        pargs[k] = kwargs.get(perm[k], ctx.__dict__.get(perm[k], None))
                logit.log("pmethod: %s( %s )" % (pmethod, repr(pargs)))
                if pmethod == "is_superuser":
                    self.permissions.is_superuser(**pargs)
                elif pmethod == "can_modify":
                    self.permissions.can_modify(**pargs)
                elif pmethod == "can_do":
                    self.permissions.can_do(**pargs)
                else:
                    self.permissions.can_view(**pargs)

            kwargs["ctx"] = ctx
            if call_args:
                values = func(self, *args)
            else:
                values = func(self, **kwargs)

            # unpack values into dictionary
            if u:
                vdict = {}
                for i in range(len(u)):
                    vdict[u[i]] = values[i]
                values = vdict

            if isinstance(values, dict):
                # merge in kwargs..
                uvdict = {}
                uvdict.update(kwargs)
                uvdict.update(values)
                values = uvdict

            logit.log("after call: values = %s" % repr(values))

            # stop Chrome from offering to translate half our pages..
            cherrypy.response.headers["Content-Language"] = "en"

            if kwargs.get("fmt", "") == "json" or rtype == "json":
                cherrypy.response.headers["Content-Type"] = "application/json"
                if isinstance(values, dict) and "ctx" in values:
                    del values["ctx"]
                return json.dumps(values, cls=JSONORMEncoder).encode("utf-8")
            elif rtype == "rawjavascript":
                cherrypy.response.headers["Content-Type"] = "text/javascript"
                return values
            elif rtype == "redirect":
                values["poms_path"] = self.path
                values["hostname"] = self.hostname
                raise cherrypy.HTTPRedirect(redirect % values)
            elif rtype == "ini":
                cherrypy.response.headers["Content-Type"] = "text/ini"
                return values
            elif t or isinstance(values, dict) and values.get("template", None):
                templ = t or values["template"]
                if confirm and kwargs.get("confirm", None) == None:
                    templ = templ.replace(".html", "_confirm.html")
                values["help_page"] = help_page
                # a few templates want to format times, etc.
                values["datetime"] = datetime
                values["time"] = time
                return self.jinja_env.get_template(templ).render(**values)
            elif rtype == "html":
                cherrypy.response.headers["Content-Type"] = "text/html"
                return values
            else:
                cherrypy.response.headers["Content-Type"] = "text/plain"
                return values

        return cherrypy.expose(logit.logstartstop(error_rewrite(method)))

    return decorator


class demo:
    @poms_method(
        p=[
            {"p": "can_do", "t": "Submission", "item_id": "submission_id"},
            {"p": "can_do", "t": "CampaignStage", "item_id": "campaign_stage_id"},
            {"p": "can_do", "t": "Campaign", "item_id": "campaign_id"},
        ],
        u=["output", "s", "campaign_stage_id", "submission_id", "job_id"],
        t="kill_jobs.html",
        confirm=True,
    )
    def kill_jobs(self, **kwargs):
        return self.taskPOMS.kill_jobs(**kwargs)

    @poms_method()
    def headers(self):
        return repr(cherrypy.request.headers)

    @poms_method(rtype="redirect", redirect="https://%(hostname)s/Shibboleth.sso/Logout")
    def sign_out(self):
        pass

    @poms_method(t="index.html")
    def index(self):
        pass
