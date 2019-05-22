import cherrypy
from jinja2 import Environment, PackageLoader
import jinja2.exceptions
from .Ctx import Ctx

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
"""
This is a stab at a decorator that will do most of the repeated things in poms_service.py  

To use it we need to

a) make it work :-)

b) fix our business methods to return dictionaries (nearly) ready to hand 
   straight to the template.render call

c) then it's just:
   ---------------
     @poms_method(p=[{"p":"can_view","t":"Campaign", "item_id":"campaign_id"}], 
                  t="show_campaign_stages.html"
     ) 
     def show_campaign_stages(**kwargs): return self.campaignPOMS.show_campaign_stages(**kwargs)
   ------------------
   for most of our methods.  

The parameters are:
  p = our permissions checks, which can be a list, and refer to kwargs.
  t = jinja template filename
  help_page = name of help page in wiki
  rtype = return type: "html", "json", or "redirect"
  redirect = where to redirect to -- % formatted against dict = kwargs + poms_path, etc.
It then
  - automatically pulls out args that go into Ctx() block
  - picks out the plist stuff and calls permission checks
  - calls the underlying method
  - formats the result either with jinja template, or as json, or does redirect

Note that we also subsume the cherrypy.expose and logit.logstartstop decorators.
"""


def poms_method(p=[], t=None, help_page="POMS_User_Documentation", rtype="html", redirect=None, u=[], confirm=None):
    def decorator(func):
        def method(self, *args, **kwargs):
            for i in range(len(args)):
                kwargs[["experiment","role","user"][i]] = args[i]
            # make context with any params in the list
            cargs = {k: kwargs.get(k, None) for k in ("experiment", "role", "tmin", "tmax", "tdays")}
            ctx = Ctx(**cargs)
            # clean context args out of kwargs
            for k in cargs:
                if k in kwargs:
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
                        pargs[k] = kwargs.get(perm[k],None)
                logit.log("pmethod: %s( %s )" % (pmethod,repr(pargs)))
                if pmethod == "is_superuser":
                    self.permissions.is_superuser(**pargs)
                elif pmethod == "can_modify":
                    self.permissions.can_modify(**pargs)
                elif pmethod == "can_do":
                    self.permissions.can_do(**pargs)
                else:
                    self.permissions.can_view(**pargs)

            kwargs["ctx"] = ctx
            values = func(self, **kwargs)

            # unpack values into dictionary
            if u:
                vdict = {}
                for i in range(len(u)):
                    vdict[u[i]] = values[i]
                values = vdict

            uvdict = {}
            uvdict.update(kwargs)
            uvdict.update(values)
            values = uvdict

            logit.log("after call: values = %s" % repr(values))

            if kwargs.get("fmt", "") == "json" or rtype == "json":
                cherrypy.response.headers["Content-Type"] = "application/json"
                return json.dumps(values, cls=JSONORMEncoder).encode("utf-8")
            elif rtype == "redirect":
                kwargs["poms_path"] = self.path
                kwargs["hostname"] = self.hostname
                raise cherrypy.HTTPRedirect(redirect % kwargs)
            elif t:
                templ = t
                if confirm and kwargs.get("confirm", None) == None:
                    templ = templ.replace(".html", "_confirm.html")
                values["help_page"] = help_page
                return self.jinja_env.get_template(templ).render(**values)
            else:
                cherrypy.response.headers["Content-Type"] = "text/plain"
                return values

        return cherrypy.expose(logit.logstartstop(method))

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
        confirm=True
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
