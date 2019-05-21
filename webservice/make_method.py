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

Note that we also subsume the cherrypy.expose and logit.startstop decorators.
"""


def poms_method(p=[], t=None, help_page="POMS_User_Documentation", rtype="html", redirect=None, u=[]):
    def decorator(func):
        def method(self, **kwargs):
            # make context with any params in the list
            cargs = {k: kwargs.get(k, None) for k in ("experiment", "role", "tmin", "tmax", "tdays")}
            ctx = Ctx(**cargs)
            # clean context args out of kwargs
            for k, v in cargs:
                del kwargs[k]

            # make permission checks
            for p in plist:
                pargs = {"ctx": ctx}
                for k in p:
                    if k == "t":
                        pargs[k] = p[k]
                    elif k == "p":
                        pmethod = p[k]
                    else:
                        pargs[k] = kwargs[p[k]]
                if pmethod == "is_superuser":
                    self.perms.is_superuser(**pargs)
                elif pmethod == "can_modify":
                    self.perms.can_modify(**pargs)
                elif pmethod == "can_do":
                    self.perms.can_do(**pargs)
                else:
                    self.perms.can_view(**pargs)

            kwargs["ctx"] = ctx
            values = func(**kwargs)

            # unpack values into dictionary
            if u:
                vdict = {}
                for i in range(len(u)):
                    vdict[u[i]] = values[i]
                values = vdict

            if kwargs.get("fmt", "") == "json" or rtype == "json":
                cherrypy.response.headers["Content-Type"] = "application/json"
                return json.dumps(values, cls=JSONORMEncoder).encode("utf-8")
            elif rtype == "redirect":
                kwargs["poms_path"] = self.path
                kwargs["hostname"] = self.hostname
                raise cherrypy.HTTPRedirect(redirect % kwargs)
            elif t:
                values["help_page"] = help_page
                return jinja_env.get_template(t).render(**values)
            else:
                cherrypy.response.headers["Content-Type"] = "text/plain"
                return values

        return cherrypy.expose(logit.startstop(method))

    return decorator

    @poms_method()
    def headers(self):
        return repr(cherrypy.request.headers)

    @poms_method(rtype="redirect", redirect="https://%(hostname)s/Shibboleth.sso/Logout")
    def sign_out(self):
        pass

    @poms_method(templ="index.html")
    def index(self):
        pass
