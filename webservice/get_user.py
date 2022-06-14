import cherrypy


def get_user():
    if cherrypy.request.headers.get("X-Shib-Userid", None) and cherrypy.request.headers["X-Shib-Userid"] != '(null)':
        username = cherrypy.request.headers["X-Shib-Userid"]
        
    elif cherrypy.request.headers.get("X-Scitoken-Scope", None) and cherrypy.request.headers["X-Scitoken-Scope"] != '(null)':
        # we have the username in the storage scope, so mooch it out of there
        m = re.match(r".*storage.create:/\S*/users/(\S*)\s.*", cherrypy.request.headers["X-Scitoken-Scope"])
        if m:
            username = m.group(1)
        else:
            username = cherrypy.config.get("standalone_test_user", None)
    elif cherrypy.config.get("standalone_test_user", None):
        username = cherrypy.config.get("standalone_test_user", None)
    return username
