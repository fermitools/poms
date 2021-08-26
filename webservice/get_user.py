import cherrypy


def get_user():
    if cherrypy.request.headers.get("X-Shib-Userid", None):
        username = cherrypy.request.headers["X-Shib-Userid"]
    if cherrypy.request.headers.get("X-SCITOKEN_SUB", None):
        username = cherrypy.request.headers["X-SCITOKEN_SUB"].replace("@fnal.gov","")
    elif cherrypy.config.get("standalone_test_user", None):
        username = cherrypy.config.get("standalone_test_user", None)
    return username
