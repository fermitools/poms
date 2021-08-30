import cherrypy


def get_user():
    if cherrypy.request.headers.get("X-Shib-Userid", None) and cherrypy.request.headers["X-Shib-Userid"] != '(null)':
        username = cherrypy.request.headers["X-Shib-Userid"]
        
    elif cherrypy.request.headers.get("X-Scitoken-Sub", None) and cherrypy.request.headers["X-Scitoken-Sub"] != '(null)':
        username = cherrypy.request.headers["X-Scitoken-Sub"].replace("@fnal.gov","")
    elif cherrypy.config.get("standalone_test_user", None):
        username = cherrypy.config.get("standalone_test_user", None)
    return username
