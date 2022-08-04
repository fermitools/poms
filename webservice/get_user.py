import cherrypy


def get_user():
    if cherrypy.request.headers.get("X-Shib-Userid", None) and cherrypy.request.headers["X-Shib-Userid"] != '(null)':
        return cherrypy.request.headers["X-Shib-Userid"]
    elif cherrypy.request.headers.get("X-Scitoken-Sub", None) and cherrypy.request.headers["X-Scitoken-Sub"] != '(null)':
        # TODO lookup actual username from FERRY since token subject is now UUID
        #return cherrypy.request.headers["X-Scitoken-Sub"].replace("@fnal.gov","")
        raise PermissionError("Tokens not implemented")
    raise PermissionError("No valid username found in request")
