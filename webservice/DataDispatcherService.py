import configparser
import os
import json
import cherrypy
from . import logit
from data_dispatcher.api import DataDispatcherClient

config = configparser.ConfigParser()
config.read(os.environ.get("WEB_CONFIG","/home/poms/poms/webservice/poms.ini"))


class DataDispatcherService:
    def __init__(self, ps):
        self.logged_in = False
        self.session_details = {"login_status": 'Not logged in'}
        self.client = None
        self.auth_info = None
        self.experiment = None
        self.poms_service = ps
        
    def set_client(self, experiment, worker_id=None, worker_id_file=None, 
                   token=None, token_file=None, token_library=None, 
                   cpu_site='DEFAULT', timeout=300):
        try:
            # Determine which servers to use
            server_url = cherrypy.config.get("HYPOT_DATA_DISPATCHER_URL") if experiment != "dune" else cherrypy.config.get("DUNE_DATA_DISPATCHER_URL")
            auth_server_url = cherrypy.config.get("HYPOT_DATA_DISPATCHER_AUTH_URL") if experiment != "dune" else cherrypy.config.get("DUNE_DATA_DISPATCHER_AUTH_URL")
            
            # Create a dispatcher client
            self.client = DataDispatcherClient(server_url=server_url, auth_server_url=auth_server_url)
            self.experiment = "HYPOT" if experiment != "dune" else experiment
            logit.log("Data-Dispatcher Service | set_client() | Client set to %s" % ("HYPOT" if experiment != "dune" else experiment))
            return True
        except Exception as e:
            logit.log("Data-Dispatcher Service | set_client() | Exception: %s" % repr(e))
            return False
    
    def login_with_password(self, experiment, username, password):
        try:
            # Validation check
            if not username or not password:
                return json.dumps({"login_status": "Failed Failed, please check username and password and try again."})
            if not experiment:
                return json.dumps({"login_status": "Failed Failed, please select experiment, then try again."})
            
            # Set new client
            logit.log("Data-Dispatcher Service  | login_with_password() | Attempting to set new client with experiment: %s" % experiment)
            if not self.set_client(experiment):
                logit.log("Data-Dispatcher Service  | login_with_password() | Failed to set up client for experiment: %s" % experiment)
                return json.dumps({"login_status": "Login Failed: Internal issue. Please contact a POMS administrator for assistance."})
            
            # Try logging in
            logit.log("Data-Dispatcher Service  | login_with_password() | Attempting login as: %s" % username)
            self.auth_info = self.client.login_password(username, password)
            if self.auth_info:
                logit.log("Data-Dispatcher Service  | login_with_password() | Logged in as %s" % username)
                self.logged_in = True
                self.session_details = {
                    "login_status": 'Logged in', 
                    "experiment": self.experiment, 
                    "dd_username":self.auth_info[0], 
                    "timestamp":self.auth_info[1]
                }
                # Dump as json here because it is a POST call
                return json.dumps(self.session_details)
            return json.dumps({"login_status":"Login Failed"})
        except Exception as e:
            logit.log("Data-Dispatcher Service | login_with_password() | Exception: %s" % repr(e))
            return {"login_status": "%s" % repr(e)}

    
    def get_login_status(self):
        try:
            logit.log("Data-Dispatcher Service | get_login_status() | Checking login status")
            return self.session_details
        except Exception as e:
            logit.log("Data-Dispatcher Service | get_login_status() | Exception: %s " % repr(e))
        return {"login_status": "Not logged in"}
        
        
    
            
