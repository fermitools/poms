import configparser
import os

import poms.webservice.logit as logit
from data_dispatcher.api import DataDispatcherClient

config = configparser.ConfigParser()
config.read(os.environ.get("WEB_CONFIG","/home/poms/poms/webservice/poms.ini"))

#config = configparser.ConfigParser()
#config.read(os.environ["WEB_CONFIG"])

class DataDispatcherService:
    def __init__(self):
        self.client=None
        
    def set_client(self, ctx, 
                   worker_id=None, 
                   worker_id_file=None, 
                   token=None, 
                   token_file=None, 
                   token_library=None, 
                   cpu_site='DEFAULT', 
                   timeout=300):
        try:
            server_url = config.get("Data_Dispatcher",'HYPOT_DATA_DISPATCHER_URL') if ctx.experiment == "dune" else config.get("Data_Dispatcher",'DUNE_DATA_DISPATCHER_URL')
            auth_server_url = config.get("Data_Dispatcher",'HYPOT_DATA_DISPATCHER_AUTH_URL') if ctx.experiment == "dune" else config.get("Data_Dispatcher",'DUNE_DATA_DISPATCHER_AUTH_URL')
            self.client = DataDispatcherClient(server_url=server_url, auth_server_url=auth_server_url)
            logit.log("Data-Dispatcher | Set Client: %s" % server_url)
        except Exception as e:
            raise e
    
    def login_password(self, username, password):
        if not self.client:
            raise AssertionError("Data-Dispatcher client not set.")
        try:
            self.client.login_password(username, password)
            return self.client.auth_info()
        except Exception as e:
            raise e
            
import configparser
import os
from io import BytesIO
import subprocess
import json
import cherrypy
import base64
from . import logit
from data_dispatcher.api import DataDispatcherClient

config = configparser.ConfigParser()
config.read(os.environ.get("WEB_CONFIG","/home/poms/poms/webservice/poms.ini"))
PIPE = -1

class DataDispatcherService:
    def __init__(self, ps):
        self.session_details = {"login_status": 'Not logged in'}
        self.client_library = None
        self.client = None
        self.dd_experiment = None
        self.role = None
        self.poms_service = ps
        
    def set_client(self, ctx, worker_id=None, worker_id_file=None, 
                   token=None, token_file=None, token_library=None, 
                   cpu_site='DEFAULT', timeout=300):
        try:
            # Determine which servers to use
            experiment = "hypot" if ctx.experiment != "dune" else ctx.experiment
            server_url = config.get("Data_Dispatcher",'HYPOT_DATA_DISPATCHER_URL') if experiment != "dune" else config.get("Data_Dispatcher",'DUNE_DATA_DISPATCHER_URL')
            auth_server_url = config.get("Data_Dispatcher",'HYPOT_DATA_DISPATCHER_AUTH_URL') if experiment != "dune" else config.get("Data_Dispatcher",'DUNE_DATA_DISPATCHER_AUTH_URL')
            
            # Create a dispatcher client
            token_library_dir="/var/run/user/%s/data_dispatcher/%s/%s/%s" %(os.geteuid(), ctx.experiment ,ctx.role, ctx.username)
            token_library = "%s/.token_library" % token_library_dir
            os.system("mkdir -p %s" % token_library_dir)
            os.system("touch %s" % token_library)
            self.client = DataDispatcherClient(server_url=server_url, auth_server_url=auth_server_url, token_library=token_library)
            self.client_library = token_library_dir
            logit.log("Data-Dispatcher Service | set_client() | Set client library to %s" % (self.client_library))
            self.dd_experiment = "hypot" if experiment != "dune" else experiment
            self.role = ctx.role
            logit.log("Data-Dispatcher Service | set_client() | Client set to %s" % (self.dd_experiment))
            return True
        except Exception as e:
            logit.log("Data-Dispatcher Service | set_client() | Exception: %s" % repr(e))
            return False
    
    def login_with_password(self, ctx, username, password):
        try:
            # Validation check
            if not username or not password:
                return json.dumps({"login_status": "Login Failed, please check username and password and try again."})
            if not ctx.experiment:
                return json.dumps({"login_status": "Login Failed, please select experiment, then try again."})
            
            # Set new client
            logit.log("Data-Dispatcher Service  | login_with_password() | Attempting to set new client with experiment: %s" % ctx.experiment)
            if not self.set_client(ctx):
                logit.log("Data-Dispatcher Service  | login_with_password() | Failed to set up client for experiment: %s" % ctx.experiment)
                return json.dumps({"login_status": "Login Failed: Internal issue. Please contact a POMS administrator for assistance."})
            
            # Try logging in
            logit.log("Data-Dispatcher Service  | login_with_password() | Attempting login as: %s" % username)
            auth_info = self.client.login_password(username, password)
            if auth_info:
                logit.log("Data-Dispatcher Service  | login_with_password() | Logged in as %s" % auth_info[0])
            return json.dumps(self.session_status(ctx, 'password')[1])

        except Exception as e:
            logit.log("Data-Dispatcher Service | login_with_password() | Exception: %s" % repr(e))
            return json.dumps({
                    "login_method": "Attempted login method: password",
                    "login_status": "%s" % repr(e).split("'")[1].replace("\n", "")
                })

    
    def login_with_token(self, ctx):
        
        try:
            if not ctx.username:
                return json.dumps({"login_status": "Login Failed, please check username and password and try again."})
            if not ctx.experiment:
                return json.dumps({"login_status": "Login Failed, please select experiment, then try again."})
            
            experiment = "hypot" if ctx.experiment != "dune" else ctx.experiment
            # Make storage location for tokens
            
            
            # Set new client
            logit.log("Data-Dispatcher Service  | login_with_token() | Attempting to set new client with experiment: %s" % ctx.experiment)
            if not self.set_client(ctx):
                logit.log("Data-Dispatcher Service  | login_with_token() | Failed to set up client for experiment: %s" % ctx.experiment)
                return json.dumps({"login_status": "Login Failed: Internal issue. Please contact a POMS administrator for assistance."})
            
            self.get_token(ctx, experiment)
            
            token = open(os.environ['BEARER_TOKEN_FILE'], 'r').read().strip()
            if ctx.role == 'production':
                logit.log("Data-Dispatcher Service  | login_with_token() | Attempting token login for : %s - (%s, %s)" % ("%spro" % ctx.experiment, experiment, ctx.role))
                auth_info = self.client.login_token("%spro" % ctx.experiment, token)
            else:
                logit.log("Data-Dispatcher Service  | login_with_token() | Attempting token login for : %s - (%s, %s)" % (ctx.username, experiment, ctx.role))
                auth_info = self.client.login_token(ctx.username, token)
            if auth_info:
                return json.dumps(self.session_status(ctx, 'token')[1])
                
            
        except Exception as e:
            logit.log("Data-Dispatcher Service | login_with_token() | Exception: %s" % repr(e))
            return json.dumps({
                "login_method": "Attempted login method: token",
                "login_status": repr(e).split("'")[1].replace("\\n", "").replace("\\", "")
            })
        return json.dumps({
                    "login_method": "Attempted login method: token",
                    "login_status": "Login failed"
                })
    
    
    def session_status(self, ctx, auth_type='Metacat Token'):
        try:
            token_library_dir = "/var/run/user/%s/data_dispatcher/%s/%s/%s" %(os.geteuid(), ctx.experiment ,ctx.role, ctx.username)
            auth_info = None
            if self.client and self.client_library == token_library_dir:
                auth_info = self.client.auth_info()
            elif not self.client or self.client and self.client_library != token_library_dir:
                if os.path.exists("%s/.token_library" % token_library_dir):
                    token_library_content = open("%s/.token_library" % token_library_dir, 'r').read()
                    if len(token_library_content.split(" ")) > 1:
                        token = token_library_content.split(" ")[1].strip()
                        self.set_client(ctx)
                        auth_info = self.client.login_token("%spro" % ctx.experiment if ctx.experiment == "dune" and ctx.role == "production" else ctx.username, token)
            if auth_info:
                    logit.log("Data-Dispatcher Service  | session_status(%s) | Logged in as %s" % (auth_type, auth_info[0]))
                    return True, {
                        "login_method" : auth_type,
                        "login_status": 'Logged in', 
                        "dd_experiment": self.dd_experiment, 
                        "dd_username":auth_info[0], 
                        "timestamp":auth_info[1]
                    }
        except Exception as e:
            logit.log("Data-Dispatcher Service  | session_status(%s) | Exception:  %s" % (auth_type, repr(e)))
            message = repr(e).split("'")[1].replace("\\n", "").replace("\\","")
            logit.log("Data-Dispatcher Service  | session_status(%s) | Failed to get session:  %s" % (auth_type, message))
            if message == "No token found":
                return False, {"login_status": "Not logged in"}
            else:
                return False, {
                        "login_method": "Attempted login method: %s" % auth_type,
                        "login_status": "%s" % message
                    }
        return False, {"login_status": "Not logged in"}
    
    
    def list_rses(self):
        logit.log("Data-Dispatcher Service  | list_rses(%s, %s) | Begin" % (self.dd_experiment, self.role))
        retval = self.client.list_rses()
        logit.log("Data-Dispatcher Service  | list_rses(%s, %s) | Success" % (self.dd_experiment, self.role))
        return retval
    
    def list_projects(self):
        logit.log("Data-Dispatcher Service  | list_projects(%s, %s) | Begin" % (self.dd_experiment, self.role))
        retval = {}
        retval['active'] = self.client.list_projects()
        retval['active_count'] = len(retval['active'])
        retval['inactive'] = self.client.list_projects(state="abandoned", not_state="active", with_files=False)
        retval['inactive_count'] = len(retval['inactive'])
        logit.log("Data-Dispatcher Service  | list_projects(%s, %s) | Success" % (self.dd_experiment, self.role))
        return retval
    
    def get_project_handles(self, project_id):
        logit.log("Data-Dispatcher Service  | get_project_handles(%s, %s) | project_id: %s | Begin" % (self.dd_experiment, self.role, project_id))
        retval = None
        msg = "Fail"
        try:
            retval = self.client.get_project(project_id, with_files=True)
            if retval:
                msg = "OK"
                logit.log("Data-Dispatcher Service  | get_project_handles(%s, %s) | project_id: %s | Success" % (self.dd_experiment, self.role, project_id))
        except Exception as e:
            retval = {"exception": repr(e)}
            logit.log("Data-Dispatcher Service  | get_project_handles(%s, %s) | project_id: %s | Exception: %s" % (self.dd_experiment, self.role, project_id, e))
            raise e
        return {"project_handles": retval, "msg": msg}
        
        
    
    
    # Temporary
    def get_token(self, ctx, experiment):
        logit.log("Data-Dispatcher Service  | get_token() | experiment: %s | Begin" % ctx.experiment)
        group = ctx.experiment if ctx.experiment == "dune" else "hypot"
        vaultfile = "%s/vt_%s" % (self.client_library, ctx.username)
        vaulttokeninfile = "--vaulttokeninfile=%s" % vaultfile if os.path.exists(vaultfile) else ""
        if ctx.role == "production" and experiment == "dune":
                
            htgettokenopts = "-a %s -i %s -r production --credkey=%spro/managedtokens/%s --vaulttokenfile=%s %s" % (
                ctx.web_config.get("tokens", "vaultserver"),
                group, 
                experiment, 
                ctx.web_config.get("tokens", "managed_tokens_server"), 
                vaultfile,
                vaulttokeninfile)
        else:
            htgettokenopts = "-a %s -r %s -i %s --credkey=%s --vaulttokenfile=%s %s" % (
                ctx.web_config.get("tokens", "vaultserver"),
                "default" if ctx.role != "production" else ctx.role, 
                group, 
                vaultfile, 
                ctx.username,
                vaultfile,
                vaulttokeninfile)
        
        
        
        os.environ.__setitem__('BEARER_TOKEN_FILE', "%s/bt_%s" % (self.client_library, ctx.username)) 
        
        cmd = "export BEARER_TOKEN_FILE=\"%s/bt_%s\"; htgettoken %s;" % (self.client_library, ctx.username, htgettokenopts)
        logit.log("Command: %s " % cmd)
        
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True, universal_newlines=True)
        so, se = p.communicate()
        p.wait()
        
        if so[:-1].__contains__("Storing bearer token"):
            logit.log("Data-Dispatcher Service  | get_token() | experiment: %s | Success" % ctx.experiment)
        else:
            logit.log("Data-Dispatcher Service  | get_token() | experiment: %s | Fail" % ctx.experiment)
        
        
        
    
            
