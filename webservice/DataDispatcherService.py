import configparser
import os
import time
from io import BytesIO
import subprocess
import json
import cherrypy
import base64
import datetime
import threading
import uuid
from . import logit
from data_dispatcher.api import DataDispatcherClient
from metacat.webapi import MetaCatClient
from poms_model import DataDispatcherProject


# Testing related
import operator
#from rucio.client import rucio_client

config = configparser.ConfigParser()
config.read(os.environ.get("WEB_CONFIG","/home/poms/poms/webservice/poms.ini"))
PIPE = -1


def timestamp_to_readable(timestamp):
    converted_datetime = datetime.datetime.fromtimestamp(timestamp)
    readable_format = converted_datetime.strftime("%A, %B %d, %Y")
    return readable_format

class DataDispatcherService:
    def __init__(self, ps):
        self.session_details = {"login_status": 'Not logged in'}
        self.client_library = None
        self.metacat_client_library = None
        self.client = None
        self.dd_experiment = None
        self.role = None
        self.poms_service = ps
        self.test = None
        
    def set_client(self, ctx, worker_id=None, worker_id_file=None, 
                   token=None, token_file=None, token_library=None, 
                   cpu_site='DEFAULT', timeout=300):
        try:
            # Determine which servers to use
            experiment = "hypot" if ctx.experiment != "dune" else ctx.experiment
            server_url = config.get("Data_Dispatcher",'HYPOT_DATA_DISPATCHER_URL') if experiment != "dune" else config.get("Data_Dispatcher",'DUNE_DATA_DISPATCHER_URL')
            auth_server_url = config.get("Data_Dispatcher",'HYPOT_DATA_DISPATCHER_AUTH_URL') if experiment != "dune" else config.get("Data_Dispatcher",'DUNE_DATA_DISPATCHER_AUTH_URL')
            metacat_server_url = config.get("Data_Dispatcher",'HYPOT_METACAT_SERVER_URL') if experiment != "dune" else config.get("Data_Dispatcher",'DUNE_METACAT_SERVER_URL')
            metacat_auth_server_url = config.get("Data_Dispatcher",'HYPOT_METACAT_AUTH_SERVER_URL') if experiment != "dune" else config.get("Data_Dispatcher",'DUNE_METACAT_AUTH_SERVER_URL')
            # Create a dispatcher client
            token_library_dir = "/var/run/user/%s/data_dispatcher/poms" %(os.geteuid()) if ctx.experiment != 'dune' else "/var/run/user/%s/data_dispatcher/%s/%s/%s" %(os.geteuid(), ctx.experiment ,ctx.role, ctx.username)
            token_library = "%s/.token_library" % token_library_dir
            metacat_token_library_dir = "/var/run/user/%s/metacat/poms" %(os.geteuid()) if ctx.experiment != 'dune' else "/var/run/user/%s/metacat/%s/%s/%s" %(os.geteuid(), ctx.experiment ,ctx.role, ctx.username)
            metacat_token_library = "%s/.token_library" % metacat_token_library_dir
            os.system("mkdir -p %s %s" % (token_library_dir, metacat_token_library_dir))
            os.system("touch %s %s" % (token_library, metacat_token_library))
            self.client = DataDispatcherClient(server_url=server_url, auth_server_url=auth_server_url, token_library=token_library)
            self.metacat_client = MetaCatClient(server_url=metacat_server_url, auth_server_url=metacat_auth_server_url, token_library=metacat_token_library)
            self.client_library = token_library_dir
            self.metacat_client_library = metacat_token_library
            logit.log("Data-Dispatcher Service | set_client() | Set client library to %s" % (self.client_library))
            self.dd_experiment = "hypot" if experiment != "dune" else experiment
            self.role = ctx.role
            logit.log("Data-Dispatcher Service | set_client() | Client set to %s - Version: %s" % (self.dd_experiment, self.client.version()))
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
            
    def login_with_x509(self, ctx):
        try:
            # Set new client
            logit.log("Data-Dispatcher Service  | login_with_x509() | System Login on behalf of: %s" % ctx.experiment)
            if not self.set_client(ctx):
                logit.log("Data-Dispatcher Service  | login_with_x509() | Failed to set up client for experiment: %s" % ctx.experiment)
                return json.dumps({"login_status": "Login Failed: Internal issue. Please contact a POMS administrator for assistance."})
            
            # Try logging in
            auth_info = self.client.login_x509('poms', config.get("Data_Dispatcher", "POMS_CERT"), config.get("Data_Dispatcher", "POMS_KEY"))
            if auth_info:
                logit.log("Data-Dispatcher Service  | login_with_x509() | Logged in as POMS")
                
            return json.dumps(self.session_status(ctx, 'x509')[1])

        except Exception as e:
            logit.log("Data-Dispatcher Service | login_with_x509() | Exception: %s" % repr(e))
            return json.dumps({
                    "login_method": "Attempted login method: x509",
                    "login_status": "%s" % repr(e).split("'")[1].replace("\n", "").replace("\\","")
                })
            
    def login_metacat(self, ctx):
        try:
            # Set new client
            logit.log("Metacat Service  | login_with_x509() | System Login on behalf of: %s" % ctx.experiment)
            # Try logging in
            auth_info = self.metacat_client.login_x509('poms', config.get("Data_Dispatcher", "POMS_CERT"), config.get("Data_Dispatcher", "POMS_KEY"))
            if auth_info:
                logit.log("Metacat Service  | login_with_x509() | Logged in as POMS")
                return True
            logit.log("Metacat Service  | login_with_x509() | Failed")
            return False

        except Exception as e:
            logit.log("Metacat Service | login_with_x509() | Exception: %s" % repr(e))
            False
    
    def session_status(self, ctx, auth_type='Metacat Token'):
        try:
            logit.log("Data-Dispatcher Service  | session_status(%s) | Begin" % (auth_type))
            token_library_dir = "/var/run/user/%s/data_dispatcher/poms" %(os.geteuid()) if ctx.experiment != 'dune' else "/var/run/user/%s/data_dispatcher/%s/%s/%s" %(os.geteuid(), ctx.experiment ,ctx.role, ctx.username)
            auth_info = None
            if self.client and self.client_library == token_library_dir:
                auth_info = self.client.auth_info()
            elif not self.client or (self.client and self.client_library != token_library_dir):
                if os.path.exists("%s/.token_library" % token_library_dir):
                    token_library_content = open("%s/.token_library" % token_library_dir, 'r').read()
                    token = None
                    if len(token_library_content.split(" ")) > 1:
                        token = token_library_content.split(" ")[1].strip()
                    self.set_client(ctx)
                    username = 'poms' if ctx.experiment != 'dune' else ctx.username
                    auth_info = self.client.login_token(username, token)
                    logit.log("Data-Dispatcher Service  | session_status(%s) | DD Logged in as %s" % (auth_type, token))
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
    
    def list_all_projects(self, ctx, **kwargs):
        logit.log("Data-Dispatcher Service  | list_all_projects(%s, %s) | Begin" % (self.dd_experiment, self.role))
        retval = {}
        retval['active'] = self.client.list_projects()
        retval['inactive'] = self.client.list_projects(state="abandoned", not_state="active", with_files=False)
        
    
        poms_attributes = self.find_poms_data_dispatcher_projects(ctx, format="html", **kwargs)  
        if poms_attributes:
            retval['poms_attributes'] = poms_attributes

        retval['active_count'] = len(retval.get("active", 0))
        retval['inactive_count'] = len(retval.get("inactive", 0))
        for item in retval.get("inactive", None):
            if item.get("idle_timeout", None):
                item["idle_timeout"]
                
        logit.log("Data-Dispatcher Service  | list_projects(%s, %s) | Success" % (self.dd_experiment, self.role))
        return retval
    
    def list_filtered_projects(self, ctx, **kwargs):
        logit.log("Data-Dispatcher Service  | list_filtered_projects(%s, %s) | Begin" % (self.dd_experiment, self.role))
        retval = {}
        retval['active'] = self.client.list_projects()
        retval['inactive'] = self.client.list_projects(state="abandoned", not_state="active", with_files=False)
        
        poms_attributes = self.find_poms_data_dispatcher_projects(ctx, format="html", **kwargs)  
        if poms_attributes is not None:
            logit.log("Data-Dispatcher Service  | list_filtered_projects(%s, %s) | Got poms project-attributes: %s" % (self.dd_experiment, self.role, poms_attributes))
            retval['active'] = [project for project in retval['active'] if project.get("project_id") in poms_attributes]
            retval['inactive'] = [project for project in retval['inactive'] if project.get("project_id") in poms_attributes]
            retval['poms_attributes'] = poms_attributes

        retval['active_count'] = len(retval.get("active", 0))
        retval['inactive_count'] = len(retval.get("inactive", 0))
        for item in retval.get("inactive", None):
            if item.get("idle_timeout", None):
                item["idle_timeout"]
                
        logit.log("Data-Dispatcher Service  | list_filtered_projects(%s, %s) | Success" % (self.dd_experiment, self.role))
        return retval
    
    def get_project_handles(self, project_id, state=None, not_state=None):
        logit.log("Data-Dispatcher Service  | get_project_handles(%s, %s) | project_id: %s | Begin" % (self.dd_experiment, self.role, project_id))
        retval = None
        msg = "Fail"
        try:
            project_info = self.client.get_project(project_id, True, True) #self.client.list_handles(project_id, state=state, not_state=not_state, with_replicas=True)
            retval = project_info.get("file_handles", []) if project_info else None
            if state:
                retval = [ handle for handle in retval if handle.state == state ]
            if not_state:
                retval = [ handle for handle in retval if handle.state != state ]
            if retval:
                msg = "OK"
                logit.log("Data-Dispatcher Service  | get_project_handles(%s, %s) | project_id: %s | Success" % (self.dd_experiment, self.role, project_id))
        except Exception as e:
            retval = {"exception": repr(e)}
            logit.log("Data-Dispatcher Service  | get_project_handles(%s, %s) | project_id: %s | Exception: %s" % (self.dd_experiment, self.role, project_id, e))
            raise e
        return {"project_handles": retval, "msg": msg, "stats": self.get_project_stats(handles=retval), "project_details": {k: project_info[k] for k in set(list(project_info.keys())) - set(["file_handles"])}}
    
    def get_project_stats(self, handles=None, project_id=None):
        retval = {"initial": 0,"done" : 0,"reserved" : 0,"failed" : 0}
        if not handles and project_id:
            handles = self.client.list_handles(project_id)
        for handle in handles:
            retval[handle.get("state")] += 1
        return retval
    
    def copy_project(self, project_id, **kwargs):
        kwargs["use_hostname"] = True
        new_project = self.client.copy_project(project_id)
        logit.log("Data-Dispatcher Service  | experiment: %s | copy_project(%s) | done" % (self.dd_experiment, project_id))
        return new_project
    
    def get_project_for_submission(self, ctx, project_id, **kwargs):
        logit.log("Data-Dispatcher Service  | experiment: %s | get_project_for_submission(%s) | submission_id: %s " % (self.dd_experiment, project_id, kwargs.get("submission_id")))
        existing_project = self.client.get_project(project_id)
        if existing_project:
            logit.log("Data-Dispatcher Service  | experiment: %s | get_project_for_submission(%s) | located project" % (self.dd_experiment, project_id))
            worker_timeout = existing_project.get('worker_timeout', None)
            idle_timeout = existing_project.get('idle_timeout', None)
            project = self.store_project(ctx, project_id = project_id, worker_timeout=worker_timeout, idle_timeout=idle_timeout, **kwargs)
            logit.log("Data-Dispatcher Service  | experiment: %s |  get_project_for_submission(%s) | stored data-dispatcher project information in database" % (self.dd_experiment, project.project_id))
            logit.log("Data-Dispatcher Service  | experiment: %s | get_project_for_submission(%s) | done" % (self.dd_experiment, project_id))
            return project
        return None
            
    def create_project(self, ctx, dataset=None, files=[], **kwargs):
        logit.log("Data-Dispatcher Service  | experiment: %s | create_project() | project_attributes: %s " % (self.dd_experiment, kwargs))
        if files:
            pass # create files based on recovery files
                   
        if dataset:
            logit.log("Data-Dispatcher Service  | experiment: %s | create_project() | Creating project from dataset: %s " % (self.dd_experiment, dataset))
            users = []
            if "creator_name" in kwargs:
                users.append(kwargs.get("creator_name"))
            if ctx.username not in users:
                users.append(ctx.username)
            self.login_metacat(ctx)
            files = self.metacat_client.query(dataset)
            logit.log("Data-Dispatcher Service  | experiment: %s | create_project() | located files from dataset: %s" % (self.dd_experiment, files))
            new_project = self.client.create_project(files, query=dataset, users=users)
            logit.log("Data-Dispatcher Service  | experiment: %s | create_project() | created data-dispatcher project: %s" % (self.dd_experiment, new_project))
            project_id = new_project.get('project_id', None)
            worker_timeout = new_project.get('worker_timeout', None)
            idle_timeout = new_project.get('idle_timeout', None)
            project = self.store_project(ctx, project_id = project_id, worker_timeout=worker_timeout, idle_timeout=idle_timeout, **kwargs)
            logit.log("Data-Dispatcher Service  | experiment: %s | create_project() | stored data-dispatcher project information in database - Project ID: %s" % (self.dd_experiment, project.project_id))
            logit.log("Data-Dispatcher Service  | experiment: %s | create_project() | done" % (self.dd_experiment))
            return project
        
        return None
    
    def query_files(self, ctx):
        logit.log("Data-Dispatcher Service  | login_metacat() | Attempting to set metacat client with experiment: %s" % ctx.experiment)
        auth_info = self.metacat_client.login_x509('poms', config.get("Data_Dispatcher", "POMS_CERT"), config.get("Data_Dispatcher", "POMS_KEY"))
        
    
    def find_poms_data_dispatcher_projects(self, ctx, format=None, **kwargs):
        try:
            logit.log("Data-Dispatcher Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | begin" % (self.dd_experiment, format))
            if not ctx:
                logit.log("Data-Dispatcher Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | fail: no ctx" % (self.dd_experiment, format))
                return {}
            
            query = ctx.db.query(DataDispatcherProject).filter(DataDispatcherProject.experiment == ctx.experiment)
            searchList = ["experiment=%s" % ctx.experiment]
            if kwargs.get("project_id", None):
                # Project id is known, so only one result would exist
                searchList.append("project_id=%s" % kwargs["project_id"])
                query = query.filter(DataDispatcherProject.project_id == kwargs["project_id"])
                project = query.one_or_none()
                logit.log("Data-Dispatcher Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | searched: %s | results: %s" % (self.dd_experiment, format, ", ".join(searchList), 1 if project else 0))
                if project:
                    return self.format_projects(ctx, format, [project])
            if kwargs.get("submission_id", None):
                # One project per submission, if this exists we can return right away
                searchList.append("submission_id=%s" % kwargs["submission_id"])
                query = query.filter(DataDispatcherProject.submission_id == kwargs["submission_id"])
                project = query.one_or_none()
                logit.log("Data-Dispatcher Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | searched: submission_id=%s | results: %s" % (self.dd_experiment, format, ", ".join(searchList), 1 if project else 0))
                if project:
                    return self.format_projects(ctx, format, [project])
                
            # project id and submission id are unknown, so we are filtering for one or more projects
            if kwargs.get("campaign_id", None):
                searchList.append("campaign_id=%s" % kwargs["campaign_id"])
                query = query.filter(DataDispatcherProject.campaign_id == kwargs["campaign_id"])
            if kwargs.get("project_name", None):
                searchList.append("project_name=%s" % kwargs["project_name"])
                query = query.filter(DataDispatcherProject.project_name == kwargs["project_name"])
            if kwargs.get("role", None):
                searchList.append("vo_role=%s" % kwargs["role"])
                query = query.filter(DataDispatcherProject.vo_role == kwargs["role"])
            if kwargs.get("campaign_stage_id", None):
                searchList.append("campaign_stage_id=%s" % kwargs["campaign_stage_id"])
                query = query.filter(DataDispatcherProject.campaign_stage_id == kwargs["campaign_stage_id"])
            if kwargs.get("campaign_stage_snapshot_id", None):
                searchList.append("campaign_stage_snapshot_id=%s" % kwargs["campaign_stage_snapshot_id"])
                query = query.filter(DataDispatcherProject.campaign_stage_snapshot_id == kwargs["campaign_stage_snapshot_id"])
            if kwargs.get("split_type", None):
                searchList.append("split_type=%s" % kwargs["split_type"])
                query = query.filter(DataDispatcherProject.split_type == kwargs["split_type"])
            if kwargs.get("last_split", None):
                searchList.append("last_split=%s" % kwargs["last_split"])
                query = query.filter(DataDispatcherProject.last_split == kwargs["last_split"])
            if kwargs.get("depends_on_submission", None):
                searchList.append("depends_on_submission=%s" % kwargs["depends_on_submission"])
                query = query.filter(DataDispatcherProject.depends_on_submission == kwargs["depends_on_submission"])
            if kwargs.get("depends_on_project", None):
                searchList.append("depends_on_project=%s" % kwargs["depends_on_project"])
                query = query.filter(DataDispatcherProject.depends_on_project == kwargs["depends_on_project"])
            if kwargs.get("recovery_tasks_parent_submission", None):
                searchList.append("recovery_tasks_parent_submission=%s" % kwargs["recovery_tasks_parent_submission"])
                query = query.filter(DataDispatcherProject.recovery_tasks_parent_submission == kwargs["recovery_tasks_parent_submission"])
            if kwargs.get("recovery_tasks_parent_project", None):
                searchList.append("recovery_tasks_parent_project=%s" % kwargs["recovery_tasks_parent_project"])
                query = query.filter(DataDispatcherProject.recovery_tasks_parent_project == kwargs["recovery_tasks_parent_project"])
            if kwargs.get("recovery_position", None):
                searchList.append("recovery_position=%s" % kwargs["recovery_position"])
                query = query.filter(DataDispatcherProject.recovery_position == kwargs["recovery_position"])
            
            projects = query.all()
            logit.log("Data-Dispatcher Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | searched: %s | results: %s" % (self.dd_experiment, format, ", ".join(searchList), len(projects)))
            if len(projects) > 0:
                return self.format_projects(ctx, projects, format)
 
        except Exception as e:
            raise e
        
        return {}
    
    def format_projects(self, ctx, results, format=None):
        logit.log("Data-Dispatcher Service  | experiment: %s | format_projects() | format: %s" % (self.dd_experiment, format))
        if not format:
            return {project.project_id: project for project in results}
             
        results_dict = {project.project_id: {k: v for k, v in project.__dict__.items() if k not in ['_sa_instance_state', 'data_dispatcher_project_index', 'created', 'creator']} for project in results}
        if format == "html":
            for proj_id, values in results_dict.items():
                string = ""
                state = "active" if values.get("active", False) else "inactive"
                for key, value in values.items():
                    if value and key != "data_dispatcher_project_idx":
                        if key == "campaign_id":
                            string += "<a target='_blank' href='/poms/campaign_overview/%s/%s?campaign_id=%s'>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (ctx.experiment, ctx.role, value, key.replace("_", " ").title(),state,key.replace("_", "-"), value)
                        elif key == "campaign_stage_id":
                            string += "<a target='_blank' href='/poms/campaign_stage_info/%s/%s?campaign_stage_id=%s'>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (ctx.experiment, ctx.role, value, key.replace("_", " ").title(),state,key.replace("_", "-"), value)
                        elif key == "submission_id" or key == "depends_on_submission" or key == "recovery_tasks_parent_submission":
                            string += "<a target='_blank' href='/poms/submission_details/%s/%s?submission_id=%s'>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (ctx.experiment, ctx.role, value, key.replace("_", " ").title(),state,key.replace("_", "-"), value)
                        elif key == "depends_on_project" or key == "recovery_tasks_parent_project":
                            string += "<a target='_blank' class='poms-dd-attribute-link' style='cursor:pointer;' onclick='getProjectHandles(this, `%s`, false)'>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (value, key.replace("_", " ").title(),key.replace("_", "-"), value)
                        elif key == "created" or key == "updated":
                            string += "<a>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (key.replace("_", " ").title(),state,key.replace("_", "-"), timestamp_to_readable(value))
                        else:
                            string += "<a>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (key.replace("_", " ").title(),state,key.replace("_", "-"), value)
                results_dict[proj_id] = string
            return results_dict
        elif format == "json":
            for key, values in results_dict.items():
                results_dict[key] = json.dumps(values)
            return results_dict
        elif format == "dict":
            return results_dict
        
        return {}
    
    def store_project(self, ctx, project_id, worker_timeout, idle_timeout, **kwargs):
        project = DataDispatcherProject()
        project.project_id = project_id
        project.worker_timeout = worker_timeout
        project.idle_timeout = idle_timeout
        if kwargs.get("project_name", None):
            project.project_name = kwargs.get("project_name", None)
        if kwargs.get("experiment", None):
            project.experiment = kwargs.get("experiment", None)
        if kwargs.get("role", None):
            project.vo_role = kwargs.get("role", None)
        if kwargs.get("creator", None):
            project.creator = kwargs.get("creator", None)
        if kwargs.get("campaign_id", None):
            project.campaign_id = kwargs.get("campaign_id", None)
        if kwargs.get("campaign_stage_id", None):
            project.campaign_stage_id =kwargs.get("campaign_stage_id", None)
        if kwargs.get("campaign_stage_snapshot_id", None):
            project.campaign_stage_snapshot_id = kwargs.get("campaign_stage_snapshot_id", None)
        if kwargs.get("submission_id", None):
            project.submission_id = kwargs.get("submission_id", None)
        if kwargs.get("split_type", None):
            project.split_type = kwargs.get("split_type", None)
        if kwargs.get("last_split", None):
            project.last_split = kwargs.get("last_split", None)
        if kwargs.get("recovery_position", None):
            project.recovery_position = kwargs.get("recovery_position", None)
        if kwargs.get("depends_on_submission", None):
            project.depends_on_submission = kwargs.get("depends_on_submission", None)
        if kwargs.get("recovery_tasks_parent_submission", None):
            project.recovery_tasks_parent_submission = kwargs.get("recovery_tasks_parent_submission", None)
        # Try getting dependents
        if project.depends_on_submission:
            project.depends_on_project = ctx.db.query(DataDispatcherProject.project_id).filter(DataDispatcherProject.campaign_id == DataDispatcherProject.campaign_id and DataDispatcherProject.submission_id == project.depends_on_submission).one_or_none()
        if project.recovery_tasks_parent_submission:
            project.depends_on_project = ctx.db.query(DataDispatcherProject.project_id).filter(DataDispatcherProject.campaign_id == DataDispatcherProject.campaign_id and DataDispatcherProject.submission_id == project.recovery_tasks_parent_submission).one_or_none()
        
        ctx.db.add(project)
        ctx.db.commit()
        return project
        
    
    def restart_project(self, project_id):
        logit.log("Data-Dispatcher Service | restart_project: %s | Start" % (project_id))
        self.client.restart_handles(project_id, all=True)
        logit.log("Data-Dispatcher Service | restart_project: %s | Complete" % (project_id))
        handles = self.client.list_handles(project_id, with_replicas=True)
        return {"project_handles": handles, "msg":"OK", "stats": self.get_project_stats(handles=handles)}
    
    def activate_project(self, project_id):
        logit.log("Data-Dispatcher Service | restart_project: %s | Start" % (project_id))
        self.client.activate_project(project_id)
        logit.log("Data-Dispatcher Service | restart_project: %s | Complete" % (project_id))
        handles = self.client.list_handles(project_id, with_replicas=True)
        return {"project_handles": handles, "msg":"OK", "stats": self.get_project_stats(handles=handles)}
    
    # For Testing Purposes only
    
    def generate_results_id(self):
        return uuid.uuid4()

    def generate_results_file(self, task_id):
        results_dir = config.get("Data_Dispatcher", "BACKGROUND_JOBS_RESULTS_DIR")
        results_file = "%s/job_%s.json" % (results_dir, task_id)
        return results_file
    
    def start_pass_fail_files_background_task(self, project_id, n_pass, n_fail):
        started = time.perf_counter()
        task_id = self.generate_results_id()
        threading.Thread(target=self.pass_fail_files, daemon=True, args=[project_id, n_pass,n_fail, task_id, started], name=str(task_id)).start()
        return {"task_id": str(task_id), "start": started}
    
    def pass_fail_files(self, project_id, n_pass, n_fail, task_id, started):
        logit.log("Data-Dispatcher Service | Process Files | Pass: %s | Fail: %s | Begin" % (n_pass, n_fail))
        self.test = DataDispatcherTest(self.client, project_id)
        passed, failed = self.test.process_files(n_pass, n_fail)
        files_left = len(self.client.list_handles(project_id, state="initial"))
        logit.log("Data-Dispatcher Service | Process Files | Complete | Passed: %s | Failed: %s | Files Remaining: %s" % (passed, failed, files_left))
        date = datetime.datetime.utcfromtimestamp(time.perf_counter() - started)
        elapsed = datetime.datetime.strftime(date, "%M:%S:%f")
        results = {"running": False, "task_id": str(task_id), "elapsed": elapsed, "success": True,
                   "message": "Updated Project: %s" % (project_id),"Passed": passed, 
                   "Failed" : failed, "Files Remaining" : files_left}
        with open("%s" % (self.generate_results_file(task_id)), 'w') as file:
            json.dump(results, file)
        
    # POMS doesn't like async methods. So instead we are 
    # running some longer running processes in the background and returning a "task_id".
    # We then use this method to track whether the process is still running.
    # Which returns a json object containing results/elapsed time
    def check_task(self, task_id, started):
        results_file = self.generate_results_file(task_id)
        results={"running": True, "task_id": str(task_id)}
        if os.path.exists(results_file):
            with open("%s" % (results_file), 'r') as file:
                results = json.load(file)
            if not results['running']:
                os.remove(results_file)
        else:
            threads = threading.enumerate()
            for thread in threads:
                if thread.name == str(task_id):
                    date = datetime.datetime.utcfromtimestamp(time.perf_counter() - float(started))
                    elapsed = datetime.datetime.strftime(date, "%M:%S:%f")
                    results['elapsed'] = elapsed
        return results
    
    
    #def get_rucio_client(self, client_type):
    #    globals()["get_rucio_client"] = lambda x: "%sClient" % str(x).capitalize(1)
    #    return self.get_rucio_client("Scope")
             
    
    #def add_scope(self, scope="poms_test"):
    #    rucio_host = "https://hypot-rucio.fnal.gov"
    #    auth_host = "https://auth-hypot-rucio.fnal.gov"
    #    account = "ltrestka"
    #    ca_cert = "/etc/grid-security/certificates"
    #    auth_type = "x509_proxy"
    #    scope_client = rucio_client.ScopeClient(rucio_host, auth_host,account,ca_cert,auth_type)
    #    return scope_client.add_scope(account, scope)
    #
    #def add_files_to_datasetadd_scope(self, scope="poms_test"):
    #    rucio_host = "https://hypot-rucio.fnal.gov"
    #    auth_host = "https://auth-hypot-rucio.fnal.gov"
    #    account = "ltrestka"
    #    ca_cert = "/etc/grid-security/certificates"
    #    auth_type = "x509_proxy"
    #    did_client = rucio_client.DIDClient(rucio_host, auth_host,account,ca_cert,auth_type)
            

class DataDispatcherTest:
    def __init__(self, client, project_id):
        self.client = client
        self.project_id = project_id
    
        
    def process_files(self, files_to_pass, files_to_fail):
        x = 1
        passed = 0
        failed = 0
        total_runs = int(files_to_pass) + int(files_to_fail)
        while  passed + failed < total_runs and total_runs > 0:
            try:
                action = "pass" if passed < int(files_to_pass) else "fail"
                
                reserved_file = self.client.next_file(self.project_id, timeout=20)
                if type(reserved_file) is dict: 
                    handle = "%s:%s" % (reserved_file.get("namespace"), reserved_file.get("name"))
                    logit.log("Data-Dispatcher Service | Process Files | Project ID: %s | Handle: %s | Run: %s of %s | State Set To: %s " % (self.project_id, handle, x, total_runs , "reserved"))
                    if action == "pass":
                        self.client.file_done(self.project_id, handle) 
                    else:
                        self.client.file_failed(self.project_id, handle, False)
                    logit.log("Data-Dispatcher Service | Process Files | Project ID: %s | Handle: %s | Run: %s of %s | State Set To: %s " % (self.project_id, handle, x, total_runs , "done" if action == "pass" else "failed"))
                    if action == "pass":
                        passed += 1
                    else:
                        failed += 1
                    x += 1
                    continue
                else:
                    if reserved_file:
                        logit.log("Data-Dispatcher Service | Process Files | Project ID: %s | Handle: %s | Run: %s of %s | State Change Request Timed Out " % (self.project_id, handle, x, total_runs))
                    else:
                        logit.log("Data-Dispatcher Service | Process Files | Project ID: %s | Project Complete" % ((self.project_id)))
                        return passed, failed
            except Exception as e:
                error = "%s" % e
                if error == "Handle not found or was not reserved":
                    logit.log("Data-Dispatcher Service | Process Files | Project ID: %s | Handle: %s | Run: %s of %s | Handle Not Found, Changing Handle" % (self.project_id, handle, x, total_runs))
                elif error == "Inactive project. State=failed":
                    logit.log("Data-Dispatcher Service | Process Files | Project ID: %s | Project Inactive" % ((self.project_id)))
                    break
                else:
                    logit.log("Data-Dispatcher Service | Process Files | Project ID: %s | Handle: %s | Run: %s of %s | Error: %s " % (self.project_id, handle, x, total_runs , e))
        logit.log("Data-Dispatcher Service | Process Files | Project ID: %s |  Returning: %s + %s < %s " % (self.project_id,  passed, failed, total_runs))
        return passed, failed
        
                

        
        
        
        
        
    
            
