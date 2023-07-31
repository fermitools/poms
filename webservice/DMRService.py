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

from rucio.client import Client as RucioClient
from sqlalchemy import and_, distinct, desc
config = configparser.ConfigParser()
config.read(os.environ.get("WEB_CONFIG","/home/poms/poms/webservice/poms.ini"))
PIPE = -1


def timestamp_to_readable(timestamp):
    if type(timestamp) != type(datetime.datetime):
        readable_format = timestamp.strftime("%A, %B %d, %Y")
    else:
        converted_datetime = datetime.datetime.fromtimestamp(timestamp)
        readable_format = converted_datetime.strftime("%A, %B %d, %Y")
    return readable_format

class DMRService:
    
    def __init__(self):
        self.db = None
        self.services_logged_in = {}
        self.experiment = "hypot"
        self.username = "poms"
        self.role = None
        self.dd_client = None
        self.dd_server_url = None
        self.dd_auth_server_url = None
        self.dd_token_file = None
        self.metacat_client = None
        self.metacat_server_url = None
        self.metacat_auth_server_url = None
        self.metacat_token_file = None
        self.rucio_client = None
        self.test = None
    
    def flush(self):
        self.db = None
        self.services_logged_in = None
        self.experiment = None
        self.username = None
        self.role = None
        self.dd_client = None
        self.dd_server_url = None
        self.dd_auth_server_url = None
        self.dd_token_file = None
        self.metacat_client = None
        self.metacat_server_url = None
        self.metacat_auth_server_url = None
        self.metacat_token_file = None
        self.rucio_client = None
        self.test = None
        
    def set_user(self, username):
        self.username = username
        
    def set_role(self, role):
        self.role = role
        
    def set_experiment(self, experiment):
        if not experiment or experiment != "dune": # Temporary until we have experiment server info
            self.experiment = "hypot"
        else:
            self.experiment = experiment
        self.set_configuration()
    
    def set_configuration(self):
        # SET DATA_DISPATCHER CREDS
        self.dd_server_url = config.get("Data_Dispatcher","%s_DATA_DISPATCHER_SERVER_URL" % self.experiment.upper())
        self.dd_auth_server_url = config.get("Data_Dispatcher","%s_DATA_DISPATCHER_AUTH_SERVER_URL" % self.experiment.upper())
        self.dd_token_file = config.get("Data_Dispatcher","%s_DATA_DISPATCHER_TOKEN_FILE" % self.experiment.upper())
        # METACAT CREDS
        self.metacat_server_url = config.get("Metacat","%s_METACAT_SERVER_URL" % self.experiment.upper())
        self.metacat_auth_server_url = config.get("Metacat","%s_METACAT_AUTH_SERVER_URL" % self.experiment.upper())
        self.metacat_token_file = config.get("Metacat","%s_METACAT_TOKEN_FILE" % self.experiment.upper())
        
        # Set token library for non-poms usage
        if self.experiment == "dune":
            self.dd_token_file = "%s/%s/%s/.token_library" % (self.dd_token_file, self.username, self.role)
            self.metacat_token_file = "%s/%s/%s/.token_library" % (self.metacat_token_file, self.username, self.role)

        # Create token file if not exist
        for file in [self.dd_token_file, self.metacat_token_file]:
            if not os.path.exists(file):
                os.system("mkdir -p %s" % file.replace("/.token_library", ""))
                os.system("touch %s" % file)
        
    def update_config_if_needed(self, db, experiment, username, role):
        self.db = db
        reset_clients = False
        if experiment == "dune":
            if self.experiment != "dune" or self.username != username or self.role != role:
                self.experiment = "dune"
                self.username = username
                self.role = role
                reset_clients = True
        elif experiment != "dune":
            if self.experiment != "hypot" or self.username != "poms":
                self.experiment = "hypot"
                self.username = "poms"
                reset_clients = True
            self.role = role
        print("DMR-Service | Config Updates (%s, %s, %s) - provided: (%s, %s, %s)" % (self.experiment, self.username, self.role, experiment, username, role))
        if reset_clients or not self.dd_client or not self.metacat_client:
            self.set_configuration()
            try:
                self.set_data_dispatcher_client()
                self.set_metacat_client()
                return True
            except Exception as e:
                logit.log("DMR-Service | Update Config | Failed To Create New Clients | Exception: %s" % repr(e))
                return False
        else:
            return True

        
    def set_data_dispatcher_client(self):
        try:
            self.dd_client = DataDispatcherClient(server_url=self.dd_server_url, auth_server_url=self.dd_auth_server_url, token_library=self.dd_token_file)
            logit.log("DMR-Service | set_data_dispatcher_client() | Client set to %s - Version: %s" % (self.experiment, self.dd_client.version()))
            return True
        except Exception as e:
            logit.log("DMR-Service | set_data_dispatcher_client() | Exception: %s" % repr(e))
            return False
    
    def set_metacat_client(self):
        try:
            self.metacat_client = MetaCatClient(server_url=self.metacat_server_url, auth_server_url=self.metacat_auth_server_url, token_library=self.metacat_token_file)
            logit.log("DMR-Service | set_metacat_client() | Client set to %s - Version: %s" % (self.experiment, self.metacat_client.get_version()))
            return True
        except Exception as e:
            logit.log("DMR-Service | set_metacat_client() | Exception: %s" % repr(e))
            return False
    
    def set_rucio_client(self):
        rucio_host = config.get("Rucio",'rucio_host')
        auth_host =  config.get("Rucio",'auth_host')
        account =  config.get("Rucio",'account')
        ca_cert =  config.get("Rucio",'ca_cert')
        auth_type =  config.get("Rucio",'auth_type')
        self.rucio_client = RucioClient(rucio_host, auth_host,account,ca_cert,auth_type)
        
    def check_rucio_for_replicas(self, dids):
        if not self.rucio_client:
            self.set_rucio_client()
        return self.rucio_client.list_replicas(dids)
    
    def login_with_password(self, username, password):
        try:
            # Validation check
            if not username or not password:
                return json.dumps({"login_status": "Login Failed, please check username and password and try again."})
            if not self.experiment:
                return json.dumps({"login_status": "Login Failed, please select experiment, then try again."})
            
            # Set new client
            logit.log("DMR-Service  | login_with_password() | Attempting to set new client with experiment: %s" % self.experiment)
            if not self.set_data_dispatcher_client():
                logit.log("DMR-Service  | login_with_password() | Failed to set up client for experiment: %s" % self.experiment)
                return json.dumps({"login_status": "Login Failed: Internal issue. Please contact a POMS administrator for assistance."})
            
            # Try logging in
            logit.log("DMR-Service  | login_with_password() | Attempting login as: %s" % username)
            auth_info = self.dd_client.login_password(username, password)
            if auth_info:
                logit.log("DMR-Service  | login_with_password() | Logged in as %s" % auth_info[0])
            return json.dumps(self.session_status('password')[1])

        except Exception as e:
            logit.log("DMR-Service | login_with_password() | Exception: %s" % repr(e))
            return json.dumps({
                    "login_method": "Attempted login method: password",
                    "login_status": "%s" % repr(e).split("'")[1].replace("\n", "")
                })
            
    def login_with_x509(self):
        try:
            # Set new client
            logit.log("DMR-Service  | login_with_x509() | System Login on behalf of: %s" % self.experiment)
            if not self.set_data_dispatcher_client():
                logit.log("DMR-Service  | login_with_x509() | Failed to set up client for experiment: %s" % self.experiment)
                return json.dumps({"login_status": "Login Failed: Internal issue. Please contact a POMS administrator for assistance."})
            
            # Try logging in
            auth_info = self.dd_client.login_x509('poms', config.get("POMS", "POMS_CERT"), config.get("POMS", "POMS_KEY"))
            if auth_info:
                logit.log("DMR-Service  | login_with_x509() | Logged in as POMS")
                
            return json.dumps(self.session_status('x509')[1])

        except Exception as e:
            logit.log("DMR-Service | login_with_x509() | Exception: %s" % repr(e))
            return json.dumps({
                    "login_method": "Attempted login method: x509",
                    "login_status": "%s" % repr(e).split("'")[1].replace("\n", "").replace("\\","")
                })
            
    def login_metacat(self):
        try:
            # Set new client
            logit.log("Metacat Service  | login_with_x509() | System Login on behalf of: %s" % self.experiment)
            # Try logging in
            auth_info = self.metacat_client.login_x509('poms', config.get("POMS", "POMS_CERT"), config.get("POMS", "POMS_KEY"))
            if auth_info:
                logit.log("Metacat Service  | login_with_x509() | Logged in as POMS")
                return True
            logit.log("Metacat Service  | login_with_x509() | Failed")
            return False

        except Exception as e:
            logit.log("Metacat Service | login_with_x509() | Exception: %s" % repr(e))
            False
    
    def begin_services(self, service="all"):
        try:
            services_logged_in = {
                "data_dispatcher":False,
                "metacat":False, 
                "rucio":False
            }
            if service == "all" or service == "data_dispatcher":
                dd_info = None
                if os.stat(self.dd_token_file).st_size == 0:
                    dd_info = self.dd_client.login_x509(self.username, config.get("POMS", "POMS_CERT"), config.get("POMS", "POMS_KEY"))
                elif self.dd_client:
                    dd_info = self.dd_client.auth_info()
                elif os.path.exists(self.dd_token_file):
                    token_library_content = open(self.dd_token_file, 'r').read()
                    if token_library_content and len(token_library_content.split(" ")) > 1:
                        dd_token = token_library_content.split(" ")[1].strip()
                        dd_info = self.dd_client.login_token(self.username, dd_token)
                    else:
                        dd_info = self.dd_client.login_x509(self.username, config.get("POMS", "POMS_CERT"), config.get("POMS", "POMS_KEY"))
                if dd_info:
                    services_logged_in["data_dispatcher"] = True
            if service == "all" or service == "metacat":
                mc_info = None
                if os.stat(self.metacat_token_file).st_size == 0:
                    mc_info = self.metacat_client.login_x509(self.username, config.get("POMS", "POMS_CERT"), config.get("POMS", "POMS_KEY"))
                elif self.metacat_client:
                    mc_info = self.metacat_client.auth_info()
                elif os.path.exists(self.metacat_token_file):
                    metacat_token_library_content = open(self.metacat_token_file , 'r').read()
                    if metacat_token_library_content and len(metacat_token_library_content.split(" ")) > 1:
                        mc_token = metacat_token_library_content.split(" ")[1].strip()
                        mc_info = self.metacat_client.login_token(self.username, mc_token)
                    else:
                        mc_info = self.metacat_client.login_x509(self.username, config.get("POMS", "POMS_CERT"), config.get("POMS", "POMS_KEY"))
                if mc_info:
                    services_logged_in["metacat"] = True
            if service == "rucio":
                self.set_rucio_client()
                if self.rucio_client:
                    services_logged_in["rucio"] = True
                    
            self.services_logged_in = services_logged_in
            
                    
        except Exception as e:
            logit.log("DMR Service | System Login | Exception: %s" % repr(e))
            False
    
    def session_status(self, auth_type='Auth_Token'):
        try:
            logit.log("DMR-Service  | get_session(%s) | Begin")
            auth_info = None
            if self.dd_client:
                auth_info = self.dd_client.auth_info()
            elif not self.dd_client and os.path.exists(self.dd_token_file):
                if os.path.exists(self.dd_token_file):
                    token_library_content = open(self.dd_token_file, 'r').read()
                    token = None
                    if len(token_library_content.split(" ")) > 1:
                        token = token_library_content.split(" ")[1].strip()
                    self.set_data_dispatcher_client()
                    auth_info = self.dd_client.login_token(self.username, token)
                    logit.log("DMR-Service  | session_status(%s) | Logged in as %s" % (auth_type, token))
            if auth_info:
                    logit.log("DMR-Service  | session_status(%s) | Logged in as %s" % (auth_type, auth_info[0]))
                    return True, {
                        "login_method" : auth_type,
                        "login_status": 'Logged in', 
                        "experiment": self.experiment, 
                        "dd_username":auth_info[0], 
                        "timestamp":auth_info[1]
                    }
        except Exception as e:
            logit.log("DMR-Service  | session_status(%s) | Exception:  %s" % (auth_type, repr(e)))
            message = repr(e).split("'")[1].replace("\\n", "").replace("\\","")
            logit.log("DMR-Service  | session_status(%s) | Failed to get session:  %s" % (auth_type, message))
            if message == "No token found":
                return False, {"login_status": "Not logged in"}
            else:
                return False, {
                        "login_method": "Attempted login method: %s" % auth_type,
                        "login_status": "%s" % message
                    }
        return False, {"login_status": "Not logged in"}
    
    
    def list_rses(self):
        logit.log("DMR-Service  | list_rses(%s, %s) | Begin" % (self.experiment, self.role))
        retval = self.dd_client.list_rses()
        logit.log("DMR-Service  | list_rses(%s, %s) | Success" % (self.experiment, self.role))
        return retval
    
    def list_all_projects(self, **kwargs):
        logit.log("DMR-Service  | list_all_projects(%s, %s) | Begin" % (self.experiment, self.role))
        retval = {}
        
        retval['projects_active'] = self.dd_client.list_projects(with_files=False)
        retval['projects_active_count'] = len(retval.get("projects_active", 0))
        for p_state in ["done", "failed","cancelled", "abandoned"]:
            projects = self.dd_client.list_projects(state=p_state,  not_state='active')
            retval["projects_%s" % p_state] = projects
            retval["projects_%s_count" % p_state] = len(projects)
    
        poms_attributes = self.find_poms_data_dispatcher_projects(format="html", **kwargs)  
        if poms_attributes:
            retval['poms_attributes'] = poms_attributes
                
        logit.log("DMR-Service  | list_projects(%s, %s) | Success" % (self.experiment, self.role))
        return retval
    
    def list_filtered_projects(self, **kwargs):
        logit.log("DMR-Service  | list_filtered_projects(%s, %s) | Begin" % (self.experiment, self.role))
        retval = {}
        
        poms_attributes = self.find_poms_data_dispatcher_projects(format="html", **kwargs)  
        if poms_attributes is not None:
            retval['poms_attributes'] = poms_attributes
            logit.log("DMR-Service  | list_filtered_projects(%s, %s) | Got poms project-attributes: %s" % (self.experiment, self.role, poms_attributes))
            retval['projects_active'] = [project for project in self.dd_client.list_projects() if project.get("project_id") in poms_attributes]
            retval['projects_active_count'] = len(retval["projects_active"])
            for p_state in ["done", "failed","cancelled", "abandoned"]:
                retval["projects_%s" % p_state] = [project for project in self.dd_client.list_projects(state=p_state,  not_state='active') if project.get("project_id") in poms_attributes] or []
                retval['projects_%s_count' % p_state] = len(retval["projects_%s" % p_state])
        else:
            for p_state in ["active", "done", "failed","cancelled", "abandoned"]:
                retval["projects_%s" % p_state] = []
                retval['projects_%s_count' % p_state] = len(retval["projects_%s" % p_state])
            
                
        logit.log("DMR-Service  | list_filtered_projects(%s, %s) | Success" % (self.experiment, self.role))
        return retval
    
    def get_project_handles(self, project_id, state=None, not_state=None):
        logit.log("DMR-Service  | get_project_handles(%s, %s) | project_id: %s | Begin" % (self.experiment, self.role, project_id))
       
        retval = None
        msg = "Fail"
        try:
            project_info = self.dd_client.get_project(project_id, True, with_replicas=True)
            retval = project_info.get("file_handles", []) if project_info else None
            if state:
                retval = [ handle for handle in retval if handle.state == state ]
            if not_state:
                retval = [ handle for handle in retval if handle.state != state ]
            if retval:
                msg = "OK"
                logit.log("DMR-Service  | get_project_handles(%s, %s) | project_id: %s | Success" % (self.experiment, self.role, project_id))
        except Exception as e:
            retval = {"exception": repr(e)}
            logit.log("DMR-Service  | get_project_handles(%s, %s) | project_id: %s | Exception: %s" % (self.experiment, self.role, project_id, e))
            raise e
        return {"project_handles": retval, "msg": msg, "stats": self.get_project_stats(handles=retval), "project_details": {k: project_info[k] for k in set(list(project_info.keys())) - set(["file_handles"])}}
    
    def get_project_stats(self, handles=None, project_id=None):
        retval = {"initial": 0,"done" : 0,"reserved" : 0,"failed" : 0}
        if not handles and project_id:
            handles = self.dd_client.list_handles(project_id)
        for handle in handles:
            retval[handle.get("state")] += 1
        return retval
    
    def get_output_file_details_for_submissions(self, dd_projects):
        output_files = []
        output_list = []
        for dd_project in dd_projects:
            project_info = self.dd_client.get_project(dd_project.project_id, True)
            all_files_query = project_info.get("query", None)
            if not all_files_query:
                handles = project_info.get("file_handles", []) if project_info else None
                file_dict={}
                for handle in handles:
                    file_dict = self.place_handle_files_with_condition(handle, file_dict)
                all_files_query = self.make_metacat_query_from_dict(file_dict)
                
            # Now get the children  
            all_files = self.metacat_client.query(all_files_query)
            if all_files:
                all_files = [tup[1] for tup in list(enumerate(all_files))]
                
            child_dids, children_produced_dict = self.get_hierarchy_info_from_files(all_files, "children")
            available_output_query = self.make_metacat_query_from_dict(children_produced_dict)
            output_files.append(available_output_query)
            output_list.append(len(child_dids))
            
        return (output_files, output_list)
    
    def get_hierarchy_info_from_files(self, files, level=None):
        parent_dids = []
        child_dids = []
        parent_files_needed_query = {}
        children_produced_query = {}
        for item in files:
            if not level or level == "parents":
                parents = item.get("parents", None)
                if parents:
                    for file in parents:
                        parent_dids.append("%s:%s" % (file.get("namespace"), file.get("name")))
                        parent_files_needed_query = self.place_handle_files_with_condition(file, parent_files_needed_query)
            if not level or level == "children":
                children = item.get("children", None)
                if children:
                    for file in children:
                        child_dids.append("%s:%s" % (file.get("namespace"), file.get("name")))
                        children_produced_query = self.place_handle_files_with_condition(file, children_produced_query)
                            
        if not level: return parent_dids, parent_files_needed_query, child_dids, children_produced_query
        elif level == "parents": return parent_dids, parent_files_needed_query
        elif level == "children": return child_dids, children_produced_query
        
    def make_metacat_query_from_dict(self, dict):
        if len(dict.keys()) == 0:
            return "fids 0" # returns nothing
        elif len(dict.keys()) == 1:
            return dict[list(dict.keys())[0]]
        else:
            return "union(%s)" % ", ".join(dict[key] for key in dict.keys())
        
    def place_handle_files_with_condition(self, handle, dict, condition=True, incr_if_condition = None):
        try:
            if condition:
                if incr_if_condition != None:
                    incr_if_condition += 1
                if handle.get("namespace") not in dict:
                    dict[handle.get("namespace")] = "files %s:%s" % (handle.get("namespace"), handle.get("name"))
                else:
                    dict[handle.get("namespace")] += ", %s" % handle.get("name")
        except Exception as e:
            logit.log("Error in placement: %s" % e)
        if incr_if_condition != None: 
            return dict, incr_if_condition
        else: 
            return dict
    
    def get_file_stats_for_submissions(self, dd_projects):
        # Return values
        statistics = []
        all_files_queries = []
        done_files_queries = []
        failed_files_queries = []
        reserved_files_queries = []
        unknown_files_queries = []
        submitted_files_queries = []
        parent_files_needed_queries = []
        available_parent_files_queries = []
        children_produced_queries = []
        available_children_queries = []
        if not dd_projects:
            return (
            all_files_queries,
            done_files_queries,
            failed_files_queries,
            reserved_files_queries,
            unknown_files_queries,
            submitted_files_queries,
            parent_files_needed_queries,
            available_parent_files_queries,
            children_produced_queries,
            available_children_queries,
            statistics
        )
        
        project_projects =  { item.project_id: item for item in dd_projects}
        project_queries = { item.project_id: [] for item in dd_projects}
        
        if not self.login_metacat():
            return
        
        for project_id in project_queries.keys():
            project = self.dd_client.get_project(project_id, True, True)
            current_project_submission = project_projects[project_id]
            
            file_handles = project.get("file_handles", []) if project else None
            if not file_handles:
                raise ValueError("Invalid Project, No files for this project.")
            
            file_stats = {"initial": 0,"done" : 0,"reserved" :0,"failed" : 0, "unknown": 0, "pct_complete":0, "submitted":0}
            all_project_files = {}
            query_done = {}
            query_failed = {}
            query_reserved = {}
            query_unknown = {}
            all_files_query = None
            
            # If the campaign has a query defining the files they want, use that as the master list,
            # dont build it based on files
            if current_project_submission.campaign_stage_obj.data_dispatcher_dataset_query:
                all_files_query = current_project_submission.campaign_stage_obj.data_dispatcher_dataset_query
                project_queries[project_id].append(all_files_query)
                query_dicts = {'query_done':query_done, 'query_failed':query_failed, 'query_reserved':query_reserved}
            else:
                query_dicts = {'all_project_files':all_project_files, 'query_done':query_done, 'query_failed':query_failed, 'query_reserved':query_reserved}
            file_stats["unknown"] = 0
            for handle in file_handles:
                # Add to appropriate count
                file_state = handle.get("state")
                file_stats[file_state] +=1
                query_unknown, file_stats["unknown"] = self.place_handle_files_with_condition(handle, query_unknown, ("replicas" in handle and len(handle.get("replicas").keys()) == 0), file_stats["unknown"])
                # Create metacat queries for files, and files with certain data_dispatcher states, separated by namespace.
                for name,item_ in query_dicts.items():
                    item_ = self.place_handle_files_with_condition(handle, item_, name == "all_project_files" or handle.get("state") == name.split("_")[1])

                    
            query_dicts["query_unknown"] = query_unknown
            # Now that the queries are populated, we can associate our queries to the project_id, and union multiple datasets if necessary
            for name, dict_ in query_dicts.items():
                project_queries[project_id].append(self.make_metacat_query_from_dict(dict_))
                # Get all files query to find parents
                if name == "all_project_files":
                    all_files_query = project_queries[project_id][-1]
                    
            # Get available parents and children
            all_files = self.metacat_client.query(all_files_query.replace("'", ""))
            if all_files:
                all_files = [tup[1] for tup in list(enumerate(all_files))]
            
            parent_dids, parent_files_needed_query, child_dids, children_produced_query = self.get_hierarchy_info_from_files(all_files)
            
            available_parent_files_query = {}
            available_children_query = {}
            file_stats["total"] = sum(1 for _ in all_files)
            file_stats["parents_needed"] = len(parent_dids)
            file_stats["children_produced"] = len(child_dids)
            file_stats["parents_available"] = 0
            file_stats["children_available"] = 0
            
            if parent_dids:        
                for file in self.check_for_replicas(parent_dids):
                    available_parent_files_query, file_stats["parents_available"] = self.place_handle_files_with_condition(file, available_parent_files_query, incr_if_condition=file_stats["parents_available"])
            if child_dids:        
                for file in self.check_for_replicas(child_dids):
                    available_children_query, file_stats["children_available"] = self.place_handle_files_with_condition(file, available_children_query, incr_if_condition=file_stats["children_available"])
                    
            # Now that the queries are populated, we can associate our queries to the project_id, and union multiple datasets if necessary
            query_dicts = {
                    "parent_files_needed_query": parent_files_needed_query, 
                    "available_parent_files_query": available_parent_files_query,
                    "children_produced_query": children_produced_query, 
                    "available_children_query": available_children_query
                }
            for name, dict_ in query_dicts.items():
                project_queries[project_id].append(self.make_metacat_query_from_dict(dict_))
            
            file_stats["submitted"] = file_stats.get("done") + file_stats.get("reserved") + file_stats.get("failed")
            file_stats["pct_complete"] = "%d%%" % int((file_stats.get("submitted") / file_stats.get("total")) * 100) if file_stats.get("total") != 0 else 0
            
            project_queries[project_id].append(file_stats)
        
        
        for project in dd_projects:
            all_files_queries.append(project_queries[project.project_id][0])
            done_files_queries.append(project_queries[project.project_id][1])
            failed_files_queries.append(project_queries[project.project_id][2])
            reserved_files_queries.append(project_queries[project.project_id][3])
            unknown_files_queries.append(project_queries[project.project_id][4])
            submitted_files_queries.append("union(%s)" % ", ".join(project_queries[project.project_id][1:4]))
            parent_files_needed_queries.append(project_queries[project.project_id][5])
            available_parent_files_queries.append(project_queries[project.project_id][6])
            children_produced_queries.append(project_queries[project.project_id][7])
            available_children_queries.append(project_queries[project.project_id][8])
            statistics.append(project_queries[project.project_id][9])
            
        return (
            all_files_queries,
            done_files_queries,
            failed_files_queries,
            reserved_files_queries,
            unknown_files_queries,
            submitted_files_queries,
            parent_files_needed_queries,
            available_parent_files_queries,
            children_produced_queries,
            available_children_queries,
            statistics
        )

        
        
    def copy_project(self, project_id, **kwargs):
        kwargs["use_hostname"] = True
        new_project = self.dd_client.copy_project(project_id)
        logit.log("DMR-Service  | experiment: %s | copy_project(%s) | done" % (self.experiment, project_id))
        return new_project
    
    def get_project_for_submission(self, project_id, **kwargs):
        logit.log("DMR-Service  | experiment: %s | get_project_for_submission(%s) | submission_id: %s " % (self.experiment, project_id, kwargs.get("submission_id")))
        existing_project = self.dd_client.get_project(project_id)
        if existing_project:
            logit.log("DMR-Service  | experiment: %s | get_project_for_submission(%s) | located project" % (self.experiment, project_id))
            worker_timeout = existing_project.get('worker_timeout', None)
            idle_timeout = existing_project.get('idle_timeout', None)
            project = self.store_project(project_id = project_id, worker_timeout=worker_timeout, idle_timeout=idle_timeout, **kwargs)
            logit.log("DMR-Service  | experiment: %s |  get_project_for_submission(%s) | stored data-dispatcher project information in database" % (self.experiment, project.project_id))
            logit.log("DMR-Service  | experiment: %s | get_project_for_submission(%s) | done" % (self.experiment, project_id))
            return project
        return None
            
    def create_project(self, username, dataset=None, files=[], **kwargs):
        logit.log("DMR-Service  | experiment: %s | create_project() | Begin " % (self.experiment))
        if files:
            pass # create files based on recovery files
                   
        if dataset:
            logit.log("DMR-Service  | experiment: %s | create_project() | Creating project from dataset: %s " % (self.experiment, dataset))
            users = []
            if "creator_name" in kwargs:
                users.append(kwargs.get("creator_name"))
            if username not in users:
                users.append(username)
            self.login_metacat()
            files = self.metacat_client.query(dataset)
            logit.log("DMR-Service  | experiment: %s | create_project() | located files from dataset: %s" % (self.experiment, files))
            new_project = self.dd_client.create_project(files, query=dataset, users=users)
            logit.log("DMR-Service  | experiment: %s | create_project() | created data-dispatcher project: %s" % (self.experiment, new_project))
            project_id = new_project.get('project_id', None)
            worker_timeout = new_project.get('worker_timeout', None)
            idle_timeout = new_project.get('idle_timeout', None)
            project = self.store_project(project_id = project_id, worker_timeout=worker_timeout, idle_timeout=idle_timeout, **kwargs)
            logit.log("DMR-Service  | experiment: %s | create_project() | stored data-dispatcher project information in database - Project ID: %s" % (self.experiment, project.project_id))
            logit.log("DMR-Service  | experiment: %s | create_project() | done" % (self.experiment))
            return project
        
        return None
    
    def query_files(self):
        logit.log("DMR-Service  | login_metacat() | Attempting to set metacat client with experiment: %s" % self.experiment)
        auth_info = self.metacat_client.login_x509('poms', config.get("Data_Dispatcher", "POMS_CERT"), config.get("Data_Dispatcher", "POMS_KEY"))
        
    
    def find_poms_data_dispatcher_projects(self, format=None, **kwargs):
        try:
            logit.log("DMR-Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | begin" % (self.experiment, format))
            if not self.db:
                logit.log("DMR-Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | fail: no database access" % (self.experiment, format))
                return {}
            
            query = self.db.query(DataDispatcherProject).filter(DataDispatcherProject.experiment == self.experiment)
            searchList = ["experiment=%s" % self.experiment]
            if kwargs.get("project_id", None):
                # Project id is known, so only one result would exist
                searchList.append("project_id=%s" % kwargs["project_id"])
                query = query.filter(DataDispatcherProject.project_id == kwargs["project_id"])
                project = query.one_or_none()
                logit.log("DMR-Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | searched: %s | results: %s" % (self.experiment, format, ", ".join(searchList), 1 if project else 0))
                if project:
                    return self.format_projects(format, [project])
            if kwargs.get("submission_id", None):
                # One project per submission, if this exists we can return right away
                searchList.append("submission_id=%s" % kwargs["submission_id"])
                query = query.filter(DataDispatcherProject.submission_id == kwargs["submission_id"])
                project = query.one_or_none()
                logit.log("DMR-Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | searched: submission_id=%s | results: %s" % (self.experiment, format, ", ".join(searchList), 1 if project else 0))
                if project:
                    return self.format_projects(format, [project])
                
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
            if kwargs.get("job_type_snapshot_id", None):
                searchList.append("job_type_snapshot_id=%s" % kwargs["job_type_snapshot_id"])
                query = query.filter(DataDispatcherProject.job_type_snapshot_id == kwargs["job_type_snapshot_id"])
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
            
            projects = query.order_by(DataDispatcherProject.created).all()
            logit.log("DMR-Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | searched: %s | results: %s" % (self.experiment, format, ", ".join(searchList), len(projects)))
            if len(projects) > 0:
                return self.format_projects(format, projects)
 
        except Exception as e:
            raise e
        
        return {}
    
    def format_projects(self, format=None, results = None):
        logit.log("DMR-Service  | experiment: %s | format_projects() | format: %s" % (self.experiment, format))
        if not format:
            return {project.project_id: project for project in results}
             
        results_dict = {project.project_id: {k: v for k, v in project.__dict__.items() if k not in ['_sa_instance_state', 'data_dispatcher_project_index', 'created', 'creator']} for project in results}
        if format == "html":
            for proj_id, values in results_dict.items():
                string = ""
                state = "active" if values.get("active", False) else "inactive"
                for key, value in values.items():
                    if value and key != "data_dispatcher_project_idx" and key != 'updater':
                        if key == "campaign_id":
                            string += "<a target='_blank' href='/poms/campaign_overview/%s/%s?campaign_id=%s'>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (self.experiment, self.role, value, key.replace("_", " ").title(),state,key.replace("_", "-"), value)
                        elif key == "campaign_stage_id":
                            string += "<a target='_blank' href='/poms/campaign_stage_info/%s/%s?campaign_stage_id=%s'>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (self.experiment, self.role, value, key.replace("_", " ").title(),state,key.replace("_", "-"), value)
                        elif key == "submission_id" or key == "depends_on_submission" or key == "recovery_tasks_parent_submission":
                            string += "<a target='_blank' href='/poms/submission_details/%s/%s?submission_id=%s'>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (self.experiment, self.role, value, key.replace("_", " ").title(),state,key.replace("_", "-"), value)
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
    
    def store_project(self, project_id, worker_timeout, idle_timeout, **kwargs):
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
        if kwargs.get("job_type_snapshot_id", None):
            project.job_type_snapshot_id = kwargs.get("job_type_snapshot_id", None)
        if kwargs.get("recovery_position", None):
            project.recovery_position = kwargs.get("recovery_position", None)
        if kwargs.get("depends_on_submission", None):
            project.depends_on_submission = kwargs.get("depends_on_submission", None)
        if kwargs.get("recovery_tasks_parent_submission", None):
            project.recovery_tasks_parent_submission = kwargs.get("recovery_tasks_parent_submission", None)
        # Try getting dependents
        if project.depends_on_submission:
            project.depends_on_project = self.db.query(DataDispatcherProject.project_id).filter(DataDispatcherProject.campaign_id == DataDispatcherProject.campaign_id and DataDispatcherProject.submission_id == project.depends_on_submission).one_or_none()
        if project.recovery_tasks_parent_submission:
            project.depends_on_project = self.db.query(DataDispatcherProject.project_id).filter(DataDispatcherProject.campaign_id == DataDispatcherProject.campaign_id and DataDispatcherProject.submission_id == project.recovery_tasks_parent_submission).one_or_none()
        
        self.db.add(project)
        self.db.commit()
        return project
        
    
    def restart_project(self, project_id):
        logit.log("DMR-Service | restart_project: %s | Start" % (project_id))
        self.dd_client.restart_handles(project_id, all=True)
        logit.log("DMR-Service | restart_project: %s | Complete" % (project_id))
        handles = self.dd_client.list_handles(project_id, with_replicas=True)
        return {"project_handles": handles, "msg":"OK", "stats": self.get_project_stats(handles=handles)}
    
    def activate_project(self, project_id):
        logit.log("DMR-Service | restart_project: %s | Start" % (project_id))
        self.dd_client.activate_project(project_id)
        logit.log("DMR-Service | restart_project: %s | Complete" % (project_id))
        handles = self.dd_client.list_handles(project_id, with_replicas=True)
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
        logit.log("DMR-Service | Process Files | Pass: %s | Fail: %s | Begin" % (n_pass, n_fail))
        self.test = DataDispatcherTest(self.dd_client, project_id)
        passed, failed = self.test.process_files(n_pass, n_fail)
        files_left = len(self.dd_client.list_handles(project_id, state="initial"))
        logit.log("DMR-Service | Process Files | Complete | Passed: %s | Failed: %s | Files Remaining: %s" % (passed, failed, files_left))
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
    
            

class DataDispatcherTest:
    def __init__(self, client, project_id):
        self.dd_client = client
        self.project_id = project_id
    
        
    def process_files(self, files_to_pass, files_to_fail):
        x = 1
        passed = 0
        failed = 0
        total_runs = int(files_to_pass) + int(files_to_fail)
        while  passed + failed < total_runs and total_runs > 0:
            try:
                action = "pass" if passed < int(files_to_pass) else "fail"
                
                reserved_file = self.dd_client.next_file(self.project_id, timeout=20)
                if type(reserved_file) is dict: 
                    handle = "%s:%s" % (reserved_file.get("namespace"), reserved_file.get("name"))
                    logit.log("DMR-Service | Process Files | Project ID: %s | Handle: %s | Run: %s of %s | State Set To: %s " % (self.project_id, handle, x, total_runs , "reserved"))
                    if action == "pass":
                        self.dd_client.file_done(self.project_id, handle) 
                    else:
                        self.dd_client.file_failed(self.project_id, handle, False)
                    logit.log("DMR-Service | Process Files | Project ID: %s | Handle: %s | Run: %s of %s | State Set To: %s " % (self.project_id, handle, x, total_runs , "done" if action == "pass" else "failed"))
                    if action == "pass":
                        passed += 1
                    else:
                        failed += 1
                    x += 1
                    continue
                else:
                    if reserved_file:
                        logit.log("DMR-Service | Process Files | Project ID: %s | Handle: %s | Run: %s of %s | State Change Request Timed Out " % (self.project_id, handle, x, total_runs))
                    else:
                        logit.log("DMR-Service | Process Files | Project ID: %s | Project Complete" % ((self.project_id)))
                        return passed, failed
            except Exception as e:
                error = "%s" % e
                if error == "Handle not found or was not reserved":
                    logit.log("DMR-Service | Process Files | Project ID: %s | Handle: %s | Run: %s of %s | Handle Not Found, Changing Handle" % (self.project_id, handle, x, total_runs))
                elif error == "Inactive project. State=failed":
                    logit.log("DMR-Service | Process Files | Project ID: %s | Project Inactive" % ((self.project_id)))
                    break
                else:
                    logit.log("DMR-Service | Process Files | Project ID: %s | Handle: %s | Run: %s of %s | Error: %s " % (self.project_id, handle, x, total_runs , e))
        logit.log("DMR-Service | Process Files | Project ID: %s |  Returning: %s + %s < %s " % (self.project_id,  passed, failed, total_runs))
        return passed, failed
        
                

        
        
        
        
        
    
            
