import configparser
import copy
from ctypes import Array
import os
import time
import re
from io import BytesIO
import subprocess
import json
from typing import Any, Dict, List
import cherrypy
import base64
import datetime
import threading
import uuid
import hashlib
import base64

import utc
from . import logit
from urllib.parse import unquote, urlencode
from poms_model import Campaign, DataDispatcherSubmission, CampaignStage, Submission
from .FilesPOMS import LocalJsonQueue
from pythreader import Timeout

import toml
from toml_parser import TConfig
from data_dispatcher.api import DataDispatcherClient
from metacat.webapi import MetaCatClient
from cryptography.fernet import Fernet
from sqlalchemy import and_, distinct, desc
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

poms_config = TConfig()
PIPE = -1

def timestamp_to_readable(timestamp):
    if type(timestamp) != type(datetime.datetime):
        readable_format = timestamp.strftime("%A, %B %d, %Y")
    else:
        converted_datetime = datetime.datetime.fromtimestamp(timestamp)
        readable_format = converted_datetime.strftime("%A, %B %d, %Y")
    return readable_format

class AuthenticationError(Exception):
    """Raised when authentication fails"""
    pass

class DMRService:
    
    def __init__(self, config = cherrypy.config.get("Shrek", {})):
        self.db = None
        self.experiment = None
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
        self.config = config
        
    
    def initialize_session(self, ctx, agent_session=None, cron_session=False):
        needs_new = False
        session_id_only = False
        if agent_session:
            ctx.role = agent_session["role"]
            ctx.experiment = agent_session["experiment"]
            ctx.username = "poms"

        if not cherrypy.session.get("Shrek", None):
            logit.log("DMR-Service | Initializing Session")
            needs_new = True
        if not needs_new and cherrypy.session.get("Shrek", {}).get("current_experiment", None) != ctx.experiment:
            logit.log("DMR-Service | Experiment Changed | Updating Session")
            needs_new = True
        if not needs_new and cherrypy.session.get("Shrek", {}).get("role", None) != ctx.role:
            logit.log("DMR-Service | Role Changed | Updating Session")
            needs_new = True
            session_id_only = True
            
        if needs_new:
            session_info = { 'user': ctx.username, 'experiment':ctx.experiment , 'role': ctx.role, 'session': cherrypy.session.id }
            secure_session_id = hashlib.sha256(json.dumps(session_info).encode()).hexdigest()
            session_info["session"] = secure_session_id
            logit.log(f"Shrek | New Session Info | {ctx.username}: {session_info}" )
            if not session_id_only:
                cherrypy.session["Shrek"] = {
                        "onboarded": self.config.get(ctx.experiment, None) is not None,
                        "current_experiment": ctx.experiment,
                        "user": ctx.username,
                        "role": ctx.role,
                        "dd_client": None,
                        "dd_status": None,
                        "mc_client": None,
                        "mc_status": None,
                        "session": secure_session_id,
                        "services_logged_in": {"data_dispatcher": False, "metacat": False}
                    }
            self.db = ctx.db if not self.db and ctx.db else cherrypy.request.db
            if cherrypy.session["Shrek"]["onboarded"]:
                return self.set_configuration(cron_session)
        cherrypy.session["Shrek"]["user"] = ctx.username
        cherrypy.session["Shrek"]["role"] = ctx.role
    
    

        
    
    def flush(self):
        self.db = None
        self.experiment = None
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
        
    
    def set_configuration(self, cron_session=False):
        # SET DATA_DISPATCHER CREDS
        experiment = cherrypy.session["Shrek"]["current_experiment"]
        self.dd_server_url = self.config[experiment]["data_dispatcher"]["DATA_DISPATCHER_URL"]
        self.dd_auth_server_url = self.config[experiment]["data_dispatcher"]["DATA_DISPATCHER_AUTH_URL"]
        self.dd_token_file = self.config[experiment]["data_dispatcher"]["TOKEN_FILE"]
        # SET METACAT CREDS
        self.metacat_server_url = self.config[experiment]["metacat"]["METACAT_SERVER_URL"]
        self.metacat_auth_server_url = self.config[experiment]["metacat"]["METACAT_AUTH_SERVER_URL"]
        self.metacat_token_file = self.config[experiment]["metacat"]["TOKEN_FILE"]

        # Create token file if not exist
        for file in [self.dd_token_file, self.metacat_token_file]:
            if not os.path.exists(file):
                os.system("mkdir -p %s" % file.replace("/.token_library", ""))
                os.system("touch %s" % file)
                
        try:
            self.set_data_dispatcher_client()
            self.set_metacat_client()
            if cron_session:
                self.dd_client = cherrypy.session.get("Shrek", {}).get("dd_client")
                self.metacat_client = cherrypy.session.get("Shrek", {}).get("mc_client")
            
            return True
        except Exception as e:
            logit.log("DMR-Service | Update Config | Failed To Create New Clients | Exception: %s" % repr(e))
            return False
        

        
    def set_data_dispatcher_client(self):
        try:
            if not cherrypy.session["Shrek"].get("dd_client", None):
                cherrypy.session["Shrek"]["dd_client"] = DataDispatcherClient(server_url=self.dd_server_url, auth_server_url=self.dd_auth_server_url, token_library=self.dd_token_file)

            cherrypy.session["Shrek"]["dd_client"] = cherrypy.session["Shrek"]["dd_client"]
            logit.log("DMR-Service | set_data_dispatcher_client() | Client set to %s - Version: %s" % (cherrypy.session["Shrek"]["current_experiment"], cherrypy.session["Shrek"]["dd_client"].version()))
            return True
        except Exception as e:
            logit.log("DMR-Service | set_data_dispatcher_client() | Exception: %s" % repr(e))
            return False
    
    def set_metacat_client(self):
        try:
            if not cherrypy.session["Shrek"].get("mc_client", None):
                cherrypy.session["Shrek"]["mc_client"] = MetaCatClient(server_url=self.metacat_server_url, auth_server_url=self.metacat_auth_server_url, token_library=self.metacat_token_file)
            logit.log("DMR-Service | set_metacat_client() | Client set to %s - Version: %s" % (cherrypy.session["Shrek"]["current_experiment"], cherrypy.session["Shrek"]["mc_client"].get_version()))
            return True
        except Exception as e:
            logit.log("DMR-Service | set_metacat_client() | Exception: %s" % repr(e))
            return False
    
    # We dont need this right now
    #def set_rucio_client(self):
    #    rucio_host = poms_config.get("Rucio",'rucio_host')
    #    auth_host =  poms_config.get("Rucio",'auth_host')
    #    account =  poms_config.get("Rucio",'account')
    #    ca_cert =  poms_config.get("Rucio",'ca_cert')
    #    auth_type =  poms_config.get("Rucio",'auth_type')
    #    self.rucio_client = RucioClient(rucio_host, auth_host,account,ca_cert,auth_type)
    #    
    #def check_rucio_for_replicas(self, dids):
    #    if not self.rucio_client:
    #        self.set_rucio_client()
    #    return self.rucio_client.list_replicas(dids)
            
    def login_with_x509(self):
        try:
            # Set new client
            logit.log(f"DMR-Service  | login_with_x509() | session: {cherrypy.session['Shrek']['session']}")
            if not cherrypy.session["Shrek"]["onboarded"]:
                logit.log("DMR-Service  | login_with_x509() | Skipped, not onboarded | experiment: %s" % cherrypy.session["Shrek"]["current_experiment"])
                return json.dumps({"login_status": "Prevented: Experiment is not Onboarded. Please contact a POMS administrator for assistance."})
            
            # Try logging in
            logit.log("DMR-Service  | login_with_x509() | Data Dispatcher Client Login on behalf of: %s" % cherrypy.session["Shrek"]["current_experiment"])
            cherrypy.session["Shrek"]["dd_status"] = cherrypy.session["Shrek"]["dd_client"].login_x509('poms', poms_config.get("POMS", "POMS_CERT"), poms_config.get("POMS", "POMS_KEY"))
            print("Login X509 | subject: %s | expiration: %s" % cherrypy.session["Shrek"]["dd_status"])
            logit.log("DMR-Service  | login_with_x509() | Metacat Client Login on behalf of: %s" % cherrypy.session["Shrek"]["current_experiment"])
            cherrypy.session["Shrek"]["mc_status"] = cherrypy.session["Shrek"]["mc_client"].login_x509('poms', poms_config.get("POMS", "POMS_CERT"), poms_config.get("POMS", "POMS_KEY"))
            if cherrypy.session["Shrek"]["dd_status"]:
                logit.log(f'DMR-Service  | login_with_x509() | Data Dispatcher Client | {cherrypy.session["Shrek"]["user"]} Logged in to DataDispatcher as POMS | session: {cherrypy.session["Shrek"]["session"]}')
            if cherrypy.session["Shrek"]["mc_status"]:
                logit.log(f'DMR-Service | login_with_x509() | Metacat Client | {cherrypy.session["Shrek"]["user"]} Logged in to Metacat as POMS | session: {cherrypy.session["Shrek"]["session"]}')
                
            return json.dumps(self.session_status('x509')[1])
        except Exception as e:
            logit.log("DMR-Service | login_with_x509() | %s" % repr(e).replace('\n', ''))
            return json.dumps({
                    "login_method": "Attempted login method: x509",
                    "login_status": f"{e}" % repr(e).replace('\n', '')
                })
            
    def login_metacat(self):
        try:
            # Set new client
            logit.log(f"Metacat Service  | login_with_x509() | session: {cherrypy.session['Shrek']['session']}")
            # Try logging in
            if not cherrypy.session["Shrek"].get("mc_client", None):
                self.set_metacat_client()
            auth_info = cherrypy.session["Shrek"]["mc_client"].login_x509('poms', poms_config.get("POMS", "POMS_CERT"), poms_config.get("POMS", "POMS_KEY"))
            cherrypy.session["Shrek"]["mc_status"] = auth_info
            if cherrypy.session["Shrek"]["mc_status"]:
                logit.log("Metacat Service  | login_with_x509() | Logged in as POMS")
                return True
            raise AuthenticationError("Invalid credentials")
        except Exception as e:
            logit.log("Metacat Service | login_with_x509() | %s" % repr(e).replace('\n', ''))
            False
    
    def begin_services(self, service="all"):
        if not cherrypy.session["Shrek"]["services_logged_in"]:
            cherrypy.session["Shrek"]["services_logged_in"] = {
                    "data_dispatcher":False,
                    "metacat":False, 
                }
        try:
            logit.log("DMR Service | Begin Services %s" % service)
            if not self.dd_token_file or not self.metacat_token_file:
                self.set_configuration()
            if service == "all" or service == "data_dispatcher":
                dd_info = None
                if os.stat(self.dd_token_file).st_size == 0:
                    dd_info = cherrypy.session["Shrek"]["dd_client"].login_x509("poms", poms_config.get("POMS", "POMS_CERT"), poms_config.get("POMS", "POMS_KEY"))
                elif cherrypy.session["Shrek"]["dd_client"]:
                    dd_info = cherrypy.session["Shrek"]["dd_client"].auth_info()
                elif os.path.exists(self.dd_token_file):
                    token_library_content = open(self.dd_token_file, 'r').read()
                    if token_library_content and len(token_library_content.split(" ")) > 1:
                        dd_token = token_library_content.split(" ")[1].strip()
                        dd_info = cherrypy.session["Shrek"]["dd_client"].login_token("poms", dd_token)
                    else:
                        dd_info = cherrypy.session["Shrek"]["dd_client"].login_x509("poms", poms_config.get("POMS", "POMS_CERT"), poms_config.get("POMS", "POMS_KEY"))
                if dd_info:
                    cherrypy.session["Shrek"]["services_logged_in"]["data_dispatcher"] = True
            if service == "all" or service == "metacat":
                mc_info = None
                if os.stat(self.metacat_token_file).st_size == 0:
                    cherrypy.session["Shrek"]["mc_status"] = cherrypy.session["Shrek"]["mc_client"].login_x509("poms", poms_config.get("POMS", "POMS_CERT"), poms_config.get("POMS", "POMS_KEY"))
                elif cherrypy.session["Shrek"]["mc_client"]:
                    mc_info = cherrypy.session["Shrek"]["mc_client"].auth_info()
                elif os.path.exists(self.metacat_token_file):
                    metacat_token_library_content = open(self.metacat_token_file, 'r').read()
                    if metacat_token_library_content and len(metacat_token_library_content.split(" ")) > 1:
                        mc_token = metacat_token_library_content.split(" ")[1].strip()
                        mc_info = cherrypy.session["Shrek"]["mc_client"].login_token("poms", mc_token)
                    else:
                        mc_info = cherrypy.session["Shrek"]["mc_client"].login_x509("poms", poms_config.get("POMS", "POMS_CERT"), poms_config.get("POMS", "POMS_KEY"))
                if mc_info:
                    cherrypy.session["Shrek"]["services_logged_in"]["metacat"] = True
                    
            
            return cherrypy.session["Shrek"]["services_logged_in"]
            
                    
        except Exception as e:
            logit.log("DMR Service | System Login | %s" % repr(e).replace("\n", ""))
            return cherrypy.session["Shrek"]["services_logged_in"]
    
    def session_status(self, auth_type='Auth_Token'):
        try:
            logit.log(f"DMR-Service  | get_session | Begin | session: {cherrypy.session['Shrek']['session']}")
            auth_info = None
            if cherrypy.session["Shrek"]["dd_client"]:
                auth_info = cherrypy.session["Shrek"]["dd_client"].auth_info()
            elif not cherrypy.session["Shrek"]["dd_client"] and os.path.exists(self.dd_token_file):
                if os.path.exists(self.dd_token_file):
                    token_library_content = open(self.dd_token_file, 'r').read()
                    token = None
                    if len(token_library_content.split(" ")) > 1:
                        token = token_library_content.split(" ")[1].strip()
                    self.set_data_dispatcher_client()
                    auth_info = cherrypy.session["Shrek"]["dd_client"].login_token("poms", token)
                    logit.log("DMR-Service  | session_status(%s) | Logged in as %s" % (auth_type, token))
            if auth_info:
                    logit.log("DMR-Service  | session_status(%s) | Logged in as %s" % (auth_type, auth_info[0]))
                    return True, {
                        "login_method" : auth_type,
                        "login_status": 'Logged in', 
                        "experiment": cherrypy.session["Shrek"]["current_experiment"], 
                        "dd_username":auth_info[0], 
                        "timestamp":auth_info[1]
                    }
        except Exception as e:
            logit.log(f"DMR-Service  | session_status |  %s | session: {cherrypy.session['Shrek']['session']} | %s" % (auth_type, repr(e).replace("\n", "")))
            message = repr(e).replace("\n", "")
            logit.log("DMR-Service  | session_status(%s) | Failed to get session:  %s" % (auth_type, message))
            if "No token found" in message:
                return False, {"login_status": "Not logged in - Authentication Failed"}
            else:
                return False, {
                        "login_method": "Attempted login method: %s" % auth_type,
                        "login_status": "%s" % message
                    }
        return False, {"login_status": "Not logged in"}
    
    
    def list_rses(self):
        logit.log(f"DMR-Service  | list_rses | Begin | session: {cherrypy.session['Shrek']['session']}")
        retval = cherrypy.session["Shrek"]["dd_client"].list_rses()
        logit.log("DMR-Service  | list_rses | Success")
        return retval
    
    def list_all_projects(self, **kwargs):
        logit.log(f"DMR-Service  | list_all_projects | Begin | session: {cherrypy.session['Shrek']['session']}")
        retval = {}
        
        retval['projects_active'] = cherrypy.session["Shrek"]["dd_client"].list_projects(with_files=False)
        retval['projects_active_count'] = len(retval.get("projects_active", 0))
        for p_state in ["done", "failed","cancelled", "abandoned"]:
            projects = cherrypy.session["Shrek"]["dd_client"].list_projects(state=p_state,  not_state='active')
            retval["projects_%s" % p_state] = projects
            retval["projects_%s_count" % p_state] = len(projects)
    
        poms_attributes = self.find_poms_data_dispatcher_projects(format="html", **kwargs)  
        if poms_attributes:
            retval['poms_attributes'] = poms_attributes
                
        logit.log("DMR-Service  | list_projects | Success")
        return retval
    
    def get_file_output_queries_for(self, dd_projects:List[Any] = None) -> Dict[int, str]:
        
        logit.log(f"DMR-Service  | get_file_output_queries_for |  Begin | session: {cherrypy.session['Shrek']['session']}")
        
        depths = {}
        projects_with_dataset = set()
        projects_without_dataset = set()
       
        for project in dd_projects:
            depths[project.campaign_stage_id] = project.output_ancestor_depth
            if project.named_dataset:
                if "project_id:" in project.named_dataset:
                    projects_without_dataset.add(project)
                else:
                    projects_with_dataset.add(project)
                    
        # Create an mql query index for the dataset output files using the provided mql query
        queries = { 
                    project.project_id if project.project_id else project.data_dispatcher_project_idx : "%s%s%s" % (
                    "children(" * depths.get(project.output_ancestor_depth, 1),
                    project.named_dataset if self.is_query(project.named_dataset) else "files from %s" % project.named_dataset,
                    ")" * depths.get(project.output_ancestor_depth, 1))
                    for project in projects_with_dataset
                }
        
        all_projects = cherrypy.session["Shrek"]["dd_client"].list_projects(state="any", not_state='ignore', with_files=True)
        project_items = {project.project_id:project for project in projects_without_dataset if project.project_id}
        
        # Update mql query index with the dataset output files using the fids of the data dispatcher project files.
        for item in all_projects:
            if item.get("project_id", None) in project_items:
                child_open = "children(" * int(depths.get(project_items[item["project_id"].campaign_stage_id],1))
                fids = f"fids {','.join([file.get('fid') for file in list(cherrypy.session['Shrek']['mc_client'].get_files(item['file_handles']))])}"
                child_close = ")" * int(depths.get(project_items[item["project_id"].campaign_stage_id],1))
                queries[item["project_id"]] = f"{child_open}{fids}{child_close}"
            
        return queries
    
    def list_filtered_projects(self, **kwargs):
        logit.log(f"DMR-Service  | list_filtered_projects | Begin | session: {cherrypy.session['Shrek']['session']}")
        retval = {}
        
        poms_attributes = self.find_poms_data_dispatcher_projects(format="html", **kwargs)  
        if poms_attributes is not None:
            retval['poms_attributes'] = poms_attributes
            logit.log("DMR-Service  | list_filtered_projects | Got poms project-attributes: %s" % poms_attributes)
            retval['projects_active'] = [project for project in cherrypy.session["Shrek"]["dd_client"].list_projects() if project.get("project_id", None) and project["project_id"] in poms_attributes]
            retval['projects_active_count'] = len(retval["projects_active"])
            for p_state in ["done", "failed","cancelled", "abandoned"]:
                retval["projects_%s" % p_state] = [project for project in cherrypy.session["Shrek"]["dd_client"].list_projects(state=p_state,  not_state='active') if project.get("project_id", None) and project["project_id"] in poms_attributes] or []
                retval['projects_%s_count' % p_state] = len(retval["projects_%s" % p_state])
        else:
            for p_state in ["active", "done", "failed","cancelled", "abandoned"]:
                retval["projects_%s" % p_state] = []
                retval['projects_%s_count' % p_state] = len(retval["projects_%s" % p_state])
            
                
        logit.log("DMR-Service  | list_filtered_projects | Success")
        return retval
    
    def get_project_handles(self, project_id, state=None, not_state=None):
        logit.log(f"DMR-Service  | get_project_handles | project_id: %s | Begin | session: {cherrypy.session['Shrek']['session']}" % project_id)
        retval = None
        msg = "Fail"
        try:
            project_info = cherrypy.session["Shrek"]["dd_client"].get_project(project_id, True, with_replicas=True)
            retval = project_info.get("file_handles", []) if project_info else None
            state = [state] if state and not isinstance(state, Array) else None
            not_state = [not_state] if not_state and not isinstance(not_state, Array) else None
            print(retval)
            print("State = %s" % state)
            print("NOT State = %s" % not_state)
            if state:
                retval = [ handle for handle in retval if handle.get("state", None) in state ]
            if not_state:
                retval = [ handle for handle in retval if handle.get("state", None) not in not_state ]
            if retval:
                msg = "OK"
                logit.log("DMR-Service  | get_project_handles | project_id: %s | Success" % project_id)
        except Exception as e:
            retval = {"exception": repr(e)}
            logit.log("DMR-Service  | get_project_handles | project_id: %s | Exception: %s" %  (project_id, e))
            raise e
        return {"project_handles": retval, "msg": msg, "stats": self.get_project_stats(handles=retval), "project_details": {k: project_info[k] for k in set(list(project_info.keys())) - set(["file_handles"])}}
    
    deft get_file_info_from_project_id(self, project_id=None, project_idx=None, metadata=False, hierarchy=False):
        files = []
        if project_id:
            project_info = cherrypy.session["Shrek"]["dd_client"].get_project(project_id, True, True)
            file_handles = project_info.get("file_handles", []) if project_info else None
            files = list(cherrypy.session["Shrek"]["mc_client"].get_files(file_handles, with_metadata=metadata, with_provenance=hierarchy))
        elif project_idx:
            project = self.db.query(DataDispatcherSubmission).filter(DataDispatcherSubmission.archive == False,DataDispatcherSubmission.data_dispatcher_project_idx == project_idx).first()
            if project and project.project_id:
                project_info = cherrypy.session["Shrek"]["dd_client"].get_project(project_id, True, True)
                file_handles = project_info.get("file_handles", []) if project_info else None
                files = list(cherrypy.session["Shrek"]["mc_client"].get_files(file_handles, with_metadata=metadata, with_provenance=hierarchy))
            elif project and project.named_dataset:
                files = list(cherrypy.session["Shrek"]["mc_client"].query(project.named_dataset, with_metadata=metadata, with_provenance=hierarchy))
        return files
    
    def list_file_urls(self, project_id=None, project_idx=None, mc_query=None):
        if not self.metacat_server_url:
            experiment = cherrypy.session.get("Shrek", {}).get("current_experiment")
            assert experiment, "Cannot determine metacat server"
            self.metacat_server_url = self.config[experiment]["metacat"]["METACAT_SERVER_URL"]
            
        file_url = "%s/gui/show_file?show_form=no&fid=%%s" % (self.metacat_server_url)
        do_all=False
        fdict = None
        if mc_query and mc_query != "None":
            fids_query=unquote(mc_query)
            fdict = { "%s:%s" % (file.get("namespace"), file.get("name")) : file_url % file.get("fid") if file["fid"] != '0' else 0 for file in list(cherrypy.session["Shrek"]["mc_client"].query(fids_query, False, False))}
            if not fdict:
                do_all=True
        if not mc_query or mc_query == "None" or do_all:
            do_all = True
            if project_id:
                fdict = { "%s:%s" % (file.get("namespace"), file.get("name")) : file_url % file.get("fid") if file["fid"] != '0' else 0 for file in self.get_file_info_from_project_id(project_id=project_id, metadata=True, hierarchy=True)}
            elif project_idx:
                fdict = { "%s:%s" % (file.get("namespace"), file.get("name")) : file_url % file.get("fid") if file["fid"] != '0' else 0 for file in self.get_file_info_from_project_id(project_idx = project_idx, metadata=True, hierarchy=True)}
        return fdict, do_all
            
    
    def get_project_stats(self, handles=None, project_id=None):
        retval = {"initial": 0,"done" : 0,"reserved" : 0,"failed" : 0}
        if not handles and project_id:
            handles = cherrypy.session["Shrek"]["dd_client"].list_handles(project_id)
        for handle in handles:
            retval[handle.get("state")] += 1
        return retval
    
    def is_query(self, named_dataset):
        # Match "{namespace}:{name}" pattern.
        pattern = r'\w+:\w+'
        match = re.search(pattern, named_dataset)
        
        if match:
            # Check if the string contains anything more than the matched pattern.
            if len(match.group()) == len(named_dataset):
                return False
            else:
                return True
        else:
            return True
    
    def get_output_file_details_for_submissions(self, dd_projects):
        output = {}
        already_did ={}
        
        start_time = time.time()
        def get_elapsed_time(started = start_time):
            elapsed_time = time.time() - started
            minutes, seconds = divmod(elapsed_time, 60)
            milliseconds = (elapsed_time - int(elapsed_time)) * 1000
            return f"Elapsed Time: {int(minutes):02d}:{int(seconds):02d}:{int(milliseconds):03d}"
            
        logit.log(f"DMR-Service  | get_output_file_details_for_submissions | Begin | session: {cherrypy.session['Shrek']['session']}")
        if not cherrypy.session["Shrek"]["mc_status"] or not self.begin_services("metacat").get("metacat", False):
            try:
                if not self.login_metacat():
                    raise AuthenticationError()
            except:
                retvals = {"total":[], "initial":[], "done":[], "failed":[], "reserved":[],"unknown":[], "submitted":[], "parents":[], "children":[], "statistics":[], "project_id":[]}
                return list(retvals.values())
        
        
        child_queries = self.get_file_output_queries_for(dd_projects)
        logit.log(f'DMR-Service  | get_output_file_details_for_submissions | Got Project Output Queries | Count: {len(set(child_queries.values()))} | {get_elapsed_time()}')
        query_index = {}
        unique_queries = {}
        total_processed = 0
        
        logit.log(f'DMR-Service  | get_output_file_details_for_submissions | Begin Running Queries | {get_elapsed_time()}')
        for id, child_query in child_queries.items():
            if child_query in query_index:
                query_index[child_query].append(id)
            else:
                query_index[child_query] = [id]
                unique_queries[child_query] = list(cherrypy.session["Shrek"]["mc_client"].query(child_query, None))
                total_processed += 1
        logit.log(f'DMR-Service  | get_output_file_details_for_submissions | Finished Running Queries | {get_elapsed_time()} ')

        for dd_project in dd_projects:
            if dd_project.data_dispatcher_project_idx in child_queries:
                id = dd_project.data_dispatcher_project_idx
                output_bit = f"project_idx={id}"
            else:
                id = dd_project.project_id
                if id in already_did:
                    output[dd_project.submission_id] = dict(already_did[id])
                    continue
                output_bit = f"project_id={id}"
                
            query = child_queries.get(id, None)
            results = list(unique_queries.get(query, []))
            if query in unique_queries:
                del unique_queries[query]
                
            n = len(results)
            mc_query = self.get_metacat_query_url(query=query)
            available_output_query = "%s&querying=output&mc_query=%s" % (output_bit, mc_query if mc_query else "" )
            output[dd_project.submission_id] = {"query": available_output_query, "length":n}
            
            if query in query_index:
                completed_project_queries = query_index[query]
                for project_id in completed_project_queries:
                    if project_id not in already_did:
                        already_did[project_id] = {"query": available_output_query, "length":n}
            
            already_did[id] = {"query": available_output_query, "length":n}
            
        logit.log(f'DMR-Service  | get_output_file_details_for_submissions | Done | {get_elapsed_time()}')
            
        return output
    
    # Recursively acquire parent or children files at the layer
    def get_family_generation(self, files, hierarchy, desired_generation, output, current_generation = 0):
        desired_files = []
        for file in files:
            hierarchy_files = file.get(hierarchy, [])
            for hierarchy_file in hierarchy_files:
                desired_files.append(hierarchy_file)
        if desired_files and desired_generation > current_generation:
            fids = list(set([file.get("fid") for file in desired_files if file.get("fid", None) and file["fid"] not in ['0', 0, None, "None"]]))
            desired_files = list(cherrypy.session["Shrek"]["mc_client"].query("fids %s" % ', '.join(fids) if fids else "", with_provenance=True))
            return self.get_family_generation(desired_files, hierarchy, desired_generation, output, current_generation + 1)
        else:
            if output == "fids": 
                return list(set([file.get("fid") for file in desired_files if file.get("fid", None) and file["fid"] not in ['0', 0, None, "None"]]))
            else: 
                return desired_files
    
    def get_hierarchy_info_from_files(self, files, relationship="all", depth=1, output="fids"):                    
        if relationship == "all": 
            return self.get_family_generation(files, "parents", depth, output), self.get_family_generation(files, "children", depth, output)
        elif relationship == "parents": 
            return self.get_family_generation(files, "parents", depth, output)
        elif relationship == "children": 
            return self.get_family_generation(files, "children", depth, output)
        
    def get_metacat_query(self, fids):
        return "fids %s"  % ", ".join(fids)
    
    def get_metacat_query_url(self, fids=None, query=None, named_dataset = None):
        try:
            if query:
                return query.replace(" ", "+").replace(",", "%2C")
            elif fids:
                return "fids+%s"  % ("%2C+".join([str(fid) for fid in fids]))
            elif named_dataset:
                return urlencode(named_dataset)
        except:
            pass
        return None
            
    
    def add_query_filters_if_necessary(self, query, filters=[]):
        if filters:
            for i in range(0, len(filters)):
                query += " %s %s" % ("where" if i == 0 else "and", filters(i))
        return query
        
    
    def get_file_stats_for_submissions(self, dd_projects):
        
        if not cherrypy.session["Shrek"].get("mc_client", None):
            self.login_metacat()
        if not dd_projects or not cherrypy.session["Shrek"]["services_logged_in"].get("metacat", False):
            logit.log(f'DMR-Service  | get_file_stats_for_submissions({cherrypy.session["Shrek"]["current_experiment"]}) | Not logged in to metacat | Status: {cherrypy.session["Shrek"].get("mc_status")} | Projects: {repr(dd_projects)}')
            return {}
        
        project_dict = {item.project_id if item.project_id else "idx: %s" % item.data_dispatcher_project_idx: item for item in dd_projects}
        project_queries = {item.project_id if item.project_id else "idx: %s" % item.data_dispatcher_project_idx: {} for item in dd_projects}
        project_index_queries = {"idx: %s" % item.data_dispatcher_project_idx: item.named_dataset for item in dd_projects if not item.project_id}
        for project_id in project_queries.keys():
            file_stats = {"total":0, "initial":0, "done":0, "failed":0, "reserved":0, "unknown":0, "submitted":0, "parents":0, "children":0}
            project_fids_dict = {"total":[], "initial":[], "done":[], "failed":[], "reserved":[],"unknown":[], "submitted":[], "parents":[], "children":[]}

            #if project_index_queries.get(project_id, None):
            #    file_handles = []
            #    is_project_submission = False
            #    all_files = list(cherrypy.session["Shrek"]["mc_client"].query(project_index_queries[project_id], with_metadata=True, with_provenance=True))
            #else:
            #    project = cherrypy.session["Shrek"]["dd_client"].get_project(project_id, True, True)
            #    is_project_submission = True
            #    file_handles = project.get("file_handles", []) if project else None
            #    if not file_handles and project_dict[project_id].named_dataset:
            #        all_files = list(cherrypy.session["Shrek"]["mc_client"].query(project_dict[project_id].named_dataset, with_metadata=True, with_provenance=True))
            #    else:
            #        all_files = list(cherrypy.session["Shrek"]["mc_client"].get_files(file_handles, with_metadata=True, with_provenance=True))
            dd_project = project_dict[project_id]
            if project_index_queries.get(project_id,None) and "project_id:" not in project_index_queries[project_id]:
                file_handles=[]
                is_project_submission = False
                mc_query = dd_project.named_dataset if self.is_query(dd_project.named_dataset) else "files from %s" % dd_project.named_dataset,
            else:
                is_project_submission = True
                project_info = cherrypy.session["Shrek"]["dd_client"].get_project(project_id, True, True)
                file_handles = project_info.get("file_handles", []) if project_info else None
                all_files = list(cherrypy.session["Shrek"]["mc_client"].get_files(file_handles)) if file_handles else []
                mc_query = "fids %s" % ','.join([file.get("fid") for file in all_files])
            
            if any(char.isalnum() for char in mc_query):
                all_files = list(cherrypy.session["Shrek"]["mc_client"].query(mc_query))
            else:
                all_files = []
            
            # Create fid library
            metacat_did_to_fid = {}
            for file in all_files:
                metacat_did_to_fid["%s:%s" % (file.get("namespace"), file.get("name"))] = file.get("fid")
            
            # Create fid lists and increment corresponding counts
            for handle in file_handles:
                file_did = "%s:%s" % (handle.get("namespace"),handle.get("name"))
                file_fid = metacat_did_to_fid.get(file_did, None)
                file_state = handle.get("state")
                # Enter basics
                project_fids_dict.get("total").append(file_fid)
                project_fids_dict.get(file_state).append(file_fid)
                
                # Enter conditionals
                if file_state == "done" or file_state == "failed" or file_state == "reserved":
                    project_fids_dict.get("submitted").append(file_fid)
                if not handle.get("replicas"):
                    project_fids_dict.get("unknown").append(file_fid)
                    
            if not is_project_submission:
                for file in all_files:
                    project_fids_dict.get("total").append(file.get("fid"))
                    project_fids_dict.get("unknown").append(file.get("fid"))
            
            if len(all_files) > 0:
                child_query= "%s%s%s" % (
                    "children(" *  dd_project.output_ancestor_depth,
                    mc_query,
                    ")" *  dd_project.output_ancestor_depth
                )
                parents_query= "parents(%s)" % mc_query
                project_fids_dict["children"] = list(cherrypy.session["Shrek"]["mc_client"].query(child_query))
                project_fids_dict["parents"] = list(cherrypy.session["Shrek"]["mc_client"].query(parents_query))
            else:
                project_fids_dict["children"] = []
                project_fids_dict["parents"] = []
                parents_query = child_query = ""
            
            
            # Now that we have all the fids:
            # Add the counts, and generate query strings for fids
            for key, val in project_fids_dict.items():
                file_stats[key] = len(val)
                if key == "total":
                    project_queries[project_id][key] = self.get_metacat_query_url(query=mc_query)
                elif key == "children":
                    project_queries[project_id][key] = self.get_metacat_query_url(query=child_query)
                elif key == "parents":
                    project_queries[project_id][key] = self.get_metacat_query_url(query=parents_query)
                else:
                    project_queries[project_id][key] = self.get_metacat_query_url(fids=val)
            
            # Calculate completion pct for the user
            file_stats["pct_complete"] = "%d%%" % int(((file_stats.get("done") + file_stats.get("failed")) / len(file_handles)) * 100) if file_handles and len(file_handles) != 0 else 0
            project_queries[project_id]["statistics"] = file_stats
            if dd_project.project_id:
                project_queries[project_id]["project_id"] = dd_project.project_id
            else:
                project_queries[project_id]["project_idx"] = dd_project.data_dispatcher_project_idx
        retval = {}
        for project in dd_projects:
            index = project.project_id if project.project_id else "idx: %s" % project.data_dispatcher_project_idx
            retval[project.submission_id] = {}
            for key in ["total", "initial", "done", "failed", "reserved","unknown", "submitted", "parents", "children", "statistics", "project_id","project_idx"]:
                if key in project_queries[index]:
                    retval[project.submission_id][key] = project_queries[index][key]
        return retval

    def _authorize_agent(self, ctx, agent_header):
            queue = LocalJsonQueue(ctx.web_config.get("POMS", "submission_records_path"))
            return queue.authorize_agent(ctx, agent_header)
        
    def calculate_dd_project_completion(self, ctx, agent_header, dd_submission_id = None, dd_submission_ids=None):
        print("Submission Agent | Authorizing Agent | %s" % agent_header)
        if not self._authorize_agent(ctx, agent_header):
            return {"error": "Unauthorized"}
        print("Submission Agent | Authorized")
        retval = {}
        self.db = self.db if self.db else cherrypy.request.db
        if not self.db:
            raise RuntimeError("Could not connect to database")
        if dd_submission_ids:
            try:
                task_ids = list(eval(dd_submission_ids))
            except:
                try:
                    if type(dd_submission_ids) == int or type(dd_submission_ids) == str:
                        task_ids = [int(dd_submission_ids)]
                except:
                    pass
        else:
            task_ids = [dd_submission_id]
            
        tasks: List[DataDispatcherSubmission] = self.db.query(DataDispatcherSubmission).filter(DataDispatcherSubmission.data_dispatcher_project_idx.in_(task_ids)).all()
        agent_sessions = {}
        print("Submission Agent | Checking Tasks | tasks: %s " % (len(tasks)))
        for task in tasks:
            if task.experiment not in agent_sessions:
                agent_sessions[task.experiment] = {}
            if task.vo_role not in agent_sessions[task.experiment]:
                agent_sessions[task.experiment][task.vo_role] = []
            agent_sessions[task.experiment][task.vo_role].append(task)
        
        def incr_comp(state):
            return 1 if state in ["done", "failed"] else 0
        
        for exp, roles in agent_sessions.items():
            for role, agent_tasks in roles.items():
                agent_session = { "role": role, "experiment": exp }
                print("Submission Agent | Updating session for Submission Agent | %s" % agent_session)
                self.initialize_session(ctx, agent_session)
                print("Submission Agent | Session Updated | exp: %s role: %s tasks: %s" % (exp, role, len(agent_tasks)))
                for task in agent_tasks:
                    if task.data_dispatcher_project_idx in retval:
                        continue
                    ncomp = 0
                    ntotal = 0
                    if task.project_id:
                        project_handles = cherrypy.session["Shrek"]["dd_client"].list_handles(task.project_id)
                        for handle in project_handles:
                            ntotal += 1
                            ncomp += incr_comp(handle.get("state"))

                        retval[task.data_dispatcher_project_idx] = ncomp * 100.0/ntotal if ntotal > 0 else None
        print("Submission Agent | Updated Tasks")
        return retval
        
    def copy_project(self, project_id, **kwargs):
        kwargs["use_hostname"] = True
        new_project = cherrypy.session["Shrek"]["dd_client"].copy_project(project_id)
        logit.log("DMR-Service  | experiment: %s | copy_project(%s) | done" % (cherrypy.session["Shrek"]["current_experiment"], project_id))
        return new_project
    
    def get_project_for_submission(self, project_id, **kwargs):
        logit.log("DMR-Service  | experiment: %s | get_project_for_submission(%s) | submission_id: %s " % (cherrypy.session["Shrek"]["current_experiment"], project_id, kwargs.get("submission_id")))
        existing_project = cherrypy.session["Shrek"]["dd_client"].get_project(project_id)
        if existing_project:
            logit.log("DMR-Service  | experiment: %s | get_project_for_submission(%s) | located project" % (cherrypy.session["Shrek"]["current_experiment"], project_id))
            kwargs["worker_timeout"] = existing_project.get('worker_timeout', None)
            kwargs["idle_timeout"] = existing_project.get('idle_timeout', None)
            project = self.store_project(project_id = project_id,  **kwargs)
            logit.log("DMR-Service  | experiment: %s |  get_project_for_submission(%s) | stored data-dispatcher project information in database" % (cherrypy.session["Shrek"]["current_experiment"], project.project_id))
            logit.log("DMR-Service  | experiment: %s | get_project_for_submission(%s) | done" % (cherrypy.session["Shrek"]["current_experiment"], project_id))
            return project
        return None
    
    def _is_OneP(self, cs):
        defaults = (cs.campaign_obj.defaults.get("defaults", {}).get("data_handling_service", {}).get("data_dispatcher", {}) 
                            if cs.campaign_obj.defaults else {})
                
        return (cs.data_dispatcher_stage_methodology == "1P" 
                    or (cs.data_dispatcher_settings 
                        and cs.data_dispatcher_settings.get("stage_methodology", "standard") == "1P")
                    or (defaults.get("data_dispatcher_stage_methodology", "standard") == "1P"))
    
    def get_campaign_dd_field(self, cs:CampaignStage, field:str, default=None):
        try:
            assert cs, "No campaign stage information provided"
            defaults = (cs.campaign_obj.defaults.get("defaults", {}).get("data_handling_service", {}).get("data_dispatcher", {}) 
                            if cs.campaign_obj.defaults else {})
            
            if hasattr(cs, field) and getattr(cs, field) != None:
                return getattr(cs, field)
            elif cs.data_dispatcher_settings and cs.data_dispatcher_settings.get(field, None):
                return cs.data_dispatcher_settings[field]
            elif defaults and defaults.get(field, None):
                return defaults[field]
            else:
                return default

        except Exception as e:
            logit.log("DMR-Service  | experiment: %s |  get_campaign_dd_field(%s) | %s" % (cherrypy.session["Shrek"]["current_experiment"], field, e))
    
    def create_project(self, username, dataset=None, files=[], **kwargs):
        logit.log("DMR-Service  | experiment: %s | create_project() | Begin " % (cherrypy.session["Shrek"]["current_experiment"]))
        users = []
        project_attributes = {}
        idle_timeout = kwargs.get("idle_timeout", None)
        worker_timeout = kwargs.get("worker_timeout", None)
        for item in [idle_timeout, worker_timeout]:
            if item and item in (None, "null", "None", 0):
                item = None
        common_attributes = kwargs.get("common_attributes", {})
        if "creator_name" in kwargs:
            users.append(kwargs.get("creator_name"))
        if username not in users:
            users.append(username)
        if "virtual" in kwargs and kwargs["virtual"] in (True, "true", "True"):
            project_attributes["virtual"] = True
        if "load_limit" in kwargs and kwargs["load_limit"]:
            project_attributes["load_limit"] = kwargs["load_limit"]
        
        
        check_stage = not (project_attributes or idle_timeout or worker_timeout)
        cs = None
            
        if files and not dataset:
            logit.log("DMR-Service  | experiment: %s | create_project() | Creating project from files | count %d" % (cherrypy.session["Shrek"]["current_experiment"], len(files)))
        if dataset and not files:
            logit.log("DMR-Service  | experiment: %s | create_project() | Creating project from dataset: %s " % (cherrypy.session["Shrek"]["current_experiment"], dataset))
            files = list(cherrypy.session["Shrek"]["mc_client"].query(dataset, with_metadata=True))
            logit.log("DMR-Service  | experiment: %s | create_project() | located files from dataset: %s" % (cherrypy.session["Shrek"]["current_experiment"], files))
        if not self.db:
            if cherrypy.request.db:
                self.db = cherrypy.request.db
            else:
                logit.log("DMR-Service  | store_project | fail: no database access | session %s" % (cherrypy.session["Shrek"]["session"]))
                raise Exception("DMR-Service  | store_project | fail: no database access")
        if "campaign_stage_id" in kwargs and check_stage:
            cs = self.db.query(CampaignStage).join(Campaign).filter(CampaignStage.campaign_stage_id == int(kwargs["campaign_stage_id"])).first()
            if cs and (cs.data_dispatcher_settings 
                       or cs.data_dispatcher_project_virtual is not None 
                       or cs.data_dispatcher_idle_timeout 
                       or cs.data_dispatcher_worker_timeout
                       or cs.data_dispatcher_load_limit):
                project_attributes["virtual"] = cs.data_dispatcher_settings.get("virtual", cs.data_dispatcher_project_virtual) or False
                project_attributes["load_limit"] = cs.data_dispatcher_settings.get("load_limit", cs.data_dispatcher_load_limit) or None
                idle_timeout = cs.data_dispatcher_settings.get("idle_timeout", cs.data_dispatcher_idle_timeout) or 259200
                worker_timeout = cs.data_dispatcher_settings.get("worker_timeout", cs.data_dispatcher_worker_timeout) or 0
        new_project = cherrypy.session["Shrek"]["dd_client"].create_project(files, 
                                                                            users=users,
                                                                            query=dataset, 
                                                                            idle_timeout=idle_timeout,
                                                                            worker_timeout=worker_timeout,
                                                                            common_attributes=common_attributes,
                                                                            project_attributes=project_attributes)
        
        logit.log("DMR-Service  | experiment: %s | create_project() | created data-dispatcher project: %s" % (cherrypy.session["Shrek"]["current_experiment"], new_project))
        
        project_id = new_project.get('project_id', None)
        worker_timeout = new_project.get('worker_timeout', worker_timeout)
        idle_timeout = new_project.get('idle_timeout', idle_timeout)
        common_attributes = new_project.get('common_attributes', common_attributes)
        project_attributes = new_project.get('project_attributes', project_attributes)
        kwargs["load_limit"] = project_attributes.get("load_limit", None)
        kwargs["idle_timeout"] = idle_timeout
        kwargs["worker_timeout"] = worker_timeout
        project = self.store_project(project_id = project_id, **kwargs)
        logit.log("DMR-Service  | experiment: %s | create_project() | Done | Created Project ID: %s" % (cherrypy.session["Shrek"]["current_experiment"], project.project_id))
        
        # OneP methodology. Do not create any more projects since one already exists.
        # Check the stage/campaign defaults for this its stage methodology setting
        # Setting the data_dispatcher_project_id in the settings for this stage will make it "1P"
        # if "campaign_stage_id" in kwargs:
        #     if not cs:
        #         cs = self.db.query(CampaignStage).join(Campaign).filter(CampaignStage.campaign_stage_id == int(kwargs["campaign_stage_id"])).first()
        #     if cs and self._is_OneP(cs):
        #         cs.data_dispatcher_project_id = project_id
        #         if cs.data_dispatcher_settings:
        #             updated_settings = copy.deepcopy(cs.data_dispatcher_settings)
        #             updated_settings["project_id"] = project_id
        #         else:
        #             updated_settings = {
        #                 "idle_timeout": cs.data_dispatcher_idle_timeout,
        #                 "worker_timeout": cs.data_dispatcher_worker_timeout,
        #                 "project_id": project_id,
        #                 "dataset_query": cs.data_dispatcher_dataset_query,
        #                 "virtual": cs.data_dispatcher_project_virtual,
        #                 "stage_methodology": "1P",
        #                 "recovery_mode": cs.data_dispatcher_recovery_mode,
        #                 "load_limit": cs.data_dispatcher_load_limit
        #             }
        #         cs.data_dispatcher_settings = updated_settings
        #         self.db.add(cs)
        #         self.db.commit()
        return project
        
    
    def find_poms_data_dispatcher_projects(self, format=None, **kwargs):
        try:
            logit.log("DMR-Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | begin" % (cherrypy.session["Shrek"]["current_experiment"], format))
            if not self.db:
                if cherrypy.request.db:
                    self.db = cherrypy.request.db
                else:
                    logit.log("DMR-Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | fail: no database access" % (cherrypy.session["Shrek"]["current_experiment"], format))
                    return {}
            
            query = self.db.query(DataDispatcherSubmission).filter(DataDispatcherSubmission.archive == False,DataDispatcherSubmission.experiment == cherrypy.session["Shrek"]["current_experiment"])
            searchList = ["experiment=%s" % cherrypy.session["Shrek"]["current_experiment"]]
            if kwargs.get("project_id", None):
                # Project id is known, so only one result would exist
                searchList.append("project_id=%s" % kwargs["project_id"])
                query = query.filter(DataDispatcherSubmission.project_id == kwargs["project_id"])
                project = query.one_or_none()
                logit.log("DMR-Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | searched: %s | results: %s" % (cherrypy.session["Shrek"]["current_experiment"], format, ", ".join(searchList), 1 if project else 0))
                if project:
                    return self.format_projects(format, [project])
            if kwargs.get("submission_id", None):
                # One project per submission, if this exists we can return right away
                searchList.append("submission_id=%s" % kwargs["submission_id"])
                query = query.filter(DataDispatcherSubmission.submission_id == kwargs["submission_id"])
                project = query.one_or_none()
                logit.log("DMR-Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | searched: submission_id=%s | results: %s" % (cherrypy.session["Shrek"]["current_experiment"], format, ", ".join(searchList), 1 if project else 0))
                if project:
                    return self.format_projects(format, [project])
                
            # project id and submission id are unknown, so we are filtering for one or more projects
            if kwargs.get("campaign_id", None):
                searchList.append("campaign_id=%s" % kwargs["campaign_id"])
                query = query.filter(DataDispatcherSubmission.campaign_id == kwargs["campaign_id"])
            if kwargs.get("project_name", None):
                searchList.append("project_name=%s" % kwargs["project_name"])
                query = query.filter(DataDispatcherSubmission.project_name == kwargs["project_name"])
            if kwargs.get("role", None):
                searchList.append("vo_role=%s" % kwargs["role"])
                query = query.filter(DataDispatcherSubmission.vo_role == kwargs["role"])
            if kwargs.get("campaign_stage_id", None):
                searchList.append("campaign_stage_id=%s" % kwargs["campaign_stage_id"])
                query = query.filter(DataDispatcherSubmission.campaign_stage_id == kwargs["campaign_stage_id"])
            if kwargs.get("campaign_stage_snapshot_id", None):
                searchList.append("campaign_stage_snapshot_id=%s" % kwargs["campaign_stage_snapshot_id"])
                query = query.filter(DataDispatcherSubmission.campaign_stage_snapshot_id == kwargs["campaign_stage_snapshot_id"])
            if kwargs.get("split_type", None):
                searchList.append("split_type=%s" % kwargs["split_type"])
                query = query.filter(DataDispatcherSubmission.split_type == kwargs["split_type"])
            if kwargs.get("last_split", None):
                searchList.append("last_split=%s" % kwargs["last_split"])
                query = query.filter(DataDispatcherSubmission.last_split == kwargs["last_split"])
            if kwargs.get("job_type_snapshot_id", None):
                searchList.append("job_type_snapshot_id=%s" % kwargs["job_type_snapshot_id"])
                query = query.filter(DataDispatcherSubmission.job_type_snapshot_id == kwargs["job_type_snapshot_id"])
            if kwargs.get("depends_on_submission", None):
                searchList.append("depends_on_submission=%s" % kwargs["depends_on_submission"])
                query = query.filter(DataDispatcherSubmission.depends_on_submission == kwargs["depends_on_submission"])
            if kwargs.get("depends_on_project", None):
                searchList.append("depends_on_project=%s" % kwargs["depends_on_project"])
                query = query.filter(DataDispatcherSubmission.depends_on_project == kwargs["depends_on_project"])
            if kwargs.get("recovery_tasks_parent_submission", None):
                searchList.append("recovery_tasks_parent_submission=%s" % kwargs["recovery_tasks_parent_submission"])
                query = query.filter(DataDispatcherSubmission.recovery_tasks_parent_submission == kwargs["recovery_tasks_parent_submission"])
            if kwargs.get("recovery_tasks_parent_project", None):
                searchList.append("recovery_tasks_parent_project=%s" % kwargs["recovery_tasks_parent_project"])
                query = query.filter(DataDispatcherSubmission.recovery_tasks_parent_project == kwargs["recovery_tasks_parent_project"])
            if kwargs.get("recovery_position", None):
                searchList.append("recovery_position=%s" % kwargs["recovery_position"])
                query = query.filter(DataDispatcherSubmission.recovery_position == kwargs["recovery_position"])
            
            projects = query.order_by(DataDispatcherSubmission.created).all()
            logit.log("DMR-Service  | experiment: %s | find_poms_data_dispatcher_projects() | format: %s | searched: %s | results: %s" % (cherrypy.session["Shrek"]["current_experiment"], format, ", ".join(searchList), len(projects)))
            if len(projects) > 0:
                return self.format_projects(format, projects)
 
        except Exception as e:
            raise e
        
        return {}
    
    def format_projects(self, format=None, results = None):
        logit.log("DMR-Service  | experiment: %s | format_projects() | format: %s" % (cherrypy.session["Shrek"]["current_experiment"], format))
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
                            string += "<a target='_blank' href='/poms/campaign_overview/%s/%s?campaign_id=%s'>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (cherrypy.session["Shrek"]["current_experiment"], cherrypy.session["Shrek"]["role"], value, key.replace("_", " ").title(),state,key.replace("_", "-"), value)
                        elif key == "campaign_stage_id":
                            string += "<a target='_blank' href='/poms/campaign_stage_info/%s/%s?campaign_stage_id=%s'>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (cherrypy.session["Shrek"]["current_experiment"], cherrypy.session["Shrek"]["role"], value, key.replace("_", " ").title(),state,key.replace("_", "-"), value)
                        elif key == "submission_id" or key == "depends_on_submission" or key == "recovery_tasks_parent_submission":
                            string += "<a target='_blank' href='/poms/submission_details/%s/%s?submission_id=%s'>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (cherrypy.session["Shrek"]["current_experiment"], cherrypy.session["Shrek"]["role"], value, key.replace("_", " ").title(),state,key.replace("_", "-"), value)
                        elif key == "depends_on_project" or key == "recovery_tasks_parent_project":
                            string += "<a target='_blank' class='poms-dd-attribute-link' style='cursor:pointer;' onclick='getProjectHandles(this, `%s`, false)'>* %s: <span class='%s-projects-poms-%s'>%s</span></a><br/>" % (proj_id, value, key.replace("_", " ").title(),key.replace("_", "-"), value)
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
    
    def store_project(self, project_id, **kwargs):
        logit.log("DMR-Service  | store_project | project_id: %s | Begin | session %s" % (str(project_id), cherrypy.session["Shrek"]["session"]))
        if not self.db:
            if cherrypy.request.db:
                self.db = cherrypy.request.db
            else:
                logit.log("DMR-Service  | store_project | fail: no database access | session %s" % (cherrypy.session["Shrek"]["session"]))
                raise Exception("DMR-Service  | store_project | fail: no database access")
        project = self.db.query(DataDispatcherSubmission).filter(DataDispatcherSubmission.project_id == project_id).first()
        project = DataDispatcherSubmission()
        project.project_id = kwargs.get("project_id", project_id)
        project.worker_timeout = kwargs.get("worker_timeout", None)
        project.idle_timeout = kwargs.get("idle_timeout", None)
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
        if kwargs.get("named_dataset", None):
            project.named_dataset = kwargs.get("named_dataset", None)
        if kwargs.get("status", None):
            project.status = kwargs.get("status", None)
        project.virtual = kwargs.get("virtual", False) in (True, "True", "true")
        project.stage_methodology = kwargs.get("stage_methodology", "standard")
        project.recovery_mode = kwargs.get("recovery_mode", "standard")
        project.load_limit = kwargs.get("load_limit", None)
        # Try getting dependents
        if project.depends_on_submission:
            project.depends_on_project = self.db.query(DataDispatcherSubmission.project_id).filter(DataDispatcherSubmission.campaign_id == DataDispatcherSubmission.campaign_id and DataDispatcherSubmission.submission_id == project.depends_on_submission).one_or_none()
        if project.recovery_tasks_parent_submission:
            project.recovery_tasks_parent_project = self.db.query(DataDispatcherSubmission.project_id).filter(DataDispatcherSubmission.campaign_id == DataDispatcherSubmission.campaign_id and DataDispatcherSubmission.submission_id == project.recovery_tasks_parent_submission).one_or_none()
        
        
        self.db.add(project)
        self.db.commit()
        return project
        
    def restart_handles(self, project_id):
        logit.log("DMR-Service | restart_project: %s | Start" % (project_id))
        cherrypy.session["Shrek"]["dd_client"].file_done(project_id,"poms_samples:96483cee-4e35-4590-934d-47229afcf15b.root")
        logit.log("DMR-Service | restart_project: %s | Complete" % (project_id))
        handles = cherrypy.session["Shrek"]["dd_client"].list_handles(project_id, with_replicas=True)
        return {"project_handles": handles, "msg":"OK", "stats": self.get_project_stats(handles=handles)}
    
    def restart_project(self, project_id):
        logit.log("DMR-Service | restart_project: %s | Start" % (project_id))
        cherrypy.session["Shrek"]["dd_client"].restart_handles(project_id, all=True)
        logit.log("DMR-Service | restart_project: %s | Complete" % (project_id))
        handles = cherrypy.session["Shrek"]["dd_client"].list_handles(project_id, with_replicas=True)
        return {"project_handles": handles, "msg":"OK", "stats": self.get_project_stats(handles=handles)}
    
    def activate_project(self, project_id):
        logit.log("DMR-Service | restart_project: %s | Start" % (project_id))
        cherrypy.session["Shrek"]["dd_client"].activate_project(project_id)
        logit.log("DMR-Service | restart_project: %s | Complete" % (project_id))
        handles = cherrypy.session["Shrek"]["dd_client"].list_handles(project_id, with_replicas=True)
        return {"project_handles": handles, "msg":"OK", "stats": self.get_project_stats(handles=handles)}
    
    # For Testing Purposes only
    
    def generate_results_id(self):
        return uuid.uuid4()

    def generate_results_file(self, task_id):
        results_dir = poms_config.get("Shrek", "BACKGROUND_JOBS_RESULTS_DIR")
        results_file = "%s/job_%s.json" % (results_dir, task_id)
        return results_file
    
    def start_pass_fail_files_background_task(self, project_id, n_pass, n_fail):
        started = time.perf_counter()
        task_id = self.generate_results_id()
        threading.Thread(target=self.pass_fail_files, daemon=True, args=[project_id, n_pass,n_fail, task_id, started], name=str(task_id)).start()
        return {"task_id": str(task_id), "start": started}
    
    def pass_fail_files(self, project_id, n_pass, n_fail, task_id, started):
        logit.log("DMR-Service | Process Files | Pass: %s | Fail: %s | Begin" % (n_pass, n_fail))
        self.test = DataDispatcherTest(cherrypy.session["Shrek"]["dd_client"], project_id)
        passed, failed = self.test.process_files(n_pass, n_fail)
        files_left = len(cherrypy.session["Shrek"]["dd_client"].list_handles(project_id, state="initial"))
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
    
    def update_status(self, ctx, dd_project_idx, status):
        dd_status_map = {
            "New": "Submitted, Pending Start",
            "Idle": "Submitted, Pending Start",
            "Running": "Running",
            "Held": "Held",
            "Failed": "Completed",
            "Completed": "Completed",
            "Located": "Completed",
            "Removed": "Removed",
            "LaunchFailed": "Submission Failed to Launch",
            "Approved": "Approved",
            "Awaiting Approval": "Submission Awaiting Approval",
            "Cancelled": "Submission Cancelled"
        }
        dd_project = ctx.db.query(DataDispatcherSubmission).filter(DataDispatcherSubmission.archive == False, DataDispatcherSubmission.data_dispatcher_project_idx == dd_project_idx).first()
        if dd_project:
            dd_project.status = dd_status_map.get(status, None)
            dd_project.updater = ctx.get_experimenter_id()
            dd_project.updated = datetime.datetime.now()
        ctx.db.commit()
        
        
    def create_recovery_dataset(self, submission: Submission, rtype, rlist=None, test=False):
        try:
            nfiles = 0 
            assert submission.data_dispatcher_submission_obj, "No data-dispatcher related information was found for this submission"
            project: DataDispatcherSubmission = submission.data_dispatcher_submission_obj
            campaign_stage: CampaignStage = project.campaign_stage_obj
            snapshot = submission.campaign_stage_snapshot_obj if submission.campaign_stage_snapshot_obj else None
            recovery_position = int(submission.recovery_position + 1) if submission.recovery_position else 1
            recovery_parent = int(submission.submission_id)
            methodology = self.get_campaign_dd_field(campaign_stage, "stage_methodology", "standard")
            mode = self.get_campaign_dd_field(campaign_stage, "recovery_mode", "standard")
            project_name = "Recovery | stage: %s | method: %s | mode: %s | parent_sid: %s | pos: %s" % (campaign_stage.name, methodology, mode, recovery_parent, recovery_position)
            recovery_files = []
            handles = []
            if rtype.name in ["state_not_done", "state_failed", "reprocess_all"]:
                if rtype.name == "reprocess_all":
                    handles = self.get_project_handles(project.project_id).get("project_handles", [])
                else:
                    not_states = ["done"] if rtype.name == "state_not_done" else None
                    states = ["failed"] if rtype.name == "state_failed" else None
                    handles = self.get_project_handles(project.project_id, state=states, not_state=not_states).get("project_handles", [])
                print(handles)
                recovery_files = list(cherrypy.session["Shrek"]["mc_client"].get_files(handles, with_metadata=True, with_provenance=True)) if methodology == "standard" else []
            elif rtype.name in ["reprocess_orphans", "added_files"]:
                cdate = submission.created.timestamp()
                handle_dict = {handle["name"]: handle for handle in self.get_project_handles(project.project_id, state=["done", "failed"]).get("project_handles", [])}
                handles = []
                print(handle_dict)
                for file in list(cherrypy.session["Shrek"]["mc_client"].get_files(handle_dict.values(), with_provenance=True)):
                    if rtype.name == "reprocess_orphans" and not len(file.get("children", [])) > 0:
                        handles.append(handle_dict[file["name"]])
                        if methodology == "standard":
                            recovery_files.append(file)
                    elif rtype.name == "added_files" and file.get("created_timestamp", datetime.datetime.min.timestamp()) > cdate:
                        handles.append(handle_dict[file["name"]])
                        if methodology == "standard":
                            recovery_files.append(file)
            
            
            nfiles = len(recovery_files) if methodology == "standard" else len(handles)
            print(project_name)
            logit.log("DMR-Service  | create_recovery_dataset | File Count: %s" % (nfiles))
            
            if nfiles == 0:
                logit.log("DMR-Service  | create_recovery_dataset | no matching files exist | recovery not needed")
                return 0, None, None
            self.db = self.db if self.db else cherrypy.request.db
            if methodology == "1P":
                # Pass a DID list of files to reset
                handles_to_reset=[f"{handle.get('namespace')}:{handle.get('name')}" for handle in handles]
                cherrypy.session["Shrek"]["dd_client"].restart_handles(project.project_id, handles=handles_to_reset)
                new_project_entry = project.__dict__.copy()
                # strip the keys we don't want
                for key in ['data_dispatcher_project_idx', 'recovery_position', 'submission_id', 'campaign_stage_obj', 'campaign_stage_snapshot_obj', 'submission_obj', 'job_type_snapshot_obj', 'experimenter_creator_obj',  '_sa_instance_state']:
                    new_project_entry.pop(key, None)
                dd_project:DataDispatcherSubmission = DataDispatcherSubmission(**new_project_entry)
                dd_project.project_name = project_name
                dd_project.recovery_position = submission.recovery_position
                dd_project.recovery_tasks_parent_submission = recovery_parent
                dd_project.recovery_tasks_parent_project = int(project.data_dispatcher_project_idx)
                dd_project.stage_methodology = methodology
                dd_project.virtual=campaign_stage.data_dispatcher_project_virtual
                dd_project.recovery_mode=campaign_stage.data_dispatcher_recovery_mode
                dd_project.project_settings=campaign_stage.data_dispatcher_settings
                
                self.db.add(dd_project)
                self.db.commit()
            else:
                logit.log("DMR-Service  | create_recovery_dataset | Files Exist | Creating Project for exp=%s" % (campaign_stage.experiment))
                dd_project:DataDispatcherSubmission = self.create_project(username=submission.experimenter_creator_obj.username, 
                                                files=recovery_files,
                                                experiment=project.experiment,
                                                role=project.vo_role,
                                                project_name=project_name,
                                                campaign_id=project.campaign_id, 
                                                campaign_stage_id=project.campaign_stage_id,
                                                split_type=project.cs_split_type if hasattr(project, 'cs_split_type') else project.split_type,
                                                last_split=project.cs_last_split if hasattr(project, 'cs_last_split') else project.last_split,
                                                campaign_stage_snapshot_id=snapshot.campaign_stage_snapshot_id if snapshot else None,
                                                recovery_position=recovery_position,
                                                creator=submission.experimenter_creator_obj.experimenter_id,
                                                creator_name=submission.experimenter_creator_obj.username,
                                                status="created",
                                                virtual=campaign_stage.data_dispatcher_project_virtual,
                                                stage_methodology=campaign_stage.data_dispatcher_stage_methodology,
                                                recovery_mode=campaign_stage.data_dispatcher_recovery_mode,
                                                 
                                                project_settings=campaign_stage.data_dispatcher_settings
                                                )
                    
            if dd_project and dd_project.data_dispatcher_project_idx:
                if methodology == "standard":
                    logit.log("DMR-Service  | create_recovery_dataset | Created Recovery Project | project_id: %s" % (dd_project.project_id))
                else:
                    logit.log("DMR-Service  | create_recovery_dataset | Added 1P Recovery | project_id: %s" % (dd_project.project_id))
                recovery = {}
                recovery["name"] = project_name
                recovery["timestamp"] = datetime.datetime.now().isoformat()
                recovery["count"] = nfiles
                recovery["exp"] = snapshot.experiment if snapshot else cherrypy.session["Shrek"]["current_experiment"]
                recovery["position"] = recovery_position
                recovery["methodology"] = methodology
                recovery["type"] = rtype.name
                recovery["dd_project_idx"] = int(dd_project.data_dispatcher_project_idx)
                params = copy.deepcopy(submission.submission_params) if submission.submission_params else {"workflow": {"recoveries": []}}
                workflow = params.get("workflow", {})
                recoveries = workflow.get("recoveries", [])
                recoveries.append(recovery)
                submission.submission_params = params
                project.campaign_stage_obj.data_dispatcher_dataset_only=False
                flag_modified(project.campaign_stage_obj, 'data_dispatcher_dataset_only')
                flag_modified(submission, 'submission_params')
                self.db.add(submission)
                self.db.commit()
                return nfiles, project_name, recovery["dd_project_idx"]
            else:
                logit.log("DMR-Service  | create_recovery_dataset | Failed")
        except IntegrityError as e:
            logit.log("DMR-Service  | create_recovery_dataset | IntegrityError: %s" % e)
            self.db.rollback()
        except Exception as e:
            logit.log("DMR-Service  | create_recovery_dataset | Exception: %s" % e)
        return 0, None, None
    
    def dependency_definition(self, submission, jobtype, i, test=False):
        dd_project = submission.data_dispatcher_submission_obj
        campaign_stage = submission.campaign_stage_obj
        campaign_stage.data_dispatcher_dataset_only=False
        # definitions for analysis users have to have the username in them
        # so they can define them in the job, we have to follow the same
        # rule here...
        if campaign_stage.creator_role == "analysis":
            project_name = "%s | %s | Dependency %d for Submission: %d" % (campaign_stage.name, campaign_stage.experimenter_creator_obj.username, i,  submission.submission_id)
        else:
            project_name = "%s | Dependency %d for Submission: %d" % (campaign_stage.name, i,  submission.submission_id)
        
        filters = []
        project_files = self.get_project_handles(project_id=dd_project.project_id)
        cur_dname_files =  list(cherrypy.session["Shrek"]["mc_client"].get_files(project_files, with_metadata=True, with_provenance=True))
        cur_dname_nfiles = len(cur_dname_files)
        
        if not dd_project or not dd_project.project_id or campaign_stage.campaign_stage_type == "generator":
            # if we're a generator, the previous stage should have declared it
            # or eventually it doesn't have a Data Dispatcher project
            project_name = project_name.replace("| Dependency", "| Gen | Dependency" )
            stored_dd_project = self.store_project(project_id=None, 
                                        worker_timeout=None, 
                                        idle_timeout=None,
                                        username=campaign_stage.experimenter_creator_obj.username, 
                                        experiment=campaign_stage.experiment,
                                        role=campaign_stage.vo_role,
                                        project_name=project_name,
                                        campaign_id=campaign_stage.campaign_id, 
                                        campaign_stage_id=campaign_stage.campaign_stage_id,
                                        split_type=campaign_stage.cs_split_type,
                                        last_split=campaign_stage.cs_last_split,
                                        creator=campaign_stage.experimenter_creator_obj.experimenter_id,
                                        creator_name=campaign_stage.experimenter_creator_obj.username,
                                        named_dataset="fids %s" % ",".split([file.get("fid") if file["fid"] != '0' else 0 for file in cur_dname_files]))
            campaign_stage.data_dispatcher_dataset_only=True
            return project_name, stored_dd_project

        if campaign_stage.campaign_stage_type in ("approval", "datatransfer"):
            query = "fids %s" % ",".split([int(file.get("fid")) for file in cur_dname_files])
        else:
            ischildof = "parents:( " * campaign_stage.output_ancestor_depth
            isclose = ")" * campaign_stage.output_ancestor_depth
            query = "%s fids %s %s " % (ischildof, ",".split([file.get("fid") if file["fid"] != '0' else 0 for file in cur_dname_files]),isclose)
            cdate = dd_project.created.strftime("%Y-%m-%dT%H:%M:%S%z")
            filters.append("created_timestamp > '%s'" % cdate)
        
        if jobtype.file_patterns.find(" ") > 0:
            # it is a dimension fragment, not just a file pattern
            filters.append(jobtype.file_patterns)
        else:
            filters.append("name like '%s'" % jobtype.file_patterns)
            
        query = self.add_query_filters_if_necessary(query, filters)
            
            # if we have an updated time past our creation, use it for the
            # time window -- this makes our dependency definition basically
            # frozen after we run, so it doesn't collect later similar projects
            # output.
            # .. nevermind, that actually exclues recovery launch output..
            # ndate = submission.updated.strftime("%Y-%m-%dT%H:%M:%S%z")
            # if ndate != cdate:
            #    filters.append("created_timestamp <= '%s'" % ndate)
            
        
        query = self.try_format_with_keywords(query, campaign_stage.campaign_obj.campaign_keywords)
        
        new_dname_files = list(cherrypy.session["Shrek"]["mc_client"].query(query, with_metadata=True, with_provenance=True))
        new_dname_nfiles = len(new_dname_files)
        logit.log("count files: %s has %d files" % (project_name, cur_dname_nfiles))
        logit.log("count files: new dimensions has %d files" % len(new_dname_files))

        # if #files in the current Data Dispatcher definition are not less than #files in the updated Data Dispatcher definition
        # we do not need to update it, so keep the Data Dispatcher definition with current dimensions
        if cur_dname_nfiles >= new_dname_nfiles:
            logit.log("Do not need to update '%s'" % project_name)
            stored_dd_project = self.create_project(username=dd_project.experimenter_creator_obj.username, 
                                    files=cur_dname_files,
                                    experiment=campaign_stage.experiment,
                                    role=campaign_stage.vo_role,
                                    project_name=project_name,
                                    campaign_id=campaign_stage.campaign_id, 
                                    campaign_stage_id=campaign_stage.campaign_stage_id,
                                    split_type=campaign_stage.cs_split_type,
                                    last_split=campaign_stage.cs_last_split,
                                    campaign_stage_snapshot_id=dd_project.campaign_stage_snapshot_obj.campaign_stage_snapshot_id,
                                    recovery_position=dd_project.recovery_position,
                                    creator=dd_project.experimenter_creator_obj.experimenter_id,
                                    creator_name=dd_project.experimenter_creator_obj.username,
                                    virtual=campaign_stage.data_dispatcher_project_virtual,
                                    stage_methodology=campaign_stage.data_dispatcher_stage_methodology,
                                    recovery_mode=campaign_stage.data_dispatcher_recovery_mode,
                                    project_settings=campaign_stage.data_dispatcher_settings,
                                    named_dataset="fids %s" % ",".split([file.get("fid") if file["fid"] != '0' else 0 for file in cur_dname_files]),
                                    status="created")
            return project_name, stored_dd_project

        try:
            depends_dd_project = self.create_project(username=dd_project.experimenter_creator_obj.username, 
                                            files=new_dname_files,
                                            experiment=campaign_stage.experiment,
                                            role=campaign_stage.vo_role,
                                            project_name=project_name,
                                            campaign_id=campaign_stage.campaign_id, 
                                            campaign_stage_id=campaign_stage.campaign_stage_id,
                                            virtual=campaign_stage.data_dispatcher_project_virtual,
                                            stage_methodology=campaign_stage.data_dispatcher_stage_methodology,
                                            recovery_mode=campaign_stage.data_dispatcher_recovery_mode,
                                            project_settings=campaign_stage.data_dispatcher_settings,
                                            split_type=campaign_stage.cs_split_type,
                                            last_split=campaign_stage.cs_last_split,
                                            campaign_stage_snapshot_id=dd_project.campaign_stage_snapshot_obj.campaign_stage_snapshot_id,
                                            recovery_position=dd_project.recovery_position,
                                            creator=dd_project.experimenter_creator_obj.experimenter_id,
                                            creator_name=dd_project.experimenter_creator_obj.username,
                                            named_dataset=query,
                                            status="created")
            
        except:
            logit.log("ignoring definition error")
        
        dependency = {}
        dependency["name"] = project_name
        dependency["timestamp"] = datetime.now().isoformat()
        dependency["current_count"] = cur_dname_nfiles
        dependency["new_count"] = new_dname_nfiles
        dependency["exp"] = dd_project.campaign_stage_snapshot_obj.experiment
        dependency["mc_query"] = query
        workflow = dd_project.submission_obj.submission_params.get("workflow", {})
        deps = workflow.get("dependencies", [])
        deps.append(dependency)
        workflow["dependencies"] = deps
        dd_project.submission_obj.submission_params["workflow"] = workflow
        dd_project.submission_obj.submission_params = submission.submission_params
        flag_modified(dd_project.submission_obj, 'submission_params')
        self.ctx.db.add(dd_project.submission_obj)
        self.ctx.db.commit()

        return project_name, depends_dd_project
    
    def create_dataset_definition(self, namespace, dataset_definition, query):
        if not cherrypy.session["Shrek"]["mc_client"].get_namespace(namespace):
            cherrypy.session["Shrek"]["mc_client"].create_namespace(namespace, owner_role="poms_user")
            logit.log("DMRService | create_dataset_definition | Created Namespace: %s" % namespace)
        
        logit.log("DMRService | create_dataset_definition | Dataset: %s | Query: %s" % (dataset_definition, query))
        try:
            cherrypy.session["Shrek"]["mc_client"].query(query, save_as=dataset_definition)
        except Exception as e:
            if not cherrypy.session["Shrek"]["mc_client"].get_dataset(dataset_definition):
                raise e
        logit.log("DMRService | create_dataset_definition | Created Dataset Definition: %s" % (dataset_definition))

        
        # Adds campaign keywords into applicable strings.
    def try_format_with_keywords(self, query, campaign_keywords=None):
        try:
            if campaign_keywords and bool(re.search(r'%\(\w+\)s', query)):
                query = query % campaign_keywords
                return query % campaign_keywords
            else:
                return query
        except Exception as e:
            logit.log("DMRService | try_format_with_keywords | Error during formatting: %s" % e)
            return query
        

class DataDispatcherProjectChecker:

    def __init__(self, ctx):
        self.n_project = 0
        self.lookup_exp_list = []
        self.lookup_submission_list = []
        self.lookup_dims_list = []
        self.ctx = ctx





class DataDispatcherTest:
    def __init__(self, client, project_id):
        cherrypy.session["Shrek"]["dd_client"] = client
        self.project_id = project_id
    
        
    def process_files(self, files_to_pass, files_to_fail):
        x = 1
        passed = 0
        failed = 0
        total_runs = int(files_to_pass) + int(files_to_fail)
        while  passed + failed < total_runs and total_runs > 0:
            try:
                action = "pass" if passed < int(files_to_pass) else "fail"
                
                reserved_file = cherrypy.session["Shrek"]["dd_client"].next_file(self.project_id, timeout=20)
                if type(reserved_file) is dict: 
                    handle = "%s:%s" % (reserved_file.get("namespace"), reserved_file.get("name"))
                    logit.log("DMR-Service | Process Files | Project ID: %s | Handle: %s | Run: %s of %s | State Set To: %s " % (self.project_id, handle, x, total_runs , "reserved"))
                    if action == "pass":
                        cherrypy.session["Shrek"]["dd_client"].file_done(self.project_id, handle) 
                    else:
                        cherrypy.session["Shrek"]["dd_client"].file_failed(self.project_id, handle, False)
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
        
                

        
        
        
        
        
    
            
