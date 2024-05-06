import base64
import copy
import json
import os

import toml

from cryptography.fernet import Fernet
from datetime import datetime
from helper_functions import record_queue_log, utc, set_run_number, set_run_start
from filelock import FileLock


class SubmissionQueue:
    def __init__(self, cfg, agent_id):
        set_run_start(datetime.now(utc))
        queue_config = toml.load(cfg.get("submission_agent", "queue_file_path"))
        self.poms_running_query = cfg.get("submission_agent", "poms_running_query")
        self.queue_path = queue_config["queue"]["queue_file"]
        self.queue_lock =  FileLock(queue_config["queue"]["queue_lock_file"])
        self.completed_submissions_path =  queue_config["queue"]["results_file"] 
        self.completed_submissions_lock =  FileLock(queue_config["queue"]["results_lock_file"])
        self.agent_id = agent_id
        self.session = None
        self.run_number = None
        self._set_instance(
            keypath = queue_config["queue"]["agent_key"], 
            secret = queue_config["queue"]["agent_secret"],
            server = queue_config["queue"]["server"] 
        )
        self.update_results = False
        
        
    def _set_instance(self, keypath, secret, server):
        
        if not self._encrypt_session(keypath, secret, server):
            record_queue_log("Failed to set session info", level="error", function="Initialize Queue")
            exit(1)
        
        for path, lock in { self.queue_path: self.queue_lock, self.completed_submissions_path: self.completed_submissions_lock}.items():
            with lock:
                if not os.path.exists(path):
                    raise AssertionError("Failed to set session info")
                try:
                    with open(path, "r+", encoding="utf-8") as file:
                        file_data = json.load(file)
                        if "run_number" in file_data and not  self.run_number:
                             self.run_number = int(file_data["run_number"])
                        file_data["_session"] = self.session
                        file.seek(0)
                        json.dump(file_data, file, indent=4)
                        file.truncate()
                except Exception as e:
                    record_queue_log("%s" % e, level="exception", function="Initialize Queue")
                    exit(1)
                
    def _encrypt_session(self, keypath, secret, server):
        with open(keypath, 'rb') as keyfile:
            key = keyfile.read()
            cipher_suite = Fernet(key)
            if not (key and cipher_suite):
                return False
        
        agent_header = cipher_suite.encrypt(self.agent_id.encode())
        agent_header = base64.b64encode(agent_header)
        agent_header = agent_header.decode('utf-8')
        
        encrypted_secret = cipher_suite.encrypt(secret.encode())
        encrypted_secret = base64.b64encode(encrypted_secret)
        encrypted_secret = encrypted_secret.decode('utf-8')
        if not encrypted_secret:
            return False
        
        encrypted_session_info = cipher_suite.encrypt(json.dumps({
            "username": "submission_agent",
            "agent_id": self.agent_id,
            "host": server,
            "secret": encrypted_secret
        }).encode())
        encrypted_session_info = base64.b64encode(encrypted_session_info)
        encrypted_session_info = encrypted_session_info.decode('utf-8')
        
        if not encrypted_session_info:
            return False
        
        self.session = encrypted_session_info
        self.agent_header = agent_header
        return True
    
    def set_session(self, session, exp_list):
        self.psess = session
        self.exp_list = exp_list
    
    def store_results(self, results):
        with self.completed_submissions_lock:
            with open(self.completed_submissions_path, "r+", encoding="utf-8") as file:
                rdata = json.load(file)
                if "completed_submissions" not in rdata:
                    rdata["completed_submissions"] = {}
                if rdata:
                    rdata["completed_submissions"].update(results)
                self.save_file(file, rdata)
            record_queue_log("Results Updated")
            self.clean_up_queue(results)
        self.update_results = False
    
    def store_ignored(self, ignored):
        with self.completed_submissions_lock:
            extra = {}
            with open(self.completed_submissions_path, "r+", encoding="utf-8") as file:
                rdata = json.load(file)
                if "ignored" not in rdata:
                    rdata["ignored"] = {
                        "submissions": [],
                        "jobs": []
                    }
                if rdata:
                    extra["new_submissions"] = [x for x in ignored["submissions"] if x not in rdata["ignored"]["submissions"]]
                    extra["new_jobs"] = [x for x in ignored["jobs"] if x not in rdata["ignored"]["jobs"]]
                    ignored["submissions"] = list(ignored["submissions"])
                    ignored["jobs"] = list(ignored["jobs"])
                    rdata["ignored"] = ignored
                self.save_file(file, rdata)
            
            if extra:
                record_queue_log("Updated ignored list", **{k: v for k, v in extra.items() if v})
            else:
                record_queue_log("Nothing new to store")
        
    def clean_up_queue(self, results):
        keys_removed = set()
        file, qdata = self.open_queue()
        for key in results.keys():
            removed = False
            if key in qdata.get("query_results", {}):
                del qdata["query_results"][key]
                removed = True
            if key in qdata.get("processing", {}):
                del qdata["processing"][key]
                removed = True
            removed_idx = False
            indexes = copy.deepcopy(qdata["indexes"])
            if key in indexes["s_to_j"]:
                del indexes["s_to_j"][key]
                removed_idx = True
            if key in indexes["j_to_s"].values():
                indexes["j_to_s"] = {k: v for k, v in qdata["indexes"]["j_to_s"].items() if v != key}
                removed_idx = True
            if removed_idx:
                qdata["indexes"] = indexes
                keys_removed.add(key)
                record_queue_log("Removed keys from index", keys=keys_removed)
            if removed:
                keys_removed.add(key)
                
        self.save_file(file, qdata)
        if len(keys_removed) > 0:
            record_queue_log("Removed keys from queue", keys=keys_removed)
    
    def post_run_cleanup(self):
        results_file, results_data = self.open_results()
        self.clean_up_queue(results_data.get("completed_submissions", {})) 
        self.save_file(results_file, results_data)
                
    def save_queue_data(self, item):
        file, data = self.open_queue()
        if "queued" in item:
            current_queue = data.get("queued", [])
            queued_tasks = [str(task["pomsTaskID"]) for task in current_queue]
            for task in item["queued"]:
                if str(task["pomsTaskID"]) not in queued_tasks:
                    current_queue.append(task)
                    print("Added task to queue: %s" % task["pomsTaskID"])
            data["queued"] = current_queue
        # Items are stored as dicts in the json, but tend to form duplicates
        # So we update the dict with the new values,
        # Then convert it to a set, then back to a dict
        if "indexes" in item:
            for index in data["indexes"].keys():
                if index in item["indexes"]:
                    data["indexes"][index] = dict(set(item["indexes"][index].items()))
        if "processing" in item:
            data["processing"] = dict(set(item["processing"].items()))
        if "query_results" in item:
            data["query_results"] = item["query_results"]
            
        self.save_file(file, data)
    
    def dequeue(self):
        file, data = self.open_queue()
        queue = data.get("queued", [])
        items = copy.deepcopy(queue) if queue and len(queue) > 0 else []
        if items:
            data["queued"] = []
        self.save_file(file, data)
        return items
            
    
    def open_queue(self):
        with self.queue_lock:
            file = open(self.queue_path, "r+", encoding="utf-8")
            data = json.load(file)
            return file, data
            
    def open_results(self):
        with self.completed_submissions_lock:
            if os.path.exists(self.completed_submissions_path):
                file = open(self.completed_submissions_path, "r+", encoding="utf-8")
                data = json.load(file)
                return file, data
            
    def save_file(self, file, data):
        data["last_update"] = datetime.now(utc).isoformat()
        file.seek(0)
        json.dump(data, file, indent=4)
        file.truncate()
        file.close()
    
    def init_files_if_not_exist(self):
        if not os.path.exists(self.queue_path):
            with open(self.queue_path, "w", encoding="utf-8") as file:
                json.dump({
                    "queued": [],
                    "indexes": {
                        "j_to_s": {},
                        "s_to_j": {}
                    },
                    "processing": {},
                    "query_results": {},
                    "current_run": 0,
                    "last_update": datetime.now(utc).isoformat()
                    }, file)
        if not os.path.exists(self.completed_submissions_path):
            with open(self.completed_submissions_path, "w", encoding="utf-8") as file:
                json.dump({
                    "completed_submissions": {},
                    "ignored": {
                        "submissions": [],
                        "jobs": []
                    },
                    "current_run": 0,
                    "last_update": datetime.now(utc).isoformat()
                    }, file)
    
    def fetch_poms_submissions(self, completed_submissions, ignored):
        try:
            assert self.psess and self.exp_list, "Session and experiment list not set"
            htr = self.psess.get(self.poms_running_query)
            flist = htr.json()
            record_queue_log("Checking for new POMS submissions")
            queue_items = []
            ddict = [ 
                        {
                            'pomsTaskID': x[0],  
                            "group": x[2], 
                            'id': x[1], 
                            "POMS_DATA_DISPATCHER_TASK_ID": x[3],  
                            "queued_at": datetime.now(utc).isoformat()
                        } 
                        for x in flist 
                        if x[2] in self.exp_list
                        and str(x[0]) not in ignored["submissions"]
                        and x[1] not in ignored["jobs"]
                    ]
            for item in ddict:
                if item["pomsTaskID"] not in completed_submissions:
                    queue_items.append(item)
            if queue_items:
                self.save_queue_data({"queued": queue_items})
                for item in queue_items:
                    record_queue_log("Added POMS submission to queue", **item)
            else:
                record_queue_log("No new POMS submissions found")
            htr.close()
        except Exception as e:
            record_queue_log("Failed: %s" % e, level="exception")
            return {}
    
    def begin_run(self):
        # Initialize files if they don't exist
        self.init_files_if_not_exist()
        
        # Open results file
        results_file, results_data = self.open_results()
        # Get the results data
        completed_submissions = results_data.get("completed_submissions")
        ignored = {
            "submissions": set(results_data.get("ignored", {}).get("submissions", [])),
            "jobs": set(results_data.get("ignored", {}).get("jobs", []))
        }
        
        # Update run number
        if "current_run" not in results_data:
            results_data["current_run"] = 0
        results_data["current_run"] += 1

        # Save and close the file
        self.save_file(results_file, results_data)
        
        # Add new items to the queue if they exist
        
        self.fetch_poms_submissions(completed_submissions, ignored)
        
        # Open queue file
        queue_file, queue_data = self.open_queue()
        
        # Get the queue data for the run
        queued_submissions = copy.deepcopy(queue_data.get("queued", []))
        running_in_queue = copy.deepcopy(queue_data.get("processing", {}))
        job_to_sub_index = copy.deepcopy(queue_data["indexes"]["j_to_s"])
        sub_to_job_index = copy.deepcopy(queue_data["indexes"]["s_to_j"])
        query_results = copy.deepcopy(queue_data.get("query_results", {}))
        
        # Update run number
        if "current_run" not in queue_data:
            queue_data["current_run"] = 0
        queue_data["current_run"] += 1
        if len(queued_submissions) > 0:
            queue_data["queued"] = []
            
        # Save and close the file
        self.save_file(queue_file, queue_data)
        
        run_rumber = results_data["current_run"]
        run_start = datetime.now(utc)
        set_run_number(run_rumber)
        set_run_start(run_start)
        # Return the data for the run
        return run_rumber, run_start, queued_submissions, running_in_queue, job_to_sub_index, sub_to_job_index, query_results, completed_submissions, ignored
        