
from rucio.client import Client as RucioClient
from rucio.client.uploadclient import UploadClient
from metacat.webapi import MetaCatClient
import json
import configparser
from datetime import datetime
import math
import os
import uuid
import shutil

class ConfigParser():
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(os.environ.get("WEB_CONFIG","/home/poms/poms/webservice/poms.ini"))
        
    
    def get(self, section, option, *, raw=False, vars=None, fallback=None):
        value = self.config.get(section, option, raw=raw, vars=vars, fallback=fallback)
        return value.strip('"') if value else value
    
class POMSRucioClient:
    def __init__(self):
        self.config = ConfigParser()
        self.metacat_server_url = self.config.get("Metacat","HYPOT_METACAT_SERVER_URL")
        self.metacat_auth_server_url = self.config.get("Metacat","HYPOT_METACAT_AUTH_SERVER_URL")
        self.metacat_token_file = self.config.get("Metacat","HYPOT_METACAT_TOKEN_FILE")
        self.metacat_client = MetaCatClient(server_url=self.metacat_server_url, auth_server_url=self.metacat_auth_server_url, token_library=self.metacat_token_file)
        self.login_metacat()
        self.set_rucio_client()
        
    def login_metacat(self):
        try:
            auth_info = self.metacat_client.login_x509('poms', self.config.get("POMS", "POMS_CERT"), self.config.get("POMS", "POMS_KEY"))
            if auth_info:
                return True
            return False
        except Exception as e:
            False
        
    def set_rucio_client(self):
        rucio_host = self.config.get("Rucio",'rucio_host')
        auth_host = self.config.get("Rucio",'auth_host')
        account = self.config.get("Rucio",'account')
        ca_cert = self.config.get("Rucio",'ca_cert')
        auth_type = self.config.get("Rucio",'auth_type')
        client_cert = self.config.get("Rucio",'client_cert')
        self.rucio_client = RucioClient(rucio_host=rucio_host, auth_host=auth_host,account=account,ca_cert=ca_cert,auth_type=auth_type)
        self.upload_client = UploadClient(self.rucio_client)
            
    def check_rucio_for_replicas(self, dids):
        if not self.rucio_client:
            self.set_rucio_client()
        return self.rucio_client.list_replicas(dids)
    
    def add_scope(self, account, scope_name):
        try:
            success = self.rucio_client.add_scope(account, scope_name)
            print(f"Add Scope | Account: {account} | Scope: {scope_name} | Status: {success}")
            return success
        except Exception as e:
            print(f"Exception: {e}")
            return False
            
    def add_container(self, scope, name):
        try:
            success = self.rucio_client.add_container(scope, name)
            print(f"Add Container | DID: {scope}:{name} | status: {success}")
        except Exception as e:
            print(f"Exception: {e}")
            return False
            
    def add_dataset(self, scope, name, rse):
        print(f"Add Dataset | DID: {scope}:{name}\n")
        try:
            success = self.rucio_client.add_dataset(scope, name, rse)
            print(f"Status: {success}")
        except Exception as e:
            print(f"Status: Fail - {e}")
    
    def get_all_files(self, directory):
        """Return a list of file paths in the given directory."""
        return [os.path.join(directory, filename) for filename in os.listdir(directory) if os.path.isfile(os.path.join(directory, filename))]

    
    def upload_file(self, path, rse, scope, dataset_name, is_folder=False):
        items = []
        if is_folder:
            for file_path in self.get_all_files(path):
                file = {"path": file_path, "rse": rse, "did_scope": scope, "dataset_scope": scope, "dataset_name": dataset_name, "register_after_upload":True}
                items.append(file)
        else:
            file = {"path": path, "rse": rse, "dataset_scope": scope, "dataset_name": dataset_name, "register_after_upload":True}
            items.append(file)
        summary_log = f"/home/poms/poms/webservice/static/samples/summaries/upload_{math.floor(datetime.now().timestamp())}.json"
        try:
            success = self.rucio_client.add_dataset(scope, dataset_name, items)
            print(f"Status: {success}")
        except Exception as e:
            print(f"Status: Fail - {e}")
        success = (self.upload_client.upload(items, summary_file_path=summary_log) == 0)
        print(f"Status: {success} | Summary: {summary_log}")
        return items, summary_log
    
    
    def generate_files(self, count):
        files_generated =[]
        folder_path = f"/home/poms/poms/webservice/static/samples/sample_generated_files/gen_{math.floor(datetime.now().timestamp())}"
        os.makedirs(folder_path, exist_ok=True)
        for i in range(0, count):
            file_name = f"{uuid.uuid4()}.root"
            with open(f"{folder_path}/{file_name}", '+wt') as file:
                file.writelines(["#!/bin/bash \n", f"echo 'I am file {i} \n", "exit 0 \n"])
            files_generated.append(file_name)
        print("Generated files: \n%s " % '\n'.join(files_generated))
        return folder_path, files_generated
    
    def cleanup_generated(self, directory_path):
        try:
            shutil.rmtree(directory_path)
            print(f"Directory '{directory_path}' and its contents removed successfully!")
        except OSError as e:
            print(f"Error removing directory: {e}")
    

client = POMSRucioClient()

def gen(user, scope, dataset_name, rse, count, add_parents=False, metadata={}):
    generated_folder, file_names = client.generate_files(count)
    try:
        client.add_scope(user,scope)
        print(f"Created scope in Rucio: {user}:{scope}")
        client.metacat_client.create_namespace(scope)
        print(f"Created Namespace in Metacat: {user}:{scope}")
    except:
        pass
    try:
        client.add_dataset(scope,dataset_name, rse)
        print(f"Created dataset in Rucio: {scope}:{dataset_name}")
        client.metacat_client.create_dataset(f"{scope}:{dataset_name}")
        print(f"Created dataset in metacat: {scope}:{dataset_name}")
    except:
        pass
    try:
        mfiles = []
        items, summary_log = client.upload_file(generated_folder, rse,scope, dataset_name, is_folder=True)
        print("Added Files to Rucio: %s" % items)
        with open(summary_log, 'r') as json_file:
            data = json.load(json_file)
            import random
            parents = list(client.metacat_client.query("files from poms_samples:gen15", with_metadata=True, with_provenance=True)) if add_parents else []
            files_to_add = []
            run_count = 0
            
            for file_did, details in data.items():
                parents_to_add = random.sample(parents, 2) if add_parents and len(parents) >= 2 else []
                if parents_to_add:
                    parents = [n for n in parents if n in parents_to_add]
                if metadata and run_count % 2 == 0:
                    metadata["core.run_number"] = 1200
                else:
                    metadata["core.run_number"] = run_count
                file = {
                    "namespace": details.get("scope", None),
                    "name":details.get("name", None),
                    "dataset_did": f"{scope}:{dataset_name}",
                    "size": details.get("bytes", None),
                    "guid": details.get("guid", None),
                    "md5":  details.get("md5", None),
                    "pfn":  details.get("pfn", None),
                    "rse":  details.get("rse", None),
                    "scope": details.get("scope", None),
                    "metadata": dict(metadata),
                    "checksums": {
                        "ad":details.get("adler32", None)
                    },
                    "parents": parents_to_add,
                }
                files_to_add.append(file)
                run_count += 1

            mfiles = client.metacat_client.declare_files(dataset=f"{scope}:{dataset_name}", files=files_to_add, namespace=scope)
    except Exception as e:
        print("Error adding Files to Metacat: %s" % e)
    print("Added Files to Metacat: %s" % mfiles)
    
    client.cleanup_generated(generated_folder)
    print("Cleaned Up")



#user="ltrestka"
#scope="poms_samples"
#dataset_name="gen8"
#rse="FNAL_DCACHE"
#count=15
#add_parents=True
#metadata=metadata
#add_parents = True
#mfiles = []
metadata={
    "core.runs" :[[1200,1,"mc"]],
    "core.run_number" :1197
}
#gen(user="ltrestka", scope="poms_samples", dataset_name="gen16", rse="FNAL_DCACHE", count=16, add_parents=True, metadata=metadata)
print(list(client.metacat_client.query("files from poms_samples:gen15", with_provenance=True)))