import configparser
from collections import deque
import urllib.request
import urllib.parse
import urllib.error
import time
import datetime
import concurrent.futures
import os
import sys

import traceback

# TODO add to spack later 
#sys.path.append("/home/poms/packages/spack/rollout2/NULL/var/spack/environments/poms_production2/.spack-env/view/lib/python3.8/site-package")
#sys.path.append("/home/poms/.local/lib/python3.8/site-packages")

import poms.webservice.logit as logit
from data_dispatcher.api import DataDispatcherClient
config = configparser.ConfigParser()
config.read(os.environ.get("WEB_CONFIG","/home/poms/poms/webservice/poms.ini"))

#config = configparser.ConfigParser()
#config.read(os.environ["WEB_CONFIG"])

class data_dispatcher:
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
            
