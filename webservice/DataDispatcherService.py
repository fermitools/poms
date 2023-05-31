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
            
